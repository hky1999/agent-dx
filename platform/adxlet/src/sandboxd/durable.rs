//! Durable execution identity and retirement. No legacy Start/Delete fallback.
use super::*;
use sha2::{Digest, Sha256};

pub(super) struct ExecutionBinding {
    pub(super) operation_id: String,
    pub(super) sandbox_id: String,
}

fn binding(node: &str, runtime: &str) -> Result<ExecutionBinding> {
    if node.trim().is_empty() || runtime.trim().is_empty() {
        return Err(Error::Invalid(
            "deployment node identity and execution ID are required".into(),
        ));
    }
    // Length-prefix components, including a versioned domain, so concatenation
    // cannot alias identities. IDs are never repurposed for another execution.
    let mut digest = Sha256::new();
    digest.update(b"adx/sandboxd/start/v1\0");
    for component in [node, runtime] {
        digest.update((component.len() as u64).to_be_bytes());
        digest.update(component.as_bytes());
    }
    let key = format!("{:x}", digest.finalize());
    Ok(ExecutionBinding {
        operation_id: format!("adx-start-{key}"),
        // 60 path-free characters fit the backend's bounded sandbox ID.
        sandbox_id: format!("sbox-{}", &key[..55]),
    })
}

fn normalize_checkpoint(path: &str) -> Result<String> {
    use std::path::Component;
    if !Path::new(path).is_absolute() {
        return Err(Error::Invalid(
            "absolute checkpoint directory required".into(),
        ));
    }
    let mut normalized = PathBuf::from("/");
    for component in Path::new(path).components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
            Component::Prefix(_) => {
                return Err(Error::Invalid("unsupported checkpoint path".into()))
            }
        }
    }
    if normalized == Path::new("/") {
        return Err(Error::Invalid("checkpoint directory cannot be root".into()));
    }
    normalized
        .into_os_string()
        .into_string()
        .map_err(|_| Error::Invalid("checkpoint directory is not UTF-8".into()))
}

fn request_before<T>(message: T, deadline: tokio::time::Instant) -> Result<Request<T>> {
    let remaining = adx_transport::deadline::remaining(deadline, Duration::MAX)
        .ok_or_else(|| unavailable("runtime operation deadline exceeded"))?;
    let mut request = Request::new(message);
    request.set_timeout(remaining);
    Ok(request)
}

// Only this pre-admission query can classify a validation refusal without
// ambiguity about a live executor. Admission/retirement failures stay fail-closed.
fn checkpoint_identity_error(error: tonic::Status) -> Error {
    match error.code() {
        tonic::Code::InvalidArgument | tonic::Code::FailedPrecondition => {
            Error::Invalid(format!("checkpoint content identity rejected: {error}"))
        }
        _ => unavailable(error),
    }
}

fn check_status(value: &proto::StartOperationStatus, expected: &ExecutionBinding) -> Result<()> {
    if value.operation_id != expected.operation_id
        || value.sandbox_id != expected.sandbox_id
        || value.resource_generation.is_empty()
    {
        return Err(unavailable(
            "start status has a missing or mismatched durable identity",
        ));
    }
    if value.retirement_requested || value.retirement_complete {
        return Err(unavailable("execution is fenced for retirement"));
    }
    Ok(())
}

impl Sandboxd {
    pub(super) fn execution_binding(&self, runtime_id: &str) -> Result<ExecutionBinding> {
        binding(&self.config.node_id, runtime_id)
    }

    pub(super) async fn execute_identified_start(
        &self,
        runtime_id: &str,
        request: Request<proto::StartRequest>,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<IpAddr> {
        let expected = self.execution_binding(runtime_id)?;
        let mut state = self.cell(runtime_id).lock_owned().await;
        if matches!(*state, StartState::Failed) {
            return Err(Error::Conflict);
        }
        if matches!(*state, StartState::Uncertain) {
            return Err(unavailable("execution outcome requires reconciliation"));
        }
        let deadline =
            deadline.unwrap_or_else(|| tokio::time::Instant::now() + self.config.rpc_timeout);
        // Refuse an expired call before any query or admission.
        let _ = request_before((), deadline)?;
        let mut start = request.into_inner();
        start.sandbox_id.clone_from(&expected.sandbox_id);
        start
            .labels
            .insert("adx.node_id".into(), self.config.node_id.clone());
        start
            .labels
            .insert("adx.start_operation".into(), expected.operation_id.clone());
        {
            let mut ids = self.backend_ids.lock().expect("shared state lock poisoned");
            if ids
                .get(runtime_id)
                .is_some_and(|id| id != &expected.sandbox_id)
            {
                return Err(unavailable(
                    "execution already maps to a different backend incarnation",
                ));
            }
            ids.insert(runtime_id.into(), expected.sandbox_id.clone());
        }
        *state = StartState::Failed;
        let adapter = self.clone();
        // Caller cancellation drops only the join handle. Retirement in another
        // adapter/process uses the daemon journal, not this local guard.
        tokio::spawn(async move {
            let mut client = adapter.client.clone();
            let restore_artifacts = if let Some(checkpoint) = &mut start.checkpoint_info {
                let directory = normalize_checkpoint(&checkpoint.checkpoint_dir)?;
                let identity = client.describe_checkpoint_artifacts(request_before(
                    proto::DescribeCheckpointArtifactsRequest { checkpoint_dir: directory.clone() },
                    deadline,
                )?).await.map_err(checkpoint_identity_error)?.into_inner();
                if identity.checkpoint_dir != directory
                    || identity.expected_root_digest.len() != 64
                    || !identity.expected_root_digest.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                {
                    return Err(unavailable("checkpoint identity query returned a mismatched directory or invalid content pin"));
                }
                checkpoint.checkpoint_dir = directory;
                Some(identity)
            } else { None };
            // The daemon compares request digest and root on every replay.
            // Never adopt a historical Get result without this comparison.
            let mut status = client.start_with_operation(request_before(
                proto::StartWithOperationRequest {
                    operation_id: expected.operation_id.clone(),
                    sandbox_id: expected.sandbox_id.clone(),
                    start: Some(start.clone()),
                    restore_artifacts,
                }, deadline,
            )?).await.map_err(unavailable)?.into_inner();
            loop {
                check_status(&status, &expected)?;
                match proto::StartOperationState::try_from(status.state) {
                    Ok(proto::StartOperationState::Succeeded) => break,
                    Ok(proto::StartOperationState::Running) => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        status = client.get_start_operation(request_before(
                            proto::GetStartOperationRequest { operation_id: expected.operation_id.clone() }, deadline,
                        )?).await.map_err(unavailable)?.into_inner();
                    }
                    Ok(proto::StartOperationState::Unknown) => {
                        *state = StartState::Uncertain;
                        return Err(unavailable("daemon cannot prove the start outcome; readers remain protected"));
                    }
                    _ => return Err(unavailable(format!("start failed or returned invalid state: {}", status.message))),
                }
            }
            // SUCCEEDED is historical. Confirm the exact generation is still
            // running rather than treating an old IP as present liveness.
            let inventory = client.list(request_before(proto::ListSandboxesRequest {
                id: expected.sandbox_id.clone(), selector: HashMap::new(),
            }, deadline)?).await.map_err(unavailable)?.into_inner().sandboxes;
            let live = inventory.first().filter(|_| inventory.len() == 1)
                .ok_or_else(|| unavailable("historical start has no unique live runtime"))?;
            if live.id != expected.sandbox_id || live.state != proto::SandboxState::Running as i32
                || live.labels.get("adx.node_id") != Some(&adapter.config.node_id)
                || live.labels.get("adx.runtime_id") != start.labels.get("adx.runtime_id")
                || live.labels.get("adx.environment_id") != start.labels.get("adx.environment_id")
                || live.labels.get("adx.generation") != start.labels.get("adx.generation")
                || live.labels.get("adx.start_operation") != Some(&expected.operation_id)
                || live.labels.get("akernel.scheduler/resource-generation") != Some(&status.resource_generation)
            {
                return Err(unavailable("live runtime does not match the durable execution generation"));
            }
            let ip: IpAddr = status.sandbox_ip.parse()
                .map_err(|_| unavailable("start status has no valid environment IP"))?;
            if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
                return Err(unavailable("start status returned a non-routable environment IP"));
            }
            *state = StartState::Started;
            Ok(ip)
        }).await.map_err(unavailable)?
    }

    pub(super) async fn retire_identified_start(
        &self,
        runtime_id: &str,
    ) -> Result<proto::RetireStartOperationResponse> {
        let expected = self.execution_binding(runtime_id)?;
        let mut state = self.cell(runtime_id).lock_owned().await;
        let result = self
            .client
            .clone()
            .retire_start_operation(self.request(proto::RetireStartOperationRequest {
                operation_id: expected.operation_id.clone(),
                sandbox_id: expected.sandbox_id.clone(),
            }))
            .await
            .map_err(unavailable)?
            .into_inner();
        if result.operation_id != expected.operation_id
            || result.sandbox_id != expected.sandbox_id
            || result.resource_generation.is_empty()
            || result.retirement_receipt.is_empty()
        {
            return Err(unavailable(
                "daemon did not return a bound positive retirement receipt",
            ));
        }
        // Extra defence against legacy/mismapped resources. Absence is not the
        // proof: the daemon receipt above fences future identified admission
        // and confirms all admitted execution/reader processes have exited.
        let remaining = self
            .client
            .clone()
            .list(self.request(proto::ListSandboxesRequest {
                id: String::new(),
                selector: HashMap::from([("adx.runtime_id".into(), runtime_id.into())]),
            }))
            .await
            .map_err(unavailable)?
            .into_inner()
            .sandboxes;
        if !remaining.is_empty() {
            return Err(unavailable(
                "runtime remains after positive retirement; retain reader references",
            ));
        }
        self.backend_ids
            .lock()
            .expect("shared state lock poisoned")
            .remove(runtime_id);
        *state = StartState::Failed;
        Ok(result)
    }

    pub(super) async fn retire_bound_reader(
        &self,
        reader: &adx_core::CheckpointReader,
    ) -> Result<adx_core::CheckpointReaderRetirement> {
        reader.identity.validate()?;
        if reader.node_id != self.config.node_id
            || self.config.node_id.trim().is_empty()
            || !adx_core::valid_runtime_id(
                &reader.identity.environment_id,
                reader.identity.ownership_generation,
                &reader.identity.runtime_id,
            )
        {
            return Err(unavailable(
                "checkpoint reader does not belong to this node and execution",
            ));
        }
        let receipt = self
            .retire_identified_start(&reader.identity.runtime_id)
            .await?;
        let backend_receipt = serde_json::to_string(&serde_json::json!({
            "protocol": "sandboxd-start-retirement/v1",
            "node_id": self.config.node_id,
            "reader": reader,
            "operation_id": receipt.operation_id,
            "sandbox_id": receipt.sandbox_id,
            "resource_generation": receipt.resource_generation,
            "retirement_receipt": receipt.retirement_receipt,
        }))
        .map_err(unavailable)?;
        Ok(adx_core::CheckpointReaderRetirement {
            reader: reader.clone(),
            backend_receipt,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stable_binding_separates_nodes_executions_and_components() {
        let a = binding("node", "i-1").unwrap();
        assert_eq!(a.operation_id, binding("node", "i-1").unwrap().operation_id);
        assert_ne!(
            a.operation_id,
            binding("other", "i-1").unwrap().operation_id
        );
        assert_ne!(
            a.operation_id,
            binding("node", "i-1-r1").unwrap().operation_id
        );
        assert_ne!(
            binding("ab", "c").unwrap().operation_id,
            binding("a", "bc").unwrap().operation_id
        );
        assert!(a.operation_id.len() <= 128 && a.sandbox_id.len() <= 63);
        assert!(binding("", "i-1").is_err());
    }
    #[test]
    fn generated_sandbox_id_matches_daemon_namespace_and_socket_budget() {
        let execution = binding("a1-source", "environment-1-r1").unwrap();
        // sandboxd/config.IsValidSandboxID requires the sbox- namespace.
        // Keep the full ID at 60 bytes for the existing Unix socket budget.
        assert!(execution.sandbox_id.starts_with("sbox-"));
        assert_eq!(execution.sandbox_id.len(), 60);
        assert!(execution.sandbox_id[5..]
            .bytes()
            .all(|b| b.is_ascii_hexdigit()));
        assert_eq!(execution.operation_id.len(), "adx-start-".len() + 64);
    }

    #[test]
    fn checkpoint_path_is_normalized_without_reading_data() {
        assert_eq!(
            normalize_checkpoint("/snapshots/a/../b/.").unwrap(),
            "/snapshots/b"
        );
        assert!(normalize_checkpoint("relative").is_err());
        assert!(normalize_checkpoint("/a/..").is_err());
    }
}
