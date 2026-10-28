//! Persist execution permission before dispatch; retain unknown attempts for cleanup.
use super::*;
use adx_core::execution::{ExecutionAuthorization, ExecutionKind, ExecutionRequest};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredExecution {
    pub authorization: ExecutionAuthorization,
    pub request_sha256: String,
    pub pending: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peak: Option<ExecutionPeak>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPeak {
    pub reservation_id: String,
    pub owner_sha256: String,
    /// Central reservation is not permission for the node to execute.
    #[serde(default)]
    pub node_accepted: bool,
}

fn target(request: &ExecutionRequest) -> Result<EnvironmentRecord> {
    validate_result(&request.source)?;
    request.target()
}

fn fingerprint(request: &ExecutionRequest) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(encode(request)?.as_bytes())))
}

impl StoredExecution {
    pub(super) fn validate(&self, environment: &StoredEnvironment) -> Result<()> {
        if let Some(peak) = &self.peak {
            super::overhead::field(&peak.reservation_id)?;
            super::overhead::digest(&peak.owner_sha256)?;
        }
        let grant = &self.authorization;
        let mut source_spec = grant.request.source.spec.clone();
        // A completed attempt does not prevent later authorized network changes.
        if !self.pending {
            source_spec.sandbox.network = environment.spec.sandbox.network.clone();
        }
        if source_spec != environment.spec
            || grant.request.source.assignment != environment.assignment
            || grant.target != target(&grant.request)?
            || self.request_sha256 != fingerprint(&grant.request)?
            || grant.node_session_id.trim().is_empty()
            || grant.coordinator_epoch == 0
            || grant
                .issued_unix_millis
                .checked_add(grant.request.timeout_millis)
                != Some(grant.deadline_unix_millis)
        {
            return Err(Error::Conflict);
        }
        let result = environment.result.as_ref().ok_or(Error::Conflict)?;
        if self.pending && !environment.invalidated {
            if result != &grant.request.source {
                return Err(Error::Conflict);
            }
        } else if result.runtime.id != grant.target.runtime.id
            || result.revision <= grant.target.revision
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}

impl Session {
    /// Atomically reserve the target execution, fenced by the full source record,
    /// current node session and Coordinator epoch. Identical pending replay returns
    /// the original deadline. Expiry or a transport error never releases its hold.
    /// Conflicting identities fail; exhausted configured capacity returns NoCapacity.
    pub async fn authorize_execution(
        &self,
        request: ExecutionRequest,
        node_session_id: &str,
        now_unix_millis: u64,
    ) -> Result<ExecutionAuthorization> {
        self.authorize_execution_inner(request, node_session_id, now_unix_millis, None)
            .await
    }

    /// Atomically reserve the replacement VM and additional restore peak.
    /// NoCapacity writes neither field. Exact lost acknowledgements retain the
    /// original target and deadline; a pending peak cannot be independently retired.
    pub async fn authorize_execution_with_peak(
        &self,
        request: ExecutionRequest,
        node_session_id: &str,
        now_unix_millis: u64,
        peak_id: &str,
        peak: OverheadReservation,
    ) -> Result<ExecutionAuthorization> {
        super::overhead::field(peak_id)?;
        peak.validate()?;
        if peak.retirement_proof_sha256.is_some() {
            return Err(Error::Conflict);
        }
        self.authorize_execution_inner(
            request,
            node_session_id,
            now_unix_millis,
            Some((peak_id.to_owned(), peak)),
        )
        .await
    }

    async fn authorize_execution_inner(
        &self,
        request: ExecutionRequest,
        node_session_id: &str,
        now_unix_millis: u64,
        peak: Option<(String, OverheadReservation)>,
    ) -> Result<ExecutionAuthorization> {
        let target = target(&request)?;
        let field = format!("environment:{}", request.source.spec.id);
        let node_field = format!("node:{}", request.source.assignment.node_id);
        for _ in 0..ATTEMPTS {
            let mut raw = self
                .store
                .allocation_view(&[HEADER.into(), field.clone(), node_field.clone()])
                .await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            let node: StoredNode = decode(raw.get(&node_field).ok_or(Error::NotFound)?)?;
            if node
                .session
                .as_ref()
                .is_none_or(|s| s.id != node_session_id || !s.routable)
                || !node.node.available
                || node.scheduling_paused
            {
                return Err(Error::Conflict);
            }
            let mut stored: StoredEnvironment = decode(raw.get(&field).ok_or(Error::NotFound)?)?;
            stored.validate()?;
            if stored.invalidated {
                return Err(Error::Conflict);
            }
            if let Some(execution) = &stored.execution {
                if execution.pending {
                    let grant = &execution.authorization;
                    if grant.request != request
                        || grant.node_session_id != node_session_id
                        || grant.coordinator_epoch != self.epoch()
                    {
                        return Err(Error::Conflict);
                    }
                    if let Some((id, want)) = &peak {
                        let link = execution.peak.as_ref().ok_or(Error::Conflict)?;
                        let old: OverheadReservation =
                            decode(raw.get(&format!("overhead:{id}")).ok_or(Error::Conflict)?)?;
                        if old != *want
                            || link.reservation_id != *id
                            || link.owner_sha256 != want.owner_sha256
                        {
                            return Err(Error::Conflict);
                        }
                    }
                    if let Some(link) = &execution.peak {
                        let old: OverheadReservation = decode(
                            raw.get(&format!("overhead:{}", link.reservation_id))
                                .ok_or(Error::Conflict)?,
                        )?;
                        old.validate()?;
                        if old.owner_sha256 != link.owner_sha256
                            || old.retirement_proof_sha256.is_some()
                        {
                            return Err(Error::Conflict);
                        }
                    }
                    let grant = grant.clone();
                    if peak.is_none() && execution.peak.as_ref().is_some_and(|p| !p.node_accepted) {
                        // Only the node's normal authorization path accepts the
                        // queued permission. Its CAS competes with source deletion;
                        // the directory must publish the source until this wins.
                        let link = stored
                            .execution
                            .as_mut()
                            .and_then(|e| e.peak.as_mut())
                            .ok_or(Error::Conflict)?;
                        link.node_accepted = true;
                        stored.validate()?;
                        budget::check_admission(&raw, &header, &stored)?;
                        header.advance()?;
                        if !self
                            .store
                            .cas(
                                raw.get(HEADER).ok_or(Error::Conflict)?,
                                &header,
                                Some((&field, encode(&stored)?)),
                            )
                            .await?
                        {
                            continue;
                        }
                    }
                    // Return the original expired permission for cleanup. The node
                    // must check its deadline immediately before physical dispatch.
                    return Ok(grant);
                }
            }
            if stored.result.as_ref() != Some(&request.source)
                || stored.assignment != request.source.assignment
            {
                return Err(Error::Conflict);
            }
            if request.kind != ExecutionKind::Restart
                && request
                    .source
                    .checkpoint
                    .as_ref()
                    .is_none_or(|cp| cp.expires_at_unix_seconds <= now_unix_millis / 1_000)
            {
                return Err(Error::Invalid("execution recovery point expired".into()));
            }
            let grant = ExecutionAuthorization {
                request: request.clone(),
                target: target.clone(),
                node_session_id: node_session_id.into(),
                coordinator_epoch: self.epoch(),
                issued_unix_millis: now_unix_millis,
                deadline_unix_millis: now_unix_millis
                    .checked_add(request.timeout_millis)
                    .ok_or(Error::Conflict)?,
            };
            let peak_field = peak.as_ref().map(|(id, _)| format!("overhead:{id}"));
            if let Some((_, reservation)) = &peak {
                let key = peak_field.as_ref().ok_or(Error::Conflict)?;
                if raw.contains_key(key)
                    || raw.keys().filter(|k| k.starts_with("overhead:")).count() >= 512
                {
                    return Err(Error::Conflict);
                }
                if header
                    .host_budgets
                    .as_ref()
                    .and_then(|b| b.get(&reservation.host_id))
                    .is_none_or(|b| !b.nodes.contains(&request.source.assignment.node_id))
                {
                    return Err(Error::Conflict);
                }
                raw.insert(key.clone(), encode(reservation)?);
                header.overhead_budget = true;
                header.queued_recovery = true;
            }
            stored.execution = Some(StoredExecution {
                authorization: grant.clone(),
                request_sha256: fingerprint(&request)?,
                pending: true,
                peak: peak.as_ref().map(|(id, p)| ExecutionPeak {
                    reservation_id: id.clone(),
                    owner_sha256: p.owner_sha256.clone(),
                    node_accepted: false,
                }),
            });
            stored.validate()?;
            budget::check_admission(&raw, &header, &stored)?;
            // Older writers must not ignore the unknown pending-execution field.
            header.schema = header.schema.max(3);
            header.advance()?;
            let mut fields = vec![(field.as_str(), encode(&stored)?)];
            if let Some(key) = &peak_field {
                fields.push((key.as_str(), raw.get(key).ok_or(Error::Conflict)?.clone()));
            }
            if self
                .store
                .cas_fields(raw.get(HEADER).ok_or(Error::Conflict)?, &header, &fields)
                .await?
            {
                return Ok(grant);
            }
        }
        Err(Error::Unavailable(
            "concurrent execution authorization; retry".into(),
        ))
    }
}

pub(super) fn check_result(
    old: &StoredEnvironment,
    result: &EnvironmentRecord,
    strict: bool,
) -> Result<()> {
    if old.result.as_ref() == Some(result) {
        return Ok(());
    }
    if let Some(execution) = old.execution.as_ref().filter(|e| e.pending) {
        if execution.peak.as_ref().is_some_and(|p| !p.node_accepted) {
            return if undispatched_cancellation(old, result) {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        let grant = &execution.authorization;
        if result.runtime.id != grant.target.runtime.id
            || result.revision <= grant.target.revision
            || result.restart_attempts != grant.target.restart_attempts
            || (result.state != EnvironmentState::Deleted
                && result.checkpoint != grant.target.checkpoint)
        {
            return Err(Error::Conflict);
        }
        if result.state == EnvironmentState::Running {
            let required = match grant.request.kind {
                ExecutionKind::Resume => Some(adx_core::LifecycleKind::Resume),
                ExecutionKind::Reload => Some(adx_core::LifecycleKind::Reload),
                _ => None,
            };
            if required.is_some_and(|kind| {
                result.last_operation.as_ref().is_none_or(|op| {
                    op.kind != kind
                        || op.id != grant.request.operation_id
                        || op.expected_revision != grant.request.expected_revision
                })
            }) {
                return Err(Error::Conflict);
            }
        }
    } else if strict {
        let previous = old.effective_record();
        if result.runtime.id != previous.runtime.id
            || (!previous.resources_held && result.resources_held)
        {
            return Err(Error::Conflict);
        }
    }
    Ok(())
}

/// A node may delete the original paused source before it accepts a queued
/// permission. This exact terminal result discards only the unconsumed grant;
/// the independent peak remains held until physical cleanup is proven.
pub(super) fn undispatched_cancellation(
    old: &StoredEnvironment,
    result: &EnvironmentRecord,
) -> bool {
    let Some(execution) = old
        .execution
        .as_ref()
        .filter(|e| e.pending && e.peak.as_ref().is_some_and(|p| !p.node_accepted))
    else {
        return false;
    };
    let source = &execution.authorization.request.source;
    result.state == EnvironmentState::Deleted
        && !result.resources_held
        && result.checkpoint.is_none()
        && result.revision > source.revision
        && result.spec == source.spec
        && result.assignment == source.assignment
        && result.runtime == source.runtime
        && result.restart_attempts == source.restart_attempts
        && !result.restart_pending
        && result.last_operation.is_none()
}

pub(super) fn validate_peak_links(
    raw: &BTreeMap<String, String>,
    replacement: Option<&StoredEnvironment>,
) -> Result<()> {
    fn check(raw: &BTreeMap<String, String>, environment: &StoredEnvironment) -> Result<()> {
        if let Some(peak) = environment
            .execution
            .as_ref()
            .filter(|e| e.pending)
            .and_then(|e| e.peak.as_ref())
        {
            let reservation: OverheadReservation = decode(
                raw.get(&format!("overhead:{}", peak.reservation_id))
                    .ok_or(Error::Conflict)?,
            )?;
            reservation.validate()?;
            if reservation.owner_sha256 != peak.owner_sha256
                || reservation.retirement_proof_sha256.is_some()
            {
                return Err(Error::Conflict);
            }
        }
        Ok(())
    }
    for (key, value) in raw {
        if let Some(id) = key.strip_prefix("environment:") {
            if replacement.is_some_and(|e| e.spec.id == id) {
                continue;
            }
            check(raw, &decode::<StoredEnvironment>(value)?)?;
        }
    }
    if let Some(environment) = replacement {
        check(raw, environment)?;
    }
    Ok(())
}

#[cfg(test)]
mod queued_publication_tests {
    use super::*;

    #[test]
    fn queued_peak_keeps_published_source_until_node_accepts() {
        let spec = EnvironmentSpec {
            runtime_profile: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            id: "queue".into(),
            tenant_id: "tenant".into(),
            image: "image".into(),
            runtime_class: "runc".into(),
            resources: adx_core::Resources {
                cpu_millis: 100,
                memory_bytes: 128,
                disk_bytes: 0,
            },
            priority: 0,
            scheduling: Default::default(),
            sandbox: Default::default(),
        };
        let assignment = adx_core::Assignment {
            environment_id: "queue".into(),
            node_id: "node".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        };
        let source = EnvironmentRecord {
            spec: spec.clone(),
            assignment: assignment.clone(),
            runtime: adx_core::Runtime {
                id: "queue-1".into(),
                ip: None,
            },
            state: EnvironmentState::Paused,
            revision: 3,
            resources_held: false,
            restart_attempts: 0,
            restart_pending: false,
            last_operation: None,
            checkpoint: Some(adx_core::RestorePoint {
                source_resources: None,
                id: "checkpoint".into(),
                origin: None,
                source_runtime_id: "queue-1".into(),
                expires_at_unix_seconds: u64::MAX,
                artifact: adx_core::CheckpointArtifact {
                    storage: "shared".into(),
                    location: "artifact".into(),
                    size_bytes: 8,
                },
            }),
        };
        let request = ExecutionRequest {
            source: source.clone(),
            kind: ExecutionKind::Resume,
            operation_id: "resume-queued".into(),
            expected_revision: source.revision,
            timeout_millis: 30_000,
        };
        let grant = ExecutionAuthorization {
            target: request.target().unwrap(),
            request: request.clone(),
            node_session_id: "boot-node".into(),
            coordinator_epoch: 1,
            issued_unix_millis: 1000,
            deadline_unix_millis: 31_000,
        };
        let stored = StoredEnvironment {
            spec,
            assignment,
            result: Some(source.clone()),
            invalidated: false,
            memory: None,
            recovery: None,
            failed_since_unix_seconds: None,
            execution: Some(StoredExecution {
                authorization: grant,
                request_sha256: fingerprint(&request).unwrap(),
                pending: true,
                peak: Some(ExecutionPeak {
                    reservation_id: "peak".into(),
                    owner_sha256: "a".repeat(64),
                    node_accepted: false,
                }),
            }),
        };
        stored.validate().unwrap();
        assert!(
            stored.resources_held(),
            "the target ceiling is already charged"
        );
        assert_eq!(
            stored.effective_record(),
            source,
            "a queued reservation is not a node dispatch or a new published Runtime"
        );
    }
}
