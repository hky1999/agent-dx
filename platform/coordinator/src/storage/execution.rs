//! Persist execution permission before dispatch; retain unknown attempts for cleanup.
use super::*;
use adx_core::execution::{ExecutionAuthorization, ExecutionKind, ExecutionRequest};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredExecution {
    pub authorization: ExecutionAuthorization,
    pub request_sha256: String,
    pub pending: bool,
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
        let target = target(&request)?;
        let field = format!("environment:{}", request.source.spec.id);
        let node_field = format!("node:{}", request.source.assignment.node_id);
        for _ in 0..ATTEMPTS {
            let raw = self
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
                    // Return the original expired permission for cleanup. The node
                    // must check its deadline immediately before physical dispatch.
                    return Ok(grant.clone());
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
            stored.execution = Some(StoredExecution {
                authorization: grant.clone(),
                request_sha256: fingerprint(&request)?,
                pending: true,
            });
            stored.validate()?;
            budget::check_admission(&raw, &header, &stored)?;
            // Older writers must not ignore the unknown pending-execution field.
            header.schema = 3;
            header.advance()?;
            if self
                .store
                .cas(
                    raw.get(HEADER).ok_or(Error::Conflict)?,
                    &header,
                    Some((&field, encode(&stored)?)),
                )
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
