//! Durable permission to replace one retired physical execution.
use crate::{EnvironmentRecord, EnvironmentState, Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionKind {
    Resume,
    Restart,
    Reload,
    Failover,
    PauseRollback,
}

/// The complete request is the replay fingerprint; source is a published record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRequest {
    pub source: EnvironmentRecord,
    pub kind: ExecutionKind,
    pub operation_id: String,
    pub expected_revision: u64,
    pub timeout_millis: u64,
}

/// Coordinator-issued permission. Expiry forbids dispatch, not budget retention.
/// `target` is Starting or Resuming; it does not assert physical execution success.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionAuthorization {
    pub request: ExecutionRequest,
    pub target: EnvironmentRecord,
    pub node_session_id: String,
    pub coordinator_epoch: u64,
    pub issued_unix_millis: u64,
    pub deadline_unix_millis: u64,
}

impl ExecutionRequest {
    /// Validate a replacement request and derive its unique target identity.
    /// Rejects live sources, invalid lifecycle transitions and counter overflow.
    pub fn target(&self) -> Result<EnvironmentRecord> {
        let request = self;

        let source = &request.source;
        source.spec.validate()?;
        if source.revision == 0
            || source.assignment.environment_id != source.spec.id
            || source.assignment.generation == 0
            || !crate::valid_runtime_id(
                &source.spec.id,
                source.assignment.generation,
                &source.runtime.id,
            )
        {
            return Err(Error::Conflict);
        }
        if source.resources_held
            || source.runtime.ip.is_some()
            || request.operation_id.trim().is_empty()
            || request.operation_id.trim() != request.operation_id
            || request.operation_id.len() > 128
            || request.expected_revision == 0
            || request.expected_revision > source.revision
            || request.timeout_millis == 0
            || request.timeout_millis > 3_600_000
        {
            return Err(Error::Invalid(
                "invalid execution authorization request".into(),
            ));
        }
        let mut next = source.clone();
        next.revision = source.revision.checked_add(1).ok_or(Error::Conflict)?;
        next.runtime = crate::Runtime {
            id: format!(
                "{}-{}-r{}",
                source.spec.id, source.assignment.generation, next.revision
            ),
            ip: None,
        };
        if next.runtime.id == source.runtime.id {
            return Err(Error::Conflict);
        }
        next.resources_held = true;
        next.restart_pending = false;
        next.last_operation = None;
        match request.kind {
            ExecutionKind::Resume | ExecutionKind::PauseRollback => {
                if source.state != EnvironmentState::Paused
                    || source.checkpoint.is_none()
                    || (request.kind == ExecutionKind::Resume
                        && request.expected_revision != source.revision)
                {
                    return Err(Error::Conflict);
                }
                next.state = EnvironmentState::Resuming;
            }
            ExecutionKind::Restart => {
                if source.state != EnvironmentState::Failed
                    || !source.restart_pending
                    || request.expected_revision != source.revision
                {
                    return Err(Error::Conflict);
                }
                next.restart_attempts = source
                    .restart_attempts
                    .checked_add(1)
                    .ok_or(Error::Conflict)?;
                if source
                    .spec
                    .lifecycle
                    .restart
                    .as_ref()
                    .is_none_or(|policy| next.restart_attempts > policy.max_attempts)
                {
                    return Err(Error::Conflict);
                }
                next.state = EnvironmentState::Starting;
            }
            ExecutionKind::Reload | ExecutionKind::Failover => {
                if source.state != EnvironmentState::Failed
                    || source.checkpoint.is_none()
                    || (request.kind == ExecutionKind::Failover && !source.spec.sandbox.failover)
                {
                    return Err(Error::Conflict);
                }
                next.state = EnvironmentState::Starting;
            }
        }
        Ok(next)
    }
}
impl ExecutionAuthorization {
    /// Validate the full request/target binding and the immutable deadline.
    pub fn validate(&self) -> Result<()> {
        if self.target != self.request.target()?
            || self.node_session_id.trim().is_empty()
            || self.coordinator_epoch == 0
            || self
                .issued_unix_millis
                .checked_add(self.request.timeout_millis)
                != Some(self.deadline_unix_millis)
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    /// Check dispatch against the local wall clock, failing closed on clock errors.
    pub fn check_dispatch_now(&self) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Unavailable("system clock before epoch".into()))?;
        let millis = u64::try_from(now.as_millis())
            .map_err(|_| Error::Unavailable("system clock overflow".into()))?;
        self.check_dispatch(millis)
    }
    /// An expired permission still owns budget but may not start its target.
    pub fn check_dispatch(&self, now_unix_millis: u64) -> Result<()> {
        self.validate()?;
        if now_unix_millis >= self.deadline_unix_millis {
            return Err(Error::Unavailable("execution authorization expired".into()));
        }
        Ok(())
    }
}
