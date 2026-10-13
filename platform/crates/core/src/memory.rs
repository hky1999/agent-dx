//! Conditional memory authorization for one physical execution. Usage is not permission.
use crate::{Assignment, Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryIdentity {
    pub assignment: Assignment,
    pub runtime_id: String,
    pub sandbox_id: String,
    pub resource_generation: String,
}

/// A complete physical readback. Callers must reject unknown or unlimited fields
/// before constructing this type; zero is not a substitute for missing evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryObservation {
    pub identity: MemoryIdentity,
    pub cpu_quota_usec: u64,
    pub cpu_period_usec: u64,
    pub memory_limit_bytes: u64,
    pub memory_high_bytes: u64,
    pub resource_revision: u64,
    pub operation_id: String,
    pub operation_pending: bool,
    pub frozen: bool,
}

/// Full immutable source and target, including the soft throttle and original timeout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryRequest {
    pub source: MemoryObservation,
    pub target_limit_bytes: u64,
    pub target_high_bytes: u64,
    pub operation_id: String,
    pub timeout_millis: u64,
}

/// Expiry fences new physical dispatch. It never cancels a durable reservation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryAuthorization {
    pub request: MemoryRequest,
    pub node_session_id: String,
    pub coordinator_epoch: u64,
    pub issued_unix_millis: u64,
    pub deadline_unix_millis: u64,
}

fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.trim() == id && id.len() <= 256 && !id.chars().any(char::is_control)
}

fn valid_limits(max: u64, high: u64) -> bool {
    max > 0
        && max <= i64::MAX as u64
        && high > 0
        && high <= max
        && max.is_multiple_of(4096)
        && high.is_multiple_of(4096)
}

impl MemoryObservation {
    /// Rejects incomplete identity, invalid finite limits, unknown revision and
    /// any physical operation whose freeze or result has not been settled.
    pub fn validate(&self) -> Result<()> {
        let id = &self.identity;
        if !valid_id(&id.assignment.environment_id)
            || !valid_id(&id.assignment.node_id)
            || !valid_id(&id.sandbox_id)
            || !valid_id(&id.resource_generation)
            || id.assignment.generation == 0
            || !crate::valid_runtime_id(
                &id.assignment.environment_id,
                id.assignment.generation,
                &id.runtime_id,
            )
            || self.cpu_quota_usec == 0
            || self.cpu_quota_usec > i64::MAX as u64
            || self.cpu_period_usec == 0
            || !valid_limits(self.memory_limit_bytes, self.memory_high_bytes)
            || self.resource_revision == 0
            || (!self.operation_id.is_empty() && !valid_id(&self.operation_id))
            || self.operation_pending
            || self.frozen
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}

impl MemoryRequest {
    /// Returns the exact settled observation required to release any reservation.
    /// Invalid source, target, timeout or revision overflow returns Conflict.
    pub fn completion(&self) -> Result<MemoryObservation> {
        self.source.validate()?;
        if !valid_limits(self.target_limit_bytes, self.target_high_bytes)
            || self.target_limit_bytes == self.source.memory_limit_bytes
            || !valid_id(&self.operation_id)
            || self.operation_id.len() > 128
            || self.operation_id == self.source.operation_id
            || self.timeout_millis == 0
            || self.timeout_millis > 3_600_000
        {
            return Err(Error::Conflict);
        }
        let mut result = self.source.clone();
        result.resource_revision = result
            .resource_revision
            .checked_add(1)
            .ok_or(Error::Conflict)?;
        result.memory_limit_bytes = self.target_limit_bytes;
        result.memory_high_bytes = self.target_high_bytes;
        result.operation_id = self.operation_id.clone();
        Ok(result)
    }
}

impl MemoryAuthorization {
    /// Rejects changed request/deadline bindings and invalid session or epoch.
    pub fn validate(&self) -> Result<()> {
        self.request.completion()?;
        if !valid_id(&self.node_session_id)
            || self.coordinator_epoch == 0
            || self
                .issued_unix_millis
                .checked_add(self.request.timeout_millis)
                != Some(self.deadline_unix_millis)
            || self.deadline_unix_millis > i64::MAX as u64
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }

    /// The dispatching node must compare its current session, current Coordinator
    /// epoch and wall clock. Replay of an old authorization remains read-only.
    pub fn check_dispatch(
        &self,
        now_unix_millis: u64,
        node_session_id: &str,
        coordinator_epoch: u64,
    ) -> Result<()> {
        self.validate()?;
        if self.node_session_id != node_session_id || self.coordinator_epoch != coordinator_epoch {
            return Err(Error::Conflict);
        }
        if now_unix_millis < self.issued_unix_millis || now_unix_millis >= self.deadline_unix_millis
        {
            return Err(Error::Unavailable(
                "memory authorization outside its dispatch window".into(),
            ));
        }
        Ok(())
    }
}
