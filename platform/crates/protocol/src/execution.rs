//! Typed execution permission; no unvalidated JSON payload crosses the RPC boundary.
use crate::{control as pb, Error, Result};
use adx_core::execution::{ExecutionAuthorization, ExecutionKind, ExecutionRequest};

impl TryFrom<pb::ExecutionRequest> for ExecutionRequest {
    type Error = Error;
    fn try_from(value: pb::ExecutionRequest) -> Result<Self> {
        let kind = match pb::ExecutionKind::try_from(value.kind) {
            Ok(pb::ExecutionKind::Resume) => ExecutionKind::Resume,
            Ok(pb::ExecutionKind::Restart) => ExecutionKind::Restart,
            Ok(pb::ExecutionKind::Reload) => ExecutionKind::Reload,
            Ok(pb::ExecutionKind::Failover) => ExecutionKind::Failover,
            Ok(pb::ExecutionKind::PauseRollback) => ExecutionKind::PauseRollback,
            _ => return Err(Error::Invalid("execution kind required".into())),
        };
        let request = Self {
            source: value
                .source
                .ok_or_else(|| Error::Invalid("execution source required".into()))?
                .try_into()?,
            kind,
            operation_id: value.operation_id,
            expected_revision: value.expected_revision,
            timeout_millis: value.timeout_millis,
        };
        request.target()?;
        Ok(request)
    }
}
impl TryFrom<ExecutionRequest> for pb::ExecutionRequest {
    type Error = Error;
    fn try_from(value: ExecutionRequest) -> Result<Self> {
        value.target()?;
        let kind = match value.kind {
            ExecutionKind::Resume => pb::ExecutionKind::Resume,
            ExecutionKind::Restart => pb::ExecutionKind::Restart,
            ExecutionKind::Reload => pb::ExecutionKind::Reload,
            ExecutionKind::Failover => pb::ExecutionKind::Failover,
            ExecutionKind::PauseRollback => pb::ExecutionKind::PauseRollback,
        };
        Ok(Self {
            source: Some(value.source.try_into()?),
            kind: kind as i32,
            operation_id: value.operation_id,
            expected_revision: value.expected_revision,
            timeout_millis: value.timeout_millis,
        })
    }
}
impl TryFrom<pb::ExecutionAuthorization> for ExecutionAuthorization {
    type Error = Error;
    fn try_from(value: pb::ExecutionAuthorization) -> Result<Self> {
        let grant = Self {
            request: value
                .request
                .ok_or_else(|| Error::Invalid("execution request required".into()))?
                .try_into()?,
            target: value
                .target
                .ok_or_else(|| Error::Invalid("execution target required".into()))?
                .try_into()?,
            node_session_id: value.node_session_id,
            coordinator_epoch: value.coordinator_epoch,
            issued_unix_millis: value.issued_unix_millis,
            deadline_unix_millis: value.deadline_unix_millis,
        };
        grant.validate()?;
        Ok(grant)
    }
}
impl TryFrom<ExecutionAuthorization> for pb::ExecutionAuthorization {
    type Error = Error;
    fn try_from(value: ExecutionAuthorization) -> Result<Self> {
        value.validate()?;
        Ok(Self {
            request: Some(value.request.try_into()?),
            target: Some(value.target.try_into()?),
            node_session_id: value.node_session_id,
            coordinator_epoch: value.coordinator_epoch,
            issued_unix_millis: value.issued_unix_millis,
            deadline_unix_millis: value.deadline_unix_millis,
        })
    }
}
