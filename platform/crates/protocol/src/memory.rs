//! Typed memory observations and grants; no JSON payload crosses the transport boundary.
use crate::{control as pb, Error, Result};
use adx_core::memory::{
    MemoryAuthorization, MemoryDecision, MemoryIdentity, MemoryObservation, MemoryPressure,
    MemoryRequest, MemorySample,
};

impl TryFrom<pb::MemoryIdentity> for MemoryIdentity {
    type Error = Error;
    fn try_from(value: pb::MemoryIdentity) -> Result<Self> {
        let value = Self {
            assignment: value
                .assignment
                .ok_or_else(|| Error::Invalid("assignment required".into()))?
                .try_into()?,
            runtime_id: value.runtime_id,
            sandbox_id: value.sandbox_id,
            resource_generation: value.resource_generation,
        };
        Ok(value)
    }
}

impl TryFrom<MemoryIdentity> for pb::MemoryIdentity {
    type Error = Error;
    fn try_from(value: MemoryIdentity) -> Result<Self> {
        Ok(Self {
            assignment: Some(value.assignment.try_into()?),
            runtime_id: value.runtime_id,
            sandbox_id: value.sandbox_id,
            resource_generation: value.resource_generation,
        })
    }
}

impl TryFrom<pb::MemoryObservation> for MemoryObservation {
    type Error = Error;
    fn try_from(value: pb::MemoryObservation) -> Result<Self> {
        let value = Self {
            identity: value
                .identity
                .ok_or_else(|| Error::Invalid("identity required".into()))?
                .try_into()?,
            cpu_quota_usec: value.cpu_quota_usec,
            cpu_period_usec: value.cpu_period_usec,
            memory_limit_bytes: value.memory_limit_bytes,
            memory_high_bytes: value.memory_high_bytes,
            resource_revision: value.resource_revision,
            operation_id: value.operation_id,
            operation_pending: value.operation_pending,
            frozen: value.frozen,
        };
        value.validate()?;
        Ok(value)
    }
}

impl TryFrom<MemoryObservation> for pb::MemoryObservation {
    type Error = Error;
    fn try_from(value: MemoryObservation) -> Result<Self> {
        value.validate()?;
        Ok(Self {
            identity: Some(value.identity.try_into()?),
            cpu_quota_usec: value.cpu_quota_usec,
            cpu_period_usec: value.cpu_period_usec,
            memory_limit_bytes: value.memory_limit_bytes,
            memory_high_bytes: value.memory_high_bytes,
            resource_revision: value.resource_revision,
            operation_id: value.operation_id,
            operation_pending: value.operation_pending,
            frozen: value.frozen,
        })
    }
}

impl TryFrom<pb::MemoryRequest> for MemoryRequest {
    type Error = Error;
    fn try_from(value: pb::MemoryRequest) -> Result<Self> {
        let value = Self {
            source: value
                .source
                .ok_or_else(|| Error::Invalid("source required".into()))?
                .try_into()?,
            target_limit_bytes: value.target_limit_bytes,
            target_high_bytes: value.target_high_bytes,
            operation_id: value.operation_id,
            timeout_millis: value.timeout_millis,
        };
        value.completion()?;
        Ok(value)
    }
}

impl TryFrom<MemoryRequest> for pb::MemoryRequest {
    type Error = Error;
    fn try_from(value: MemoryRequest) -> Result<Self> {
        value.completion()?;
        Ok(Self {
            source: Some(value.source.try_into()?),
            target_limit_bytes: value.target_limit_bytes,
            target_high_bytes: value.target_high_bytes,
            operation_id: value.operation_id,
            timeout_millis: value.timeout_millis,
        })
    }
}

impl TryFrom<pb::MemoryAuthorization> for MemoryAuthorization {
    type Error = Error;
    fn try_from(value: pb::MemoryAuthorization) -> Result<Self> {
        let value = Self {
            request: value
                .request
                .ok_or_else(|| Error::Invalid("request required".into()))?
                .try_into()?,
            node_session_id: value.node_session_id,
            coordinator_epoch: value.coordinator_epoch,
            issued_unix_millis: value.issued_unix_millis,
            deadline_unix_millis: value.deadline_unix_millis,
        };
        value.validate()?;
        Ok(value)
    }
}

impl TryFrom<MemoryAuthorization> for pb::MemoryAuthorization {
    type Error = Error;
    fn try_from(value: MemoryAuthorization) -> Result<Self> {
        value.validate()?;
        Ok(Self {
            request: Some(value.request.try_into()?),
            node_session_id: value.node_session_id,
            coordinator_epoch: value.coordinator_epoch,
            issued_unix_millis: value.issued_unix_millis,
            deadline_unix_millis: value.deadline_unix_millis,
        })
    }
}

impl TryFrom<pb::MemoryPressure> for MemoryPressure {
    type Error = Error;
    fn try_from(value: pb::MemoryPressure) -> Result<Self> {
        let value = Self {
            high_events: value.high_events,
            oom_events: value.oom_events,
            oom_kill_events: value.oom_kill_events,
        };
        Ok(value)
    }
}

impl TryFrom<MemoryPressure> for pb::MemoryPressure {
    type Error = Error;
    fn try_from(value: MemoryPressure) -> Result<Self> {
        Ok(Self {
            high_events: value.high_events,
            oom_events: value.oom_events,
            oom_kill_events: value.oom_kill_events,
        })
    }
}

impl TryFrom<pb::MemorySample> for MemorySample {
    type Error = Error;
    fn try_from(value: pb::MemorySample) -> Result<Self> {
        let value = Self {
            observation: value
                .observation
                .ok_or_else(|| Error::Invalid("observation required".into()))?
                .try_into()?,
            usage_bytes: value.usage_bytes,
            peak_bytes: value.peak_bytes,
            cpu_usage_ns: value.cpu_usage_ns,
            pressure: value.pressure.map(TryInto::try_into).transpose()?,
            observed_unix_millis: value.observed_unix_millis,
        };
        value.observation.validate()?;
        Ok(value)
    }
}

impl TryFrom<MemorySample> for pb::MemorySample {
    type Error = Error;
    fn try_from(value: MemorySample) -> Result<Self> {
        value.observation.validate()?;
        Ok(Self {
            observation: Some(value.observation.try_into()?),
            usage_bytes: value.usage_bytes,
            peak_bytes: value.peak_bytes,
            cpu_usage_ns: value.cpu_usage_ns,
            pressure: value.pressure.map(TryInto::try_into).transpose()?,
            observed_unix_millis: value.observed_unix_millis,
        })
    }
}

impl TryFrom<pb::MemoryDecision> for MemoryDecision {
    type Error = Error;
    fn try_from(value: pb::MemoryDecision) -> Result<Self> {
        let value = Self {
            observed: value
                .observed
                .ok_or_else(|| Error::Invalid("observed required".into()))?
                .try_into()?,
            held_resources: resources(value.held_resources)?,
            authorization: value.authorization.map(TryInto::try_into).transpose()?,
            node_session_id: value.node_session_id,
            coordinator_epoch: value.coordinator_epoch,
        };
        value.validate()?;
        Ok(value)
    }
}

impl TryFrom<MemoryDecision> for pb::MemoryDecision {
    type Error = Error;
    fn try_from(value: MemoryDecision) -> Result<Self> {
        value.validate()?;
        Ok(Self {
            observed: Some(value.observed.try_into()?),
            held_resources: Some(value.held_resources.into()),
            authorization: value.authorization.map(TryInto::try_into).transpose()?,
            node_session_id: value.node_session_id,
            coordinator_epoch: value.coordinator_epoch,
        })
    }
}

fn resources(value: Option<pb::Resources>) -> Result<adx_core::Resources> {
    let r = value.ok_or_else(|| Error::Invalid("held resources required".into()))?;
    let r = adx_core::Resources {
        cpu_millis: r.cpu_millis,
        memory_bytes: r.memory_bytes,
        disk_bytes: r.disk_bytes,
    };
    r.validate()?;
    Ok(r)
}
