//! Physical memory observations and conditional dispatch. The Coordinator owns
//! permission and budget; this adapter never infers permission from kernel usage.
use super::{operations, proto, unavailable, BackendIdentity, Sandboxd, StartState};
use adx_core::memory::{
    MemoryAuthorization, MemoryIdentity, MemoryObservation, MemoryPressure, MemorySample,
};
use adx_core::{EnvironmentRecord, EnvironmentState, Error, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct MemoryDispatch {
    pub authorization: MemoryAuthorization,
    pub request_sha256: String,
}
impl MemoryDispatch {
    pub(super) fn validate(
        &self,
        runtime_id: &str,
        backend: Option<&BackendIdentity>,
        retired_generation: Option<&str>,
    ) -> Result<()> {
        self.authorization.validate()?;
        let identity = &self.authorization.request.source.identity;
        if identity.runtime_id != runtime_id
            || match backend {
                Some(b) => {
                    b.id != identity.sandbox_id
                        || b.resource_generation != identity.resource_generation
                }
                None => retired_generation != Some(identity.resource_generation.as_str()),
            }
            || self.request_sha256 != operations::fingerprint(&self.authorization)?
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}
fn check_record(record: &EnvironmentRecord) -> Result<()> {
    record.spec.validate()?;
    if record.state != EnvironmentState::Running
        || !record.resources_held
        || record.revision == 0
        || record.spec.id != record.assignment.environment_id
        || record.spec.sandbox.elastic_memory.is_none()
        || !adx_core::valid_runtime_id(
            &record.spec.id,
            record.assignment.generation,
            &record.runtime.id,
        )
    {
        return Err(Error::Conflict);
    }
    Ok(())
}
fn high(record: &EnvironmentRecord, limit: u64) -> Result<u64> {
    let policy = record
        .spec
        .sandbox
        .elastic_memory
        .as_ref()
        .ok_or(Error::Conflict)?;
    if limit < record.spec.resources.memory_bytes
        || limit > record.spec.execution_ceiling().memory_bytes
        || !limit.is_multiple_of(1_048_576)
    {
        return Err(Error::Conflict);
    }
    let rounded = (u128::from(limit) * u128::from(policy.high_per_mille)).div_ceil(1000);
    let aligned = u64::try_from(rounded / 4096 * 4096).map_err(unavailable)?;
    if aligned == 0 || aligned >= limit {
        return Err(Error::Conflict);
    }
    Ok(aligned)
}
fn sample(
    record: &EnvironmentRecord,
    physical: &BackendIdentity,
    value: proto::StatsResponse,
) -> Result<MemorySample> {
    let ceiling = record.spec.execution_ceiling();
    let expected_policy = serde_json::json!({
        "version":1,
        "CPU":{"initial":ceiling.cpu_millis,"ceiling":ceiling.cpu_millis},
        "Memory":{"initial":record.spec.resources.memory_bytes / 1_048_576,"ceiling":ceiling.memory_bytes / 1_048_576}
    });
    if !value.cpu_limits_known
        || !value.memory_usage_known
        || !value.memory_limit_known
        || !value.memory_high_known
        || value.memory_high_unlimited
        || !value.frozen_known
        || !value.resource_revision_known
        || value.resource_generation != physical.resource_generation
        || value.elastic_policy.len() > 4096
        || serde_json::from_str::<serde_json::Value>(&value.elastic_policy).map_err(unavailable)?
            != expected_policy
        || u128::from(u64::try_from(value.cpu_quota_usec).map_err(unavailable)?) * 1000
            != u128::from(ceiling.cpu_millis) * u128::from(value.cpu_period_usec)
        || value.memory_high_bytes != high(record, value.memory_limit_bytes)?
    {
        return Err(unavailable(
            "incomplete or mismatched physical memory observation",
        ));
    }
    let observation = MemoryObservation {
        identity: MemoryIdentity {
            assignment: record.assignment.clone(),
            runtime_id: record.runtime.id.clone(),
            sandbox_id: physical.id.clone(),
            resource_generation: physical.resource_generation.clone(),
        },
        cpu_quota_usec: u64::try_from(value.cpu_quota_usec).map_err(unavailable)?,
        cpu_period_usec: value.cpu_period_usec,
        memory_limit_bytes: value.memory_limit_bytes,
        memory_high_bytes: value.memory_high_bytes,
        resource_revision: value.resource_revision,
        operation_id: value.resource_operation_id,
        operation_pending: value.resource_operation_pending,
        frozen: value.frozen,
    };
    observation.validate()?;
    Ok(MemorySample {
        observation,
        usage_bytes: value.memory_usage_bytes,
        peak_bytes: value
            .memory_peak_known
            .then_some(value.memory_max_usage_bytes),
        cpu_usage_ns: value.cpu_usage_known.then_some(value.cpu_usage_ns),
        pressure: value.memory_pressure_known.then_some(MemoryPressure {
            high_events: value.memory_high_events,
            oom_events: value.memory_oom_events,
            oom_kill_events: value.memory_oom_kill_events,
        }),
        observed_unix_millis: operations::now_millis()?,
    })
}
impl Sandboxd {
    async fn memory_readback(&self, record: &EnvironmentRecord) -> Result<MemorySample> {
        let physical = self
            .physical_identity(&record.runtime.id)
            .await?
            .ok_or(Error::NotFound)?;
        let value = self
            .client
            .clone()
            .stats(self.request(proto::StatsRequest {
                id: physical.id.clone(),
            }))
            .await
            .map_err(unavailable)?
            .into_inner();
        sample(record, &physical, value)
    }
    pub(super) async fn memory_observation(
        &self,
        record: &EnvironmentRecord,
    ) -> Result<MemorySample> {
        check_record(record)?;
        let state = self.cell(&record.runtime.id).lock_owned().await;
        if !matches!(*state, StartState::Started(_)) {
            return Err(Error::Conflict);
        }
        let observed = self.memory_readback(record).await;
        let dispatch = self
            .operations
            .lock()
            .map_err(unavailable)?
            .get(&record.runtime.id)
            .and_then(|entry| entry.memory.clone());
        if let Some(dispatch) = dispatch {
            let grant = &dispatch.authorization;
            let settled = observed.as_ref().is_ok_and(|sample| {
                grant
                    .request
                    .completion()
                    .is_ok_and(|done| done == sample.observation)
                    || grant
                        .request
                        .unchanged()
                        .is_ok_and(|done| done == sample.observation)
            });
            if !settled && operations::now_millis()? >= grant.deadline_unix_millis {
                return self.memory_resolution_locked(record, grant).await;
            }
        }
        observed
    }

    pub(super) async fn memory_resolution(
        &self,
        record: &EnvironmentRecord,
        authorization: &MemoryAuthorization,
    ) -> Result<MemorySample> {
        check_record(record)?;
        let state = self.cell(&record.runtime.id).lock_owned().await;
        if !matches!(*state, StartState::Started(_)) {
            return Err(Error::Conflict);
        }
        self.memory_resolution_locked(record, authorization).await
    }

    // Caller owns the per-Runtime lifecycle lock. Only the resolution RPC is
    // sent, even after a session/epoch change; this never renews or applies a grant.
    async fn memory_resolution_locked(
        &self,
        record: &EnvironmentRecord,
        authorization: &MemoryAuthorization,
    ) -> Result<MemorySample> {
        authorization.validate()?;
        let request = &authorization.request;
        let source = &request.source;
        if source.identity.assignment != record.assignment
            || source.identity.runtime_id != record.runtime.id
            || source.memory_high_bytes != high(record, source.memory_limit_bytes)?
            || request.target_high_bytes != high(record, request.target_limit_bytes)?
        {
            return Err(Error::Conflict);
        }
        let mut entry = self
            .operations
            .lock()
            .map_err(unavailable)?
            .get(&record.runtime.id)
            .cloned()
            .ok_or(Error::Conflict)?;
        let dispatch = MemoryDispatch {
            authorization: authorization.clone(),
            request_sha256: operations::fingerprint(authorization)?,
        };
        dispatch.validate(&record.runtime.id, entry.backend.as_ref(), None)?;
        if entry.memory.as_ref().is_some_and(|old| {
            (old.authorization.request.operation_id == request.operation_id
                && old.request_sha256 != dispatch.request_sha256)
                || (old.authorization.request.operation_id != request.operation_id
                    && old.authorization.request.source.resource_revision
                        >= source.resource_revision)
        }) {
            return Err(Error::Conflict);
        }
        if entry
            .memory
            .as_ref()
            .is_none_or(|old| old.request_sha256 != dispatch.request_sha256)
        {
            entry.memory = Some(dispatch);
            let journal = self
                .journal
                .as_ref()
                .ok_or_else(|| unavailable("memory resolution requires a durable node journal"))?;
            journal
                .save(record.runtime.id.clone(), entry.clone())
                .await?;
            self.operations
                .lock()
                .map_err(unavailable)?
                .insert(record.runtime.id.clone(), entry);
        }
        let expected = proto::ResourceLimits {
            cpu_quota_usec: i64::try_from(source.cpu_quota_usec).map_err(unavailable)?,
            cpu_period_usec: source.cpu_period_usec,
            memory_limit_bytes: source.memory_limit_bytes,
        };
        let target = proto::ResourceLimits {
            memory_limit_bytes: request.target_limit_bytes,
            ..expected
        };
        let response = self
            .client
            .clone()
            .resolve_resource_operation(self.request(proto::ResolveResourceOperationRequest {
                original: Some(proto::ResizeResourcesRequest {
                    sandbox_id: source.identity.sandbox_id.clone(),
                    resource_generation: source.identity.resource_generation.clone(),
                    expected: Some(expected),
                    target: Some(target),
                    deadline_unix_ms:
                        i64::try_from(authorization.deadline_unix_millis).map_err(unavailable)?,
                    operation_id: request.operation_id.clone(),
                    expected_revision: source.resource_revision,
                }),
                shrink: request.target_limit_bytes < source.memory_limit_bytes,
            }))
            .await
            .map_err(unavailable)?
            .into_inner();
        let physical = BackendIdentity {
            id: source.identity.sandbox_id.clone(),
            resource_generation: source.identity.resource_generation.clone(),
        };
        let actual = sample(
            record,
            &physical,
            response
                .actual
                .ok_or_else(|| unavailable("memory resolution has no physical readback"))?,
        )?;
        let unchanged = request.unchanged()?;
        if (actual.observation != request.completion()? && actual.observation != unchanged)
            || (response.complete && actual.observation == unchanged)
        {
            return Err(Error::Conflict);
        }
        let fresh = self.memory_readback(record).await?;
        if fresh.observation != actual.observation {
            return Err(Error::Conflict);
        }
        Ok(fresh)
    }

    pub(super) async fn memory_mutation(
        &self,
        record: &EnvironmentRecord,
        authorization: &MemoryAuthorization,
        session: &str,
        epoch: u64,
    ) -> Result<MemorySample> {
        check_record(record)?;
        authorization.check_dispatch(operations::now_millis()?, session, epoch)?;
        let source = &authorization.request.source;
        if source.identity.assignment != record.assignment
            || source.identity.runtime_id != record.runtime.id
            || source.memory_high_bytes != high(record, source.memory_limit_bytes)?
            || authorization.request.target_high_bytes
                != high(record, authorization.request.target_limit_bytes)?
        {
            return Err(Error::Conflict);
        }
        let adapter = self.clone();
        let record = record.clone();
        let authorization = authorization.clone();
        let session = session.to_owned();
        // The accepted task and per-runtime lock outlive an RPC caller. Removal
        // or checkpoint cannot race an adapter call whose outcome is unknown.
        tokio::spawn(async move {
            let state = adapter.cell(&record.runtime.id).lock_owned().await;
            if !matches!(*state, StartState::Started(_)) {
                return Err(Error::Conflict);
            }
            authorization.check_dispatch(operations::now_millis()?, &session, epoch)?;
            let fingerprint = operations::fingerprint(&authorization)?;
            let mut entry = adapter
                .operations
                .lock()
                .map_err(unavailable)?
                .get(&record.runtime.id)
                .cloned()
                .ok_or(Error::Conflict)?;
            if entry.memory.as_ref().is_some_and(|old| {
                old.authorization.request.operation_id == authorization.request.operation_id
                    && old.request_sha256 != fingerprint
            }) {
                return Err(Error::Conflict);
            }
            let current = adapter.memory_readback(&record).await?;
            let completion = authorization.request.completion()?;
            if current.observation == completion {
                return Ok(current);
            }
            if current.observation != authorization.request.source {
                return Err(Error::Conflict);
            }
            let journal = adapter
                .journal
                .as_ref()
                .ok_or_else(|| unavailable("memory dispatch requires a durable node journal"))?;
            entry.memory = Some(MemoryDispatch {
                authorization: authorization.clone(),
                request_sha256: fingerprint,
            });
            journal
                .save(record.runtime.id.clone(), entry.clone())
                .await?;
            adapter
                .operations
                .lock()
                .map_err(unavailable)?
                .insert(record.runtime.id.clone(), entry);
            let now = operations::now_millis()?;
            authorization.check_dispatch(now, &session, epoch)?;
            let request = &authorization.request;
            let source = &request.source;
            let expected = proto::ResourceLimits {
                cpu_quota_usec: i64::try_from(source.cpu_quota_usec).map_err(unavailable)?,
                cpu_period_usec: source.cpu_period_usec,
                memory_limit_bytes: source.memory_limit_bytes,
            };
            let target = proto::ResourceLimits {
                memory_limit_bytes: request.target_limit_bytes,
                ..expected
            };
            let timeout = adapter.config.rpc_timeout.min(Duration::from_millis(
                authorization.deadline_unix_millis - now,
            ));
            let response = if request.target_limit_bytes > source.memory_limit_bytes {
                let mut wire = tonic::Request::new(proto::ResizeResourcesRequest {
                    sandbox_id: source.identity.sandbox_id.clone(),
                    resource_generation: source.identity.resource_generation.clone(),
                    expected: Some(expected),
                    target: Some(target),
                    deadline_unix_ms: i64::try_from(authorization.deadline_unix_millis)
                        .map_err(unavailable)?,
                    operation_id: request.operation_id.clone(),
                    expected_revision: source.resource_revision,
                });
                wire.set_timeout(timeout);
                adapter.client.clone().resize_resources(wire).await
            } else {
                let mut wire = tonic::Request::new(proto::ShrinkResourcesRequest {
                    sandbox_id: source.identity.sandbox_id.clone(),
                    resource_generation: source.identity.resource_generation.clone(),
                    expected: Some(expected),
                    target: Some(target),
                    deadline_unix_ms: i64::try_from(authorization.deadline_unix_millis)
                        .map_err(unavailable)?,
                    operation_id: request.operation_id.clone(),
                    expected_revision: source.resource_revision,
                });
                wire.set_timeout(timeout);
                adapter.client.clone().shrink_resources(wire).await
            }
            .map_err(unavailable)?
            .into_inner();
            if !response.complete || !response.update_error.is_empty() {
                return Err(unavailable(
                    "physical memory operation is not complete; reservation retained",
                ));
            }
            let physical = BackendIdentity {
                id: source.identity.sandbox_id.clone(),
                resource_generation: source.identity.resource_generation.clone(),
            };
            let reply = sample(
                &record,
                &physical,
                response
                    .actual
                    .ok_or_else(|| unavailable("memory operation has no actual readback"))?,
            )?;
            if reply.observation != completion {
                return Err(Error::Conflict);
            }
            // A success reply alone cannot settle shrink: read current unfrozen
            // state independently, retaining exact revision and operation identity.
            let actual = adapter.memory_readback(&record).await?;
            if actual.observation != completion {
                return Err(Error::Conflict);
            }
            Ok(actual)
        })
        .await
        .map_err(unavailable)?
    }
}
