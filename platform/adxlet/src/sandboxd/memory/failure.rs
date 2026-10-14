//! A terminal partial write can require failure cleanup, never direct release.
use super::{
    check_physical_stats, check_record, high, operations, proto, resolution_request, unavailable,
    BackendIdentity, EnvironmentRecord, Error, MemoryAuthorization, Result, Sandboxd, StartState,
};

// Only the mixed source/target values produced by this original operation are
// accepted. An unknown counter, foreign revision or arbitrary external change
// is not a failure proof. memory.high may exceed memory.max after a partial
// shrink; that state is intentionally never accepted as a settled allocation.
fn partial_limits(
    record: &EnvironmentRecord,
    grant: &MemoryAuthorization,
    value: &proto::StatsResponse,
) -> Result<Option<(u64, u64)>> {
    let request = &grant.request;
    let source = &request.source;
    let done = request.completion()?;
    let physical = BackendIdentity {
        id: source.identity.sandbox_id.clone(),
        resource_generation: source.identity.resource_generation.clone(),
    };
    check_physical_stats(record, &physical, value)?;
    if value.resource_revision != done.resource_revision
        || value.resource_operation_id != request.operation_id
        || value.resource_operation_pending
        || value.frozen
        || u64::try_from(value.cpu_quota_usec).map_err(unavailable)? != source.cpu_quota_usec
        || value.cpu_period_usec != source.cpu_period_usec
        || ![source.memory_limit_bytes, request.target_limit_bytes]
            .contains(&value.memory_limit_bytes)
        || ![source.memory_high_bytes, request.target_high_bytes].contains(&value.memory_high_bytes)
    {
        return Err(Error::Conflict);
    }
    let limits = (value.memory_limit_bytes, value.memory_high_bytes);
    Ok(
        (limits != (source.memory_limit_bytes, source.memory_high_bytes)
            && limits != (request.target_limit_bytes, request.target_high_bytes))
            .then_some(limits),
    )
}

impl Sandboxd {
    pub(in crate::sandboxd) async fn memory_failure_requires_retirement(
        &self,
        record: &EnvironmentRecord,
    ) -> Result<bool> {
        check_record(record)?;
        let state = self.cell(&record.runtime.id).lock_owned().await;
        if !matches!(*state, StartState::Started(_)) {
            return Err(Error::Conflict);
        }
        let entry = self
            .operations
            .lock()
            .map_err(unavailable)?
            .get(&record.runtime.id)
            .cloned()
            .ok_or(Error::Conflict)?;
        let Some(dispatch) = &entry.memory else {
            return Ok(false);
        };
        if self.journal.is_none() {
            return Err(Error::Conflict);
        }
        dispatch.validate(&record.runtime.id, entry.backend.as_ref(), None)?;
        let grant = &dispatch.authorization;
        let request = &grant.request;
        let source = &request.source;
        if source.identity.assignment != record.assignment
            || source.identity.runtime_id != record.runtime.id
            || source.memory_high_bytes != high(record, source.memory_limit_bytes)?
            || request.target_high_bytes != high(record, request.target_limit_bytes)?
        {
            return Err(Error::Conflict);
        }
        if operations::now_millis()? < grant.deadline_unix_millis {
            return Ok(false);
        }
        // This authenticated call verifies the original durable request
        // fingerprint. It never dispatches the old resource change again.
        let response = self
            .client
            .clone()
            .resolve_resource_operation(self.request(resolution_request(grant)?))
            .await
            .map_err(unavailable)?
            .into_inner();
        let actual = response.actual.ok_or(Error::Conflict)?;
        let Some(partial) = partial_limits(record, grant, &actual)? else {
            return Ok(false);
        };
        if response.complete {
            return Err(Error::Conflict);
        }
        let fresh = self
            .client
            .clone()
            .stats(self.request(proto::StatsRequest {
                id: source.identity.sandbox_id.clone(),
            }))
            .await
            .map_err(unavailable)?
            .into_inner();
        if partial_limits(record, grant, &fresh)? != Some(partial) {
            return Err(Error::Conflict);
        }
        Ok(true)
    }
}
