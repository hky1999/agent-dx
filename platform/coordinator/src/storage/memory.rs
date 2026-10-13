//! Dynamic memory is accounted by the same host CAS as creation and recovery.
use super::*;
use adx_core::memory::{MemoryAuthorization, MemoryObservation, MemoryRequest};
use adx_core::Resources;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredMemoryOperation {
    pub authorization: MemoryAuthorization,
    pub request_sha256: String,
}

/// The last settled physical state and every operation belonging to that runtime.
/// Completed requests remain replayable until physical retirement clears the account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredMemory {
    pub confirmed: MemoryObservation,
    pub pending: Option<StoredMemoryOperation>,
    pub completed: BTreeMap<String, StoredMemoryOperation>,
}

fn fingerprint(request: &MemoryRequest) -> Result<String> {
    Ok(format!("{:x}", Sha256::digest(encode(request)?.as_bytes())))
}

fn check_observation(
    environment: &StoredEnvironment,
    observation: &MemoryObservation,
) -> Result<()> {
    observation.validate()?;
    let result = environment.result.as_ref().ok_or(Error::Conflict)?;
    let ceiling = environment.spec.execution_resources();
    if observation.identity.assignment != environment.assignment
        || observation.identity.runtime_id != result.runtime.id
        || !result.resources_held
        || observation.memory_limit_bytes < environment.spec.resources.memory_bytes
        || observation.memory_limit_bytes > ceiling.memory_bytes
        || u128::from(observation.cpu_quota_usec) * 1000
            != u128::from(ceiling.cpu_millis) * u128::from(observation.cpu_period_usec)
    {
        return Err(Error::Conflict);
    }
    Ok(())
}

impl StoredMemoryOperation {
    fn validate(&self, environment: &StoredEnvironment) -> Result<()> {
        let grant = &self.authorization;
        grant.validate()?;
        check_observation(environment, &grant.request.source)?;
        check_observation(environment, &grant.request.completion()?)?;
        if self.request_sha256 != fingerprint(&grant.request)? {
            return Err(Error::Conflict);
        }
        Ok(())
    }
}

impl StoredMemory {
    pub(super) fn validate(&self, environment: &StoredEnvironment) -> Result<()> {
        check_observation(environment, &self.confirmed)?;
        if environment.execution.as_ref().is_some_and(|e| e.pending)
            || environment.recovery.as_ref().is_some_and(|r| r.pending)
        {
            return Err(Error::Conflict);
        }
        if let Some(operation) = &self.pending {
            operation.validate(environment)?;
            if operation.authorization.request.source != self.confirmed
                || self
                    .completed
                    .contains_key(&operation.authorization.request.operation_id)
            {
                return Err(Error::Conflict);
            }
        }
        for (id, operation) in &self.completed {
            operation.validate(environment)?;
            let completion = operation.authorization.request.completion()?;
            if id != &operation.authorization.request.operation_id
                || completion.identity != self.confirmed.identity
                || completion.resource_revision > self.confirmed.resource_revision
                || (completion.resource_revision == self.confirmed.resource_revision
                    && completion != self.confirmed)
            {
                return Err(Error::Conflict);
            }
        }
        Ok(())
    }

    fn held_bytes(&self) -> u64 {
        self.confirmed.memory_limit_bytes.max(
            self.pending
                .as_ref()
                .map_or(0, |op| op.authorization.request.target_limit_bytes),
        )
    }
}

impl StoredEnvironment {
    /// Full static execution allowance until a complete physical memory account
    /// exists. Pending growth holds its target; pending shrink holds its source.
    pub(super) fn held_resources(&self) -> Resources {
        let mut resources = self.spec.execution_resources();
        if let Some(memory) = &self.memory {
            resources.memory_bytes = memory.held_bytes();
        }
        resources
    }
}

fn check_session(
    raw: &BTreeMap<String, String>,
    assignment: &Assignment,
    session: &str,
    admitting: bool,
) -> Result<()> {
    let node: StoredNode = decode(
        raw.get(&format!("node:{}", assignment.node_id))
            .ok_or(Error::NotFound)?,
    )?;
    if node.node.id != assignment.node_id
        || node.shard_id != assignment.shard_id
        || node
            .session
            .as_ref()
            .is_none_or(|s| s.id != session || !s.routable)
        || !node.node.available
        || (admitting && node.scheduling_paused)
    {
        return Err(Error::Conflict);
    }
    Ok(())
}

fn check_live(environment: &StoredEnvironment) -> Result<()> {
    environment.validate()?;
    if environment.invalidated
        || environment
            .result
            .as_ref()
            .is_none_or(|r| r.state != EnvironmentState::Running || !r.resources_held)
        || environment.execution.as_ref().is_some_and(|e| e.pending)
        || environment.recovery.as_ref().is_some_and(|r| r.pending)
    {
        return Err(Error::Conflict);
    }
    Ok(())
}

impl Session {
    /// Install the initial verified physical state without releasing any static
    /// reservation. Requires configured host capacity and a current live session.
    /// Partial, smaller, conflicting or retired observations return Conflict.
    pub async fn initialize_memory(
        &self,
        observation: MemoryObservation,
        node_session_id: &str,
    ) -> Result<()> {
        observation.validate()?;
        let field = format!(
            "environment:{}",
            observation.identity.assignment.environment_id
        );
        let node_field = format!("node:{}", observation.identity.assignment.node_id);
        for _ in 0..ATTEMPTS {
            let raw = self
                .store
                .allocation_view(&[HEADER.into(), field.clone(), node_field.clone()])
                .await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            if header.host_budgets.is_none() {
                return Err(Error::Conflict);
            }
            check_session(
                &raw,
                &observation.identity.assignment,
                node_session_id,
                true,
            )?;
            let mut environment: StoredEnvironment =
                decode(raw.get(&field).ok_or(Error::NotFound)?)?;
            check_live(&environment)?;
            check_observation(&environment, &observation)?;
            if let Some(account) = &environment.memory {
                return if account.confirmed == observation {
                    Ok(())
                } else {
                    Err(Error::Conflict)
                };
            }
            if observation.memory_limit_bytes != environment.spec.execution_resources().memory_bytes
                || observation.resource_revision != 1
                || !observation.operation_id.is_empty()
            {
                return Err(Error::Conflict);
            }
            environment.memory = Some(StoredMemory {
                confirmed: observation.clone(),
                pending: None,
                completed: BTreeMap::new(),
            });
            environment.validate()?;
            budget::check_holds(&raw, &header, Some(&environment))?;
            // Older writers must reject dynamic holds instead of dropping them.
            header.schema = header.schema.max(4);
            header.advance()?;
            if self
                .store
                .cas(
                    raw.get(HEADER).ok_or(Error::Conflict)?,
                    &header,
                    Some((&field, encode(&environment)?)),
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::Unavailable(
            "concurrent memory initialization; retry".into(),
        ))
    }

    /// Persist intent and reserve growth atomically with all host allocations.
    /// Exact replay returns the original grant, including an expired or old-epoch
    /// grant for reconciliation. Only check_dispatch permits physical dispatch.
    /// Conflicting identity/state fails; insufficient capacity returns NoCapacity.
    pub async fn authorize_memory(
        &self,
        request: MemoryRequest,
        node_session_id: &str,
        now_unix_millis: u64,
    ) -> Result<MemoryAuthorization> {
        request.completion()?;
        let field = format!(
            "environment:{}",
            request.source.identity.assignment.environment_id
        );
        let node_field = format!("node:{}", request.source.identity.assignment.node_id);
        for _ in 0..ATTEMPTS {
            let raw = self
                .store
                .allocation_view(&[HEADER.into(), field.clone(), node_field.clone()])
                .await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            if header.host_budgets.is_none() || header.schema < 4 {
                return Err(Error::Conflict);
            }
            check_session(
                &raw,
                &request.source.identity.assignment,
                node_session_id,
                true,
            )?;
            let mut environment: StoredEnvironment =
                decode(raw.get(&field).ok_or(Error::NotFound)?)?;
            check_live(&environment)?;
            let account = environment.memory.as_ref().ok_or(Error::Conflict)?;
            let existing = account
                .pending
                .as_ref()
                .filter(|op| op.authorization.request.operation_id == request.operation_id)
                .or_else(|| account.completed.get(&request.operation_id));
            if let Some(existing) = existing {
                let grant = &existing.authorization;
                return if grant.request == request && grant.node_session_id == node_session_id {
                    Ok(grant.clone())
                } else {
                    Err(Error::Conflict)
                };
            }
            if account.pending.is_some() || account.confirmed != request.source {
                return Err(Error::Conflict);
            }
            check_observation(&environment, &request.completion()?)?;
            let grant = MemoryAuthorization {
                request: request.clone(),
                node_session_id: node_session_id.into(),
                coordinator_epoch: self.epoch(),
                issued_unix_millis: now_unix_millis,
                deadline_unix_millis: now_unix_millis
                    .checked_add(request.timeout_millis)
                    .ok_or(Error::Conflict)?,
            };
            grant.validate()?;
            environment.memory.as_mut().ok_or(Error::Conflict)?.pending =
                Some(StoredMemoryOperation {
                    authorization: grant.clone(),
                    request_sha256: fingerprint(&request)?,
                });
            environment.validate()?;
            budget::check_admission(&raw, &header, &environment)?;
            header.advance()?;
            if self
                .store
                .cas(
                    raw.get(HEADER).ok_or(Error::Conflict)?,
                    &header,
                    Some((&field, encode(&environment)?)),
                )
                .await?
            {
                return Ok(grant);
            }
        }
        Err(Error::Unavailable(
            "concurrent memory authorization; retry".into(),
        ))
    }

    /// Settle only the exact authorized physical outcome after independent thaw.
    /// Unknown, partial, frozen and foreign outcomes retain the entire reservation.
    /// A restarted Coordinator may settle an old grant but may not dispatch it.
    pub async fn complete_memory(
        &self,
        grant: MemoryAuthorization,
        observation: MemoryObservation,
        node_session_id: &str,
    ) -> Result<()> {
        grant.validate()?;
        observation.validate()?;
        if grant.node_session_id != node_session_id || observation != grant.request.completion()? {
            return Err(Error::Conflict);
        }
        let field = format!(
            "environment:{}",
            observation.identity.assignment.environment_id
        );
        let node_field = format!("node:{}", observation.identity.assignment.node_id);
        for _ in 0..ATTEMPTS {
            let raw = self
                .store
                .allocation_view(&[HEADER.into(), field.clone(), node_field.clone()])
                .await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            if header.host_budgets.is_none() || header.schema < 4 {
                return Err(Error::Conflict);
            }
            check_session(
                &raw,
                &observation.identity.assignment,
                node_session_id,
                false,
            )?;
            let mut environment: StoredEnvironment =
                decode(raw.get(&field).ok_or(Error::NotFound)?)?;
            check_live(&environment)?;
            let account = environment.memory.as_mut().ok_or(Error::Conflict)?;
            if let Some(done) = account.completed.get(&grant.request.operation_id) {
                return if done.authorization == grant {
                    Ok(())
                } else {
                    Err(Error::Conflict)
                };
            }
            let pending = account.pending.as_ref().ok_or(Error::Conflict)?;
            if pending.authorization != grant {
                return Err(Error::Conflict);
            }
            let pending = account.pending.take().ok_or(Error::Conflict)?;
            account.confirmed = observation.clone();
            account
                .completed
                .insert(grant.request.operation_id.clone(), pending);
            environment.validate()?;
            budget::check_holds(&raw, &header, Some(&environment))?;
            header.advance()?;
            if self
                .store
                .cas(
                    raw.get(HEADER).ok_or(Error::Conflict)?,
                    &header,
                    Some((&field, encode(&environment)?)),
                )
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::Unavailable(
            "concurrent memory completion; retry".into(),
        ))
    }
}

/// Physical retirement already has its own lifecycle proof. Preserve all memory
/// holds until that existing path confirms the same Runtime and Assignment gone.
pub(super) fn retire(
    environment: &mut StoredEnvironment,
    result: &EnvironmentRecord,
) -> Result<()> {
    if let Some(account) = &environment.memory {
        if account.confirmed.identity.assignment != result.assignment
            || account.confirmed.identity.runtime_id != result.runtime.id
        {
            return Err(Error::Conflict);
        }
        if !result.resources_held {
            environment.memory = None;
        }
    }
    Ok(())
}
