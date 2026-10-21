//! Dynamic memory is accounted by the same host CAS as creation and recovery.
use super::*;
mod control;
pub use control::MemoryControlPolicy;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MemoryWindow {
    last_unix_millis: u64,
    low_since_unix_millis: Option<u64>,
    low_samples: u32,
    #[serde(default)]
    last_counted_unix_millis: u64,
}

use adx_core::memory::{MemoryAuthorization, MemoryObservation, MemoryRequest};
use adx_core::Resources;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredMemoryOperation {
    pub authorization: MemoryAuthorization,
    pub request_sha256: String,
    /// A closed daemon operation that advanced revision without changing limits.
    #[serde(default)]
    pub unchanged: bool,
}

/// The last settled physical state, pending grant, bounded replay window and
/// durable closure floor. A forgotten ID cannot bind a new physical source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredMemory {
    #[serde(default)]
    pub(crate) window: MemoryWindow,
    pub confirmed: MemoryObservation,
    pub pending: Option<StoredMemoryOperation>,
    pub completed: BTreeMap<String, StoredMemoryOperation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_source_revision: Option<u64>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub latest_completed_issued_unix_millis: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
}
fn canonical_operation_id(epoch: u64, source: &MemoryObservation) -> Result<String> {
    Ok(format!(
        "memory-{}-{}-{:x}",
        epoch,
        source.resource_revision,
        Sha256::digest(encode(source)?.as_bytes())
    ))
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
    let ceiling = environment.spec.execution_ceiling();
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
    fn compact_history(&mut self) -> Result<()> {
        const COMPLETED_HISTORY_LIMIT: usize = 64;
        if self.closed_source_revision.is_some() || self.completed.len() > COMPLETED_HISTORY_LIMIT {
            self.latest_completed_issued_unix_millis =
                self.latest_completed_issued_unix_millis.max(
                    self.completed
                        .values()
                        .map(|op| op.authorization.issued_unix_millis)
                        .max()
                        .unwrap_or(0),
                );
        }
        while self.completed.len() > COMPLETED_HISTORY_LIMIT {
            let (id, source) = self
                .completed
                .iter()
                .min_by_key(|(id, op)| (op.authorization.request.source.resource_revision, *id))
                .map(|(id, op)| {
                    (
                        id.clone(),
                        op.authorization.request.source.resource_revision,
                    )
                })
                .ok_or(Error::Conflict)?;
            self.completed.remove(&id).ok_or(Error::Conflict)?;
            self.closed_source_revision = Some(
                self.closed_source_revision
                    .map_or(source, |old| old.max(source)),
            );
        }
        Ok(())
    }
    pub(super) fn validate(&self, environment: &StoredEnvironment) -> Result<()> {
        check_observation(environment, &self.confirmed)?;
        if self.closed_source_revision.is_some_and(|floor| {
            floor >= self.confirmed.resource_revision
                || self.completed.len() > 64
                || self.latest_completed_issued_unix_millis == 0
        }) {
            return Err(Error::Conflict);
        }
        if environment.execution.as_ref().is_some_and(|e| e.pending)
            || environment.recovery.as_ref().is_some_and(|r| r.pending)
        {
            return Err(Error::Conflict);
        }
        if let Some(operation) = &self.pending {
            operation.validate(environment)?;
            if operation.unchanged
                || operation.authorization.request.source != self.confirmed
                || self
                    .completed
                    .contains_key(&operation.authorization.request.operation_id)
            {
                return Err(Error::Conflict);
            }
        }
        for (id, operation) in &self.completed {
            operation.validate(environment)?;
            let completion = if operation.unchanged {
                operation.authorization.request.unchanged()?
            } else {
                operation.authorization.request.completion()?
            };
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
    /// Birth allowance for new creation; recovery keeps the permitted ceiling
    /// until complete physical readback. Unknown growth holds its full target.
    pub(crate) fn held_resources(&self) -> Resources {
        let mut resources = if self.execution.is_some() || self.recovery.is_some() {
            self.spec.execution_ceiling()
        } else {
            self.spec.execution_resources()
        };
        if let Some(memory) = &self.memory {
            resources.memory_bytes = memory.held_bytes();
        }
        resources
    }
}

// Availability gates new admission; settlement still requires the current routable owner.
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
        || (admitting && (!node.node.available || node.scheduling_paused))
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
    /// Install a complete birth observation under a current live session.
    /// First creation must match its initial allowance. A completed elastic
    /// recovery may settle its unused ceiling reservation from exact readback.
    /// Partial, conflicting or retired observations return Conflict.
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
            let recovered_elastic = environment.spec.sandbox.elastic_memory.is_some()
                && (environment.execution.is_some() || environment.recovery.is_some());
            if (!recovered_elastic
                && observation.memory_limit_bytes
                    != environment.spec.execution_resources().memory_bytes)
                || observation.memory_limit_bytes > environment.held_resources().memory_bytes
                || observation.resource_revision != 1
                || !observation.operation_id.is_empty()
            {
                return Err(Error::Conflict);
            }
            environment.memory = Some(StoredMemory {
                window: MemoryWindow::default(),
                confirmed: observation.clone(),
                pending: None,
                completed: BTreeMap::new(),
                closed_source_revision: None,
                latest_completed_issued_unix_millis: 0,
            });
            environment.validate()?;
            budget::check_holds(&raw, &header, Some(&environment))?;
            // Older writers must reject dynamic holds instead of dropping them.
            header.schema = header.schema.max(6);
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
            if let Some(floor) = account.closed_source_revision {
                if request.source.resource_revision <= floor
                    || request.operation_id
                        != canonical_operation_id(self.epoch(), &request.source)?
                {
                    return Err(Error::Conflict);
                }
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
                    unchanged: false,
                });
            environment.validate()?;
            budget::check_admission(&raw, &header, &environment)?;
            header.schema = header.schema.max(6);
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
        if observation != grant.request.completion()? {
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
                return if done.authorization == grant && !done.unchanged {
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
            account.compact_history()?;
            if account.closed_source_revision.is_some() {
                header.schema = header.schema.max(8)
            }
            environment.validate()?;
            budget::check_holds(&raw, &header, Some(&environment))?;
            header.schema = header.schema.max(6);
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
