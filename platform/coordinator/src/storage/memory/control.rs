//! The Coordinator owns policy, reservation and settlement in one host-budget CAS.
use super::*;
use adx_core::memory::{MemoryDecision, MemorySample};

/// Explicit operator opt-in. Missing configuration permits observation and
/// settlement but never creates a new automatic operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MemoryControlPolicy {
    pub grow_per_mille: u16,
    pub shrink_per_mille: u16,
    pub target_per_mille: u16,
    pub cooldown_millis: u64,
    pub low_usage_millis: u64,
    pub low_usage_samples: u32,
    pub max_sample_age_millis: u64,
    pub operation_timeout_millis: u64,
}
impl Default for MemoryControlPolicy {
    fn default() -> Self {
        Self {
            grow_per_mille: 650,
            shrink_per_mille: 300,
            target_per_mille: 500,
            cooldown_millis: 5000,
            low_usage_millis: 30000,
            low_usage_samples: 5,
            max_sample_age_millis: 10000,
            operation_timeout_millis: 10000,
        }
    }
}
impl MemoryControlPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.shrink_per_mille == 0
            || self.shrink_per_mille >= self.target_per_mille
            || self.target_per_mille >= self.grow_per_mille
            || self.grow_per_mille >= 1000
            || self.cooldown_millis < 1000
            || self.low_usage_millis < self.cooldown_millis
            || self.low_usage_samples < 2
            || !(1000..=60000).contains(&self.max_sample_age_millis)
            || !(1..=60000).contains(&self.operation_timeout_millis)
        {
            return Err(Error::Invalid(
                "invalid automatic memory thresholds, observation window or deadline".into(),
            ));
        }
        Ok(())
    }
    fn target(
        &self,
        env: &StoredEnvironment,
        sample: &MemorySample,
        now: u64,
    ) -> Result<Option<u64>> {
        let account = env.memory.as_ref().ok_or(Error::Conflict)?;
        if account.pending.is_some()
            || account.completed.values().any(|op| {
                now.saturating_sub(op.authorization.issued_unix_millis) < self.cooldown_millis
            })
        {
            return Ok(None);
        }
        let max = sample.observation.memory_limit_bytes;
        let usage = u128::from(sample.usage_bytes) * 1000;
        let growing = usage >= u128::from(max) * u128::from(self.grow_per_mille);
        let shrinking = usage < u128::from(max) * u128::from(self.shrink_per_mille)
            && account.window.low_samples >= self.low_usage_samples
            && account
                .window
                .low_since_unix_millis
                .is_some_and(|start| now.saturating_sub(start) >= self.low_usage_millis);
        if !growing && !shrinking {
            return Ok(None);
        }
        const MIB: u64 = 1 << 20;
        let desired = usage
            .div_ceil(u128::from(self.target_per_mille))
            .div_ceil(u128::from(MIB))
            * u128::from(MIB);
        let desired = u64::try_from(desired).unwrap_or(u64::MAX).clamp(
            env.spec.resources.memory_bytes,
            env.spec.execution_ceiling().memory_bytes,
        );
        let desired = if growing {
            desired.min(max.saturating_mul(2))
        } else {
            desired
        };
        Ok(((growing && desired > max) || (shrinking && desired < max)).then_some(desired))
    }
}

impl Session {
    /// Authenticate the owner at the RPC boundary before calling. Every write is
    /// fenced by the current node session and Coordinator epoch. Pending and
    /// expired operations retain max(source, target); only exact readback settles.
    pub async fn reconcile_memory(
        &self,
        sample: MemorySample,
        node_session_id: &str,
        now: u64,
        policy: Option<&MemoryControlPolicy>,
    ) -> Result<MemoryDecision> {
        sample.observation.validate()?;
        if let Some(policy) = policy {
            policy.validate()?;
        }
        let freshness = policy.map_or(10000, |p| p.max_sample_age_millis);
        if sample.observed_unix_millis > now || now - sample.observed_unix_millis > freshness {
            return Err(Error::Conflict);
        }
        let observation = &sample.observation;
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
                false,
            )?;
            let node: StoredNode = decode(raw.get(&node_field).ok_or(Error::NotFound)?)?;
            if node.node.memory_control_version != 1 {
                return Err(Error::Conflict);
            }
            let mut env: StoredEnvironment = decode(raw.get(&field).ok_or(Error::NotFound)?)?;
            check_live(&env)?;
            check_observation(&env, observation)?;
            let high_per_mille = env
                .spec
                .sandbox
                .elastic_memory
                .as_ref()
                .ok_or(Error::Conflict)?
                .high_per_mille;
            let high = |max: u64| -> Result<u64> {
                let bytes =
                    (u128::from(max) * u128::from(high_per_mille)).div_ceil(1000) / 4096 * 4096;
                u64::try_from(bytes).map_err(|_| Error::Conflict)
            };
            if observation.memory_high_bytes != high(observation.memory_limit_bytes)? {
                return Err(Error::Conflict);
            }
            if env.memory.is_none() {
                let recovered = env.execution.is_some() || env.recovery.is_some();
                if (!recovered
                    && observation.memory_limit_bytes
                        != env.spec.execution_resources().memory_bytes)
                    || observation.memory_limit_bytes > env.held_resources().memory_bytes
                    || observation.resource_revision != 1
                    || !observation.operation_id.is_empty()
                {
                    return Err(Error::Conflict);
                }
                env.memory = Some(StoredMemory {
                    confirmed: observation.clone(),
                    pending: None,
                    completed: BTreeMap::new(),
                    window: MemoryWindow::default(),
                });
            }
            let account = env.memory.as_mut().ok_or(Error::Conflict)?;
            if let Some(op) = &account.pending {
                if *observation == op.authorization.request.completion()? {
                    let op = account.pending.take().ok_or(Error::Conflict)?;
                    account
                        .completed
                        .insert(op.authorization.request.operation_id.clone(), op);
                    account.confirmed = observation.clone();
                    account.window = MemoryWindow::default();
                } else if *observation != account.confirmed {
                    return Err(Error::Conflict);
                }
            } else if *observation != account.confirmed {
                return Err(Error::Conflict);
            }
            let new_sample = sample.observed_unix_millis > account.window.last_unix_millis;
            if new_sample {
                if let Some(p) = policy {
                    let low = u128::from(sample.usage_bytes) * 1000
                        < u128::from(observation.memory_limit_bytes)
                            * u128::from(p.shrink_per_mille);
                    let contiguous = sample
                        .observed_unix_millis
                        .saturating_sub(account.window.last_unix_millis)
                        <= freshness;
                    if low && contiguous && account.window.low_since_unix_millis.is_some() {
                        if sample
                            .observed_unix_millis
                            .saturating_sub(account.window.last_counted_unix_millis)
                            >= 1000
                        {
                            account.window.low_samples =
                                account.window.low_samples.saturating_add(1);
                            account.window.last_counted_unix_millis = sample.observed_unix_millis;
                        }
                    } else if low {
                        account.window.low_since_unix_millis = Some(sample.observed_unix_millis);
                        account.window.low_samples = 1;
                        account.window.last_counted_unix_millis = sample.observed_unix_millis;
                    } else {
                        account.window.low_since_unix_millis = None;
                        account.window.low_samples = 0;
                    }
                } else {
                    account.window.low_since_unix_millis = None;
                    account.window.low_samples = 0;
                }
                account.window.last_unix_millis = sample.observed_unix_millis;
            }
            if new_sample && !node.scheduling_paused {
                if let Some(p) = policy {
                    if let Some(target) = p.target(&env, &sample, now)? {
                        let request = MemoryRequest {
                            source: observation.clone(),
                            target_limit_bytes: target,
                            target_high_bytes: high(target)?,
                            operation_id: format!(
                                "memory-{}-{}-{:x}",
                                self.epoch(),
                                observation.resource_revision,
                                Sha256::digest(encode(observation)?.as_bytes())
                            ),
                            timeout_millis: p.operation_timeout_millis,
                        };
                        let grant = MemoryAuthorization {
                            request,
                            node_session_id: node_session_id.into(),
                            coordinator_epoch: self.epoch(),
                            issued_unix_millis: now,
                            deadline_unix_millis: now
                                .checked_add(p.operation_timeout_millis)
                                .ok_or(Error::Conflict)?,
                        };
                        grant.validate()?;
                        let operation = StoredMemoryOperation {
                            request_sha256: fingerprint(&grant.request)?,
                            authorization: grant,
                        };
                        env.memory.as_mut().ok_or(Error::Conflict)?.pending = Some(operation);
                        match budget::check_admission(&raw, &header, &env) {
                            Ok(()) => {
                                let window =
                                    &mut env.memory.as_mut().ok_or(Error::Conflict)?.window;
                                window.low_since_unix_millis = None;
                                window.low_samples = 0;
                            }
                            Err(Error::NoCapacity) => {
                                env.memory.as_mut().ok_or(Error::Conflict)?.pending = None
                            }
                            Err(error) => return Err(error),
                        }
                    }
                }
            }
            env.validate()?;
            budget::check_holds(&raw, &header, Some(&env))?;
            let decision = MemoryDecision {
                observed: observation.clone(),
                held_resources: env.held_resources(),
                authorization: env
                    .memory
                    .as_ref()
                    .and_then(|m| m.pending.as_ref())
                    .map(|op| op.authorization.clone()),
                node_session_id: node_session_id.into(),
                coordinator_epoch: self.epoch(),
            };
            decision.validate()?;
            header.schema = header.schema.max(5);
            header.advance()?;
            if self
                .store
                .cas(
                    raw.get(HEADER).ok_or(Error::Conflict)?,
                    &header,
                    Some((&field, encode(&env)?)),
                )
                .await?
            {
                return Ok(decision);
            }
        }
        Err(Error::Unavailable(
            "concurrent memory observation; retry".into(),
        ))
    }
}
