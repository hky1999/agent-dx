//! Trusted physical-host capacity, persisted with the control CAS header.
use super::*;
use adx_core::Resources;
use std::collections::BTreeSet;

/// Operator-owned capacity for one physical host. Node reports cannot change it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostBudget {
    pub capacity: Resources,
    pub nodes: BTreeSet<String>,
}

/// Durable scalar capacity and holds. Node liveness does not change these values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostBudgetStatus {
    pub capacity: Resources,
    pub held: Resources,
    pub available: Resources,
    pub nodes: BTreeSet<String>,
    pub memory_accounts: HostMemoryAccounts,
}

pub(super) fn validate_budgets(budgets: &BTreeMap<String, HostBudget>) -> Result<()> {
    if budgets.is_empty() {
        return Err(Error::Invalid("host budgets must not be empty".into()));
    }
    let mut nodes = BTreeSet::new();
    for (host, budget) in budgets {
        budget.capacity.validate()?;
        if host.trim().is_empty() || host.trim() != host || budget.nodes.is_empty() {
            return Err(Error::Invalid(
                "host identity and member nodes required".into(),
            ));
        }
        for node in &budget.nodes {
            if node.trim().is_empty() || node.trim() != node || !nodes.insert(node) {
                return Err(Error::Invalid(
                    "each node must belong to exactly one host".into(),
                ));
            }
        }
    }
    Ok(())
}

fn statuses(
    raw: &BTreeMap<String, String>,
    budgets: &BTreeMap<String, HostBudget>,
    replacement: Option<&StoredEnvironment>,
) -> Result<BTreeMap<String, HostBudgetStatus>> {
    validate_budgets(budgets)?;
    let mut out: BTreeMap<_, _> = budgets
        .iter()
        .map(|(id, budget)| {
            (
                id.clone(),
                HostBudgetStatus {
                    capacity: budget.capacity,
                    held: Resources::default(),
                    available: budget.capacity,
                    nodes: budget.nodes.clone(),
                    memory_accounts: HostMemoryAccounts::default(),
                },
            )
        })
        .collect();
    let mut add = |record: &StoredEnvironment| -> Result<()> {
        if !record.resources_held() {
            return Ok(());
        }
        let status = out
            .values_mut()
            .find(|status| status.nodes.contains(&record.assignment.node_id))
            .ok_or_else(|| Error::Invalid("held execution has no trusted host budget".into()))?;
        let amount = record.held_resources();
        status.memory_accounts.add(record, amount.memory_bytes)?;
        let sum = |a: u64, b: u64| a.checked_add(b).ok_or(Error::NoCapacity);
        status.held = Resources {
            cpu_millis: sum(status.held.cpu_millis, amount.cpu_millis)?,
            memory_bytes: sum(status.held.memory_bytes, amount.memory_bytes)?,
            disk_bytes: sum(status.held.disk_bytes, amount.disk_bytes)?,
        };
        if !status.held.fits(&status.capacity) {
            return Err(Error::NoCapacity);
        }
        status.available = status.capacity.saturating_sub(status.held);
        Ok(())
    };
    for (field, value) in raw {
        if let Some(id) = field.strip_prefix("environment:") {
            if replacement.is_some_and(|record| record.spec.id == id) {
                continue;
            }
            let record: StoredEnvironment = decode(value)?;
            record.validate()?;
            if record.spec.id != id {
                return Err(Error::Conflict);
            }
            add(&record)?;
        }
    }
    if let Some(record) = replacement {
        add(record)?;
    }
    Ok(out)
}

impl Session {
    /// Install a trusted host budget once. Replays must match exactly.
    /// Existing holds must fit; changing or removing a published budget requires
    /// a separate, explicit migration rather than a Coordinator restart.
    pub async fn configure_host_budgets(
        &self,
        budgets: BTreeMap<String, HostBudget>,
    ) -> Result<()> {
        validate_budgets(&budgets)?;
        self.store.require_durable_budget();
        for _ in 0..ATTEMPTS {
            let raw = self.store.raw().await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            if let Some(existing) = &header.host_budgets {
                return if existing == &budgets {
                    Ok(())
                } else {
                    Err(Error::Conflict)
                };
            }
            statuses(&raw, &budgets, None)?;
            // Older writers must reject the durable-acknowledgement contract.
            header.schema = header.schema.max(7);
            header.host_budgets = Some(budgets.clone());
            header.advance()?;
            if self
                .store
                .cas(raw.get(HEADER).ok_or(Error::Conflict)?, &header, None)
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::Unavailable(
            "concurrent host budget configuration; retry".into(),
        ))
    }

    /// Read capacity and all durable holds from one Redis snapshot.
    /// An empty map means this namespace has no configured host budget.
    pub async fn host_budgets(&self) -> Result<BTreeMap<String, HostBudgetStatus>> {
        let raw = self.store.raw().await?;
        let header = self.header(&raw.get(HEADER).cloned())?;
        match &header.host_budgets {
            Some(budgets) => statuses(&raw, budgets, None),
            None => Ok(BTreeMap::new()),
        }
    }
}

/// Capacity calculation and the caller's write share an opaque header CAS.
/// Do not read a second snapshot here: it would permit competing reservations.
pub(super) fn check_holds(
    raw: &BTreeMap<String, String>,
    header: &Header,
    replacement: Option<&StoredEnvironment>,
) -> Result<()> {
    if let Some(budgets) = &header.host_budgets {
        statuses(raw, budgets, replacement)?;
    }
    Ok(())
}

pub(super) fn check_admission(
    raw: &BTreeMap<String, String>,
    header: &Header,
    record: &StoredEnvironment,
) -> Result<()> {
    if record.spec.sandbox.elastic_memory.is_some() && header.host_budgets.is_none() {
        return Err(Error::Conflict);
    }
    if header.host_budgets.is_some() {
        let node: StoredNode = decode(
            raw.get(&format!("node:{}", record.assignment.node_id))
                .ok_or(Error::NotFound)?,
        )?;
        if record.spec.sandbox.elastic_memory.is_some() && node.node.memory_control_version != 1 {
            return Err(Error::Conflict);
        }
        if !node.node.available
            || node
                .session
                .as_ref()
                .is_none_or(|session| !session.routable)
        {
            return Err(Error::Unavailable(
                "host budget target is not accepting allocations".into(),
            ));
        }
    }
    check_pressure(header, &record.assignment.node_id)?;
    check_holds(raw, header, Some(record))
}

/// Mutually exclusive memory partitions sum to held memory. Diagnostic overlap
/// views (unknown and restore) must not be added to those partitions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostMemoryAccounts {
    pub birth_bytes: u64,
    pub settled_extra_bytes: u64,
    pub in_flight_extra_bytes: u64,
    pub competing_ungranted_bytes: u64,
    pub unknown_result_bytes: u64,
    pub restore_in_flight_bytes: u64,
}
impl HostMemoryAccounts {
    fn add(&mut self, record: &StoredEnvironment, held: u64) -> Result<()> {
        let birth = record.spec.resources.memory_bytes;
        let confirmed = record
            .memory
            .as_ref()
            .map_or(birth, |m| m.confirmed.memory_limit_bytes);
        let settled = confirmed.max(birth).min(held);
        let sum = |a: &mut u64, b: u64| -> Result<()> {
            *a = a.checked_add(b).ok_or(Error::NoCapacity)?;
            Ok(())
        };
        sum(&mut self.birth_bytes, birth)?;
        sum(&mut self.settled_extra_bytes, settled.saturating_sub(birth))?;
        sum(
            &mut self.in_flight_extra_bytes,
            held.saturating_sub(settled),
        )?;
        sum(
            &mut self.competing_ungranted_bytes,
            record
                .spec
                .execution_ceiling()
                .memory_bytes
                .saturating_sub(held),
        )?;
        let restore = record.execution.as_ref().is_some_and(|e| e.pending)
            || record.recovery.as_ref().is_some_and(|r| r.pending);
        let unknown = restore
            || record
                .result
                .as_ref()
                .is_none_or(|r| r.resources_held && r.state != EnvironmentState::Running)
            || record.memory.as_ref().is_some_and(|m| m.pending.is_some());
        if unknown {
            sum(&mut self.unknown_result_bytes, held)?;
        }
        if restore {
            sum(&mut self.restore_in_flight_bytes, held)?;
        }
        Ok(())
    }
}

pub(super) fn validate_pressure_sources(
    budgets: Option<&BTreeMap<String, HostBudget>>,
    sources: &BTreeMap<String, adx_core::pressure::PressureSource>,
) -> Result<()> {
    if sources.is_empty() {
        return Ok(());
    }
    let budgets = budgets.ok_or(Error::Conflict)?;
    if budgets.keys().ne(sources.keys()) {
        return Err(Error::Conflict);
    }
    for (host, source) in sources {
        source.validate()?;
        if source.identity.host_id != *host {
            return Err(Error::Conflict);
        }
    }
    Ok(())
}

impl Session {
    /// One-way installation of the pressure contract. Restart configuration must
    /// match exactly; omitting it cannot silently disable a durable gate.
    pub async fn configure_host_pressure(
        &self,
        sources: BTreeMap<String, adx_core::pressure::PressureSource>,
    ) -> Result<()> {
        for _ in 0..ATTEMPTS {
            let raw = self.store.raw().await?;
            let mut header = self.header(&raw.get(HEADER).cloned())?;
            validate_pressure_sources(header.host_budgets.as_ref(), &sources)?;
            if header.host_pressure_admission == sources {
                return Ok(());
            }
            if !header.host_pressure_admission.is_empty() || sources.is_empty() {
                return Err(Error::Conflict);
            }
            header.host_pressure_admission = sources.clone();
            header.advance()?;
            if self
                .store
                .cas(raw.get(HEADER).ok_or(Error::Conflict)?, &header, None)
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::Unavailable(
            "concurrent pressure configuration; retry".into(),
        ))
    }
}

fn check_pressure(header: &Header, node: &str) -> Result<()> {
    if header.host_pressure_admission.is_empty() {
        return Ok(());
    }
    let (host, _) = header
        .host_budgets
        .as_ref()
        .ok_or(Error::Conflict)?
        .iter()
        .find(|(_, b)| b.nodes.contains(node))
        .ok_or(Error::Conflict)?;
    header
        .host_pressure_admission
        .get(host)
        .ok_or(Error::Conflict)?
        .check_now()
        .map_err(|_| Error::NoCapacity)?;
    Ok(())
}

pub(super) fn check_memory_admission(
    raw: &BTreeMap<String, String>,
    header: &Header,
    record: &StoredEnvironment,
) -> Result<()> {
    let grant = &record
        .memory
        .as_ref()
        .and_then(|m| m.pending.as_ref())
        .ok_or(Error::Conflict)?
        .authorization;
    if grant.request.target_limit_bytes <= grant.request.source.memory_limit_bytes {
        check_holds(raw, header, Some(record))
    } else {
        check_admission(raw, header, record)
    }
}
