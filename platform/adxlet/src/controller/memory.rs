//! Runs inside the existing per-Environment actor, serialized with lifecycle work.
use super::Controller;
use adx_core::{memory::MemoryDecision, Error, Result};

impl Controller {
    fn apply_memory_hold(&self, decision: &MemoryDecision) -> Result<()> {
        decision.validate()?;
        if decision.observed.identity.assignment != self.record.assignment
            || decision.observed.identity.runtime_id != self.record.runtime.id
        {
            return Err(Error::Conflict);
        }
        self.services
            .admission
            .lock()
            .expect("shared state lock poisoned")
            .memory_hold(&self.record, decision.held_resources)
    }
    pub(super) async fn adjust_memory(&mut self) -> Result<()> {
        if self.record.spec.sandbox.elastic_memory.is_none() {
            return Ok(());
        }
        tokio::time::timeout(self.services.operation_timeout, self.adjust_memory_inner())
            .await
            .map_err(|_| {
                Error::Unavailable("memory control timed out; reservations retained".into())
            })?
    }
    async fn adjust_memory_inner(&mut self) -> Result<()> {
        let sample = self.services.runtime.observe_memory(&self.record).await?;
        let decision = self.services.sink.reconcile_memory(&sample).await?;
        if decision.observed != sample.observation {
            return Err(Error::Conflict);
        }
        decision.validate()?;
        if decision.observed.identity.assignment != self.record.assignment
            || decision.observed.identity.runtime_id != self.record.runtime.id
        {
            return Err(Error::Conflict);
        }
        if let Some(grant) = &decision.authorization {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|_| Error::Unavailable("system clock before epoch".into()))?
                .as_millis();
            let now = u64::try_from(now)
                .map_err(|_| Error::Unavailable("system clock overflow".into()))?;
            let actual = if grant
                .check_dispatch(now, &decision.node_session_id, decision.coordinator_epoch)
                .is_ok()
            {
                self.apply_memory_hold(&decision)?;
                self.services
                    .runtime
                    .apply_memory(
                        &self.record,
                        grant,
                        &decision.node_session_id,
                        decision.coordinator_epoch,
                    )
                    .await?
            } else {
                // Resolution cannot increase physical limits. It must remain
                // possible when local growth admission is already closed.
                self.services
                    .runtime
                    .resolve_memory(&self.record, grant)
                    .await?
            };
            let settled = self.services.sink.reconcile_memory(&actual).await?;
            if settled.observed != actual.observation {
                return Err(Error::Conflict);
            }
            self.apply_memory_hold(&settled)?;
        } else {
            self.apply_memory_hold(&decision)?;
        }
        Ok(())
    }
}
