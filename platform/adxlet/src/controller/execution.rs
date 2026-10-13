//! Replacement execution requires a published source and a durable center reservation.
use super::*;
use adx_core::execution::{ExecutionAuthorization, ExecutionKind, ExecutionRequest};

impl Controller {
    pub(super) async fn prepare_execution(
        &mut self,
        kind: ExecutionKind,
        operation_id: String,
        expected_revision: u64,
    ) -> Result<ExecutionAuthorization> {
        if self
            .services
            .execution_reconciliation_required
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Unavailable(
                "execution authorization requires reconciliation".into(),
            ));
        }
        if self.held || self.record.resources_held {
            return Err(Error::Conflict);
        }
        if self.sync().await?.durability != Durability::Published {
            return Err(Error::Unavailable(
                "execution source is not published".into(),
            ));
        }
        let request = ExecutionRequest {
            source: self.record.clone(),
            kind,
            operation_id,
            expected_revision,
            timeout_millis: u64::try_from(self.services.operation_timeout.as_millis())
                .map_err(|_| Error::Conflict)?,
        };
        let target = request.target()?;
        // Check local availability before reserving centrally. If the reply is
        // unknown, no physical work has occurred here; the center retains its hold.
        self.services
            .admission
            .lock()
            .expect("shared state lock poisoned")
            .reserve(
                &target.runtime.id,
                &self.record.spec,
                &self.record.assignment,
            )?;
        let granted = timeout(self.services.operation_timeout, async {
            self.services.runtime.prepare_execution(&target).await?;
            self.services.sink.authorize_execution(&request).await
        })
        .await
        .unwrap_or_else(|_| {
            Err(Error::Unavailable(
                "execution authorization timed out".into(),
            ))
        })
        .and_then(|grant| {
            grant.validate()?;
            if grant.request != request || grant.target != target {
                return Err(Error::Conflict);
            }
            Ok(grant)
        });
        let grant = match granted {
            Ok(grant) => grant,
            Err(error) => {
                // NoCapacity is a definitive admission rejection. Other failures may
                // hide a durable reservation or invalidate the local source identity.
                if error != Error::NoCapacity {
                    self.services
                        .execution_reconciliation_required
                        .store(true, std::sync::atomic::Ordering::Release);
                }
                self.services
                    .admission
                    .lock()
                    .expect("shared state lock poisoned")
                    .release(&target.runtime.id)?;
                return Err(error);
            }
        };
        self.record = grant.target.clone();
        self.held = true;
        self.durability = None;
        self.idle = None;
        self.health_failures = 0;
        Ok(grant)
    }
}
