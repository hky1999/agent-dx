//! History acknowledgments cannot authorize lifecycle or budget changes.
use super::*;
use adx_core::history::{HistoryBoundary, HistoryIdentity};
use operations::HistoryAttempt;

pub(super) fn daemon_identity(value: HistoryIdentity) -> proto::HistoryIdentity {
    proto::HistoryIdentity {
        authority: value.authority,
        environment_id: value.environment_id,
        runtime_id: value.runtime_id,
        assignment_generation: value.assignment_generation,
    }
}
fn daemon_boundary(value: &HistoryBoundary) -> proto::HistoryBoundary {
    proto::HistoryBoundary {
        authority: value.authority.clone(),
        coordinator_epoch: value.coordinator_epoch,
        assignment_generation: value.assignment_generation,
        catalog_revision: value.catalog_revision,
        protected: value
            .protected
            .values()
            .map(|p| proto::HistoryProtection {
                environment_id: p.assignment.environment_id.clone(),
                assignment_generation: p.assignment.generation,
                specification_sha256: p.specification_sha256.clone(),
                minimum_runtime_revision: p.minimum_runtime_revision,
                environment_revision: p.environment_revision,
            })
            .collect(),
    }
}
impl Sandboxd {
    pub(super) fn history_identity(&self, runtime_id: &str) -> Result<Option<HistoryIdentity>> {
        let boundary = self.history_boundary.lock().map_err(unavailable)?;
        let Some(b) = boundary.as_ref() else {
            return if self.config.history_reclamation {
                Err(unavailable("history authority is not installed"))
            } else {
                Ok(None)
            };
        };
        let identity = HistoryIdentity::from_runtime(b.authority.clone(), runtime_id.into())?;
        if b.is_closed(&identity)? {
            return Err(Error::Conflict);
        };
        Ok(Some(identity))
    }
    /// Called under the manager's exclusive lifecycle gate. The durable intent
    /// survives a lost daemon reply and is retried before choosing another batch.
    pub async fn collect_history(&self, boundary: HistoryBoundary) -> Result<usize> {
        boundary.validate()?;
        let journal = self
            .journal
            .as_ref()
            .ok_or_else(|| unavailable("persistent history journal required"))?;
        let attempts = {
            let records = self.operations.lock().map_err(unavailable)?;
            let mut candidates = Vec::new();
            for (id, record) in records.iter() {
                if !record.history_terminal() {
                    continue;
                }
                let Ok(identity) =
                    HistoryIdentity::from_runtime(boundary.authority.clone(), id.clone())
                else {
                    continue;
                };
                if boundary.is_closed(&identity)? {
                    candidates.push(HistoryAttempt {
                        runtime_id: id.clone(),
                        record: record.clone(),
                    });
                    if candidates.len() == 32 {
                        break;
                    }
                }
            }
            candidates
        };
        let pending = journal.install_history(boundary.clone(), attempts).await?;
        // A failed write never advances the in-memory admission boundary.
        *self.history_boundary.lock().map_err(unavailable)? = Some(boundary.clone());
        let mut candidates = Vec::new();
        for a in &pending {
            if let Some(start) = &a.record.start {
                let identity = HistoryIdentity::from_runtime(
                    boundary.authority.clone(),
                    a.runtime_id.clone(),
                )?;
                if start
                    .history_identity
                    .as_ref()
                    .is_some_and(|original| original != &identity)
                {
                    return Err(Error::Conflict);
                }
                let (sandbox_id, resource_generation) = a
                    .record
                    .backend
                    .as_ref()
                    .map(|b| (b.id.clone(), b.resource_generation.clone()))
                    .unwrap_or_else(|| (format!("sbox-{}", start.id), String::new()));
                candidates.push(proto::HistoryCandidate {
                    operation_id: start.id.clone(),
                    request_fingerprint: start.fingerprint.clone(),
                    deadline_unix_millis: start.deadline_unix_millis,
                    identity: Some(daemon_identity(identity)),
                    sandbox_id,
                    resource_generation,
                });
            }
        }
        let mut batches = std::collections::VecDeque::from([candidates]);
        while let Some(batch) = batches.pop_front() {
            let expected: std::collections::BTreeSet<_> =
                batch.iter().map(|c| c.operation_id.clone()).collect();
            let result = self
                .client
                .clone()
                .collect_history(self.request(proto::CollectHistoryRequest {
                    boundary: Some(daemon_boundary(&boundary)),
                    candidates: batch.clone(),
                }))
                .await;
            match result {
                Ok(reply) => {
                    let ack = reply.into_inner();
                    if ack.acknowledged_operation_ids.len() > 64 {
                        return Err(unavailable(
                            "history acknowledgment exceeds bounded response",
                        ));
                    }
                    let received: std::collections::BTreeSet<_> =
                        ack.acknowledged_operation_ids.into_iter().collect();
                    if !expected.is_subset(&received) {
                        return Err(unavailable("history acknowledgment omitted an operation"));
                    }
                }
                Err(error) if error.code() == Code::ResourceExhausted && batch.len() > 1 => {
                    let middle = batch.len() / 2;
                    batches.push_front(batch[middle..].to_vec());
                    batches.push_front(batch[..middle].to_vec());
                }
                Err(error) => return Err(unavailable(error)),
            }
        }
        journal.complete_history(pending.clone()).await?;
        let mut records = self.operations.lock().map_err(unavailable)?;
        let mut starts = self.starts.lock().map_err(unavailable)?;
        let mut backends = self.backend_ids.lock().map_err(unavailable)?;
        for a in &pending {
            if records
                .get(&a.runtime_id)
                .is_some_and(|r| !r.history_terminal())
            {
                return Err(Error::Conflict);
            }
            records.remove(&a.runtime_id);
            starts.remove(&a.runtime_id);
            backends.remove(&a.runtime_id);
        }
        Ok(pending.len())
    }
}
