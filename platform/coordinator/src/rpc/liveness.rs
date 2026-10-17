//! Preserve an execution when Coordinator work delayed an authenticated heartbeat.
//! A probe is permitted once per uncommitted fresh receipt. It closes admission,
//! never grants resources, and cannot revive an already invalidated owner.
use super::*;
use futures_util::{stream, StreamExt};
use tokio::time::Instant;

#[derive(Clone, Copy)]
struct Receipt {
    sequence: u64,
    received: Instant,
}

struct Arrivals {
    session: String,
    accepted: u64,
    consumed: u64,
    registered: bool,
    first: Option<Receipt>,
    latest: Option<Receipt>,
}

impl Arrivals {
    fn floor(&self) -> u64 {
        self.accepted.max(self.consumed)
    }

    fn retain_pending(&mut self) {
        let floor = self.floor();
        self.first = self.first.filter(|r| r.sequence > floor);
        self.latest = self.latest.filter(|r| r.sequence > floor);
        if self.first.is_none() {
            self.first = self.latest;
        }
    }
}

#[derive(Default)]
pub(super) struct HeartbeatArrivals(Mutex<BTreeMap<String, Arrivals>>);

impl HeartbeatArrivals {
    pub(super) fn restored(nodes: &BTreeMap<String, StoredNode>) -> Self {
        let entries = nodes
            .iter()
            .filter_map(|(id, node)| {
                let session = node.session.as_ref().filter(|s| !s.id.is_empty())?;
                Some((
                    id.clone(),
                    Arrivals {
                        session: session.id.clone(),
                        accepted: session.sequence,
                        consumed: 0,
                        registered: false,
                        first: None,
                        latest: None,
                    },
                ))
            })
            .collect();
        Self(Mutex::new(entries))
    }

    pub(super) async fn observe(&self, report: &pb::RegisterNodeRequest, received: Instant) {
        let mut entries = self.0.lock().await;
        let Some(arrivals) = entries.get_mut(&report.node_id) else {
            return;
        };
        let fresh = report.heartbeat_sequence > arrivals.floor()
            || (!arrivals.registered
                && report.reconciling
                && report.heartbeat_sequence == arrivals.accepted
                && report.heartbeat_sequence > arrivals.consumed);
        if arrivals.session != report.session_id || !fresh {
            return;
        }
        let receipt = Receipt {
            sequence: report.heartbeat_sequence,
            received,
        };
        if arrivals.first.is_none() {
            arrivals.first = Some(receipt);
        }
        if arrivals
            .latest
            .is_none_or(|old| old.sequence < receipt.sequence)
        {
            arrivals.latest = Some(receipt);
        }
    }

    pub(super) async fn registered(&self, report: &pb::RegisterNodeRequest) {
        let mut entries = self.0.lock().await;
        let arrivals = entries
            .entry(report.node_id.clone())
            .or_insert_with(|| Arrivals {
                session: report.session_id.clone(),
                accepted: 0,
                consumed: 0,
                registered: true,
                first: None,
                latest: None,
            });
        if arrivals.session != report.session_id {
            *arrivals = Arrivals {
                session: report.session_id.clone(),
                accepted: 0,
                consumed: 0,
                registered: true,
                first: None,
                latest: None,
            };
        }
        arrivals.accepted = arrivals.accepted.max(report.heartbeat_sequence);
        arrivals.registered = true;
        arrivals.retain_pending();
    }

    async fn take(&self, id: &str, session: &str, since: Instant, timeout: Duration) -> bool {
        let mut entries = self.0.lock().await;
        let Some(arrivals) = entries.get_mut(id).filter(|a| a.session == session) else {
            return false;
        };
        let Some(receipt) = arrivals.first else {
            return false;
        };
        // A packet first received after expiry cannot rescue its old owner.
        if receipt.received.saturating_duration_since(since) >= timeout {
            return false;
        }
        arrivals.consumed = arrivals.consumed.max(receipt.sequence);
        arrivals.retain_pending();
        true
    }
}

#[derive(Clone)]
enum OwnerContact {
    Live {
        registered: Instant,
        reachable: Instant,
    },
    Recovering(Instant),
}

#[derive(Clone)]
struct OwnerProbe {
    id: String,
    session: String,
    address: String,
    epoch: u64,
    contact: OwnerContact,
}

impl OwnerProbe {
    fn current(state: &State, id: &str) -> Option<Self> {
        let node = state.nodes.get(id)?;
        let session = node.session.as_ref().filter(|s| !s.id.is_empty())?;
        let contact = if let Some(live) = state.live.get(id) {
            if live.expired || live.session != session.id {
                return None;
            }
            OwnerContact::Live {
                registered: live.last_seen,
                reachable: live.last_reachable,
            }
        } else {
            OwnerContact::Recovering(*state.recovering.get(id)?)
        };
        Some(Self {
            id: id.into(),
            session: session.id.clone(),
            address: node.address.clone(),
            epoch: state.session.epoch(),
            contact,
        })
    }

    fn since(&self) -> Instant {
        match self.contact {
            OwnerContact::Live {
                registered,
                reachable,
            } => registered.max(reachable),
            OwnerContact::Recovering(since) => since,
        }
    }

    fn confirm(&self, state: &mut State, received: Instant) {
        if state.session.epoch() != self.epoch
            || state.nodes.get(&self.id).is_none_or(|node| {
                node.address != self.address
                    || node.session.as_ref().is_none_or(|s| s.id != self.session)
            })
        {
            return;
        }
        match self.contact {
            OwnerContact::Live {
                registered,
                reachable,
            } => {
                if let Some(live) = state.live.get_mut(&self.id).filter(|live| {
                    !live.expired
                        && live.session == self.session
                        && live.last_seen == registered
                        && live.last_reachable == reachable
                }) {
                    // Registration freshness is intentionally unchanged. The node
                    // must reconcile before receiving allocations or memory grants.
                    live.last_reachable = received;
                }
            }
            OwnerContact::Recovering(since) => {
                if state.live.contains_key(&self.id) {
                    return;
                }
                if let Some(current) = state.recovering.get_mut(&self.id).filter(|v| **v == since) {
                    *current = received;
                }
            }
        }
    }
}

impl State {
    fn close_owner_admission(&mut self, id: &str) -> Result<()> {
        let node = self.nodes.get_mut(id).ok_or(Error::Conflict)?;
        node.node.available = false;
        if let Some(live) = self.live.get_mut(id) {
            live.inspected = false;
        }
        if let Err(error) = self.scheduler.register(node.node.clone()) {
            self.needs_recovery = true;
            return Err(error);
        }
        Ok(())
    }
}

impl CoordinatorRpc {
    pub(super) async fn node_session_at(
        &self,
        address: &str,
    ) -> Option<pb::GetNodeSessionResponse> {
        let endpoint = self
            .0
            .node_tls
            .endpoint(address)
            .ok()?
            .connect_timeout(self.0.timeout)
            .timeout(self.0.timeout);
        let channel = endpoint.connect().await.ok()?;
        pb::node_service_client::NodeServiceClient::new(self.0.node_tls.wrap(channel))
            .get_session(pb::GetNodeSessionRequest {})
            .await
            .ok()
            .map(Response::into_inner)
    }

    pub(super) async fn state_for_owner_expiry(
        &self,
    ) -> Result<tokio::sync::OwnedMutexGuard<State>> {
        // Cancellation of the observer must not discard an already consumed
        // receipt or its in-flight proof. The worker owns its state guard.
        let service = self.clone();
        tokio::spawn(async move { service.owner_expiry_state().await })
            .await
            .map_err(|_| Error::Unavailable("node liveness check worker stopped".into()))?
    }

    async fn owner_expiry_state(&self) -> Result<tokio::sync::OwnedMutexGuard<State>> {
        // Serialize expiry decisions while still allowing ordinary state readers
        // and writers to run during network probes. Another expiry check waits
        // for this proof rather than invalidating its owner in the meantime.
        let _expiry = self.0.owner_expiry.lock().await;
        loop {
            let mut state = self.0.state.clone().lock_owned().await;
            state.recover_authoritative_state().await?;
            state.healthy()?;
            let mut probes = Vec::new();
            let ids: Vec<_> = state
                .nodes
                .keys()
                .filter(|id| state.owner_overdue(id, self.0.heartbeat_timeout))
                .cloned()
                .collect();
            for id in ids {
                let Some(probe) = OwnerProbe::current(&state, &id) else {
                    continue;
                };
                if self
                    .0
                    .heartbeat_arrivals
                    .take(&id, &probe.session, probe.since(), self.0.heartbeat_timeout)
                    .await
                {
                    state.close_owner_admission(&id)?;
                    probes.push(probe);
                }
            }
            if probes.is_empty() {
                return Ok(state);
            }
            self.0.changed.notify_waiters();
            drop(state);
            let replies = stream::iter(probes.into_iter().map(|probe| async move {
                let actual = self.node_session_at(&probe.address).await;
                let matches = actual.is_some_and(|actual| {
                    actual.node_id == probe.id && actual.session_id == probe.session
                });
                (probe, matches.then(Instant::now))
            }))
            .buffer_unordered(4)
            .collect::<Vec<_>>()
            .await;
            let mut state = self.0.state.clone().lock_owned().await;
            for (probe, received) in replies {
                if let Some(received) = received {
                    probe.confirm(&mut state, received);
                }
            }
            // Another writer may have failed while probing. Recover and check the
            // current owner again; a late probe never bypasses authoritative recovery.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report(sequence: u64) -> pb::RegisterNodeRequest {
        pb::RegisterNodeRequest {
            node_id: "node".into(),
            session_id: "boot".into(),
            heartbeat_sequence: sequence,
            ..Default::default()
        }
    }

    async fn ready() -> HeartbeatArrivals {
        let arrivals = HeartbeatArrivals::default();
        arrivals.registered(&report(2)).await;
        arrivals
    }

    #[tokio::test]
    async fn accepted_or_stale_duplicates_do_not_create_rescue_receipts() {
        let arrivals = ready().await;
        let now = Instant::now();
        arrivals.observe(&report(2), now).await;
        arrivals.observe(&report(1), now).await;
        assert!(
            !arrivals
                .take("node", "boot", now, Duration::from_secs(1))
                .await
        );
    }

    #[tokio::test]
    async fn fresh_receipt_is_consumed_once() {
        let arrivals = ready().await;
        let now = Instant::now();
        arrivals.observe(&report(3), now).await;
        assert!(
            arrivals
                .take("node", "boot", now, Duration::from_secs(1))
                .await
        );
        arrivals.observe(&report(3), now).await;
        assert!(
            !arrivals
                .take("node", "boot", now, Duration::from_secs(1))
                .await
        );
    }

    #[tokio::test]
    async fn receipt_received_after_expiry_does_not_rescue_owner() {
        let arrivals = ready().await;
        let now = Instant::now();
        arrivals
            .observe(&report(3), now + Duration::from_secs(2))
            .await;
        assert!(
            !arrivals
                .take("node", "boot", now, Duration::from_secs(1))
                .await
        );
    }

    #[tokio::test]
    async fn wrong_session_or_unknown_node_is_not_recorded() {
        let arrivals = ready().await;
        let now = Instant::now();
        let mut wrong = report(3);
        wrong.session_id = "different".into();
        arrivals.observe(&wrong, now).await;
        wrong = report(3);
        wrong.node_id = "different".into();
        arrivals.observe(&wrong, now).await;
        assert!(
            !arrivals
                .take("node", "boot", now, Duration::from_secs(1))
                .await
        );
        assert_eq!(arrivals.0.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn newest_receipt_survives_consumption_of_earlier_packet() {
        let arrivals = ready().await;
        let now = Instant::now();
        arrivals.observe(&report(3), now).await;
        arrivals.observe(&report(4), now).await;
        assert!(
            arrivals
                .take("node", "boot", now, Duration::from_secs(1))
                .await
        );
        assert!(
            arrivals
                .take("node", "boot", now, Duration::from_secs(1))
                .await
        );
        assert!(
            !arrivals
                .take("node", "boot", now, Duration::from_secs(1))
                .await
        );
    }

    #[tokio::test]
    async fn successful_registration_clears_only_accepted_receipts() {
        let arrivals = ready().await;
        let now = Instant::now();
        arrivals.observe(&report(3), now).await;
        arrivals.observe(&report(4), now).await;
        arrivals.registered(&report(3)).await;
        assert!(
            arrivals
                .take("node", "boot", now, Duration::from_secs(1))
                .await
        );
        assert!(
            !arrivals
                .take("node", "boot", now, Duration::from_secs(1))
                .await
        );
    }

    #[tokio::test]
    async fn replacement_session_cannot_inherit_old_receipts() {
        let arrivals = ready().await;
        let now = Instant::now();
        arrivals.observe(&report(3), now).await;
        let mut new = report(1);
        new.session_id = "new-boot".into();
        arrivals.registered(&new).await;
        arrivals.observe(&report(4), now).await;
        assert!(
            !arrivals
                .take("node", "boot", now, Duration::from_secs(1))
                .await
        );
        assert!(
            !arrivals
                .take("node", "new-boot", now, Duration::from_secs(1))
                .await
        );
    }

    #[tokio::test]
    async fn restored_owner_can_repeat_its_first_reconciling_packet_once() {
        let arrivals = HeartbeatArrivals(Mutex::new(BTreeMap::from([(
            "node".into(),
            Arrivals {
                session: "boot".into(),
                accepted: 2,
                consumed: 0,
                registered: false,
                first: None,
                latest: None,
            },
        )])));
        let now = Instant::now();
        let mut r = report(2);
        arrivals.observe(&r, now).await;
        assert!(
            !arrivals
                .take("node", "boot", now, Duration::from_secs(1))
                .await
        );
        r.reconciling = true;
        arrivals.observe(&r, now).await;
        assert!(
            arrivals
                .take("node", "boot", now, Duration::from_secs(1))
                .await
        );
        arrivals.observe(&r, now).await;
        assert!(
            !arrivals
                .take("node", "boot", now, Duration::from_secs(1))
                .await
        );
    }
}
