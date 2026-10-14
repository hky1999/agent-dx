use adx_core::{
    memory::*, Assignment, EnvironmentRecord, EnvironmentSpec, Error, Resources, Result,
};
use adxlet::{Adxlet, Durability, Readiness, Routes, RuntimeDriver, StateSink};
use async_trait::async_trait;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};
const MIB: u64 = 1 << 20;
fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
struct Rig {
    node: Mutex<Weak<Adxlet>>,
    actual: Mutex<Option<MemoryObservation>>,
    grant: Mutex<Option<MemoryAuthorization>>,
    target: Mutex<Option<u64>>,
    lose_apply_reply: AtomicBool,
    fail_center: AtomicBool,
    wrong_identity: AtomicBool,
    applied: AtomicUsize,
    resolved: AtomicUsize,
    fail_observe: AtomicBool,
    retirement_proven: AtomicBool,
    fail_remove: AtomicBool,
    activity_unknown: AtomicBool,
    active_requests: AtomicUsize,
    activate_during_memory: AtomicBool,
}
impl Rig {
    fn sample(&self) -> MemorySample {
        MemorySample {
            observation: self.actual.lock().unwrap().clone().unwrap(),
            usage_bytes: 32 * MIB,
            peak_bytes: None,
            cpu_usage_ns: None,
            pressure: None,
            observed_unix_millis: now(),
        }
    }
}
#[async_trait]
impl RuntimeDriver for Rig {
    async fn prepare_execution(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
    async fn start(
        &self,
        spec: &EnvironmentSpec,
        id: &str,
        generation: u64,
        _: &[adx_core::scheduling::DeviceAllocation],
    ) -> Result<std::net::IpAddr> {
        *self.actual.lock().unwrap() = Some(MemoryObservation {
            identity: MemoryIdentity {
                assignment: Assignment {
                    environment_id: spec.id.clone(),
                    node_id: "node".into(),
                    shard_id: 0,
                    generation,
                    devices: vec![],
                },
                runtime_id: id.into(),
                sandbox_id: "sandbox".into(),
                resource_generation: "generation".into(),
            },
            cpu_quota_usec: 50000,
            cpu_period_usec: 100000,
            memory_limit_bytes: 128 * MIB,
            memory_high_bytes: 96 * MIB,
            resource_revision: 1,
            operation_id: String::new(),
            operation_pending: false,
            frozen: false,
        });
        Ok("10.0.0.2".parse().unwrap())
    }
    async fn is_running(&self, _: &str) -> Result<bool> {
        Ok(self.actual.lock().unwrap().is_some())
    }
    async fn remove(&self, _: &str) -> Result<()> {
        if self.fail_remove.load(Ordering::SeqCst) {
            return Err(Error::Unavailable("physical retirement unknown".into()));
        }
        *self.actual.lock().unwrap() = None;
        Ok(())
    }
    async fn observe_memory(&self, _: &EnvironmentRecord) -> Result<MemorySample> {
        if self.activate_during_memory.swap(false, Ordering::SeqCst) {
            self.active_requests.store(1, Ordering::SeqCst);
        }
        if self.fail_observe.load(Ordering::SeqCst) {
            return Err(Error::Unavailable("memory observation unavailable".into()));
        }
        Ok(self.sample())
    }
    async fn memory_retirement_required(&self, _: &EnvironmentRecord) -> Result<bool> {
        Ok(self.retirement_proven.load(Ordering::SeqCst))
    }
    async fn resolve_memory(
        &self,
        _: &EnvironmentRecord,
        grant: &MemoryAuthorization,
    ) -> Result<MemorySample> {
        assert_eq!(self.sample().observation, grant.request.source);
        let mut unchanged = grant.request.source.clone();
        unchanged.resource_revision += 1;
        unchanged.operation_id = grant.request.operation_id.clone();
        *self.actual.lock().unwrap() = Some(unchanged);
        self.resolved.fetch_add(1, Ordering::SeqCst);
        Ok(self.sample())
    }
    async fn apply_memory(
        &self,
        _: &EnvironmentRecord,
        grant: &MemoryAuthorization,
        session: &str,
        epoch: u64,
    ) -> Result<MemorySample> {
        grant.check_dispatch(now(), session, epoch)?;
        assert_eq!(
            self.node
                .lock()
                .unwrap()
                .upgrade()
                .unwrap()
                .used()
                .memory_bytes,
            grant
                .request
                .source
                .memory_limit_bytes
                .max(grant.request.target_limit_bytes),
            "local hold must precede physical mutation"
        );
        assert_eq!(
            *self.grant.lock().unwrap(),
            Some(grant.clone()),
            "center must reserve before physical mutation"
        );
        assert_eq!(self.sample().observation, grant.request.source);
        self.applied.fetch_add(1, Ordering::SeqCst);
        *self.actual.lock().unwrap() = Some(grant.request.completion()?);
        if self.lose_apply_reply.swap(false, Ordering::SeqCst) {
            return Err(Error::Unavailable(
                "lost reply after physical effect".into(),
            ));
        }
        Ok(self.sample())
    }
}
#[async_trait]
impl StateSink for Rig {
    async fn commit(&self, _: &EnvironmentRecord) -> Result<Durability> {
        Ok(Durability::Published)
    }
    async fn authorize_execution(
        &self,
        _: &adx_core::execution::ExecutionRequest,
    ) -> Result<adx_core::execution::ExecutionAuthorization> {
        Err(Error::Conflict)
    }
    async fn reconcile_memory(&self, sample: &MemorySample) -> Result<MemoryDecision> {
        if self.fail_center.load(Ordering::SeqCst) {
            return Err(Error::Unavailable("center unavailable".into()));
        }
        let mut pending = self.grant.lock().unwrap();
        if pending.as_ref().is_some_and(|grant| {
            let mut unchanged = grant.request.source.clone();
            unchanged.resource_revision += 1;
            unchanged.operation_id = grant.request.operation_id.clone();
            grant.request.completion().unwrap() == sample.observation
                || unchanged == sample.observation
        }) {
            *pending = None;
        }
        if pending.is_none() {
            if let Some(target) = self.target.lock().unwrap().take() {
                let issued = now();
                *pending = Some(MemoryAuthorization {
                    request: MemoryRequest {
                        source: sample.observation.clone(),
                        target_limit_bytes: target,
                        target_high_bytes: target * 3 / 4,
                        operation_id: format!("change-{}", sample.observation.resource_revision),
                        timeout_millis: 10000,
                    },
                    node_session_id: "boot".into(),
                    coordinator_epoch: 1,
                    issued_unix_millis: issued,
                    deadline_unix_millis: issued + 10000,
                });
            }
        }
        let mut observed = sample.observation.clone();
        if self.wrong_identity.load(Ordering::SeqCst) {
            observed.identity.resource_generation = "foreign".into();
        }
        Ok(MemoryDecision {
            observed,
            held_resources: Resources {
                cpu_millis: 500,
                disk_bytes: 0,
                memory_bytes: sample
                    .observation
                    .memory_limit_bytes
                    .max(pending.as_ref().map_or(0, |g| g.request.target_limit_bytes)),
            },
            authorization: pending.clone(),
            node_session_id: "boot".into(),
            coordinator_epoch: 1,
        })
    }
}
#[async_trait]
impl Readiness for Rig {
    async fn activity(&self, _: &EnvironmentRecord) -> Result<(u64, u64)> {
        if self.activity_unknown.load(Ordering::SeqCst) {
            return Err(Error::Unavailable("activity unknown".into()));
        }
        Ok((1, self.active_requests.load(Ordering::SeqCst) as u64))
    }
    async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl Routes for Rig {
    async fn activity(&self, _: &EnvironmentRecord) -> Result<(String, u64, u64)> {
        Ok(("proxy-session".into(), 1, 0))
    }
    async fn activate(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
    async fn retire(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}
async fn rig() -> (Arc<Rig>, Arc<Adxlet>, adxlet::EnvironmentHandle) {
    rig_with_idle(0).await
}
async fn rig_with_idle(
    idle_timeout_seconds: u64,
) -> (Arc<Rig>, Arc<Adxlet>, adxlet::EnvironmentHandle) {
    let rig = Arc::new(Rig {
        node: Mutex::default(),
        actual: Mutex::default(),
        grant: Mutex::default(),
        target: Mutex::new(Some(256 * MIB)),
        lose_apply_reply: AtomicBool::new(false),
        fail_center: AtomicBool::new(false),
        wrong_identity: AtomicBool::new(false),
        applied: AtomicUsize::new(0),
        resolved: AtomicUsize::new(0),
        fail_observe: AtomicBool::new(false),
        retirement_proven: AtomicBool::new(false),
        fail_remove: AtomicBool::new(false),
        activity_unknown: AtomicBool::new(false),
        active_requests: AtomicUsize::new(0),
        activate_during_memory: AtomicBool::new(false),
    });
    let node = Arc::new(Adxlet::new(
        "node".into(),
        rig.clone(),
        rig.clone(),
        rig.clone(),
        rig.clone(),
    ));
    *rig.node.lock().unwrap() = Arc::downgrade(&node);
    node.update_capacity(
        Resources {
            cpu_millis: 1000,
            memory_bytes: 512 * MIB,
            disk_bytes: 0,
        },
        Duration::from_secs(60),
    )
    .unwrap();
    let spec = EnvironmentSpec {
        id: "env".into(),
        tenant_id: "tenant".into(),
        image: "image".into(),
        runtime_class: "runsc".into(),
        resources: Resources {
            cpu_millis: 500,
            memory_bytes: 128 * MIB,
            disk_bytes: 0,
        },
        priority: 0,
        runtime_profile: None,
        snapshot_id: None,
        lifecycle: adx_core::lifecycle::LifecyclePolicy {
            idle_timeout_seconds,
            ..Default::default()
        },
        env: Default::default(),
        scheduling: Default::default(),
        sandbox: adx_core::sandbox::SandboxOptions {
            limits: adx_core::sandbox::ResourceLimits {
                cpu_millis: 500,
                memory_bytes: 256 * MIB,
                disk_bytes: 0,
            },
            elastic_memory: Some(ElasticMemoryPolicy {
                high_per_mille: 750,
            }),
            ..Default::default()
        },
    };
    let handle = node
        .environment(
            spec,
            Assignment {
                environment_id: "env".into(),
                node_id: "node".into(),
                shard_id: 0,
                generation: 1,
                devices: vec![],
            },
        )
        .unwrap();
    handle.create().await.unwrap();
    (rig, node, handle)
}
#[tokio::test]
async fn memory_actor_grows_and_shrinks_with_center_and_local_holds_before_physical_work() {
    let (rig, node, handle) = rig().await;
    assert_eq!(node.used().memory_bytes, 128 * MIB);
    handle.tick().await.unwrap();
    assert_eq!(node.used().memory_bytes, 256 * MIB);
    *rig.target.lock().unwrap() = Some(128 * MIB);
    handle.tick().await.unwrap();
    assert_eq!(node.used().memory_bytes, 128 * MIB);
    assert_eq!(rig.applied.load(Ordering::SeqCst), 2);
    handle.delete().await.unwrap();
    handle.delete().await.unwrap();
    assert_eq!(node.used(), Resources::default());
}
#[tokio::test]
async fn memory_actor_lost_reply_keeps_hold_and_next_observation_settles_without_reexecution() {
    let (rig, node, handle) = rig().await;
    rig.lose_apply_reply.store(true, Ordering::SeqCst);
    assert!(handle.tick().await.is_err());
    assert_eq!(node.used().memory_bytes, 256 * MIB);
    assert!(rig.grant.lock().unwrap().is_some());
    handle.tick().await.unwrap();
    assert!(rig.grant.lock().unwrap().is_none());
    assert_eq!(rig.applied.load(Ordering::SeqCst), 1);
    handle.delete().await.unwrap();
}
#[tokio::test]
async fn memory_actor_denies_foreign_decision_and_center_outage_before_mutation() {
    let (rig, node, handle) = rig().await;
    rig.fail_center.store(true, Ordering::SeqCst);
    assert!(handle.tick().await.is_err());
    rig.fail_center.store(false, Ordering::SeqCst);
    rig.wrong_identity.store(true, Ordering::SeqCst);
    assert!(handle.tick().await.is_err());
    assert_eq!(rig.applied.load(Ordering::SeqCst), 0);
    assert_eq!(node.used().memory_bytes, 128 * MIB);
    handle.delete().await.unwrap();
}
#[tokio::test]
async fn memory_actor_local_capacity_or_pressure_prevents_dispatch_after_center_reservation() {
    let (rig, node, handle) = rig().await;
    node.update_capacity(
        Resources {
            cpu_millis: 500,
            memory_bytes: 128 * MIB,
            disk_bytes: 0,
        },
        Duration::from_secs(60),
    )
    .unwrap();
    assert_eq!(handle.tick().await, Err(Error::NoCapacity));
    assert!(rig.grant.lock().unwrap().is_some());
    assert_eq!(rig.applied.load(Ordering::SeqCst), 0);
    node.update_capacity(
        Resources {
            cpu_millis: 500,
            memory_bytes: 256 * MIB,
            disk_bytes: 0,
        },
        Duration::from_secs(60),
    )
    .unwrap();
    node.set_pressure(true);
    assert_eq!(handle.tick().await, Err(Error::NoCapacity));
    node.set_pressure(false);
    handle.tick().await.unwrap();
    assert_eq!(rig.applied.load(Ordering::SeqCst), 1);
    handle.delete().await.unwrap();
}

#[tokio::test]
async fn memory_actor_resolves_expired_grant_even_when_local_growth_admission_is_closed() {
    let (rig, node, handle) = rig().await;
    node.update_capacity(
        Resources {
            cpu_millis: 500,
            memory_bytes: 128 * MIB,
            disk_bytes: 0,
        },
        Duration::from_secs(60),
    )
    .unwrap();
    assert_eq!(handle.tick().await, Err(Error::NoCapacity));
    {
        let mut pending = rig.grant.lock().unwrap();
        let grant = pending.as_mut().unwrap();
        grant.issued_unix_millis = 1;
        grant.deadline_unix_millis = 1 + grant.request.timeout_millis;
    }
    handle
        .tick()
        .await
        .expect("expiry reconciliation must not require new local growth capacity");
    assert_eq!(rig.applied.load(Ordering::SeqCst), 0);
    assert_eq!(rig.resolved.load(Ordering::SeqCst), 1);
    assert_eq!(node.used().memory_bytes, 128 * MIB);
    assert!(rig.grant.lock().unwrap().is_none());
    handle.delete().await.unwrap();
    assert_eq!(node.used(), Resources::default());
}
#[tokio::test]
async fn memory_actor_resolves_old_epoch_or_session_without_redispatching() {
    for old_session in [false, true] {
        let (rig, node, handle) = rig().await;
        node.set_pressure(true);
        assert_eq!(handle.tick().await, Err(Error::NoCapacity));
        node.set_pressure(false);
        {
            let mut pending = rig.grant.lock().unwrap();
            let grant = pending.as_mut().unwrap();
            if old_session {
                grant.node_session_id = "old-boot".into();
            } else {
                grant.coordinator_epoch = 2;
            }
        }
        handle
            .tick()
            .await
            .expect("an old authorization may only be reconciled");
        assert_eq!(rig.applied.load(Ordering::SeqCst), 0);
        assert_eq!(rig.resolved.load(Ordering::SeqCst), 1);
        assert!(rig.grant.lock().unwrap().is_none());
        assert_eq!(node.used().memory_bytes, 128 * MIB);
        handle.delete().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn memory_actor_unknown_observation_does_not_block_configured_idle_retirement() {
    let (rig, node, handle) = rig_with_idle(10).await;
    rig.fail_observe.store(true, Ordering::SeqCst);
    assert!(handle.tick().await.is_err());
    assert_eq!(node.used().memory_bytes, 128 * MIB);
    tokio::time::advance(Duration::from_secs(10)).await;
    let deleted = handle
        .tick()
        .await
        .expect("memory errors must not block independently proven idle deletion")
        .unwrap();
    assert_eq!(deleted.record.state, adx_core::EnvironmentState::Deleted);
    assert_eq!(node.used(), Resources::default());
    assert!(rig.actual.lock().unwrap().is_none());
    handle.delete().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn memory_actor_center_memory_failure_does_not_block_idle_retirement() {
    let (rig, node, handle) = rig_with_idle(10).await;
    rig.fail_center.store(true, Ordering::SeqCst);
    assert!(handle.tick().await.is_err());
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(handle
        .tick()
        .await
        .expect("independent lifecycle publication remains available")
        .is_some());
    assert_eq!(node.used(), Resources::default());
    assert_eq!(rig.applied.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn memory_actor_proven_partial_write_retires_only_after_physical_confirmation() {
    let (rig, node, handle) = rig().await;
    handle.tick().await.unwrap();
    assert_eq!(node.used().memory_bytes, 256 * MIB);
    rig.fail_observe.store(true, Ordering::SeqCst);
    rig.retirement_proven.store(true, Ordering::SeqCst);
    rig.fail_remove.store(true, Ordering::SeqCst);
    assert!(handle.tick().await.is_err());
    let failed = handle.sync().await.unwrap().record;
    assert_eq!(
        failed.state,
        adx_core::EnvironmentState::Failed,
        "proven partial writes must enter the existing failure cleanup path"
    );
    assert!(failed.resources_held);
    assert_eq!(node.used().memory_bytes, 256 * MIB);
    assert!(rig.actual.lock().unwrap().is_some());
    rig.fail_remove.store(false, Ordering::SeqCst);
    handle.tick().await.unwrap();
    assert!(!handle.sync().await.unwrap().record.resources_held);
    assert_eq!(node.used(), Resources::default());
    assert_eq!(rig.applied.load(Ordering::SeqCst), 1);
    handle.delete().await.unwrap();
    handle.delete().await.unwrap();
}
#[tokio::test(start_paused = true)]
async fn memory_actor_unknown_memory_cannot_authorize_retirement_or_bypass_activity() {
    let (rig, node, handle) = rig_with_idle(10).await;
    rig.fail_observe.store(true, Ordering::SeqCst);
    rig.active_requests.store(1, Ordering::SeqCst);
    assert!(handle.tick().await.is_err());
    tokio::time::advance(Duration::from_secs(20)).await;
    assert!(handle.tick().await.is_err());
    rig.active_requests.store(0, Ordering::SeqCst);
    rig.activity_unknown.store(true, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(20)).await;
    assert!(handle.tick().await.is_err());
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        adx_core::EnvironmentState::Running
    );
    assert_eq!(node.used().memory_bytes, 128 * MIB);
    rig.activity_unknown.store(false, Ordering::SeqCst);
    assert!(handle.tick().await.is_err());
    tokio::time::advance(Duration::from_secs(9)).await;
    assert!(handle.tick().await.is_err());
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        adx_core::EnvironmentState::Running
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(handle.tick().await.unwrap().is_some());
    assert_eq!(node.used(), Resources::default());
}

#[tokio::test(start_paused = true)]
async fn memory_actor_activity_after_memory_wait_invalidates_previous_idle_observation() {
    let (rig, node, handle) = rig_with_idle(10).await;
    rig.fail_observe.store(true, Ordering::SeqCst);
    assert!(handle.tick().await.is_err());
    tokio::time::advance(Duration::from_secs(10)).await;
    rig.activate_during_memory.store(true, Ordering::SeqCst);
    assert!(
        handle.tick().await.is_err(),
        "an activity observation taken before memory work cannot authorize idle deletion"
    );
    assert_eq!(
        handle.sync().await.unwrap().record.state,
        adx_core::EnvironmentState::Running
    );
    assert_eq!(node.used().memory_bytes, 128 * MIB);
    assert!(rig.actual.lock().unwrap().is_some());
    rig.active_requests.store(0, Ordering::SeqCst);
    assert!(handle.tick().await.is_err());
    tokio::time::advance(Duration::from_secs(9)).await;
    assert!(handle.tick().await.is_err());
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(handle.tick().await.unwrap().is_some());
    assert_eq!(node.used(), Resources::default());
}
