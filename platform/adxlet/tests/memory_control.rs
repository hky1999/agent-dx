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
        *self.actual.lock().unwrap() = None;
        Ok(())
    }
    async fn observe_memory(&self, _: &EnvironmentRecord) -> Result<MemorySample> {
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
        if pending
            .as_ref()
            .is_some_and(|grant| grant.request.completion().unwrap() == sample.observation)
        {
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
    async fn wait_ready(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl Routes for Rig {
    async fn activate(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
    async fn retire(&self, _: &EnvironmentRecord) -> Result<()> {
        Ok(())
    }
}
async fn rig() -> (Arc<Rig>, Arc<Adxlet>, adxlet::EnvironmentHandle) {
    let rig = Arc::new(Rig {
        node: Mutex::default(),
        actual: Mutex::default(),
        grant: Mutex::default(),
        target: Mutex::new(Some(256 * MIB)),
        lose_apply_reply: AtomicBool::new(false),
        fail_center: AtomicBool::new(false),
        wrong_identity: AtomicBool::new(false),
        applied: AtomicUsize::new(0),
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
        lifecycle: Default::default(),
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
