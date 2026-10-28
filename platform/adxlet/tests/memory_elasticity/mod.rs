use super::*;
use serde_json::json;
const MIB: u64 = 1024 * 1024;
fn elastic_spec() -> EnvironmentSpec {
    let mut value = serde_json::to_value(spec()).unwrap();
    value["resources"]["cpu_millis"] = json!(500);
    value["resources"]["memory_bytes"] = json!(128 * MIB);
    value["sandbox"]["limits"]["memory_bytes"] = json!(512 * MIB);
    value["sandbox"]["elastic_memory"] = json!({"high_per_mille":750});
    serde_json::from_value(value).unwrap()
}
#[test]
fn elastic_birth_uses_initial_memory_and_persists_independent_ceiling() {
    let spec = elastic_spec();
    spec.validate().unwrap();
    let request = start_request(&spec, "i-1", 1, &[], &Config::default()).unwrap();
    assert_eq!(
        request.resources["Memory"], 128.0,
        "initial allocation must match the budget, not the ceiling"
    );
    assert_eq!(request.resources["CPU"], 500.0);
    let policy: serde_json::Value =
        serde_json::from_str(&request.labels["akernel.scheduler/elastic-resources"]).unwrap();
    assert_eq!(
        policy,
        json!({"version":1,"CPU":{"initial":500,"ceiling":500},"Memory":{"initial":128,"ceiling":512}})
    );
    assert_eq!(spec.execution_resources().memory_bytes, 128 * MIB);
    let roundtrip: adx_protocol::control::EnvironmentSpec = spec.clone().into();
    let decoded: EnvironmentSpec = roundtrip.try_into().unwrap();
    assert_eq!(
        serde_json::to_value(decoded).unwrap()["sandbox"]["elastic_memory"],
        json!({"high_per_mille":750})
    );
}
#[test]
fn elastic_birth_rejects_unenforceable_policy_before_dispatch() {
    let original = serde_json::to_value(elastic_spec()).unwrap();
    for case in 0..7 {
        let mut value = original.clone();
        match case {
            0 => value["sandbox"]["elastic_memory"]["high_per_mille"] = json!(0),
            1 => value["sandbox"]["elastic_memory"]["high_per_mille"] = json!(1000),
            2 => value["resources"]["memory_bytes"] = json!(128 * MIB + 4096),
            3 => value["sandbox"]["limits"]["memory_bytes"] = json!(512 * MIB + 4096),
            4 => value["runtime_class"] = json!("kata"),
            5 => value["snapshot_id"] = json!("snapshot"),
            _ => value["sandbox"]["limits"]["memory_bytes"] = json!((1u64 << 53) + MIB),
        }
        let rejected = match serde_json::from_value::<EnvironmentSpec>(value) {
            Ok(spec) => start_request(&spec, "i-1", 1, &[], &Config::default()).is_err(),
            Err(_) => true,
        };
        assert!(rejected, "case {case} must fail before sandboxd dispatch");
    }
}

#[test]
fn elastic_boundary_rejects_cpu_below_the_enforceable_quota() {
    let mut spec = elastic_spec();
    spec.resources.cpu_millis = 1;
    assert!(start_request(&spec, "i-1", 1, &[], &Config::default()).is_err());
}
#[test]
fn elastic_boundary_rejects_soft_limit_below_readback_alignment() {
    let mut spec = elastic_spec();
    spec.resources.memory_bytes = MIB;
    spec.sandbox.elastic_memory.as_mut().unwrap().high_per_mille = 1;
    assert!(start_request(&spec, "i-1", 1, &[], &Config::default()).is_err());
}

use adx_core::memory::{MemoryAuthorization, MemoryRequest};
#[derive(Clone)]
pub(super) struct MemoryServer {
    pub stats: Arc<Mutex<Option<StatsResponse>>>,
    pub grows: Arc<Mutex<Vec<ResizeResourcesRequest>>>,
    pub resolutions: Arc<Mutex<Vec<ResolveResourceOperationRequest>>>,
    pub shrinks: Arc<Mutex<Vec<ShrinkResourcesRequest>>>,
    pub reply: Arc<Mutex<Option<ResizeResourcesResponse>>>,
    pub after_reply: Arc<Mutex<Option<StatsResponse>>>,
    pub lose_reply: bool,
    pub entered: Arc<Semaphore>,
    pub release: Arc<Semaphore>,
}
impl Default for MemoryServer {
    fn default() -> Self {
        Self {
            stats: Arc::default(),
            grows: Arc::default(),
            resolutions: Arc::default(),
            shrinks: Arc::default(),
            reply: Arc::default(),
            after_reply: Arc::default(),
            lose_reply: false,
            entered: Arc::new(Semaphore::new(0)),
            release: Arc::new(Semaphore::new(1)),
        }
    }
}
impl MemoryServer {
    pub(super) fn resolve(
        &self,
        request: ResolveResourceOperationRequest,
    ) -> Result<Response<ResizeResourcesResponse>, Box<Status>> {
        self.resolutions.lock().unwrap().push(request.clone());
        let original = request
            .original
            .ok_or_else(|| Status::invalid_argument("original request required"))?;
        let mut state = self.stats.lock().unwrap();
        let actual = state.as_mut().unwrap();
        if original.resource_generation != actual.resource_generation
            || actual.resource_operation_pending
            || actual.frozen
        {
            return Err(Box::new(Status::failed_precondition(
                "unresolved physical state",
            )));
        }
        if actual.resource_revision == original.expected_revision {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;
            if original.deadline_unix_ms > now {
                return Err(Box::new(Status::failed_precondition(
                    "original command has not expired",
                )));
            }
            let expected = original.expected.unwrap();
            assert_eq!(expected.memory_limit_bytes, actual.memory_limit_bytes);
            actual.resource_revision += 1;
            actual.resource_operation_id = original.operation_id.clone();
        } else if actual.resource_revision != original.expected_revision + 1
            || actual.resource_operation_id != original.operation_id
        {
            return Err(Box::new(Status::failed_precondition("foreign operation")));
        }
        if self.lose_reply {
            return Err(Box::new(Status::unavailable("resolution reply lost")));
        }
        let reply = self
            .reply
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(ResizeResourcesResponse {
                actual: Some(actual.clone()),
                complete: actual.memory_limit_bytes == original.target.unwrap().memory_limit_bytes,
                update_error: String::new(),
            });
        if let Some(changed) = self.after_reply.lock().unwrap().clone() {
            *actual = changed;
        }
        Ok(Response::new(reply))
    }
    pub async fn apply(
        &self,
        target: ResourceLimits,
        id: String,
        revision: u64,
    ) -> Result<Response<ResizeResourcesResponse>, Status> {
        self.entered.add_permits(1);
        let _permit = self.release.acquire().await.unwrap();
        let mut stats = self.stats.lock().unwrap();
        let actual = stats.as_mut().unwrap();
        actual.cpu_quota_usec = target.cpu_quota_usec;
        actual.cpu_period_usec = target.cpu_period_usec;
        actual.memory_limit_bytes = target.memory_limit_bytes;
        actual.memory_high_bytes = target.memory_limit_bytes * 3 / 4;
        actual.resource_operation_id = id;
        actual.resource_revision = revision + 1;
        if self.lose_reply {
            return Err(Status::unavailable("reply lost after write"));
        }
        let reply = self
            .reply
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(ResizeResourcesResponse {
                actual: Some(actual.clone()),
                complete: true,
                update_error: String::new(),
            });
        if let Some(changed) = self.after_reply.lock().unwrap().clone() {
            *actual = changed;
        }
        Ok(Response::new(reply))
    }
}
fn physical_stats() -> StatsResponse {
    StatsResponse {
        memory_usage_bytes: 48 * MIB, memory_usage_known: true,
        memory_limit_bytes: 128 * MIB, memory_limit_known: true,
        memory_high_bytes: 96 * MIB, memory_high_known: true,
        cpu_quota_usec: 50_000, cpu_period_usec: 100_000, cpu_limits_known: true,
        resource_generation: "physical-generation-1".into(),
        resource_revision: 1, resource_revision_known: true, frozen_known: true,
        elastic_policy: json!({"version":1,"CPU":{"initial":500,"ceiling":500},"Memory":{"initial":128,"ceiling":512}}).to_string(),
        ..Default::default()
    }
}
fn running_record() -> EnvironmentRecord {
    let mut record = prepared_execution_target();
    record.spec = elastic_spec();
    record.state = EnvironmentState::Running;
    record.runtime.id = "i-1".into();
    record.runtime.ip = Some("10.0.0.2".parse().unwrap());
    record
}
fn grant(sample: &adx_core::memory::MemorySample, target: u64) -> MemoryAuthorization {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    MemoryAuthorization {
        request: MemoryRequest {
            source: sample.observation.clone(),
            target_limit_bytes: target,
            target_high_bytes: target * 3 / 4,
            operation_id: format!("memory-{}", sample.observation.resource_revision),
            timeout_millis: 10_000,
        },
        node_session_id: "session-1".into(),
        coordinator_epoch: 2,
        issued_unix_millis: now,
        deadline_unix_millis: now + 10_000,
    }
}
async fn memory_fixture(memory: MemoryServer) -> (Sandboxd, Harness, EnvironmentRecord) {
    let record = running_record();
    let (adapter, harness) = connect(Server {
        memory,
        ..Default::default()
    })
    .await;
    adapter
        .start(
            &record.spec,
            &record.runtime.id,
            record.assignment.generation,
            &[],
        )
        .await
        .unwrap();
    (adapter, harness, record)
}
#[tokio::test]
async fn physical_memory_grows_and_shrinks_with_exact_identity_revision_and_deadline() {
    let memory = MemoryServer::default();
    *memory.stats.lock().unwrap() = Some(physical_stats());
    let (adapter, _harness, record) = memory_fixture(memory.clone()).await;
    let birth = adapter.observe_memory(&record).await.unwrap();
    assert_eq!(birth.observation.memory_limit_bytes, 128 * MIB);
    assert_eq!(birth.peak_bytes, None);
    assert_eq!(birth.pressure, None);
    let grow = grant(&birth, 256 * MIB);
    let after = adapter
        .apply_memory(&record, &grow, "session-1", 2)
        .await
        .unwrap();
    assert_eq!(after.observation, grow.request.completion().unwrap());
    let wire = memory.grows.lock().unwrap()[0].clone();
    assert_eq!(wire.sandbox_id, "generated-backend-id");
    assert_eq!(wire.resource_generation, "physical-generation-1");
    assert_eq!(wire.expected_revision, 1);
    assert_eq!(wire.operation_id, grow.request.operation_id);
    assert_eq!(wire.deadline_unix_ms, grow.deadline_unix_millis as i64);
    assert_eq!(wire.expected.unwrap().memory_limit_bytes, 128 * MIB);
    assert_eq!(wire.target.unwrap().cpu_quota_usec, 50_000);
    let shrink = grant(&after, 128 * MIB);
    assert_eq!(
        adapter
            .apply_memory(&record, &shrink, "session-1", 2)
            .await
            .unwrap()
            .observation,
        shrink.request.completion().unwrap()
    );
    assert_eq!(memory.shrinks.lock().unwrap().len(), 1);
    adapter
        .apply_memory(&record, &shrink, "session-1", 2)
        .await
        .unwrap();
    assert_eq!(
        memory.shrinks.lock().unwrap().len(),
        1,
        "matching completed replay is read-only"
    );
}
#[tokio::test]
async fn physical_memory_refuses_unknown_or_foreign_readback() {
    let memory = MemoryServer::default();
    *memory.stats.lock().unwrap() = Some(physical_stats());
    let (adapter, _harness, record) = memory_fixture(memory.clone()).await;
    adapter.observe_memory(&record).await.unwrap();
    for case in 0..15 {
        let mut stats = physical_stats();
        match case {
            0 => stats.memory_usage_known = false,
            1 => stats.memory_limit_known = false,
            2 => stats.cpu_limits_known = false,
            3 => stats.resource_revision_known = false,
            4 => stats.memory_high_known = false,
            5 => stats.memory_high_unlimited = true,
            6 => stats.frozen_known = false,
            7 => stats.frozen = true,
            8 => stats.resource_operation_pending = true,
            9 => stats.resource_generation = "foreign".into(),
            10 => stats.cpu_quota_usec = 60_000,
            11 => stats.memory_high_bytes = 100 * MIB,
            12 => stats.elastic_policy = String::new(),
            13 => stats.resource_revision = 0,
            _ => stats.elastic_policy = stats.elastic_policy.replace("512", "1024"),
        }
        *memory.stats.lock().unwrap() = Some(stats);
        assert!(
            adapter.observe_memory(&record).await.is_err(),
            "case {case}"
        );
    }
}
#[tokio::test]
async fn physical_memory_lost_reply_retains_identity_for_read_only_reconciliation() {
    let memory = MemoryServer {
        lose_reply: true,
        ..Default::default()
    };
    *memory.stats.lock().unwrap() = Some(physical_stats());
    let (adapter, harness, record) = memory_fixture(memory.clone()).await;
    let request = grant(&adapter.observe_memory(&record).await.unwrap(), 256 * MIB);
    assert!(adapter
        .apply_memory(&record, &request, "session-1", 2)
        .await
        .is_err());
    drop(adapter);
    let adapter = reconnect_with_journal(&harness).await;
    assert_eq!(
        adapter.observe_memory(&record).await.unwrap().observation,
        request.request.completion().unwrap()
    );
    assert_eq!(memory.grows.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn physical_memory_rejects_invalid_authority_or_bounds_before_dispatch() {
    let memory = MemoryServer::default();
    *memory.stats.lock().unwrap() = Some(physical_stats());
    let (adapter, _harness, record) = memory_fixture(memory.clone()).await;
    let original = grant(&adapter.observe_memory(&record).await.unwrap(), 256 * MIB);
    for case in 0..12 {
        let mut g = original.clone();
        let mut r = record.clone();
        let mut session = "session-1";
        let mut epoch = 2;
        match case {
            0 => session = "session-2",
            1 => epoch = 3,
            2 => {
                g.issued_unix_millis = 1;
                g.deadline_unix_millis = 10_001;
            }
            3 => g.request.source.identity.resource_generation = "other".into(),
            4 => g.request.source.identity.assignment.node_id = "other".into(),
            5 => g.request.target_high_bytes += 4096,
            6 => {
                g.request.target_limit_bytes = 1024 * MIB;
                g.request.target_high_bytes = 768 * MIB;
            }
            7 => {
                g.request.target_limit_bytes = 64 * MIB;
                g.request.target_high_bytes = 48 * MIB;
            }
            8 => {
                g.request.timeout_millis = 60_001;
                g.deadline_unix_millis = g.issued_unix_millis + 60_001;
            }
            9 => g.request.operation_id = "unicode-操作".into(),
            10 => r.spec.sandbox.elastic_memory = None,
            _ => r.state = EnvironmentState::Paused,
        }
        assert!(
            adapter.apply_memory(&r, &g, session, epoch).await.is_err(),
            "case {case}"
        );
    }
    assert!(memory.grows.lock().unwrap().is_empty());
    assert!(memory.shrinks.lock().unwrap().is_empty());
}
#[tokio::test]
async fn physical_memory_requires_complete_reply_and_independent_unfrozen_readback() {
    for case in 0..9 {
        let memory = MemoryServer::default();
        *memory.stats.lock().unwrap() = Some(physical_stats());
        let (adapter, _harness, record) = memory_fixture(memory.clone()).await;
        let g = grant(&adapter.observe_memory(&record).await.unwrap(), 256 * MIB);
        let mut actual = physical_stats();
        actual.memory_limit_bytes = 256 * MIB;
        actual.memory_high_bytes = 192 * MIB;
        actual.resource_revision = 2;
        actual.resource_operation_id = g.request.operation_id.clone();
        let mut reply = ResizeResourcesResponse {
            actual: Some(actual),
            complete: true,
            update_error: String::new(),
        };
        match case {
            0 => reply.complete = false,
            1 => reply.update_error = "partial write".into(),
            2 => reply.actual = None,
            3 => reply.actual.as_mut().unwrap().resource_generation = "other".into(),
            4 => reply.actual.as_mut().unwrap().resource_operation_id = "other".into(),
            5 => reply.actual.as_mut().unwrap().frozen = true,
            6 => reply.actual.as_mut().unwrap().resource_operation_pending = true,
            7 => reply.actual.as_mut().unwrap().resource_revision = 3,
            _ => reply.actual.as_mut().unwrap().memory_high_known = false,
        }
        *memory.reply.lock().unwrap() = Some(reply);
        assert!(
            adapter
                .apply_memory(&record, &g, "session-1", 2)
                .await
                .is_err(),
            "case {case}"
        );
        assert_eq!(memory.grows.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn physical_memory_requires_durable_intent_before_dispatch() {
    let memory = MemoryServer::default();
    *memory.stats.lock().unwrap() = Some(physical_stats());
    let (adapter, harness, record) = memory_fixture(memory.clone()).await;
    let g = grant(&adapter.observe_memory(&record).await.unwrap(), 256 * MIB);
    std::fs::remove_file(harness._directory.path().join("operations.sqlite")).unwrap();
    assert!(adapter
        .apply_memory(&record, &g, "session-1", 2)
        .await
        .is_err());
    assert!(memory.grows.lock().unwrap().is_empty());
}
#[tokio::test]
async fn physical_memory_cancelled_caller_keeps_lock_and_intent_until_reply() {
    let memory = MemoryServer {
        release: Arc::new(Semaphore::new(0)),
        ..Default::default()
    };
    *memory.stats.lock().unwrap() = Some(physical_stats());
    let (adapter, harness, record) = memory_fixture(memory.clone()).await;
    let g = grant(&adapter.observe_memory(&record).await.unwrap(), 256 * MIB);
    let task = {
        let a = adapter.clone();
        let r = record.clone();
        let g = g.clone();
        tokio::spawn(async move { a.apply_memory(&r, &g, "session-1", 2).await })
    };
    tokio::time::timeout(Duration::from_secs(3), memory.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let database = harness._directory.path().join("operations.sqlite");
    let db = rusqlite::Connection::open(&database).unwrap();
    let payload: String = db
        .query_row(
            "SELECT payload FROM operations WHERE runtime_id=?1",
            [&record.runtime.id],
            |row| row.get(0),
        )
        .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(
        payload["memory"]["authorization"],
        serde_json::to_value(&g).unwrap()
    );
    assert_eq!(
        payload["memory"]["request_sha256"].as_str().unwrap().len(),
        64
    );
    let version: u32 = db
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 2);
    drop(db);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let delete = {
        let a = adapter.clone();
        let id = record.runtime.id.clone();
        tokio::spawn(async move { a.remove(&id).await })
    };
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(!delete.is_finished());
    memory.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(3), delete)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(adapter);
    let adapter = reconnect_with_journal(&harness).await;
    adapter.remove(&record.runtime.id).await.unwrap();
    assert_eq!(memory.grows.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn physical_memory_rechecks_deadline_after_waiting_for_serial_lock() {
    let memory = MemoryServer {
        release: Arc::new(Semaphore::new(0)),
        ..Default::default()
    };
    *memory.stats.lock().unwrap() = Some(physical_stats());
    let (adapter, _harness, record) = memory_fixture(memory.clone()).await;
    let g = grant(&adapter.observe_memory(&record).await.unwrap(), 256 * MIB);
    let first = {
        let a = adapter.clone();
        let r = record.clone();
        let g = g.clone();
        tokio::spawn(async move { a.apply_memory(&r, &g, "session-1", 2).await })
    };
    tokio::time::timeout(Duration::from_secs(3), memory.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let mut short = g.clone();
    short.request.operation_id = "queued-expiry".into();
    short.request.timeout_millis = 100;
    short.deadline_unix_millis = short.issued_unix_millis + 100;
    let second = {
        let a = adapter.clone();
        let r = record.clone();
        tokio::spawn(async move { a.apply_memory(&r, &short, "session-1", 2).await })
    };
    tokio::time::sleep(Duration::from_millis(150)).await;
    memory.release.add_permits(1);
    first.await.unwrap().unwrap();
    assert!(second.await.unwrap().is_err());
    assert_eq!(memory.grows.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn physical_memory_completed_replay_rejects_changed_fingerprint() {
    let memory = MemoryServer::default();
    *memory.stats.lock().unwrap() = Some(physical_stats());
    let (adapter, _harness, record) = memory_fixture(memory.clone()).await;
    let mut g = grant(&adapter.observe_memory(&record).await.unwrap(), 256 * MIB);
    adapter
        .apply_memory(&record, &g, "session-1", 2)
        .await
        .unwrap();
    g.issued_unix_millis += 1;
    g.deadline_unix_millis += 1;
    assert!(adapter
        .apply_memory(&record, &g, "session-1", 2)
        .await
        .is_err());
    assert_eq!(memory.grows.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn physical_memory_success_reply_cannot_replace_independent_readback() {
    for case in 0..3 {
        let memory = MemoryServer::default();
        *memory.stats.lock().unwrap() = Some(physical_stats());
        let (adapter, _harness, record) = memory_fixture(memory.clone()).await;
        let g = grant(&adapter.observe_memory(&record).await.unwrap(), 256 * MIB);
        let mut changed = physical_stats();
        changed.memory_limit_bytes = 256 * MIB;
        changed.memory_high_bytes = 192 * MIB;
        changed.resource_revision = 2;
        changed.resource_operation_id = g.request.operation_id.clone();
        match case {
            0 => changed.frozen = true,
            1 => changed.resource_operation_id = "foreign".into(),
            _ => changed.memory_high_known = false,
        }
        *memory.after_reply.lock().unwrap() = Some(changed);
        assert!(
            adapter
                .apply_memory(&record, &g, "session-1", 2)
                .await
                .is_err(),
            "case {case}: an apparently complete reply must not settle mismatched current state"
        );
        assert_eq!(memory.grows.lock().unwrap().len(), 1);
    }
}

fn expired_grant(sample: &adx_core::memory::MemorySample, target: u64) -> MemoryAuthorization {
    let mut value = grant(sample, target);
    value.issued_unix_millis = 1;
    value.deadline_unix_millis = 1 + value.request.timeout_millis;
    value
}
#[tokio::test]
async fn physical_memory_resolution_fences_expired_request_and_reopens_journal_without_mutation() {
    let memory = MemoryServer::default();
    *memory.stats.lock().unwrap() = Some(physical_stats());
    let (adapter, harness, record) = memory_fixture(memory.clone()).await;
    let request = expired_grant(&adapter.observe_memory(&record).await.unwrap(), 256 * MIB);
    let actual = adapter.resolve_memory(&record, &request).await.unwrap();
    assert_eq!(actual.observation, request.request.unchanged().unwrap());
    let wire = memory.resolutions.lock().unwrap()[0].clone();
    assert!(!wire.shrink);
    let original = wire.original.unwrap();
    assert_eq!(
        original.deadline_unix_ms,
        request.deadline_unix_millis as i64
    );
    assert_eq!(
        original.expected_revision,
        request.request.source.resource_revision
    );
    assert_eq!(original.operation_id, request.request.operation_id);
    assert_eq!(original.expected.unwrap().memory_limit_bytes, 128 * MIB);
    assert_eq!(original.target.unwrap().memory_limit_bytes, 256 * MIB);
    assert!(memory.grows.lock().unwrap().is_empty());
    assert!(memory.shrinks.lock().unwrap().is_empty());
    drop(adapter);
    let adapter = reconnect_with_journal(&harness).await;
    assert_eq!(
        adapter.observe_memory(&record).await.unwrap().observation,
        actual.observation
    );
    let mut changed = request.clone();
    changed.deadline_unix_millis += 1;
    changed.issued_unix_millis += 1;
    assert!(adapter.resolve_memory(&record, &changed).await.is_err());
    assert_eq!(
        memory.resolutions.lock().unwrap().len(),
        1,
        "changed fingerprint must be rejected before RPC"
    );
    let next = grant(&actual, 256 * MIB);
    adapter
        .apply_memory(&record, &next, "session-1", 2)
        .await
        .unwrap();
    assert_eq!(memory.grows.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn physical_memory_resolution_lost_reply_is_observed_after_journal_reopen() {
    let memory = MemoryServer {
        lose_reply: true,
        ..Default::default()
    };
    *memory.stats.lock().unwrap() = Some(physical_stats());
    let (adapter, harness, record) = memory_fixture(memory.clone()).await;
    let request = expired_grant(&adapter.observe_memory(&record).await.unwrap(), 256 * MIB);
    assert!(adapter.resolve_memory(&record, &request).await.is_err());
    drop(adapter);
    let adapter = reconnect_with_journal(&harness).await;
    assert_eq!(
        adapter.observe_memory(&record).await.unwrap().observation,
        request.request.unchanged().unwrap()
    );
    assert_eq!(memory.resolutions.lock().unwrap().len(), 1);
    assert!(memory.grows.lock().unwrap().is_empty());
}
#[tokio::test]
async fn physical_memory_resolution_retains_unknown_result_and_rechecks_independent_stats() {
    for case in 0..7 {
        let memory = MemoryServer::default();
        *memory.stats.lock().unwrap() = Some(physical_stats());
        let (adapter, _harness, record) = memory_fixture(memory.clone()).await;
        let request = expired_grant(&adapter.observe_memory(&record).await.unwrap(), 256 * MIB);
        let mut actual = physical_stats();
        actual.resource_revision = 2;
        actual.resource_operation_id = request.request.operation_id.clone();
        let mut response = ResizeResourcesResponse {
            actual: Some(actual.clone()),
            complete: false,
            update_error: String::new(),
        };
        match case {
            0 => response.complete = true,
            1 => response.actual.as_mut().unwrap().resource_revision = 3,
            2 => response.actual.as_mut().unwrap().resource_operation_id = "foreign".into(),
            3 => response.actual.as_mut().unwrap().resource_operation_pending = true,
            4 => response.actual.as_mut().unwrap().frozen = true,
            5 => response.actual.as_mut().unwrap().resource_generation = "foreign".into(),
            _ => {
                actual.memory_limit_bytes = 256 * MIB;
                actual.memory_high_bytes = 192 * MIB;
                *memory.after_reply.lock().unwrap() = Some(actual);
            }
        }
        *memory.reply.lock().unwrap() = Some(response);
        assert!(
            adapter.resolve_memory(&record, &request).await.is_err(),
            "case {case}"
        );
        assert!(memory.grows.lock().unwrap().is_empty());
        assert!(memory.shrinks.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn physical_memory_resolution_never_dispatches_a_live_grant_from_old_authority() {
    let memory = MemoryServer::default();
    *memory.stats.lock().unwrap() = Some(physical_stats());
    let (adapter, _harness, record) = memory_fixture(memory.clone()).await;
    let mut request = grant(&adapter.observe_memory(&record).await.unwrap(), 256 * MIB);
    request.node_session_id = "old-session".into();
    request.coordinator_epoch = 1;
    assert!(adapter.resolve_memory(&record, &request).await.is_err());
    assert_eq!(memory.resolutions.lock().unwrap().len(), 1);
    assert_eq!(
        memory
            .stats
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .resource_revision,
        1
    );
    assert!(memory.grows.lock().unwrap().is_empty());
    assert!(memory.shrinks.lock().unwrap().is_empty());
}

fn partial_stats(request: &MemoryAuthorization) -> StatsResponse {
    let mut stats = physical_stats();
    stats.resource_revision = request.request.source.resource_revision + 1;
    stats.resource_operation_id = request.request.operation_id.clone();
    stats.memory_limit_bytes = request.request.target_limit_bytes;
    stats.memory_high_bytes = request.request.source.memory_high_bytes;
    stats
}
#[tokio::test]
async fn physical_memory_retirement_proves_closed_partial_growth_and_shrink_after_journal_reopen() {
    for shrinking in [false, true] {
        let memory = MemoryServer::default();
        let mut source = physical_stats();
        if shrinking {
            source.memory_limit_bytes = 256 * MIB;
            source.memory_high_bytes = 192 * MIB;
        }
        *memory.stats.lock().unwrap() = Some(source);
        let (adapter, harness, record) = memory_fixture(memory.clone()).await;
        let request = expired_grant(
            &adapter.observe_memory(&record).await.unwrap(),
            if shrinking { 128 * MIB } else { 256 * MIB },
        );
        let partial = partial_stats(&request);
        *memory.stats.lock().unwrap() = Some(partial.clone());
        *memory.reply.lock().unwrap() = Some(ResizeResourcesResponse {
            actual: Some(partial.clone()),
            complete: false,
            update_error: "partial write".into(),
        });
        assert!(adapter.resolve_memory(&record, &request).await.is_err());
        assert!(
            adapter
                .memory_retirement_required(&record)
                .await
                .expect("closed partial proof"),
            "partial growth and shrink require explicit failure cleanup"
        );
        drop(adapter);
        let reopened = reconnect_with_journal(&harness).await;
        assert!(reopened.memory_retirement_required(&record).await.unwrap());
        assert_eq!(*memory.stats.lock().unwrap(), Some(partial));
        assert!(memory.grows.lock().unwrap().is_empty());
        assert!(memory.shrinks.lock().unwrap().is_empty());
        assert_eq!(
            memory
                .resolutions
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .original
                .as_ref()
                .unwrap()
                .deadline_unix_ms,
            request.deadline_unix_millis as i64
        );
    }
}
#[tokio::test]
async fn physical_memory_retirement_rejects_unknown_foreign_live_or_inconsistent_results() {
    for case in 0..18 {
        let memory = MemoryServer::default();
        *memory.stats.lock().unwrap() = Some(physical_stats());
        let (adapter, _harness, record) = memory_fixture(memory.clone()).await;
        let sample = adapter.observe_memory(&record).await.unwrap();
        let request = if case == 13 {
            grant(&sample, 256 * MIB)
        } else {
            expired_grant(&sample, 256 * MIB)
        };
        let mut partial = partial_stats(&request);
        // Save the original intent through the resolver; this never dispatches writes.
        *memory.stats.lock().unwrap() = Some(partial.clone());
        *memory.reply.lock().unwrap() = Some(ResizeResourcesResponse {
            actual: Some(partial.clone()),
            complete: false,
            update_error: String::new(),
        });
        assert!(adapter.resolve_memory(&record, &request).await.is_err());
        let mut complete = false;
        match case {
            0 => partial.resource_generation = "foreign".into(),
            1 => partial.resource_revision += 1,
            2 => partial.resource_operation_id = "foreign".into(),
            3 => partial.resource_operation_pending = true,
            4 => partial.frozen = true,
            5 => partial.frozen_known = false,
            6 => partial.memory_limit_known = false,
            7 => partial.memory_high_known = false,
            8 => partial.memory_high_unlimited = true,
            9 => partial.cpu_quota_usec += 1,
            10 => partial.memory_limit_bytes = 512 * MIB,
            11 => partial.memory_high_bytes = 80 * MIB,
            12 => complete = true,
            13 => (),
            14 => partial.cpu_period_usec += 1,
            15 => partial.elastic_policy = "{}".into(),
            16 => partial.resource_revision_known = false,
            _ => {
                let mut other = partial.clone();
                other.memory_high_bytes = 192 * MIB;
                *memory.after_reply.lock().unwrap() = Some(other);
            }
        }
        *memory.stats.lock().unwrap() = Some(partial.clone());
        *memory.reply.lock().unwrap() = Some(ResizeResourcesResponse {
            actual: Some(partial),
            complete,
            update_error: String::new(),
        });
        assert!(
            !matches!(adapter.memory_retirement_required(&record).await, Ok(true)),
            "case {case} must not authorize cleanup"
        );
        assert!(memory.grows.lock().unwrap().is_empty());
        assert!(memory.shrinks.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn physical_memory_retirement_requires_original_journal_and_does_not_reject_exact_results() {
    let memory = MemoryServer::default();
    *memory.stats.lock().unwrap() = Some(physical_stats());
    let (adapter, _harness, record) = memory_fixture(memory.clone()).await;
    let request = expired_grant(&adapter.observe_memory(&record).await.unwrap(), 256 * MIB);
    *memory.stats.lock().unwrap() = Some(partial_stats(&request));
    assert!(!adapter.memory_retirement_required(&record).await.unwrap());
    assert!(memory.resolutions.lock().unwrap().is_empty());
    let mut result = partial_stats(&request);
    for target in [false, true] {
        result.memory_limit_bytes = if target { 256 * MIB } else { 128 * MIB };
        result.memory_high_bytes = result.memory_limit_bytes * 3 / 4;
        *memory.stats.lock().unwrap() = Some(result.clone());
        *memory.reply.lock().unwrap() = Some(ResizeResourcesResponse {
            actual: Some(result.clone()),
            complete: target,
            update_error: String::new(),
        });
        adapter.resolve_memory(&record, &request).await.unwrap();
        assert!(!adapter.memory_retirement_required(&record).await.unwrap());
    }
}

#[tokio::test]
async fn physical_memory_retirement_requires_confirmed_daemon_reply() {
    let memory = MemoryServer {
        lose_reply: true,
        ..Default::default()
    };
    *memory.stats.lock().unwrap() = Some(physical_stats());
    let (adapter, _harness, record) = memory_fixture(memory.clone()).await;
    let request = expired_grant(&adapter.observe_memory(&record).await.unwrap(), 256 * MIB);
    *memory.stats.lock().unwrap() = Some(partial_stats(&request));
    assert!(adapter.resolve_memory(&record, &request).await.is_err());
    assert!(adapter.memory_retirement_required(&record).await.is_err());
    assert!(memory.grows.lock().unwrap().is_empty());
    assert!(memory.shrinks.lock().unwrap().is_empty());
}

fn elastic_restore_permission() -> adx_core::execution::ExecutionAuthorization {
    let mut request = prepared_execution_permission().request;
    request.source.spec = elastic_spec();
    request.source.state = EnvironmentState::Paused;
    request.source.restart_pending = false;
    request.source.checkpoint = Some(adx_core::RestorePoint {
        source_resources: None,
        origin: None,
        id: "elastic-checkpoint".into(),
        artifact: adx_core::CheckpointArtifact {
            storage: "shared".into(),
            location: "/checkpoint".into(),
            size_bytes: 8,
        },
        expires_at_unix_seconds: u64::MAX,
        source_runtime_id: request.source.runtime.id.clone(),
    });
    request.kind = adx_core::execution::ExecutionKind::Resume;
    request.timeout_millis = 60_000;
    execution_authority::authorize(&request).unwrap()
}

fn assert_elastic_restore_request(request: &StartRequest) {
    assert_eq!(
        request.resources["Memory"], 512.0,
        "restore must use the reserved ceiling while loading the checkpoint"
    );
    assert_eq!(request.resources["CPU"], 500.0);
    let policy: serde_json::Value =
        serde_json::from_str(&request.labels["akernel.scheduler/elastic-resources"]).unwrap();
    assert_eq!(
        policy,
        json!({"version":1,"CPU":{"initial":500,"ceiling":500},"Memory":{"initial":128,"ceiling":512}})
    );
    assert!(request.checkpoint_info.is_some());
}

#[tokio::test]
async fn elastic_restore_dispatches_reserved_ceiling_and_preserves_original_policy() {
    let server = Server::default();
    let (adapter, harness) = connect(server.clone()).await;
    let specification = elastic_spec();
    let path = harness._directory.path().join("checkpoint");
    adapter
        .restore(&specification, "i-1-r2", 1, &[], &path)
        .await
        .unwrap();
    let starts = server.starts.lock().unwrap();
    assert_eq!(starts.len(), 1);
    assert_elastic_restore_request(&starts[0]);
    assert_eq!(specification.execution_resources().memory_bytes, 128 * MIB);
    assert_eq!(
        start_request(&specification, "other-1", 1, &[], &Config::default())
            .unwrap()
            .resources["Memory"],
        128.0
    );
}

#[tokio::test]
async fn elastic_authorized_restore_replays_original_ceiling_after_journal_reopen() {
    let server = Server::default();
    let (adapter, harness) = connect(server.clone()).await;
    let grant = elastic_restore_permission();
    adapter.prepare_execution(&grant.target).await.unwrap();
    drop(adapter);
    let adapter = reconnect_with_journal(&harness).await;
    let path = harness._directory.path().join("checkpoint");
    let ip = adapter
        .execute_authorized(
            &grant,
            Some(adxlet::ExecutionRestore {
                path: &path,
                origin: None,
            }),
        )
        .await
        .unwrap();
    drop(adapter);
    let adapter = reconnect_with_journal(&harness).await;
    assert_eq!(
        adapter
            .execute_authorized(
                &grant,
                Some(adxlet::ExecutionRestore {
                    path: &path,
                    origin: None
                })
            )
            .await
            .unwrap(),
        ip
    );
    let starts = server.starts.lock().unwrap();
    assert_eq!(
        starts.len(),
        1,
        "journal replay cannot start another physical execution"
    );
    assert_elastic_restore_request(&starts[0]);
    assert_eq!(
        grant.target.spec.execution_resources().memory_bytes,
        128 * MIB
    );
    assert_eq!(
        grant.target.spec.execution_ceiling().memory_bytes,
        512 * MIB
    );
}

#[tokio::test]
async fn elastic_expired_restore_permission_cannot_dispatch_ceiling() {
    let server = Server::default();
    let (adapter, harness) = connect(server.clone()).await;
    let mut grant = elastic_restore_permission();
    adapter.prepare_execution(&grant.target).await.unwrap();
    grant.issued_unix_millis -= 120_000;
    grant.deadline_unix_millis -= 120_000;
    let path = harness._directory.path().join("checkpoint");
    assert!(adapter
        .execute_authorized(
            &grant,
            Some(adxlet::ExecutionRestore {
                path: &path,
                origin: None
            })
        )
        .await
        .is_err());
    assert!(server.starts.lock().unwrap().is_empty());
}

#[tokio::test]
async fn fixed_restore_keeps_its_original_requested_bound() {
    let server = Server::default();
    let (adapter, harness) = connect(server.clone()).await;
    let specification = spec();
    let expected = start_request(&specification, "i-1-r2", 1, &[], &Config::default()).unwrap();
    let path = harness._directory.path().join("checkpoint");
    adapter
        .restore(&specification, "i-1-r2", 1, &[], &path)
        .await
        .unwrap();
    let starts = server.starts.lock().unwrap();
    assert_eq!(starts.len(), 1);
    assert_eq!(starts[0].resources, expected.resources);
    assert!(!starts[0]
        .labels
        .contains_key("akernel.scheduler/elastic-resources"));
}

#[tokio::test]
async fn independent_minimum_keeps_birth_and_authorizes_a_lower_physical_limit() {
    let memory = MemoryServer::default();
    let mut stats = physical_stats();
    stats.elastic_policy = json!({"version":2,"CPU":{"initial":500,"ceiling":500},
        "Memory":{"initial":128,"ceiling":512},"memory_minimum_mib":64})
    .to_string();
    *memory.stats.lock().unwrap() = Some(stats);
    let mut record = running_record();
    record
        .spec
        .sandbox
        .elastic_memory
        .as_mut()
        .unwrap()
        .minimum_bytes = Some(64 * MIB);
    let (adapter, _harness) = connect(Server {
        memory: memory.clone(),
        ..Default::default()
    })
    .await;
    adapter
        .start(
            &record.spec,
            &record.runtime.id,
            record.assignment.generation,
            &[],
        )
        .await
        .unwrap();
    let birth = adapter.observe_memory(&record).await.unwrap();
    assert_eq!(birth.observation.memory_limit_bytes, 128 * MIB);
    let shrink = grant(&birth, 64 * MIB);
    let after = adapter
        .apply_memory(&record, &shrink, "session-1", 2)
        .await
        .unwrap();
    assert_eq!(after.observation, shrink.request.completion().unwrap());
    let below = grant(&after, 32 * MIB);
    assert!(adapter
        .apply_memory(&record, &below, "session-1", 2)
        .await
        .is_err());
    assert_eq!(memory.shrinks.lock().unwrap().len(), 1);
}
