use super::*;
use adx_core::memory::ElasticMemoryPolicy;
use adx_core::{EnvironmentState, Runtime};

const MIB: u64 = 1 << 20;

fn resources(memory_mib: u64) -> Resources {
    Resources {
        cpu_millis: 500,
        memory_bytes: memory_mib * MIB,
        disk_bytes: 1024 * MIB,
    }
}

fn record(minimum: Option<u64>) -> EnvironmentRecord {
    EnvironmentRecord {
        spec: EnvironmentSpec {
            id: "environment".into(),
            tenant_id: "tenant".into(),
            image: "fixed-image".into(),
            runtime_class: "runsc".into(),
            resources: resources(2048),
            priority: 0,
            runtime_profile: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            sandbox: adx_core::sandbox::SandboxOptions {
                limits: adx_core::sandbox::ResourceLimits {
                    cpu_millis: 500,
                    memory_bytes: 2048 * MIB,
                    disk_bytes: 1024 * MIB,
                },
                elastic_memory: Some(ElasticMemoryPolicy {
                    high_per_mille: 750,
                    minimum_bytes: minimum.map(|value| value * MIB),
                }),
                ..Default::default()
            },
        },
        assignment: Assignment {
            environment_id: "environment".into(),
            node_id: "node".into(),
            shard_id: 0,
            generation: 7,
            devices: vec![],
        },
        state: EnvironmentState::Running,
        revision: 1,
        runtime: Runtime {
            id: "environment-7".into(),
            ip: None,
        },
        resources_held: true,
        restart_attempts: 0,
        restart_pending: false,
        checkpoint: None,
        last_operation: None,
    }
}

fn admission(record: &EnvironmentRecord) -> Admission {
    let mut ledger = ResourceLedger::new(Resources {
        cpu_millis: 3000,
        memory_bytes: 4096 * MIB,
        disk_bytes: 4096 * MIB,
    });
    ledger.reserve(&record.runtime.id, resources(2048)).unwrap();
    ledger.reserve("neighbor", resources(256)).unwrap();
    Admission {
        ledger,
        devices: DeviceLedger::default(),
        devices_valid_until: None,
        valid_until: Some(Instant::now() + Duration::from_secs(60)),
        maintenance: false,
        pressure: false,
        host_pressure: None,
        runtime_classes: None,
    }
}

#[test]
fn memory_hold_uses_independent_minimum_for_shrink_and_sub_birth_regrowth() {
    let record = record(Some(640));
    record.spec.validate().unwrap();
    let mut admission = admission(&record);
    admission.memory_hold(&record, resources(640)).unwrap();
    assert_eq!(
        admission.ledger.reservation(&record.runtime.id),
        Some(resources(640))
    );
    assert_eq!(
        admission.ledger.reservation("neighbor"),
        Some(resources(256))
    );
    assert_eq!(admission.ledger.used().memory_bytes, (640 + 256) * MIB);
    admission.memory_hold(&record, resources(960)).unwrap();
    assert_eq!(
        admission.ledger.reservation(&record.runtime.id),
        Some(resources(960))
    );
    assert_eq!(
        admission.ledger.reservation("neighbor"),
        Some(resources(256))
    );
    assert_eq!(record.spec.resources, resources(2048));
}

#[test]
fn memory_hold_rejects_regrowth_without_capacity_and_preserves_neighbors() {
    let record = record(Some(640));
    let mut admission = admission(&record);
    admission.memory_hold(&record, resources(640)).unwrap();
    admission
        .ledger
        .reserve("competitor", resources(2048))
        .unwrap();
    let used = admission.ledger.used();
    assert_eq!(
        admission.memory_hold(&record, resources(2048)),
        Err(Error::NoCapacity)
    );
    assert_eq!(admission.ledger.used(), used);
    assert_eq!(
        admission.ledger.reservation("neighbor"),
        Some(resources(256))
    );
    assert_eq!(
        admission.ledger.reservation("competitor"),
        Some(resources(2048))
    );
    assert_eq!(
        admission.ledger.reservation(&record.runtime.id),
        Some(resources(640))
    );
}

#[test]
fn memory_hold_can_reduce_under_pressure_but_cannot_regrow() {
    let record = record(Some(640));
    let mut admission = admission(&record);
    admission.pressure = true;
    admission.memory_hold(&record, resources(640)).unwrap();
    assert_eq!(
        admission.memory_hold(&record, resources(960)),
        Err(Error::NoCapacity)
    );
    assert_eq!(
        admission.ledger.reservation(&record.runtime.id),
        Some(resources(640))
    );
    assert_eq!(
        admission.ledger.reservation("neighbor"),
        Some(resources(256))
    );
}

#[test]
fn memory_hold_can_reduce_with_expired_observation_but_cannot_regrow() {
    let record = record(Some(640));
    let mut admission = admission(&record);
    admission.valid_until = None;
    admission.memory_hold(&record, resources(640)).unwrap();
    assert_eq!(
        admission.memory_hold(&record, resources(960)),
        Err(Error::NoCapacity)
    );
    assert_eq!(
        admission.ledger.reservation(&record.runtime.id),
        Some(resources(640))
    );
}

#[test]
fn memory_hold_rejects_floor_ceiling_cpu_and_disk_changes_without_mutation() {
    let record = record(Some(640));
    let mut admission = admission(&record);
    let used = admission.ledger.used();
    for target in [
        resources(639),
        resources(2049),
        Resources {
            cpu_millis: 499,
            ..resources(2048)
        },
        Resources {
            disk_bytes: 1023 * MIB,
            ..resources(2048)
        },
    ] {
        assert_eq!(admission.memory_hold(&record, target), Err(Error::Conflict));
        assert_eq!(admission.ledger.used(), used);
    }
}

#[test]
fn memory_hold_without_independent_minimum_keeps_birth_floor() {
    let record = record(None);
    let mut admission = admission(&record);
    assert_eq!(
        admission.memory_hold(&record, resources(640)),
        Err(Error::Conflict)
    );
    assert_eq!(
        admission.ledger.reservation(&record.runtime.id),
        Some(resources(2048))
    );
}

#[test]
fn memory_hold_cannot_borrow_a_foreign_runtime_reservation() {
    let mut record = record(Some(640));
    let mut admission = admission(&record);
    record.runtime.id = "foreign-runtime".into();
    let used = admission.ledger.used();
    assert_eq!(
        admission.memory_hold(&record, resources(640)),
        Err(Error::Conflict)
    );
    assert_eq!(admission.ledger.used(), used);
    assert_eq!(
        admission.ledger.reservation("neighbor"),
        Some(resources(256))
    );
}
