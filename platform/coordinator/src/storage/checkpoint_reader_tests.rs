use super::{next_result, StoredEnvironment};
use adx_core::{
    Assignment, CheckpointArtifact, CheckpointReaders, EnvironmentRecord, EnvironmentSpec,
    EnvironmentState, Error, Resources, RestorePoint, Runtime,
};

fn point(name: &str, generation: u64) -> RestorePoint {
    RestorePoint {
        id: name.into(),
        artifact: CheckpointArtifact {
            storage: "shared_fs".into(),
            location: name.into(),
            size_bytes: 1,
        },
        origin: None,
        source_runtime_id: format!("reader-env-{generation}"),
        expires_at_unix_seconds: None,
    }
}

fn running() -> EnvironmentRecord {
    let mut record = EnvironmentRecord {
        checkpoint_readers: CheckpointReaders::known_empty(),
        failure: None,
        restart_attempts: 0,
        restart_pending: false,
        spec: EnvironmentSpec {
            id: "reader-env".into(),
            tenant_id: "reader-tenant".into(),
            image: "image".into(),
            runtime_class: "runsc".into(),
            resources: Resources {
                cpu_millis: 1,
                memory_bytes: 1,
                disk_bytes: 1,
            },
            priority: 0,
            runtime_profile: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            execution: Default::default(),
        },
        assignment: Assignment {
            environment_id: "reader-env".into(),
            node_id: "reader-source".into(),
            shard_id: 0,
            generation: 1,
            devices: Vec::new(),
        },
        state: EnvironmentState::Running,
        revision: 2,
        runtime: Runtime {
            id: "reader-env-1-r1".into(),
            ip: Some("127.0.0.1".parse().unwrap()),
        },
        resources_held: true,
        checkpoint: Some(point("a", 1)),
        last_operation: None,
    };
    record.register_checkpoint_reader(point("a", 1)).unwrap();
    record
}

fn stored(record: EnvironmentRecord) -> StoredEnvironment {
    StoredEnvironment {
        reader_reservations: Vec::new(),
        reader_retirements: Vec::new(),
        resume: None,
        failed_since_unix_seconds: None,
        recovery: None,
        invalidated: false,
        spec: record.spec.clone(),
        assignment: record.assignment.clone(),
        result: Some(record),
    }
}

#[test]
fn authority_json_restart_keeps_reader_a_when_latest_checkpoint_is_b() {
    let previous = running();
    let mut next = previous.clone();
    next.revision += 1;
    next.checkpoint = Some(point("b", 1));
    let persisted: StoredEnvironment =
        serde_json::from_str(&serde_json::to_string(&stored(previous)).unwrap()).unwrap();
    assert_eq!(next_result(&persisted, &next), Ok(true));
    let persisted: StoredEnvironment =
        serde_json::from_str(&serde_json::to_string(&stored(next)).unwrap()).unwrap();
    let record = persisted.result.unwrap();
    assert_eq!(record.current_checkpoint_reader().unwrap().id, "a");
    assert_eq!(
        record
            .checkpoint_points()
            .map(|p| p.id.as_str())
            .collect::<Vec<_>>(),
        vec!["b", "a"]
    );
}

#[test]
fn running_commit_cannot_drop_its_actual_checkpoint_reader() {
    let previous = running();
    let mut next = previous.clone();
    next.revision += 1;
    next.checkpoint = Some(point("b", 1));
    next.checkpoint_readers.readers.clear();
    assert_eq!(next_result(&stored(previous), &next), Err(Error::Conflict));
}

#[test]
fn paused_commit_cannot_release_its_reader_without_a_backend_retirement_receipt() {
    let previous = running();
    let mut next = previous.clone();
    next.revision += 1;
    next.state = EnvironmentState::Paused;
    next.resources_held = false;
    next.runtime.ip = None;
    next.release_checkpoint_reader().unwrap();
    assert!(next.checkpoint_readers.reclaimable());
    assert_eq!(next_result(&stored(previous), &next), Err(Error::Conflict));
}

#[test]
fn paused_commit_releases_its_reader_only_after_exact_retirement_was_persisted() {
    let previous = running();
    let reader = previous.current_checkpoint_reader_entry().unwrap().clone();
    let mut next = previous.clone();
    next.revision += 1;
    next.state = EnvironmentState::Paused;
    next.resources_held = false;
    next.runtime.ip = None;
    next.release_checkpoint_reader().unwrap();
    let mut authority = stored(previous);
    authority
        .reader_retirements
        .push(adx_core::CheckpointReaderRetirement {
            reader,
            backend_receipt: "test-only:confirmed-retirement".into(),
        });
    assert_eq!(next_result(&authority, &next), Ok(true));
}

#[test]
fn ownership_transfer_and_invalidation_do_not_release_the_source_reader() {
    let mut previous = running();
    previous.assignment.node_id = "reader-target".into();
    previous.assignment.generation = 2;
    previous.runtime.id = "reader-env-2-r1".into();
    previous.runtime.ip = None;
    previous.resources_held = false;
    previous.state = EnvironmentState::Failed;
    previous.checkpoint = Some(point("b", 2));
    previous.validate_checkpoint_readers().unwrap();
    let mut next = previous.clone();
    next.revision += 1;
    next.checkpoint_readers.readers.clear();
    for invalidated in [false, true] {
        let mut authority = stored(previous.clone());
        authority.invalidated = invalidated;
        assert_eq!(next_result(&authority, &next), Err(Error::Conflict));
    }
}

#[test]
fn historical_unknown_readers_cannot_be_promoted_to_known_empty_by_a_commit() {
    let mut previous = running();
    previous.checkpoint_readers = Default::default();
    let authority = stored(previous.clone());
    assert_eq!(next_result(&authority, &previous), Ok(false));
    let mut next = previous;
    next.revision += 1;
    next.checkpoint_readers = CheckpointReaders::known_empty();
    assert_eq!(next_result(&authority, &next), Err(Error::Conflict));
}

#[test]
fn deleted_legacy_record_is_not_a_proof_that_all_readers_stopped() {
    let mut record = running();
    record.state = EnvironmentState::Deleted;
    record.resources_held = false;
    record.runtime.ip = None;
    record.checkpoint_readers = Default::default();
    let persisted: StoredEnvironment =
        serde_json::from_str(&serde_json::to_string(&stored(record)).unwrap()).unwrap();
    let record = persisted.result.unwrap();
    assert_eq!(record.state, EnvironmentState::Deleted);
    assert!(!record.checkpoint_readers.reclaimable());
}
