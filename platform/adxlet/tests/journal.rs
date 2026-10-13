mod execution_authority;
use adx_core::{
    Assignment, EnvironmentRecord, EnvironmentSpec, EnvironmentState, Error, Resources, Result,
};
use adxlet::{journal::JournalSink, Durability, StateSink};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Coordinator {
    failure: Mutex<Option<Error>>,
    records: Mutex<Vec<EnvironmentRecord>>,
}
#[async_trait]
impl StateSink for Coordinator {
    async fn authorize_execution(
        &self,
        request: &adx_core::execution::ExecutionRequest,
    ) -> adx_core::Result<adx_core::execution::ExecutionAuthorization> {
        if let Some(error) = self.failure.lock().unwrap().clone() {
            return Err(error);
        }
        execution_authority::authorize(request)
    }
    async fn commit(&self, record: &EnvironmentRecord) -> Result<Durability> {
        if let Some(e) = self.failure.lock().unwrap().clone() {
            return Err(e);
        }
        self.records.lock().unwrap().push(record.clone());
        Ok(Durability::Published)
    }
}
fn record(revision: u64) -> EnvironmentRecord {
    EnvironmentRecord {
        restart_attempts: 0,
        restart_pending: false,
        spec: EnvironmentSpec {
            runtime_profile: None,
            snapshot_id: None,
            lifecycle: Default::default(),
            env: Default::default(),
            scheduling: Default::default(),
            id: "i".into(),
            tenant_id: "t".into(),
            image: "image".into(),
            runtime_class: "runc".into(),
            resources: Resources {
                cpu_millis: 1,
                memory_bytes: 1,
                disk_bytes: 0,
            },
            priority: 0,
            sandbox: Default::default(),
        },
        assignment: Assignment {
            environment_id: "i".into(),
            node_id: "n".into(),
            shard_id: 0,
            generation: 1,
            devices: vec![],
        },
        runtime: adx_core::Runtime {
            id: "i-1".into(),
            ip: Some("10.0.0.2".parse().unwrap()),
        },
        state: EnvironmentState::Running,
        revision,
        resources_held: true,
        checkpoint: None,
        last_operation: None,
    }
}

#[tokio::test]
async fn healthy_commits_do_not_create_or_require_a_local_database() {
    let coordinator = Arc::new(Coordinator::default());
    let tmp = tempfile::tempdir().unwrap();
    let not_directory = tmp.path().join("file");
    std::fs::write(&not_directory, "not a directory").unwrap();
    let sink = JournalSink::new(not_directory.join("journal.sqlite"), coordinator.clone());
    assert_eq!(
        sink.commit(&record(2)).await.unwrap(),
        Durability::Published
    );
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    assert!(sink.commit(&record(3)).await.is_err());
}

#[tokio::test]
async fn outage_is_durable_and_replays_in_order_before_new_results() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("journal.sqlite");
    let coordinator = Arc::new(Coordinator::default());
    let sink = JournalSink::new(path.clone(), coordinator.clone());
    assert_eq!(
        sink.commit(&record(2)).await.unwrap(),
        Durability::Published
    );
    assert!(!path.exists());
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    for revision in [3, 4, 4] {
        assert_eq!(
            sink.commit(&record(revision)).await.unwrap(),
            Durability::Journaled
        );
    }
    assert_eq!(sink.pending().await.unwrap(), 2);
    assert_eq!(sink.commit(&record(3)).await, Err(Error::Conflict));
    drop(sink);
    let sink = JournalSink::new(path, coordinator.clone());
    // A restart must first obtain the authoritative catalog, even if the caller
    // knows a locally journaled identity.
    assert!(sink.commit(&record(5)).await.is_err());
    *coordinator.failure.lock().unwrap() = None;
    sink.recover(&[record(2)]).await.unwrap();
    assert_eq!(
        sink.commit(&record(5)).await.unwrap(),
        Durability::Published
    );
    assert_eq!(sink.pending().await.unwrap(), 0);
    assert_eq!(
        coordinator
            .records
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.revision)
            .collect::<Vec<_>>(),
        [2, 3, 4, 5]
    );
}

#[tokio::test]
async fn rejected_ownership_is_not_treated_as_an_outage() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("journal.sqlite");
    let coordinator = Arc::new(Coordinator::default());
    *coordinator.failure.lock().unwrap() = Some(Error::Conflict);
    let sink = JournalSink::new(path.clone(), coordinator);
    assert_eq!(sink.commit(&record(2)).await, Err(Error::Conflict));
    assert!(!path.exists());
}

#[tokio::test]
async fn recovery_discards_retired_ownership_and_already_committed_results() {
    let tmp = tempfile::tempdir().unwrap();
    let path = tmp.path().join("journal.sqlite");
    let coordinator = Arc::new(Coordinator::default());
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    let sink = JournalSink::new(path.clone(), coordinator.clone());
    sink.commit(&record(3)).await.unwrap();
    drop(sink);
    let sink = JournalSink::new(path.clone(), coordinator.clone());
    *coordinator.failure.lock().unwrap() = None;
    let mut replacement = record(4);
    replacement.assignment.generation = 2;
    sink.recover(&[replacement]).await.unwrap();
    assert!(coordinator.records.lock().unwrap().is_empty());
    assert_eq!(sink.pending().await.unwrap(), 0);

    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    sink.commit(&record(4)).await.unwrap();
    drop(sink);
    *coordinator.failure.lock().unwrap() = None;
    let sink = JournalSink::new(path, coordinator.clone());
    sink.recover(&[record(4)]).await.unwrap();
    assert!(coordinator.records.lock().unwrap().is_empty());
    assert_eq!(sink.pending().await.unwrap(), 0);
}

#[tokio::test]
async fn authoritative_failure_discards_newer_local_running_result() {
    let root = tempfile::tempdir().unwrap();
    let coordinator = Arc::new(Coordinator::default());
    let journal = JournalSink::new(root.path().join("outage.sqlite"), coordinator.clone());
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    assert_eq!(
        journal.commit(&record(100)).await.unwrap(),
        Durability::Journaled
    );
    *coordinator.failure.lock().unwrap() = None;
    let mut failed = record(3);
    failed.state = EnvironmentState::Failed;
    failed.resources_held = false;
    failed.runtime.ip = None;
    journal.recover(&[failed]).await.unwrap();
    journal.flush().await.unwrap();
    assert!(coordinator.records.lock().unwrap().is_empty());
}

#[tokio::test]
async fn local_result_journal_never_authorizes_a_new_execution_during_outage() {
    let tmp = tempfile::tempdir().unwrap();
    let coordinator = Arc::new(Coordinator::default());
    let sink = JournalSink::new(tmp.path().join("journal.sqlite"), coordinator.clone());
    let mut source = record(3);
    source.state = EnvironmentState::Paused;
    source.resources_held = false;
    source.runtime.ip = None;
    source.checkpoint = Some(adx_core::RestorePoint {
        id: "checkpoint".into(),
        origin: None,
        source_runtime_id: "i-1".into(),
        expires_at_unix_seconds: u64::MAX,
        artifact: adx_core::CheckpointArtifact {
            storage: "local".into(),
            location: "artifact".into(),
            size_bytes: 1,
        },
    });
    *coordinator.failure.lock().unwrap() = Some(Error::Unavailable("offline".into()));
    assert_eq!(sink.commit(&source).await.unwrap(), Durability::Journaled);
    let request = adx_core::execution::ExecutionRequest {
        source: source.clone(),
        kind: adx_core::execution::ExecutionKind::Resume,
        operation_id: "resume".into(),
        expected_revision: 3,
        timeout_millis: 1_000,
    };
    assert!(matches!(
        sink.authorize_execution(&request).await,
        Err(Error::Unavailable(_))
    ));
    assert_eq!(sink.pending().await.unwrap(), 1);
    assert!(coordinator.records.lock().unwrap().is_empty());
    *coordinator.failure.lock().unwrap() = None;
    let grant = sink.authorize_execution(&request).await.unwrap();
    assert_eq!(grant.request.source, source);
    assert_eq!(sink.pending().await.unwrap(), 0);
    assert_eq!(*coordinator.records.lock().unwrap(), vec![source]);
}
