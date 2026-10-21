//! Local physical-operation facts, not a second budget or authoritative catalog.
//! A missing or corrupt entry never proves retirement of an unknown execution.
use super::{unavailable, valid_resource_generation, BackendIdentity, StartState};
use adx_core::{Error, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub(super) fn now_millis() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(unavailable)?
        .as_millis()
        .try_into()
        .map_err(unavailable)
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct OperationIntent {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_identity: Option<adx_core::history::HistoryIdentity>,
    pub id: String,
    pub kind: String,
    pub fingerprint: String,
    pub deadline_unix_millis: u64,
}
impl OperationIntent {
    pub fn new(kind: &str, fingerprint: String, duration: Duration) -> Result<Self> {
        let now = now_millis()?;
        let duration: u64 = duration.as_millis().try_into().map_err(unavailable)?;
        let deadline_unix_millis = now
            .checked_add(duration)
            .ok_or_else(|| Error::Invalid("operation deadline overflow".into()))?;
        Ok(Self {
            history_identity: None,
            id: uuid::Uuid::new_v4().to_string(),
            kind: kind.into(),
            fingerprint,
            deadline_unix_millis,
        })
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct OperationRecord {
    pub state: StartState,
    pub start: Option<OperationIntent>,
    pub action: Option<OperationIntent>,
    pub backend: Option<BackendIdentity>,
    pub retired_generation: Option<String>,
    #[serde(default)]
    pub memory: Option<super::memory::MemoryDispatch>,
}
impl Default for OperationRecord {
    fn default() -> Self {
        Self {
            state: StartState::Idle,
            start: None,
            action: None,
            backend: None,
            retired_generation: None,
            memory: None,
        }
    }
}
impl OperationRecord {
    fn validate(&self, id: &str) -> Result<()> {
        if id.is_empty() || id.len() > 4096 || id.chars().any(char::is_whitespace) {
            return Err(unavailable("invalid operation runtime identity"));
        }
        if let Some(memory) = &self.memory {
            memory.validate(
                id,
                self.backend.as_ref(),
                self.retired_generation.as_deref(),
            )?;
        }
        for intent in self.start.iter().chain(self.action.iter()) {
            if uuid::Uuid::parse_str(&intent.id).is_err()
                || !matches!(
                    intent.kind.as_str(),
                    "start" | "start_operation" | "checkpoint" | "delete"
                )
                || intent.fingerprint.len() != 64
                || intent.deadline_unix_millis == 0
                || !intent.fingerprint.bytes().all(|b| b.is_ascii_hexdigit())
            {
                return Err(unavailable("invalid durable operation intent"));
            }
        }
        if self.backend.as_ref().is_some_and(|backend| {
            backend.id.is_empty()
                || (!backend.resource_generation.is_empty()
                    && !valid_resource_generation(&backend.resource_generation))
        }) || self
            .retired_generation
            .as_ref()
            .is_some_and(|generation| !valid_resource_generation(generation))
        {
            return Err(unavailable("invalid durable physical identity"));
        }
        if let StartState::Prepared(fingerprint) | StartState::Cancelled(fingerprint) = &self.state
        {
            if fingerprint.len() != 64
                || !fingerprint.bytes().all(|b| b.is_ascii_hexdigit())
                || self.start.is_some()
                || self.action.is_some()
                || self.backend.is_some()
                || self.retired_generation.is_some()
                || self.memory.is_some()
            {
                return Err(unavailable("invalid durable unstarted execution"));
            }
        }
        let has_physical = self
            .backend
            .as_ref()
            .is_some_and(|backend| valid_resource_generation(&backend.resource_generation));
        if matches!(self.state, StartState::Idle)
            || (matches!(self.state, StartState::StartUncertain) && self.start.is_none())
            || (matches!(self.state, StartState::NotCreated)
                && (self.start.is_none()
                    || self.backend.is_some()
                    || self.retired_generation.is_some()))
            || (matches!(
                self.state,
                StartState::Started(_)
                    | StartState::Settled
                    | StartState::CheckpointUncertain
                    | StartState::DeleteUncertain
            ) && !has_physical)
            || (matches!(self.state, StartState::Failed)
                && self.backend.is_none()
                && self.retired_generation.is_none())
        {
            return Err(unavailable("incomplete durable operation outcome"));
        }
        Ok(())
    }
}

// Hash the entire request, including sensitive fields, without writing its bytes.
// Protobuf HashMap iteration order is unstable, so sort JSON maps recursively.
pub(super) fn fingerprint<T: Serialize>(request: &T) -> Result<String> {
    fn canonicalize(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(object) => {
                object.sort_keys();
                for child in object.values_mut() {
                    canonicalize(child);
                }
            }
            serde_json::Value::Array(array) => {
                for child in array {
                    canonicalize(child);
                }
            }
            _ => (),
        }
    }
    let mut value = serde_json::to_value(request).map_err(unavailable)?;
    canonicalize(&mut value);
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&value).map_err(unavailable)?)
    ))
}

#[derive(Clone)]
pub(super) struct OperationJournal {
    path: PathBuf,
    // The stable lock file is never unlinked. A second process cannot overwrite
    // facts using a stale in-memory snapshot of this same journal.
    lock: Arc<File>,
    identity: Arc<File>,
}
impl OperationJournal {
    pub async fn open(path: PathBuf) -> Result<(Self, BTreeMap<String, OperationRecord>)> {
        tokio::task::spawn_blocking(move || {
            if !path.is_absolute() {
                return Err(Error::Invalid(
                    "absolute sandboxd operation journal path required".into(),
                ));
            }
            let parent = path
                .parent()
                .ok_or_else(|| Error::Invalid("journal parent required".into()))?;
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(parent)
                .map_err(unavailable)?;
            let name = path
                .file_name()
                .ok_or_else(|| Error::Invalid("journal file name required".into()))?;
            let path = fs::canonicalize(parent).map_err(unavailable)?.join(name);
            if path
                .with_extension("sqlite.lock")
                .try_exists()
                .map_err(unavailable)?
                && !path.try_exists().map_err(unavailable)?
            {
                return Err(unavailable(
                    "existing operation journal database is missing",
                ));
            }
            let lock = Arc::new(private_file(&path.with_extension("sqlite.lock"))?);
            lock.try_lock().map_err(unavailable)?;
            let file = private_file(&path)?;
            file.sync_all().map_err(unavailable)?;
            let connection = database(&path)?;
            let mut query = connection
                .prepare("SELECT runtime_id,payload FROM operations")
                .map_err(unavailable)?;
            let rows = query
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(unavailable)?;
            let mut records = BTreeMap::new();
            for row in rows {
                let (id, payload) = row.map_err(unavailable)?;
                let record: OperationRecord =
                    serde_json::from_str(&payload).map_err(unavailable)?;
                record.validate(&id)?;
                records.insert(id, record);
            }
            File::open(path.parent().ok_or(Error::Conflict)?)
                .and_then(|file| file.sync_all())
                .map_err(unavailable)?;
            Ok((
                Self {
                    path,
                    lock,
                    identity: Arc::new(file),
                },
                records,
            ))
        })
        .await
        .map_err(unavailable)?
    }

    pub async fn save(&self, id: String, record: OperationRecord) -> Result<()> {
        record.validate(&id)?;
        let path = self.path.clone();
        let lock = self.lock.clone();
        let identity = self.identity.clone();
        tokio::task::spawn_blocking(move || {
            // Never recreate a journal removed from a running process.
            let metadata = fs::symlink_metadata(&path).map_err(unavailable)?;
            if !metadata.is_file() || metadata.permissions().mode() & 0o777 != 0o600 {
                return Err(unavailable("operation journal file changed"));
            }
            let mut connection = existing_database(&path,&lock,&identity)?;
            let transaction = connection.transaction().map_err(unavailable)?;
            let payload = serde_json::to_string(&record).map_err(unavailable)?;
            // Old binaries must refuse a journal containing memory dispatches.
            // Keep the fence after retirement rather than silently downgrading.
            let version:u32=transaction.query_row("PRAGMA user_version",[],|row|row.get(0)).map_err(unavailable)?;
            if record.memory.is_some() && version<2 {
                transaction.execute_batch("PRAGMA user_version=2;").map_err(unavailable)?;
            }
            let previous: Option<String> = transaction.query_row("SELECT payload FROM operations WHERE runtime_id=?1", [&id], |row| row.get(0)).optional().map_err(unavailable)?;
            if previous.as_ref() == Some(&payload) { return Ok(()); }
            transaction.execute("INSERT INTO operations(runtime_id,payload) VALUES (?1,?2) ON CONFLICT(runtime_id) DO UPDATE SET payload=excluded.payload", params![id, payload]).map_err(unavailable)?;
            transaction.execute("INSERT INTO operation_history(runtime_id,payload) VALUES (?1,?2)", params![id, payload]).map_err(unavailable)?;
            // The complete current record remains authoritative. Audit-only
            // transitions are a bounded window, compacted in this same commit.
            transaction.execute("DELETE FROM operation_history WHERE runtime_id=?1 AND sequence NOT IN (SELECT sequence FROM operation_history WHERE runtime_id=?1 ORDER BY sequence DESC LIMIT 8)", [&id]).map_err(unavailable)?;
            transaction.commit().map_err(unavailable)?;
            File::open(path.parent().ok_or(Error::Conflict)?).and_then(|file| file.sync_all()).map_err(unavailable)?;
            Ok(())
        }).await.map_err(unavailable)?
    }
}

fn private_file(path: &std::path::Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
        .map_err(unavailable)?;
    let metadata = file.metadata().map_err(unavailable)?;
    if !metadata.is_file() || metadata.permissions().mode() & 0o777 != 0o600 {
        return Err(unavailable(
            "operation journal requires private regular files",
        ));
    }
    Ok(file)
}
fn database(path: &std::path::Path) -> Result<Connection> {
    let connection = Connection::open(path).map_err(unavailable)?;
    connection
        .busy_timeout(Duration::from_secs(2))
        .map_err(unavailable)?;
    let version: u32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(unavailable)?;
    if version > 3 {
        return Err(unavailable("unsupported operation journal schema"));
    }
    connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
        CREATE TABLE IF NOT EXISTS operations (runtime_id TEXT PRIMARY KEY NOT NULL, payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS operation_history (sequence INTEGER PRIMARY KEY AUTOINCREMENT, runtime_id TEXT NOT NULL, payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS history_boundary (slot INTEGER PRIMARY KEY CHECK(slot=1), payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS history_pending (slot INTEGER PRIMARY KEY CHECK(slot=1), payload TEXT NOT NULL);
").map_err(unavailable)?;
    if version == 0 {
        connection
            .execute_batch("PRAGMA user_version=1;")
            .map_err(unavailable)?;
    }
    Ok(connection)
}

#[cfg(test)]
mod history_reclamation_tests {
    use super::*;

    #[tokio::test]
    async fn completed_runtime_audit_history_is_bounded_without_losing_latest_facts() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("operations.sqlite");
        let (journal, records) = OperationJournal::open(path.clone()).await.unwrap();
        assert!(records.is_empty());
        let physical_generation = uuid::Uuid::new_v4().to_string();
        let runtime_id = "history-contract-7";
        let mut expected = String::new();
        for _ in 0..24 {
            let record = OperationRecord {
                state: StartState::Failed,
                start: None,
                action: Some(
                    OperationIntent::new("delete", "a".repeat(64), Duration::from_secs(30))
                        .unwrap(),
                ),
                backend: Some(BackendIdentity {
                    id: "sbox-history-contract".into(),
                    resource_generation: physical_generation.clone(),
                }),
                retired_generation: Some(physical_generation.clone()),
                memory: None,
            };
            expected = serde_json::to_string(&record).unwrap();
            journal.save(runtime_id.into(), record).await.unwrap();
        }
        let connection = database(&path).unwrap();
        let latest: String = connection
            .query_row(
                "SELECT payload FROM operations WHERE runtime_id=?1",
                [runtime_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            latest, expected,
            "compaction must retain the complete authoritative current record"
        );
        let retained: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM operation_history WHERE runtime_id=?1",
                [runtime_id],
                |row| row.get(0),
            )
            .unwrap();
        assert!(retained <= 8, "retired runtime audit history grew to {retained} rows instead of the bounded eight-transition window");
    }
}

mod history;
#[derive(Clone, Serialize, Deserialize)]
pub(super) struct HistoryAttempt {
    pub runtime_id: String,
    pub record: OperationRecord,
}

fn existing_database(path: &std::path::Path, lock: &File, identity: &File) -> Result<Connection> {
    for (name, original) in [
        (path.to_path_buf(), identity),
        (path.with_extension("sqlite.lock"), lock),
    ] {
        let current = fs::symlink_metadata(name).map_err(unavailable)?;
        let held = original.metadata().map_err(unavailable)?;
        if !current.is_file()
            || current.permissions().mode() & 0o777 != 0o600
            || current.dev() != held.dev()
            || current.ino() != held.ino()
            || current.nlink() != 1
        {
            return Err(unavailable(
                "original operation journal file or lock changed",
            ));
        }
    }
    database(path)
}
