//! The fence and reclamation intent are committed before the daemon handshake.
use super::*;
use adx_core::history::HistoryBoundary;

fn metadata<T: serde::de::DeserializeOwned>(
    connection: &Connection,
    table: &str,
) -> Result<Option<T>> {
    let payload: Option<String> = connection
        .query_row(
            &format!("SELECT payload FROM {table} WHERE slot=1"),
            [],
            |row| row.get(0),
        )
        .optional()
        .map_err(unavailable)?;
    payload
        .map(|v| serde_json::from_str(&v).map_err(unavailable))
        .transpose()
}
fn validate_attempts(attempts: &[HistoryAttempt]) -> Result<()> {
    if attempts.len() > 32 {
        return Err(Error::Conflict);
    }
    let mut seen = std::collections::BTreeSet::new();
    for attempt in attempts {
        attempt.record.validate(&attempt.runtime_id)?;
        if !seen.insert(&attempt.runtime_id) || !attempt.record.history_terminal() {
            return Err(Error::Conflict);
        }
    }
    Ok(())
}
impl OperationRecord {
    pub(in crate::sandboxd) fn history_terminal(&self) -> bool {
        matches!(
            self.state,
            StartState::NotCreated | StartState::Cancelled(_)
        ) || matches!(self.state, StartState::Failed)
            && self
                .backend
                .as_ref()
                .is_some_and(|b| self.retired_generation.as_ref() == Some(&b.resource_generation))
    }
}
impl OperationJournal {
    pub(in crate::sandboxd) async fn history_has_facts(&self, runtime: String) -> Result<bool> {
        let path = self.path.clone();
        let lock = self.lock.clone();
        let identity = self.identity.clone();
        tokio::task::spawn_blocking(move || {
            let connection = existing_database(&path, &lock, &identity)?;
            let current: Option<String> = connection
                .query_row(
                    "SELECT payload FROM operations WHERE runtime_id=?1",
                    [&runtime],
                    |row| row.get(0),
                )
                .optional()
                .map_err(unavailable)?;
            let pending = metadata::<Vec<HistoryAttempt>>(&connection, "history_pending")?
                .unwrap_or_default();
            Ok(current.is_some() || pending.iter().any(|a| a.runtime_id == runtime))
        })
        .await
        .map_err(unavailable)?
    }
    /// Older final records discarded the sandbox ID while retaining retirement.
    /// Recover only the ID from the same already-retired complete journal fact;
    /// the audit row does not establish a new retirement or release any budget.
    pub(in crate::sandboxd) async fn recover_retired_bindings(
        &self,
        mut records: BTreeMap<String, OperationRecord>,
    ) -> Result<BTreeMap<String, OperationRecord>> {
        for (id, record) in records.iter_mut() {
            if !matches!(record.state, StartState::Failed)
                || record.retired_generation.is_none()
                || record.backend.is_some()
            {
                continue;
            }
            let path = self.path.clone();
            let lock = self.lock.clone();
            let identity = self.identity.clone();
            let runtime = id.clone();
            let current = record.clone();
            let binding=tokio::task::spawn_blocking(move||->Result<Option<BackendIdentity>>{
                let connection=existing_database(&path,&lock,&identity)?;
                let mut query=connection.prepare("SELECT payload FROM operation_history WHERE runtime_id=?1 ORDER BY sequence DESC").map_err(unavailable)?;
                let rows=query.query_map([&runtime],|row|row.get::<_,String>(0)).map_err(unavailable)?;
                for row in rows {
                    let original:OperationRecord=serde_json::from_str(&row.map_err(unavailable)?).map_err(unavailable)?;original.validate(&runtime)?;
                    if original.retired_generation!=current.retired_generation || serde_json::to_string(&original.start).map_err(unavailable)?!=serde_json::to_string(&current.start).map_err(unavailable)? || serde_json::to_string(&original.action).map_err(unavailable)?!=serde_json::to_string(&current.action).map_err(unavailable)? {continue}
                    if let Some(backend)=original.backend.filter(|b|Some(&b.resource_generation)==current.retired_generation.as_ref()){return Ok(Some(backend))}
                }
                Ok(None)
            }).await.map_err(unavailable)??;
            if let Some(backend) = binding {
                record.backend = Some(backend);
                self.save(id.clone(), record.clone()).await?;
            }
        }
        Ok(records)
    }
    pub(in crate::sandboxd) async fn history_boundary(&self) -> Result<Option<HistoryBoundary>> {
        let path = self.path.clone();
        let lock = self.lock.clone();
        let identity = self.identity.clone();
        tokio::task::spawn_blocking(move || {
            let connection = existing_database(&path, &lock, &identity)?;
            let b: Option<HistoryBoundary> = metadata(&connection, "history_boundary")?;
            if let Some(b) = &b {
                b.validate()?
            };
            Ok(b)
        })
        .await
        .map_err(unavailable)?
    }
    pub(in crate::sandboxd) async fn install_history(
        &self,
        boundary: HistoryBoundary,
        attempts: Vec<HistoryAttempt>,
    ) -> Result<Vec<HistoryAttempt>> {
        boundary.validate()?;
        validate_attempts(&attempts)?;
        let path = self.path.clone();
        let lock = self.lock.clone();
        let identity = self.identity.clone();
        tokio::task::spawn_blocking(move||{
   let mut connection=existing_database(&path,&lock,&identity)?;let transaction=connection.transaction().map_err(unavailable)?;
   if let Some(old)=metadata::<HistoryBoundary>(&transaction,"history_boundary")?{old.check_advance(&boundary)?}
   let pending=metadata::<Vec<HistoryAttempt>>(&transaction,"history_pending")?.filter(|p|!p.is_empty()).unwrap_or(attempts);
   validate_attempts(&pending)?;
   for a in &pending {
    let current:String=transaction.query_row("SELECT payload FROM operations WHERE runtime_id=?1",[&a.runtime_id],|row|row.get(0)).map_err(unavailable)?;
    let current:OperationRecord=serde_json::from_str(&current).map_err(unavailable)?;
    if serde_json::to_string(&current).map_err(unavailable)?!=serde_json::to_string(&a.record).map_err(unavailable)? {return Err(Error::Conflict)}

    let identity=adx_core::history::HistoryIdentity::from_runtime(boundary.authority.clone(),a.runtime_id.clone())?;
    if !boundary.is_closed(&identity)?{return Err(Error::Conflict)}
   }
   transaction.execute("INSERT INTO history_boundary VALUES(1,?1) ON CONFLICT(slot) DO UPDATE SET payload=excluded.payload",[serde_json::to_string(&boundary).map_err(unavailable)?]).map_err(unavailable)?;
   transaction.execute("INSERT INTO history_pending VALUES(1,?1) ON CONFLICT(slot) DO UPDATE SET payload=excluded.payload",[serde_json::to_string(&pending).map_err(unavailable)?]).map_err(unavailable)?;
   transaction.execute_batch("PRAGMA user_version=3;").map_err(unavailable)?;transaction.commit().map_err(unavailable)?;Ok(pending)
  }).await.map_err(unavailable)?
    }
    pub(in crate::sandboxd) async fn complete_history(
        &self,
        attempts: Vec<HistoryAttempt>,
    ) -> Result<()> {
        validate_attempts(&attempts)?;
        let path = self.path.clone();
        let lock = self.lock.clone();
        let identity = self.identity.clone();
        tokio::task::spawn_blocking(move || {
            let mut connection = existing_database(&path, &lock, &identity)?;
            let transaction = connection.transaction().map_err(unavailable)?;
            let pending = metadata::<Vec<HistoryAttempt>>(&transaction, "history_pending")?
                .ok_or(Error::Conflict)?;
            if serde_json::to_string(&pending).map_err(unavailable)?
                != serde_json::to_string(&attempts).map_err(unavailable)?
            {
                return Err(Error::Conflict);
            }
            for a in attempts {
                let current: Option<String> = transaction
                    .query_row(
                        "SELECT payload FROM operations WHERE runtime_id=?1",
                        [&a.runtime_id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(unavailable)?;
                if current.as_ref().is_some_and(|v| {
                    serde_json::from_str::<OperationRecord>(v)
                        .ok()
                        .is_none_or(|r| {
                            serde_json::to_string(&r).ok() != serde_json::to_string(&a.record).ok()
                        })
                }) {
                    return Err(Error::Conflict);
                }
                transaction
                    .execute(
                        "DELETE FROM operations WHERE runtime_id=?1",
                        [&a.runtime_id],
                    )
                    .map_err(unavailable)?;
                transaction
                    .execute(
                        "DELETE FROM operation_history WHERE runtime_id=?1",
                        [&a.runtime_id],
                    )
                    .map_err(unavailable)?;
            }
            transaction
                .execute("UPDATE history_pending SET payload='[]' WHERE slot=1", [])
                .map_err(unavailable)?;
            transaction.commit().map_err(unavailable)?;
            if let Err(error) = connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);") {
                adx_observability::warn!(
                    "history transaction committed; WAL truncation deferred: {error}"
                );
            }
            Ok(())
        })
        .await
        .map_err(unavailable)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn missing_database_cannot_be_recreated_by_history_paths_or_reopen() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("operations.sqlite");
        let (journal, _) = OperationJournal::open(path.clone()).await.unwrap();
        fs::remove_file(&path).unwrap();
        assert!(journal.history_boundary().await.is_err());
        assert!(!path.exists());
        assert!(journal.history_has_facts("missing-1".into()).await.is_err());
        assert!(!path.exists());
        drop(journal);
        assert!(OperationJournal::open(path.clone()).await.is_err());
        assert!(!path.exists());
    }
    #[tokio::test]
    async fn retired_binding_is_recovered_only_from_matching_complete_fact() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("operations.sqlite");
        let (journal, _) = OperationJournal::open(path.clone()).await.unwrap();
        let generation = uuid::Uuid::new_v4().to_string();
        let original = OperationRecord {
            state: StartState::DeleteUncertain,
            start: Some(
                OperationIntent::new("start_operation", "a".repeat(64), Duration::from_secs(30))
                    .unwrap(),
            ),
            action: Some(
                OperationIntent::new("delete", "b".repeat(64), Duration::from_secs(30)).unwrap(),
            ),
            backend: Some(BackendIdentity {
                id: "original-sandbox".into(),
                resource_generation: generation.clone(),
            }),
            retired_generation: Some(generation),
            memory: None,
        };
        journal
            .save("legacy-7".into(), original.clone())
            .await
            .unwrap();
        let mut old = original.clone();
        old.state = StartState::Failed;
        old.backend = None;
        journal.save("legacy-7".into(), old.clone()).await.unwrap();
        let mut foreign = old.clone();
        foreign.start = Some(
            OperationIntent::new("start_operation", "c".repeat(64), Duration::from_secs(30))
                .unwrap(),
        );
        journal
            .save("foreign-8".into(), foreign.clone())
            .await
            .unwrap();
        let repaired = journal
            .recover_retired_bindings(BTreeMap::from([
                ("legacy-7".into(), old),
                ("foreign-8".into(), foreign),
            ]))
            .await
            .unwrap();
        assert!(repaired["legacy-7"].history_terminal());
        assert!(repaired["foreign-8"].backend.is_none());
        drop(journal);
        let (_, reopened) = OperationJournal::open(path).await.unwrap();
        assert_eq!(
            reopened["legacy-7"].backend.as_ref().unwrap().id,
            "original-sandbox"
        );
    }
    #[tokio::test]
    async fn boundary_and_pending_ack_survive_reopen_without_losing_unknown_facts() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("operations.sqlite");
        let (journal, _) = OperationJournal::open(path.clone()).await.unwrap();
        let generation = uuid::Uuid::new_v4().to_string();
        let retired = OperationRecord {
            state: StartState::Failed,
            start: Some(
                OperationIntent::new("start_operation", "a".repeat(64), Duration::from_secs(30))
                    .unwrap(),
            ),
            action: None,
            backend: Some(BackendIdentity {
                id: "sbox-original".into(),
                resource_generation: generation.clone(),
            }),
            retired_generation: Some(generation),
            memory: None,
        };
        let unknown = OperationRecord {
            state: StartState::StartUncertain,
            start: Some(
                OperationIntent::new("start_operation", "b".repeat(64), Duration::from_secs(30))
                    .unwrap(),
            ),
            ..OperationRecord::default()
        };
        journal
            .save("closed-7".into(), retired.clone())
            .await
            .unwrap();
        journal
            .save("unknown-8".into(), unknown.clone())
            .await
            .unwrap();
        let b = HistoryBoundary {
            authority: "test".into(),
            coordinator_epoch: 2,
            assignment_generation: 9,
            catalog_revision: 10,
            protected: BTreeMap::new(),
        };
        let pending = journal
            .install_history(
                b.clone(),
                vec![HistoryAttempt {
                    runtime_id: "closed-7".into(),
                    record: retired,
                }],
            )
            .await
            .unwrap();
        let mut rollback = b.clone();
        rollback.assignment_generation = 8;
        assert!(journal.install_history(rollback, vec![]).await.is_err());
        assert_eq!(journal.history_boundary().await.unwrap(), Some(b.clone()));
        drop(journal);
        let (journal, records) = OperationJournal::open(path).await.unwrap();
        assert_eq!(records.len(), 2);
        let retry = journal.install_history(b, vec![]).await.unwrap();
        assert_eq!(
            serde_json::to_string(&retry).unwrap(),
            serde_json::to_string(&pending).unwrap()
        );
        let mut changed = retry[0].record.clone();
        changed.state = StartState::StartUncertain;
        journal.save("closed-7".into(), changed).await.unwrap();
        assert!(
            journal.complete_history(retry.clone()).await.is_err(),
            "changed or uncertain facts must survive a stale acknowledgment"
        );
        journal
            .save("closed-7".into(), retry[0].record.clone())
            .await
            .unwrap();
        journal.complete_history(retry).await.unwrap();
        let connection = database(&journal.path).unwrap();
        let ids = connection
            .prepare("SELECT runtime_id FROM operations ORDER BY runtime_id")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(ids, vec!["unknown-8"]);
        let payload: String = connection
            .query_row(
                "SELECT payload FROM operations WHERE runtime_id='unknown-8'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(payload, serde_json::to_string(&unknown).unwrap());
        let version: u32 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 3);
    }
}
