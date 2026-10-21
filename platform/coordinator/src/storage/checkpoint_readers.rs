//! Reader dependencies share the Environment CAS, not its owner's lifecycle revision.
use super::*;
use adx_core::{CheckpointReader, CheckpointReaderRetirement, RestorePoint};

fn same_execution(a: &CheckpointReader, b: &CheckpointReader) -> bool {
    a.node_id == b.node_id && a.identity == b.identity
}

impl StoredEnvironment {
    pub(crate) fn reader_retired(&self, reader: &CheckpointReader) -> bool {
        self.reader_retirements
            .iter()
            .any(|receipt| same_execution(&receipt.reader, reader))
    }

    pub fn retained_checkpoint_points(&self) -> impl Iterator<Item = &RestorePoint> {
        self.result
            .iter()
            .flat_map(|record| {
                record.checkpoint.iter().chain(
                    record
                        .checkpoint_readers
                        .readers
                        .iter()
                        .filter(|reader| !self.reader_retired(reader))
                        .map(|reader| &reader.checkpoint),
                )
            })
            .chain(
                self.reader_reservations
                    .iter()
                    .map(|reader| &reader.checkpoint),
            )
    }

    pub(super) fn validate_reader_dependencies(&self) -> Result<()> {
        self.effective_record().validate_checkpoint_readers()?;
        let mut evidence = self.effective_record();
        evidence.checkpoint_readers.readers = self
            .reader_reservations
            .iter()
            .cloned()
            .chain(
                self.reader_retirements
                    .iter()
                    .map(|receipt| receipt.reader.clone()),
            )
            .collect();
        evidence.validate_checkpoint_readers()?;
        for receipt in &self.reader_retirements {
            receipt.validate()?;
        }
        Ok(())
    }
}

fn requested_reader(
    stored: &StoredEnvironment,
    record: &EnvironmentRecord,
) -> Result<CheckpointReader> {
    record.validate_checkpoint_readers()?;
    let reader = record
        .current_checkpoint_reader_entry()
        .ok_or(Error::Conflict)?;
    if stored.invalidated
        || stored.assignment != record.assignment
        || stored.spec != record.spec
        || !matches!(
            record.state,
            EnvironmentState::Starting | EnvironmentState::Resuming
        )
        || record.revision == 0
        || !record.resources_held
        || record.runtime.ip.is_some()
        || record.checkpoint.as_ref() != Some(&reader.checkpoint)
        || stored.reader_retired(reader)
        || stored
            .result
            .as_ref()
            .is_some_and(|previous| previous.state == EnvironmentState::Deleted)
    {
        return Err(Error::Conflict);
    }
    Ok(reader.clone())
}

fn add_reservation(stored: &mut StoredEnvironment, reader: CheckpointReader) -> Result<bool> {
    if stored.reader_retired(&reader) {
        return Err(Error::Conflict);
    }
    if let Some(existing) = stored
        .reader_reservations
        .iter()
        .find(|existing| same_execution(existing, &reader))
    {
        return if *existing == reader {
            Ok(false)
        } else {
            Err(Error::Conflict)
        };
    }
    stored.reader_reservations.push(reader);
    stored.validate()?;
    Ok(true)
}

fn apply_retirement(
    stored: &mut StoredEnvironment,
    receipt: &CheckpointReaderRetirement,
) -> Result<bool> {
    receipt.validate()?;
    if let Some(existing) = stored
        .reader_retirements
        .iter()
        .find(|existing| same_execution(&existing.reader, &receipt.reader))
    {
        return if existing == receipt {
            Ok(false)
        } else {
            Err(Error::Conflict)
        };
    }
    if !stored.reader_reservations.contains(&receipt.reader)
        && !stored
            .result
            .as_ref()
            .is_some_and(|record| record.checkpoint_readers.readers.contains(&receipt.reader))
    {
        return Err(Error::Conflict);
    }
    stored
        .reader_reservations
        .retain(|reader| !same_execution(reader, &receipt.reader));
    stored.reader_retirements.push(receipt.clone());
    stored.validate()?;
    Ok(true)
}

pub(super) fn reader_node(node: &StoredNode, id: &str, session_id: &str) -> Result<()> {
    if session_id.is_empty()
        || node.node.id != id
        || node
            .session
            .as_ref()
            .is_none_or(|session| session.id != session_id)
    {
        return Err(Error::Conflict);
    }
    Ok(())
}

impl Session {
    /// Persist before restore side effects. Retries retain the same reader identity;
    /// timeout is unknown, never permission to start using only a local journal.
    pub async fn reserve_checkpoint_reader(
        &self,
        record: &EnvironmentRecord,
        node_session_id: &str,
    ) -> Result<StoredEnvironment> {
        let field = format!("environment:{}", record.spec.id);
        for _ in 0..ATTEMPTS {
            let [header_value, value, node_value] = self
                .store
                .fields([
                    HEADER.into(),
                    field.clone(),
                    format!("node:{}", record.assignment.node_id),
                ])
                .await?;
            let mut header = self.header(&header_value)?;
            let node: StoredNode = decode(node_value.as_deref().ok_or(Error::NotFound)?)?;
            reader_node(&node, &record.assignment.node_id, node_session_id)?;
            let mut stored: StoredEnvironment = decode(value.as_deref().ok_or(Error::NotFound)?)?;
            stored.validate()?;
            let reader = requested_reader(&stored, record)?;
            if stored
                .reader_reservations
                .iter()
                .any(|existing| same_execution(existing, &reader))
            {
                add_reservation(&mut stored, reader)?;
                return Ok(stored);
            }
            if stored
                .result
                .as_ref()
                .is_some_and(|previous| previous.revision >= record.revision)
                || reader.checkpoint.expired(collection::now()?)
            {
                return Err(Error::Conflict);
            }
            let snapshot_guard = if stored
                .result
                .as_ref()
                .and_then(|previous| previous.checkpoint.as_ref())
                == Some(&reader.checkpoint)
            {
                None
            } else if stored.result.as_ref().is_some_and(|previous| {
                previous.state == EnvironmentState::Running
                    && reader.checkpoint.artifact.storage == "local"
                    && reader.checkpoint.origin.is_none()
                    && reader.checkpoint.source_runtime_id == previous.runtime.id
            }) {
                // A failed remote publication may recover the same owner's freshly
                // captured local checkpoint. It is not a cross-node recovery point.
                None
            } else {
                if stored.result.is_some() {
                    return Err(Error::Conflict);
                }
                let snapshot_id = stored.spec.snapshot_id.as_deref().ok_or(Error::Conflict)?;
                let raw = self
                    .snapshot_raw(snapshot_id)
                    .await?
                    .ok_or(Error::NotFound)?;
                let snapshot: adx_core::snapshots::Snapshot = decode(&raw)?;
                snapshot.validate()?;
                if snapshot.template.tenant_id != stored.spec.tenant_id
                    || !snapshot
                        .references
                        .contains(&adx_core::snapshots::Reference::Restore {
                            environment_id: stored.spec.id.clone(),
                        })
                    || reader.checkpoint.origin.as_ref() != Some(&snapshot.origin()?)
                    || reader.checkpoint.source_runtime_id != snapshot.source_runtime_id
                    || reader.checkpoint.artifact == snapshot.artifact
                    || reader.checkpoint.artifact.size_bytes != snapshot.artifact.size_bytes
                {
                    return Err(Error::Conflict);
                }
                Some((snapshot_id.to_owned(), raw))
            };
            add_reservation(&mut stored, reader)?;
            header.advance()?;
            // Snapshot references have their own hash: compare exact bytes in the
            // same transaction instead of assuming they bump the control header.
            let mut command = redis::cmd("EVAL");
            command.arg(r#"
                if redis.call('HGET', KEYS[1], 'header') ~= ARGV[1] then return 0 end
                if ARGV[5] ~= '' and redis.call('HGET', KEYS[2], ARGV[5]) ~= ARGV[6] then return 0 end
                redis.call('HSET', KEYS[1], 'header', ARGV[2], ARGV[3], ARGV[4]); return 1
            "#).arg(2).arg(&self.store.key).arg(self.snapshots_key())
                .arg(header_value.as_deref().ok_or(Error::Conflict)?).arg(encode(&header)?)
                .arg(&field).arg(encode(&stored)?)
                .arg(snapshot_guard.as_ref().map_or("", |guard| guard.0.as_str()))
                .arg(snapshot_guard.as_ref().map_or("", |guard| guard.1.as_str()));
            if self.store.query::<u8>(command).await? == 1 {
                self.store
                    .notify_committed_change(header.revision, vec![field]);
                return Ok(stored);
            }
        }
        Err(Error::Unavailable(
            "checkpoint reader reservation contention; retry same reader".into(),
        ))
    }

    /// The authenticated node must first obtain the backend's durable stop
    /// receipt, binding both no delayed start and confirmed exit to this reader.
    /// Persist it independently of the current owner's lifecycle result/revision.
    pub async fn release_checkpoint_reader(
        &self,
        receipt: &CheckpointReaderRetirement,
        node_session_id: &str,
    ) -> Result<StoredEnvironment> {
        receipt.validate()?;
        let field = format!("environment:{}", receipt.reader.identity.environment_id);
        for _ in 0..ATTEMPTS {
            let [header_value, value, node_value] = self
                .store
                .fields([
                    HEADER.into(),
                    field.clone(),
                    format!("node:{}", receipt.reader.node_id),
                ])
                .await?;
            let mut header = self.header(&header_value)?;
            let node: StoredNode = decode(node_value.as_deref().ok_or(Error::NotFound)?)?;
            reader_node(&node, &receipt.reader.node_id, node_session_id)?;
            let mut stored: StoredEnvironment = decode(value.as_deref().ok_or(Error::NotFound)?)?;
            stored.validate()?;
            if !apply_retirement(&mut stored, receipt)? {
                return Ok(stored);
            }
            header.advance()?;
            if self
                .store
                .cas(
                    header_value.as_deref().ok_or(Error::Conflict)?,
                    &header,
                    Some((&field, encode(&stored)?)),
                )
                .await?
            {
                return Ok(stored);
            }
        }
        Err(Error::Unavailable(
            "checkpoint reader retirement contention; retry same receipt".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (StoredEnvironment, EnvironmentRecord) {
        let spec: EnvironmentSpec = serde_json::from_value(serde_json::json!({
            "id":"e", "tenant_id":"t", "image":"image", "runtime_class":"runsc", "priority":0,
            "resources":{"cpu_millis":1,"memory_bytes":1,"disk_bytes":1}
        }))
        .unwrap();
        let assignment = Assignment {
            environment_id: "e".into(),
            node_id: "n".into(),
            generation: 1,
            shard_id: 0,
            devices: vec![],
        };
        let point = RestorePoint {
            id: "a".into(),
            source_runtime_id: "e-1".into(),
            origin: None,
            expires_at_unix_seconds: None,
            artifact: adx_core::CheckpointArtifact {
                storage: "shared".into(),
                location: "a".into(),
                size_bytes: 1,
            },
        };
        let paused = EnvironmentRecord {
            spec: spec.clone(),
            assignment: assignment.clone(),
            state: EnvironmentState::Paused,
            revision: 4,
            runtime: adx_core::Runtime {
                id: "e-1".into(),
                ip: None,
            },
            resources_held: false,
            checkpoint: Some(point.clone()),
            checkpoint_readers: adx_core::CheckpointReaders::known_empty(),
            last_operation: None,
            restart_attempts: 0,
            restart_pending: false,
            failure: None,
        };
        let stored = StoredEnvironment {
            reader_reservations: vec![],
            reader_retirements: vec![],
            resume: None,
            failed_since_unix_seconds: None,
            recovery: None,
            invalidated: false,
            spec,
            assignment,
            result: Some(paused.clone()),
        };
        let mut request = paused;
        request.state = EnvironmentState::Starting;
        request.revision += 1;
        request.resources_held = true;
        request.runtime.id = "e-1-r5".into();
        request.register_checkpoint_reader(point).unwrap();
        (stored, request)
    }
    #[test]
    fn reservation_is_durable_without_rewriting_owner_result() {
        let (mut stored, request) = fixture();
        let result = stored.result.clone();
        let reader = requested_reader(&stored, &request).unwrap();
        assert!(add_reservation(&mut stored, reader.clone()).unwrap());
        assert!(!add_reservation(&mut stored, reader.clone()).unwrap());
        assert_eq!(stored.result, result);
        assert!(stored.resources_held());
        let restored: StoredEnvironment = decode(&encode(&stored).unwrap()).unwrap();
        assert_eq!(restored.reader_reservations, vec![reader.clone()]);
        assert!(restored
            .effective_record()
            .checkpoint_readers
            .readers
            .contains(&reader));
        assert!(restored
            .retained_checkpoint_points()
            .any(|point| point == &reader.checkpoint));
    }
    #[test]
    fn receipt_is_exact_idempotent_and_prevents_reader_resurrection() {
        let (mut stored, request) = fixture();
        let reader = requested_reader(&stored, &request).unwrap();
        add_reservation(&mut stored, reader.clone()).unwrap();
        let result = stored.result.clone();
        let receipt = CheckpointReaderRetirement {
            reader: reader.clone(),
            backend_receipt: "backend:fenced-and-exited".into(),
        };
        let mut wrong = receipt.clone();
        wrong.reader.checkpoint.artifact.location = "other".into();
        assert_eq!(apply_retirement(&mut stored, &wrong), Err(Error::Conflict));
        assert!(apply_retirement(&mut stored, &receipt).unwrap());
        assert!(!apply_retirement(&mut stored, &receipt).unwrap());
        assert_eq!(stored.result, result);
        assert!(stored.reader_reservations.is_empty());
        assert_eq!(add_reservation(&mut stored, reader), Err(Error::Conflict));
        assert_eq!(requested_reader(&stored, &request), Err(Error::Conflict));
    }
    #[test]
    fn stale_owner_and_changed_reader_content_are_not_new_reservations() {
        let (mut stored, mut request) = fixture();
        let reader = requested_reader(&stored, &request).unwrap();
        add_reservation(&mut stored, reader.clone()).unwrap();
        let mut changed = reader;
        changed.checkpoint.artifact.location = "changed".into();
        assert_eq!(add_reservation(&mut stored, changed), Err(Error::Conflict));
        request.assignment.generation += 1;
        assert!(requested_reader(&stored, &request).is_err());
    }
}
