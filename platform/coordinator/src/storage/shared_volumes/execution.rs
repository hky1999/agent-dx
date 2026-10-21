//! Atomic execution-sized reservation/retirement in the existing volume hash.
use super::*;
use adx_core::shared_volume_attachments::attachments_for_record;
use std::collections::BTreeSet;

const MAX_NODE_ATTACHMENTS: usize = 4096;
const MAX_NODE_ATTACHMENT_BYTES: usize = 2 * 1024 * 1024;

fn validate_batch(receipts: &[SharedVolumeAttachmentRetirement]) -> Result<()> {
    if receipts.is_empty() || receipts.len() > adx_core::execution::MAX_SHARED_VOLUME_MOUNTS {
        return Err(Error::Invalid(
            "bounded volume retirement batch required".into(),
        ));
    }
    let first = &receipts[0].attachment;
    let mut ids = BTreeSet::new();
    for receipt in receipts {
        receipt.validate()?;
        if !receipt.attachment.same_execution(first)
            || receipt.attachment.tenant_id != first.tenant_id
            || !ids.insert(receipt.attachment.volume_id.as_str().to_owned())
        {
            return Err(Error::Conflict);
        }
    }
    Ok(())
}

fn validate_start(stored: &StoredEnvironment, record: &EnvironmentRecord) -> Result<()> {
    if validate_execution(stored, record).is_ok() {
        return Ok(());
    }
    // Explicit checkpoint reload can replace a Running execution without a
    // separately committed cold-restart record. The old writer still requires
    // its exact backend retirement before the volume CAS can admit this one.
    stored.validate()?;
    let old = stored.result.as_ref().ok_or(Error::Conflict)?;
    let previous = RuntimeIdentity {
        environment_id: old.spec.id.clone(),
        runtime_id: old.runtime.id.clone(),
        ownership_generation: old.assignment.generation,
    };
    let next = RuntimeIdentity {
        environment_id: record.spec.id.clone(),
        runtime_id: record.runtime.id.clone(),
        ownership_generation: record.assignment.generation,
    };
    next.validate_restore_from(&previous)?;
    if stored.invalidated
        || stored.spec != record.spec
        || stored.assignment != record.assignment
        || old.state != EnvironmentState::Running
        || !old.resources_held
        || record.state != EnvironmentState::Starting
        || !record.resources_held
        || record.revision <= old.revision
        || record.restart_attempts != old.restart_attempts
        || record.checkpoint.is_none()
        || record.checkpoint != old.checkpoint
        || record.runtime.id
            != format!(
                "{}-{}-r{}",
                record.spec.id, record.assignment.generation, record.revision
            )
    {
        return Err(Error::Conflict);
    }
    Ok(())
}

impl Session {
    async fn volume_execution_rows(
        &self,
        node_id: &str,
        session_id: &str,
        environment_id: Option<&str>,
        ids: &[SharedVolumeId],
    ) -> Result<Vec<Option<String>>> {
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            local out = {redis.call('HGET',KEYS[1],'header'),
                         redis.call('HGET',KEYS[1],ARGV[1])}
            if ARGV[2] ~= '' then table.insert(out,redis.call('HGET',KEYS[1],ARGV[2])) end
            for i=3,#ARGV do table.insert(out,redis.call('HGET',KEYS[2],ARGV[i])) end
            return out
        "#,
            )
            .arg(2)
            .arg(&self.store.key)
            .arg(self.shared_volumes_key())
            .arg(format!("node:{node_id}"))
            .arg(
                environment_id
                    .map(|id| format!("environment:{id}"))
                    .unwrap_or_default(),
            );
        for id in ids {
            command.arg(format!("volume:{}", id.as_str()));
        }
        let rows: Vec<Option<String>> = self.store.query(command).await?;
        if rows.len() != 2 + usize::from(environment_id.is_some()) + ids.len() {
            return Err(Error::Conflict);
        }
        self.header(&rows[0])?;
        let node: StoredNode = decode(rows[1].as_deref().ok_or(Error::NotFound)?)?;
        reader_node(&node, node_id, session_id)?;
        Ok(rows)
    }

    pub(super) async fn volume_execution_cas(
        &self,
        header: &str,
        node: (&str, &str),
        changes: &[(SharedVolumeId, String, StoredSharedVolume)],
    ) -> Result<bool> {
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            if redis.call('HGET',KEYS[1],'header') ~= ARGV[1] then return 0 end
            if redis.call('HGET',KEYS[1],ARGV[2]) ~= ARGV[3] then return 0 end
            for i=4,#ARGV,3 do
              if redis.call('HGET',KEYS[2],ARGV[i]) ~= ARGV[i+1] then return 0 end
            end
            for i=4,#ARGV,3 do redis.call('HSET',KEYS[2],ARGV[i],ARGV[i+2]) end
            return 1
        "#,
            )
            .arg(2)
            .arg(&self.store.key)
            .arg(self.shared_volumes_key())
            .arg(header)
            .arg(format!("node:{}", node.0))
            .arg(node.1);
        for (id, old, next) in changes {
            next.validate()?;
            command
                .arg(format!("volume:{}", id.as_str()))
                .arg(old)
                .arg(encode(next)?);
        }
        Ok(self.store.query::<u8>(command).await? == 1)
    }

    /// One CAS admits every canonical mount, or none. A failed/late response is
    /// resolved by execution-bound retirement, never by a local journal/TTL.
    pub async fn reserve_shared_volumes(
        &self,
        record: &EnvironmentRecord,
        node_session: &str,
    ) -> Result<Vec<SharedVolumeAttachment>> {
        let attachments = attachments_for_record(record)?;
        if attachments.is_empty() {
            return Err(Error::Invalid("shared volume mounts required".into()));
        }
        let ids: Vec<_> = attachments.iter().map(|a| a.volume_id.clone()).collect();
        for _ in 0..ATTEMPTS {
            let rows = self
                .volume_execution_rows(
                    &record.assignment.node_id,
                    node_session,
                    Some(&record.spec.id),
                    &ids,
                )
                .await?;
            let header = rows[0].as_deref().ok_or(Error::Conflict)?;
            let node_raw = rows[1].as_deref().ok_or(Error::NotFound)?;
            let node: StoredNode = decode(node_raw)?;
            let storage_id = super::identity::node_storage_id(&node)?;
            let stored: StoredEnvironment = decode(rows[2].as_deref().ok_or(Error::NotFound)?)?;
            validate_start(&stored, record)?;
            let mut changes = Vec::new();
            for (index, attachment) in attachments.iter().enumerate() {
                let raw = rows[index + 3].as_deref().ok_or(Error::NotFound)?;
                let mut next: StoredSharedVolume = decode(raw)?;
                next.validate()?;
                if next.permissions.id() != &attachment.volume_id {
                    return Err(Error::Conflict);
                }
                next.permissions
                    .check_mount_permission(&attachment.tenant_id, &attachment.mount())?;
                if next
                    .retirements
                    .iter()
                    .any(|old| old.attachment.same_execution(attachment))
                {
                    return Err(Error::Conflict);
                }
                next.bind_storage(storage_id)?;
                if let Some(old) = next
                    .attachments
                    .iter()
                    .find(|old| old.same_execution(attachment))
                {
                    if old != attachment {
                        return Err(Error::Conflict);
                    }
                } else {
                    if next.attachments.len() >= MAX_ATTACHMENTS {
                        return Err(Error::NoCapacity);
                    }
                    if attachment.access == SharedVolumeAccess::ReadWrite
                        && next
                            .attachments
                            .iter()
                            .any(|old| old.access == SharedVolumeAccess::ReadWrite)
                    {
                        return Err(Error::Conflict);
                    }
                    if next.retirements.len() >= RECONCILE_HISTORY_AT {
                        if !self.prune_volume_retirements(&mut next, header).await? {
                            break;
                        }
                        if next.retirements.len() >= RECONCILE_HISTORY_AT {
                            return Err(Error::Unavailable(
                                "volume retirement reconciliation required".into(),
                            ));
                        }
                    }
                    next.attachments.push(attachment.clone());
                    next.advance()?;
                }
                changes.push((attachment.volume_id.clone(), raw.to_owned(), next));
            }
            if changes.len() == attachments.len()
                && self
                    .volume_execution_cas(header, (&record.assignment.node_id, node_raw), &changes)
                    .await?
            {
                return Ok(attachments);
            }
        }
        Err(Error::Unavailable(
            "shared volume batch admission contention".into(),
        ))
    }

    /// Persist stop tombstones even if a timed-out reservation has not arrived.
    /// A receipt attests this execution stopped AND all its delayed starts fenced.
    pub async fn release_shared_volumes(
        &self,
        receipts: &[SharedVolumeAttachmentRetirement],
        node_session: &str,
    ) -> Result<()> {
        validate_batch(receipts)?;
        let ids: Vec<_> = receipts
            .iter()
            .map(|r| r.attachment.volume_id.clone())
            .collect();
        for _ in 0..ATTEMPTS {
            let rows = self
                .volume_execution_rows(&receipts[0].attachment.node_id, node_session, None, &ids)
                .await?;
            let header = rows[0].as_deref().ok_or(Error::Conflict)?;
            let mut changes = Vec::new();
            for (index, receipt) in receipts.iter().enumerate() {
                let raw = rows[index + 2].as_deref().ok_or(Error::NotFound)?;
                let mut next: StoredSharedVolume = decode(raw)?;
                next.validate()?;
                if next.permissions.id() != &receipt.attachment.volume_id {
                    return Err(Error::Conflict);
                }
                if let Some(old) = next
                    .retirements
                    .iter()
                    .find(|old| old.attachment.same_execution(&receipt.attachment))
                {
                    if old != receipt {
                        return Err(Error::Conflict);
                    }
                } else {
                    if let Some(index) = next
                        .attachments
                        .iter()
                        .position(|old| old.same_execution(&receipt.attachment))
                    {
                        if next.attachments[index] != receipt.attachment {
                            return Err(Error::Conflict);
                        }
                        next.attachments.remove(index);
                    }
                    if next.retirements.len() >= RECONCILE_HISTORY_AT
                        && !self.prune_volume_retirements(&mut next, header).await?
                    {
                        break;
                    }
                    if next.retirements.len() >= MAX_RETIREMENTS {
                        return Err(Error::Unavailable(
                            "volume retirement reconciliation required".into(),
                        ));
                    }
                    next.retirements.push(receipt.clone());
                    next.advance()?;
                }
                changes.push((receipt.attachment.volume_id.clone(), raw.to_owned(), next));
            }
            if changes.len() == receipts.len()
                && self
                    .volume_execution_cas(
                        header,
                        (
                            &receipts[0].attachment.node_id,
                            rows[1].as_deref().ok_or(Error::NotFound)?,
                        ),
                        &changes,
                    )
                    .await?
            {
                return Ok(());
            }
        }
        Err(Error::Unavailable(
            "shared volume batch retirement contention".into(),
        ))
    }

    /// Atomic complete node view of the existing volume authority, including
    /// old ownership generations absent from this node's Environment catalog.
    /// Explicit size limits fail closed instead of returning a truncated view.
    pub async fn node_shared_volume_attachments(
        &self,
        node_id: &str,
        node_session: &str,
    ) -> Result<Vec<SharedVolumeAttachment>> {
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            local rows=redis.call('HGETALL',KEYS[2])
            local out={}
            local bytes=0
            for i=1,#rows,2 do
              if string.sub(rows[i],1,7)=='volume:' then
                bytes=bytes+string.len(rows[i+1])
                if #out>=4096 or bytes>16777216 then
                  return redis.error_reply('volume reconciliation exceeds bounded snapshot')
                end
                table.insert(out,{rows[i],rows[i+1]})
              end
            end
            return {redis.call('HGET',KEYS[1],'header'),
                    redis.call('HGET',KEYS[1],ARGV[1]),out}
        "#,
            )
            .arg(2)
            .arg(&self.store.key)
            .arg(self.shared_volumes_key())
            .arg(format!("node:{node_id}"));
        let (header, node, rows): (Option<String>, Option<String>, Vec<(String, String)>) =
            self.store.query(command).await?;
        self.header(&header)?;
        let node: StoredNode = decode(node.as_deref().ok_or(Error::NotFound)?)?;
        reader_node(&node, node_id, node_session)?;
        let mut result = Vec::new();
        for (field, raw) in rows {
            let record: StoredSharedVolume = decode(&raw)?;
            record.validate()?;
            if field != format!("volume:{}", record.permissions.id().as_str()) {
                return Err(Error::Conflict);
            }
            result.extend(
                record
                    .attachments
                    .into_iter()
                    .filter(|a| a.node_id == node_id),
            );
            if result.len() > MAX_NODE_ATTACHMENTS {
                return Err(Error::Unavailable(
                    "node volume reconciliation exceeds bounded snapshot".into(),
                ));
            }
        }
        result.sort_by_key(|a| {
            (
                a.identity.environment_id.clone(),
                a.identity.ownership_generation,
                a.identity.runtime_id.clone(),
                a.volume_id.as_str().to_owned(),
            )
        });
        if serde_json::to_vec(&result)
            .map_err(|e| Error::Unavailable(e.to_string()))?
            .len()
            > MAX_NODE_ATTACHMENT_BYTES
        {
            return Err(Error::Unavailable(
                "node volume reconciliation exceeds bounded response".into(),
            ));
        }
        Ok(result)
    }
}
