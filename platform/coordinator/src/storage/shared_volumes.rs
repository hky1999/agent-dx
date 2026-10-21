//! Application-volume metadata under the existing Coordinator epoch.
//! Like snapshots, records use an adjacent Redis hash in the same cluster slot.
//! Every mutation compares the exact control header AND exact old volume bytes;
//! volume mutations do not change Environment route versions.
mod execution;
mod identity;
use super::{
    checkpoint_readers::reader_node, decode, encode, Session, StoredEnvironment, StoredNode,
    ATTEMPTS, HEADER,
};
use adx_core::{
    runtime::RuntimeIdentity,
    shared_volume_attachments::{SharedVolumeAttachment, SharedVolumeAttachmentRetirement},
    shared_volumes::{
        SharedVolumeAccess, SharedVolumeGrant, SharedVolumeId, SharedVolumeMount,
        SharedVolumeRecord, SharedVolumeSubpath,
    },
    EnvironmentRecord, EnvironmentState, Error, Result,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const MAX_ATTACHMENTS: usize = 256;
const RECONCILE_HISTORY_AT: usize = 512;
const MAX_RETIREMENTS: usize = RECONCILE_HISTORY_AT + MAX_ATTACHMENTS;

/// Durable reservations survive Coordinator/node-session changes, never TTL out.
/// Required lists deliberately do not default to empty on historical omission.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredSharedVolume {
    pub permissions: SharedVolumeRecord,
    /// Includes attachment mutations, unlike the permission-only revision.
    pub revision: u64,
    /// Immutable after first physical-store admission. Old records stay unbound.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_id: Option<String>,
    /// Only newly created records may acquire their first storage binding after
    /// a pre-admission stop. Missing fields in historical records remain false.
    #[serde(default)]
    pub storage_binding_pending: bool,
    pub attachments: Vec<SharedVolumeAttachment>,
    pub retirements: Vec<SharedVolumeAttachmentRetirement>,
}

impl StoredSharedVolume {
    pub fn validate(&self) -> Result<()> {
        self.permissions.validate()?;
        if let Some(id) = &self.storage_id {
            identity::validate_storage_id(id)?;
        }
        if self.revision < self.permissions.revision()
            || self.attachments.len() > MAX_ATTACHMENTS
            || self.retirements.len() > MAX_RETIREMENTS
        {
            return Err(Error::Conflict);
        }
        let mut writers = 0;
        for (index, attachment) in self.attachments.iter().enumerate() {
            attachment.validate()?;
            if &attachment.volume_id != self.permissions.id()
                || self.attachments[..index]
                    .iter()
                    .any(|old| old.same_execution(attachment))
                || self
                    .retirements
                    .iter()
                    .any(|old| old.attachment.same_execution(attachment))
            {
                return Err(Error::Conflict);
            }
            writers += usize::from(attachment.access == SharedVolumeAccess::ReadWrite);
        }
        for (index, receipt) in self.retirements.iter().enumerate() {
            receipt.validate()?;
            if &receipt.attachment.volume_id != self.permissions.id()
                || self.retirements[..index]
                    .iter()
                    .any(|old| old.attachment.same_execution(&receipt.attachment))
            {
                return Err(Error::Conflict);
            }
        }
        if writers > 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }

    fn advance(&mut self) -> Result<()> {
        self.revision = self.revision.checked_add(1).ok_or(Error::Conflict)?;
        Ok(())
    }
}

impl Session {
    fn shared_volumes_key(&self) -> String {
        format!("{}:shared-volumes", self.store.key)
    }

    async fn shared_volume_raw(
        &self,
        id: &SharedVolumeId,
    ) -> Result<(String, String, StoredSharedVolume)> {
        let mut command = redis::cmd("EVAL");
        command
            .arg("return {redis.call('HGET',KEYS[1],'header'),redis.call('HGET',KEYS[2],ARGV[1])}")
            .arg(2)
            .arg(&self.store.key)
            .arg(self.shared_volumes_key())
            .arg(format!("volume:{}", id.as_str()));
        let (header, raw): (Option<String>, Option<String>) = self.store.query(command).await?;
        self.header(&header)?;
        let header = header.ok_or(Error::Conflict)?;
        let raw = raw.ok_or(Error::NotFound)?;
        let record: StoredSharedVolume = decode(&raw)?;
        record.validate()?;
        if record.permissions.id() != id {
            return Err(Error::Conflict);
        }
        Ok((header, raw, record))
    }

    async fn shared_volume_cas(
        &self,
        header: &str,
        raw: &str,
        next: &StoredSharedVolume,
    ) -> Result<bool> {
        next.validate()?;
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            if redis.call('HGET',KEYS[1],'header') ~= ARGV[1] then return 0 end
            if redis.call('HGET',KEYS[2],ARGV[2]) ~= ARGV[3] then return 0 end
            redis.call('HSET',KEYS[2],ARGV[2],ARGV[4]); return 1
        "#,
            )
            .arg(2)
            .arg(&self.store.key)
            .arg(self.shared_volumes_key())
            .arg(header)
            .arg(format!("volume:{}", next.permissions.id().as_str()))
            .arg(raw)
            .arg(encode(next)?);
        Ok(self.store.query::<u8>(command).await? == 1)
    }

    /// Metadata creation, replayable by a tenant-scoped request UUID.
    /// Physical filesystem preparation and public API readiness are separate.
    pub async fn create_shared_volume(
        &self,
        tenant: &str,
        request_id: &SharedVolumeId,
    ) -> Result<StoredSharedVolume> {
        let permissions = SharedVolumeRecord::new(
            SharedVolumeId::parse(uuid::Uuid::new_v4().to_string())?,
            tenant.to_owned(),
        )?;
        let next = StoredSharedVolume {
            permissions,
            revision: 1,
            storage_id: None,
            storage_binding_pending: true,
            attachments: vec![],
            retirements: vec![],
        };
        let request = format!(
            "request:{:x}:{}",
            Sha256::digest(tenant.as_bytes()),
            request_id.as_str()
        );
        for _ in 0..ATTEMPTS {
            let [header] = self.store.fields([HEADER.into()]).await?;
            self.header(&header)?;
            let mut lookup = redis::cmd("HGET");
            lookup.arg(self.shared_volumes_key()).arg(&request);
            let existing: Option<String> = self.store.query(lookup).await?;
            if let Some(id) = existing {
                return self
                    .get_shared_volume(&SharedVolumeId::parse(id)?, tenant)
                    .await;
            }
            let mut command = redis::cmd("EVAL");
            command
                .arg(
                    r#"
                if redis.call('HGET',KEYS[1],'header') ~= ARGV[1] then return 0 end
                if redis.call('HEXISTS',KEYS[2],ARGV[2]) == 1 then return 0 end
                if redis.call('HEXISTS',KEYS[2],ARGV[3]) == 1 then return 0 end
                redis.call('HSET',KEYS[2],ARGV[2],ARGV[4],ARGV[3],ARGV[5]); return 1
            "#,
                )
                .arg(2)
                .arg(&self.store.key)
                .arg(self.shared_volumes_key())
                .arg(header.as_deref().ok_or(Error::Conflict)?)
                .arg(&request)
                .arg(format!("volume:{}", next.permissions.id().as_str()))
                .arg(next.permissions.id().as_str())
                .arg(encode(&next)?);
            if self.store.query::<u8>(command).await? == 1 {
                return Ok(next);
            }
        }
        Err(Error::Unavailable(
            "shared volume creation contention; retry same request ID".into(),
        ))
    }

    /// Internal owner view; an RPC must use its verified tenant and project output.
    pub async fn get_shared_volume(
        &self,
        id: &SharedVolumeId,
        tenant: &str,
    ) -> Result<StoredSharedVolume> {
        let (_, _, record) = self.shared_volume_raw(id).await?;
        if record.permissions.owner_tenant_id() != tenant {
            return Err(Error::NotFound);
        }
        Ok(record)
    }

    async fn mutate_shared_volume_permissions(
        &self,
        id: &SharedVolumeId,
        tenant: &str,
        expected_revision: u64,
        change: impl Fn(&mut SharedVolumeRecord) -> Result<bool>,
    ) -> Result<StoredSharedVolume> {
        for _ in 0..ATTEMPTS {
            let (header, raw, mut next) = self.shared_volume_raw(id).await?;
            if next.permissions.owner_tenant_id() != tenant {
                return Err(Error::NotFound);
            }
            if next.permissions.revision() != expected_revision {
                return Err(Error::Conflict);
            }
            if !change(&mut next.permissions)? {
                return Ok(next);
            }
            next.advance()?;
            if self.shared_volume_cas(&header, &raw, &next).await? {
                return Ok(next);
            }
        }
        Err(Error::Unavailable(
            "shared volume permission contention".into(),
        ))
    }

    pub async fn grant_shared_volume(
        &self,
        id: &SharedVolumeId,
        tenant: &str,
        expected_revision: u64,
        grant: SharedVolumeGrant,
    ) -> Result<StoredSharedVolume> {
        self.mutate_shared_volume_permissions(id, tenant, expected_revision, |record| {
            record.grant(tenant, grant.clone())
        })
        .await
    }

    /// Does not free running/reserved attachments or claim immediate unmount.
    pub async fn revoke_shared_volume(
        &self,
        id: &SharedVolumeId,
        tenant: &str,
        expected_revision: u64,
        grantee: &str,
        subpath: &SharedVolumeSubpath,
    ) -> Result<StoredSharedVolume> {
        self.mutate_shared_volume_permissions(id, tenant, expected_revision, |record| {
            record.revoke(tenant, grantee, subpath)
        })
        .await
    }

    /// Persist before a mount/start side effect. RPC wiring must authenticate
    /// the node and bind the requested mount to the canonical Environment spec.
    pub async fn reserve_shared_volume(
        &self,
        record: &EnvironmentRecord,
        node_session: &str,
        mount: &SharedVolumeMount,
    ) -> Result<StoredSharedVolume> {
        let attachment = SharedVolumeAttachment {
            volume_id: mount.volume_id.clone(),
            subpath: mount.subpath.clone(),
            target: mount.target.clone(),
            access: mount.access,
            tenant_id: record.spec.tenant_id.clone(),
            node_id: record.assignment.node_id.clone(),
            identity: RuntimeIdentity {
                environment_id: record.spec.id.clone(),
                runtime_id: record.runtime.id.clone(),
                ownership_generation: record.assignment.generation,
            },
        };
        attachment.validate()?;
        for _ in 0..ATTEMPTS {
            let [header, environment, node] = self
                .store
                .fields([
                    HEADER.into(),
                    format!("environment:{}", record.spec.id),
                    format!("node:{}", attachment.node_id),
                ])
                .await?;
            self.header(&header)?;
            let node_raw = node.as_deref().ok_or(Error::NotFound)?;
            let node: StoredNode = decode(node_raw)?;
            reader_node(&node, &attachment.node_id, node_session)?;
            let storage_id = identity::node_storage_id(&node)?;
            let stored: StoredEnvironment = decode(environment.as_deref().ok_or(Error::NotFound)?)?;
            validate_execution(&stored, record)?;
            let (volume_header, raw, mut next) = self.shared_volume_raw(&mount.volume_id).await?;
            if header.as_deref() != Some(volume_header.as_str()) {
                continue;
            }
            next.permissions
                .check_mount_permission(&attachment.tenant_id, mount)?;
            if next
                .retirements
                .iter()
                .any(|old| old.attachment.same_execution(&attachment))
            {
                return Err(Error::Conflict);
            }
            next.bind_storage(storage_id)?;
            if let Some(old) = next
                .attachments
                .iter()
                .find(|old| old.same_execution(&attachment))
            {
                return if old == &attachment {
                    Ok(next)
                } else {
                    Err(Error::Conflict)
                };
            }
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
                if !self
                    .prune_volume_retirements(&mut next, header.as_deref().ok_or(Error::Conflict)?)
                    .await?
                {
                    continue;
                }
                if next.retirements.len() >= RECONCILE_HISTORY_AT {
                    return Err(Error::Unavailable(
                        "volume retirement reconciliation required".into(),
                    ));
                }
            }
            next.attachments.push(attachment.clone());
            next.advance()?;
            let changes = [(mount.volume_id.clone(), raw, next.clone())];
            if self
                .volume_execution_cas(
                    header.as_deref().ok_or(Error::Conflict)?,
                    (&attachment.node_id, node_raw),
                    &changes,
                )
                .await?
            {
                return Ok(next);
            }
        }
        Err(Error::Unavailable(
            "shared volume attachment contention; retry same execution".into(),
        ))
    }

    /// Authenticated current node + its exact strong backend stop receipt.
    /// Old Environment ownership may have moved; that never releases its I/O.
    pub async fn release_shared_volume(
        &self,
        receipt: &SharedVolumeAttachmentRetirement,
        node_session: &str,
    ) -> Result<StoredSharedVolume> {
        receipt.validate()?;
        for _ in 0..ATTEMPTS {
            let [header, node] = self
                .store
                .fields([
                    HEADER.into(),
                    format!("node:{}", receipt.attachment.node_id),
                ])
                .await?;
            self.header(&header)?;
            let node: StoredNode = decode(node.as_deref().ok_or(Error::NotFound)?)?;
            reader_node(&node, &receipt.attachment.node_id, node_session)?;
            let (volume_header, raw, mut next) = self
                .shared_volume_raw(&receipt.attachment.volume_id)
                .await?;
            if header.as_deref() != Some(volume_header.as_str()) {
                continue;
            }
            if let Some(old) = next
                .retirements
                .iter()
                .find(|old| old.attachment.same_execution(&receipt.attachment))
            {
                return if old == receipt {
                    Ok(next)
                } else {
                    Err(Error::Conflict)
                };
            }
            let index = next
                .attachments
                .iter()
                .position(|old| old == &receipt.attachment)
                .ok_or(Error::Conflict)?;
            if next.retirements.len() >= RECONCILE_HISTORY_AT
                && !self
                    .prune_volume_retirements(&mut next, header.as_deref().ok_or(Error::Conflict)?)
                    .await?
            {
                continue;
            }
            if next.retirements.len() >= MAX_RETIREMENTS {
                return Err(Error::Unavailable(
                    "volume retirement reconciliation required".into(),
                ));
            }
            next.attachments.remove(index);
            next.retirements.push(receipt.clone());
            next.advance()?;
            if self
                .shared_volume_cas(header.as_deref().ok_or(Error::Conflict)?, &raw, &next)
                .await?
            {
                return Ok(next);
            }
        }
        Err(Error::Unavailable(
            "shared volume retirement contention; retry same receipt".into(),
        ))
    }

    async fn prune_volume_retirements(
        &self,
        volume: &mut StoredSharedVolume,
        header: &str,
    ) -> Result<bool> {
        let catalog = self.store.raw().await?;
        if catalog.get(HEADER).map(String::as_str) != Some(header) {
            return Ok(false);
        }
        self.header(&Some(header.to_owned()))?;
        let mut keep = Vec::new();
        for receipt in &volume.retirements {
            let identity = &receipt.attachment.identity;
            let Some(raw) = catalog.get(&format!("environment:{}", identity.environment_id)) else {
                continue;
            };
            let stored: StoredEnvironment = decode(raw)?;
            stored.validate()?;
            let obsolete = stored.invalidated
                || stored.assignment.generation > identity.ownership_generation
                || stored.result.as_ref().is_some_and(|record| {
                    matches!(
                        record.state,
                        EnvironmentState::Deleted | EnvironmentState::Paused
                    ) || record.state == EnvironmentState::Failed && !record.restart_pending
                        || record.runtime.id != identity.runtime_id
                            && RuntimeIdentity {
                                environment_id: record.spec.id.clone(),
                                runtime_id: record.runtime.id.clone(),
                                ownership_generation: record.assignment.generation,
                            }
                            .validate_restore_from(identity)
                            .is_ok()
                });
            if !obsolete {
                keep.push(receipt.clone());
            }
        }
        volume.retirements = keep;
        Ok(true)
    }
}

fn validate_execution(stored: &StoredEnvironment, requested: &EnvironmentRecord) -> Result<()> {
    stored.validate()?;
    if stored.invalidated
        || stored.spec != requested.spec
        || stored.assignment != requested.assignment
        || !requested.resources_held
        || requested.revision == 0
    {
        return Err(Error::Conflict);
    }
    let base = format!("{}-{}", requested.spec.id, requested.assignment.generation);
    let Some(previous) = &stored.result else {
        return if requested.state == EnvironmentState::Starting
            && requested.runtime.id == base
            && requested.restart_attempts == 0
        {
            Ok(())
        } else {
            Err(Error::Conflict)
        };
    };
    if previous.runtime.id == requested.runtime.id {
        return if previous.state == EnvironmentState::Running
            && previous.resources_held
            && requested.state == EnvironmentState::Running
            && requested.revision >= previous.revision
        {
            Ok(())
        } else {
            Err(Error::Conflict)
        };
    }
    let old = RuntimeIdentity {
        environment_id: previous.spec.id.clone(),
        runtime_id: previous.runtime.id.clone(),
        ownership_generation: previous.assignment.generation,
    };
    let new = RuntimeIdentity {
        environment_id: requested.spec.id.clone(),
        runtime_id: requested.runtime.id.clone(),
        ownership_generation: requested.assignment.generation,
    };
    new.validate_restore_from(&old)?;
    let resume = previous.state == EnvironmentState::Paused
        && !previous.resources_held
        && requested.state == EnvironmentState::Resuming;
    let restart = previous.state == EnvironmentState::Failed
        && previous.restart_pending
        && !previous.resources_held
        && requested.state == EnvironmentState::Starting
        && previous.restart_attempts.checked_add(1) == Some(requested.restart_attempts);
    if requested.revision > previous.revision && (resume || restart) {
        Ok(())
    } else {
        Err(Error::Conflict)
    }
}
