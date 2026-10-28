//! Template records and snapshot pins commit together under the Coordinator epoch.
use super::*;
use adx_core::templates::{Template, TemplateState};

impl Session {
    fn templates_key(&self) -> String {
        format!("{}:environment-templates", self.store.key)
    }

    async fn template_raw(&self, id: &str) -> Result<Option<String>> {
        let [header] = self.store.fields([HEADER.into()]).await?;
        self.header(&header)?;
        let mut command = redis::cmd("HGET");
        command.arg(self.templates_key()).arg(id);
        self.store.query(command).await
    }

    pub async fn get_template(&self, id: &str, tenant: &str) -> Result<Template> {
        let raw = self.template_raw(id).await?.ok_or(Error::NotFound)?;
        let item: Template = decode(&raw)?;
        item.validate()?;
        if item.tenant_id != tenant || item.state != TemplateState::Ready {
            return Err(Error::NotFound);
        }
        Ok(item)
    }

    pub async fn list_templates(&self, tenant: &str) -> Result<Vec<Template>> {
        let [header] = self.store.fields([HEADER.into()]).await?;
        self.header(&header)?;
        let mut command = redis::cmd("HVALS");
        command.arg(self.templates_key());
        let raw: Vec<String> = self.store.query(command).await?;
        let mut items = Vec::new();
        for value in raw {
            let item: Template = decode(&value)?;
            item.validate()?;
            if item.tenant_id == tenant && item.state == TemplateState::Ready {
                items.push(item);
            }
        }
        items.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(items)
    }

    async fn template_cas(
        &self,
        old_template: Option<&str>,
        next_template: &Template,
        old_snapshot: &str,
        next_snapshot: &adx_core::snapshots::Snapshot,
    ) -> Result<bool> {
        next_template.validate()?;
        next_snapshot.validate()?;
        let _access = self
            .store
            .lock_artifacts(std::slice::from_ref(&next_snapshot.artifact))?;
        let [header] = self.store.fields([HEADER.into()]).await?;
        self.header(&header)?;
        let mut command = redis::cmd("EVAL");
        command
            .arg(
                r#"
            if redis.call('HGET',KEYS[1],'epoch') ~= ARGV[1] then return 0 end
            if redis.call('HGET',KEYS[2],ARGV[2]) ~= ARGV[3] then return 0 end
            local old = redis.call('HGET',KEYS[3],ARGV[5])
            if ARGV[6] == '' then
                if old then return 0 end
            elseif old ~= ARGV[6] then return 0 end
            redis.call('HSET',KEYS[2],ARGV[2],ARGV[4])
            redis.call('HSET',KEYS[3],ARGV[5],ARGV[7])
            return 1
        "#,
            )
            .arg(3)
            .arg(&self.store.key)
            .arg(self.snapshots_key())
            .arg(self.templates_key())
            .arg(self.epoch.to_string())
            .arg(&next_snapshot.id)
            .arg(old_snapshot)
            .arg(encode(next_snapshot)?)
            .arg(&next_template.id)
            .arg(old_template.unwrap_or(""))
            .arg(encode(next_template)?);
        let mut inventory = self.inspection.lock().await;
        let mut committed = inventory.take();
        match self.store.query::<u64>(command).await {
            Ok(1) => {
                if let Some(value) = committed.as_mut() {
                    value
                        .snapshots
                        .insert(next_snapshot.id.clone(), next_snapshot.clone());
                }
                *inventory = committed;
                Ok(true)
            }
            Ok(_) => {
                *inventory = committed;
                Ok(false)
            }
            Err(error) => {
                *inventory = None;
                Err(error)
            }
        }
    }

    pub async fn import_template(&self, item: Template) -> Result<Template> {
        item.validate()?;
        if item.state != TemplateState::Ready {
            return Err(Error::Invalid("new template must be ready".into()));
        }
        for _ in 0..ATTEMPTS {
            if let Some(raw) = self.template_raw(&item.id).await? {
                let existing: Template = decode(&raw)?;
                existing.validate()?;
                return if existing == item {
                    Ok(existing)
                } else {
                    Err(Error::Conflict)
                };
            }
            let raw = self
                .snapshot_raw(&item.snapshot_id)
                .await?
                .ok_or(Error::NotFound)?;
            let mut snapshot: adx_core::snapshots::Snapshot = decode(&raw)?;
            snapshot.validate()?;
            if snapshot.template.tenant_id != item.tenant_id {
                return Err(Error::NotFound);
            }
            snapshot.acquire(&item.tenant_id, item.reference())?;
            if self.template_cas(None, &item, &raw, &snapshot).await? {
                return Ok(item);
            }
        }
        Err(Error::Unavailable("template publication contention".into()))
    }

    /// Unregister only this catalog owner; the caller-owned snapshot and children survive.
    pub async fn delete_template(&self, id: &str, tenant: &str) -> Result<()> {
        for _ in 0..ATTEMPTS {
            let old = self.template_raw(id).await?.ok_or(Error::NotFound)?;
            let mut item: Template = decode(&old)?;
            item.validate()?;
            if item.tenant_id != tenant {
                return Err(Error::NotFound);
            }
            if item.state == TemplateState::Deleted {
                return Ok(());
            }
            let raw = self
                .snapshot_raw(&item.snapshot_id)
                .await?
                .ok_or(Error::NotFound)?;
            let mut snapshot: adx_core::snapshots::Snapshot = decode(&raw)?;
            snapshot.validate()?;
            if snapshot.template.tenant_id != tenant {
                return Err(Error::NotFound);
            }
            snapshot.release(&item.reference())?;
            item.state = TemplateState::Deleted;
            if self
                .template_cas(Some(&old), &item, &raw, &snapshot)
                .await?
            {
                return Ok(());
            }
        }
        Err(Error::Unavailable("template deletion contention".into()))
    }
}
