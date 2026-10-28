//! Tenant-owned immutable registrations of reusable snapshots.
use crate::{snapshots::Reference, Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TemplateState {
    Ready,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Template {
    pub id: String,
    pub tenant_id: String,
    pub snapshot_id: String,
    pub name: String,
    pub state: TemplateState,
}

impl Template {
    pub fn validate(&self) -> Result<()> {
        let valid =
            |s: &str| !s.trim().is_empty() && s.len() <= 256 && !s.chars().any(char::is_control);
        let route_id = !self.id.is_empty()
            && self.id.len() <= 128
            && self
                .id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_');
        if !route_id || !valid(&self.tenant_id) || !valid(&self.snapshot_id) || !valid(&self.name) {
            return Err(Error::Invalid("invalid environment template".into()));
        }
        Ok(())
    }

    pub fn reference(&self) -> Reference {
        Reference::CatalogTemplate {
            template_id: self.id.clone(),
        }
    }
}
