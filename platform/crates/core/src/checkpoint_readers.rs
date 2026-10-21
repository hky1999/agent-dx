//! Durable execution-owned reads in the existing Environment authority.
use crate::{runtime::RuntimeIdentity, EnvironmentRecord, Error, RestorePoint, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointReader {
    pub node_id: String,
    pub identity: RuntimeIdentity,
    pub checkpoint: RestorePoint,
}

/// Historical omissions are incomplete, never proof of no readers.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointReaders {
    #[serde(default)]
    pub complete: bool,
    #[serde(default)]
    pub readers: Vec<CheckpointReader>,
}
impl CheckpointReaders {
    pub fn is_unknown_empty(&self) -> bool {
        !self.complete && self.readers.is_empty()
    }
    pub fn known_empty() -> Self {
        Self {
            complete: true,
            readers: Vec::new(),
        }
    }
    pub fn reclaimable(&self) -> bool {
        self.complete && self.readers.is_empty()
    }
}
impl EnvironmentRecord {
    pub fn current_checkpoint_reader_entry(&self) -> Option<&CheckpointReader> {
        self.checkpoint_readers
            .readers
            .iter()
            .find(|reader| self.is_current_reader(reader))
    }
    pub fn validate_checkpoint_readers(&self) -> Result<()> {
        let mut seen = BTreeSet::new();
        for r in &self.checkpoint_readers.readers {
            r.identity.validate()?;
            let p = &r.checkpoint;
            if r.node_id.trim().is_empty()
                || r.identity.environment_id != self.spec.id
                || r.identity.ownership_generation > self.assignment.generation
                || (r.identity.ownership_generation == self.assignment.generation
                    && r.node_id != self.assignment.node_id)
                || !seen.insert((
                    r.node_id.as_str(),
                    &r.identity.runtime_id,
                    r.identity.ownership_generation,
                ))
                || p.id.is_empty()
                || p.source_runtime_id.is_empty()
                || p.artifact.storage.is_empty()
                || p.artifact.location.is_empty()
                || p.artifact.size_bytes == 0
                || p.expires_at_unix_seconds == Some(0)
            {
                return Err(Error::Conflict);
            }
            if let Some(origin) = &p.origin {
                origin.validate()?;
                if origin.runtime_id != p.source_runtime_id {
                    return Err(Error::Conflict);
                }
            } else if !crate::valid_runtime_id(
                &r.identity.environment_id,
                r.identity.ownership_generation,
                &p.source_runtime_id,
            ) {
                return Err(Error::Conflict);
            }
        }
        Ok(())
    }
    pub fn checkpoint_points(&self) -> impl Iterator<Item = &RestorePoint> {
        self.checkpoint.iter().chain(
            self.checkpoint_readers
                .readers
                .iter()
                .map(|r| &r.checkpoint),
        )
    }
    fn is_current_reader(&self, r: &CheckpointReader) -> bool {
        r.node_id == self.assignment.node_id
            && r.identity.environment_id == self.spec.id
            && r.identity.ownership_generation == self.assignment.generation
            && r.identity.runtime_id == self.runtime.id
    }
    pub fn current_checkpoint_reader(&self) -> Option<&RestorePoint> {
        self.checkpoint_readers
            .readers
            .iter()
            .find(|r| self.is_current_reader(r))
            .map(|r| &r.checkpoint)
    }
    pub fn register_checkpoint_reader(&mut self, checkpoint: RestorePoint) -> Result<()> {
        self.validate_checkpoint_readers()?;
        if let Some(old) = self.current_checkpoint_reader() {
            return if *old == checkpoint {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        self.checkpoint_readers.readers.push(CheckpointReader {
            node_id: self.assignment.node_id.clone(),
            identity: RuntimeIdentity {
                environment_id: self.spec.id.clone(),
                runtime_id: self.runtime.id.clone(),
                ownership_generation: self.assignment.generation,
            },
            checkpoint,
        });
        if let Err(error) = self.validate_checkpoint_readers() {
            self.checkpoint_readers.readers.pop();
            return Err(error);
        }
        Ok(())
    }
    /// Call only after remove() confirms this exact execution absent.
    /// Other nodes and older executions require their own stop proof.
    pub fn release_checkpoint_reader(&mut self) -> Option<RestorePoint> {
        let i = self
            .checkpoint_readers
            .readers
            .iter()
            .position(|r| self.is_current_reader(r))?;
        Some(self.checkpoint_readers.readers.remove(i).checkpoint)
    }
}

/// A trusted node obtains this durable, execution-bound receipt from its backend.
/// An empty inventory, timeout, or legacy idempotent Delete is not a receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointReaderRetirement {
    pub reader: CheckpointReader,
    pub backend_receipt: String,
}

impl CheckpointReaderRetirement {
    pub fn validate(&self) -> Result<()> {
        self.reader.identity.validate()?;
        if self.reader.node_id.trim().is_empty()
            || self.backend_receipt.is_empty()
            || self.backend_receipt.len() > 4096
            || self.backend_receipt.chars().any(char::is_control)
        {
            return Err(Error::Invalid(
                "invalid checkpoint reader retirement receipt".into(),
            ));
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Assignment, CheckpointArtifact, EnvironmentSpec, EnvironmentState, Resources, Runtime,
    };
    fn record() -> EnvironmentRecord {
        EnvironmentRecord {
            checkpoint_readers: CheckpointReaders::known_empty(),
            failure: None,
            restart_attempts: 0,
            restart_pending: false,
            spec: EnvironmentSpec {
                id: "e".into(),
                tenant_id: "t".into(),
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
                environment_id: "e".into(),
                node_id: "n".into(),
                shard_id: 0,
                generation: 1,
                devices: Vec::new(),
            },
            state: EnvironmentState::Running,
            revision: 2,
            runtime: Runtime {
                id: "e-1-r1".into(),
                ip: Some("127.0.0.1".parse().unwrap()),
            },
            resources_held: true,
            checkpoint: None,
            last_operation: None,
        }
    }
    fn point(name: &str) -> RestorePoint {
        RestorePoint {
            id: name.into(),
            origin: None,
            source_runtime_id: "e-1".into(),
            expires_at_unix_seconds: None,
            artifact: CheckpointArtifact {
                storage: "shared_fs".into(),
                location: name.into(),
                size_bytes: 1,
            },
        }
    }
    #[test]
    fn recovery_point_replacement_and_json_restart_preserve_actual_reader() {
        let mut r = record();
        r.register_checkpoint_reader(point("a")).unwrap();
        r.checkpoint = Some(point("b"));
        let restored: EnvironmentRecord =
            serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
        assert_eq!(restored.current_checkpoint_reader().unwrap().id, "a");
        assert_eq!(
            restored
                .checkpoint_points()
                .map(|p| p.id.as_str())
                .collect::<Vec<_>>(),
            vec!["b", "a"]
        );
    }
    #[test]
    fn historical_omission_is_unknown_not_known_empty() {
        let mut value = serde_json::to_value(record()).unwrap();
        value.as_object_mut().unwrap().remove("checkpoint_readers");
        let r: EnvironmentRecord = serde_json::from_value(value).unwrap();
        assert!(!r.checkpoint_readers.reclaimable());
        assert!(!r.checkpoint_readers.complete);
    }
    #[test]
    fn conflicting_duplicate_and_invalid_registration_are_rejected() {
        let mut r = record();
        let mut invalid = point("x");
        invalid.artifact.size_bytes = 0;
        assert!(r.register_checkpoint_reader(invalid).is_err());
        assert!(r.checkpoint_readers.readers.is_empty());
        r.register_checkpoint_reader(point("a")).unwrap();
        assert_eq!(
            r.register_checkpoint_reader(point("b")),
            Err(Error::Conflict)
        );
        r.checkpoint_readers
            .readers
            .push(r.checkpoint_readers.readers[0].clone());
        assert_eq!(r.validate_checkpoint_readers(), Err(Error::Conflict));
    }
    #[test]
    fn transfer_keeps_source_reader_until_its_own_stop_proof() {
        let mut r = record();
        r.register_checkpoint_reader(point("a")).unwrap();
        r.assignment.node_id = "target".into();
        r.assignment.generation = 2;
        r.runtime.id = "e-2-r1".into();
        r.validate_checkpoint_readers().unwrap();
        let mut b = point("b");
        b.origin = Some(RuntimeIdentity {
            environment_id: "e".into(),
            runtime_id: "e-1".into(),
            ownership_generation: 1,
        });
        r.register_checkpoint_reader(b).unwrap();
        assert_eq!(r.release_checkpoint_reader().unwrap().id, "b");
        assert_eq!(r.checkpoint_readers.readers[0].checkpoint.id, "a");
        assert!(!r.checkpoint_readers.reclaimable());
    }
}
