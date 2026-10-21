use crate::control as pb;
use adx_core::{
    history::{HistoryBoundary, HistoryProtection},
    Error, Result,
};
impl TryFrom<pb::HistoryBoundary> for HistoryBoundary {
    type Error = Error;
    fn try_from(value: pb::HistoryBoundary) -> Result<Self> {
        let mut protected = std::collections::BTreeMap::new();
        for p in value.protected {
            let assignment: adx_core::Assignment =
                p.assignment.ok_or(Error::Conflict)?.try_into()?;
            let id = assignment.environment_id.clone();
            if protected
                .insert(
                    id,
                    HistoryProtection {
                        assignment,
                        specification_sha256: p.specification_sha256,
                        minimum_runtime_revision: p.minimum_runtime_revision,
                        environment_revision: p.environment_revision,
                    },
                )
                .is_some()
            {
                return Err(Error::Conflict);
            }
        }
        let b = Self {
            authority: value.authority,
            coordinator_epoch: value.coordinator_epoch,
            assignment_generation: value.assignment_generation,
            catalog_revision: value.catalog_revision,
            protected,
        };
        b.validate()?;
        Ok(b)
    }
}
impl TryFrom<HistoryBoundary> for pb::HistoryBoundary {
    type Error = Error;
    fn try_from(value: HistoryBoundary) -> Result<Self> {
        value.validate()?;
        Ok(Self {
            authority: value.authority,
            coordinator_epoch: value.coordinator_epoch,
            assignment_generation: value.assignment_generation,
            catalog_revision: value.catalog_revision,
            protected: value
                .protected
                .into_values()
                .map(|p| {
                    Ok(pb::HistoryProtection {
                        assignment: Some(p.assignment.try_into()?),
                        specification_sha256: p.specification_sha256,
                        minimum_runtime_revision: p.minimum_runtime_revision,
                        environment_revision: p.environment_revision,
                    })
                })
                .collect::<Result<_>>()?,
        })
    }
}
