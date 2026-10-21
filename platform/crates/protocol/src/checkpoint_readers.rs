use crate::{control, Error, Result};
impl From<adx_core::CheckpointReaders> for control::CheckpointReaders {
    fn from(v: adx_core::CheckpointReaders) -> Self {
        Self {
            complete: v.complete,
            readers: v
                .readers
                .into_iter()
                .map(|r| control::CheckpointReader {
                    node_id: r.node_id,
                    identity: Some(r.identity.into()),
                    checkpoint: Some(r.checkpoint.into()),
                })
                .collect(),
        }
    }
}
impl TryFrom<control::CheckpointReaders> for adx_core::CheckpointReaders {
    type Error = Error;
    fn try_from(v: control::CheckpointReaders) -> Result<Self> {
        let readers = v
            .readers
            .into_iter()
            .map(|r| {
                Ok(adx_core::CheckpointReader {
                    node_id: r.node_id,
                    identity: r
                        .identity
                        .ok_or_else(|| Error::Invalid("reader identity required".into()))?
                        .try_into()?,
                    checkpoint: r
                        .checkpoint
                        .ok_or_else(|| Error::Invalid("reader checkpoint required".into()))?
                        .try_into()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            complete: v.complete,
            readers,
        })
    }
}
