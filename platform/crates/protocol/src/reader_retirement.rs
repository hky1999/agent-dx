use crate::{control, Error, Result};

impl From<adx_core::CheckpointReader> for control::CheckpointReader {
    fn from(reader: adx_core::CheckpointReader) -> Self {
        Self {
            node_id: reader.node_id,
            identity: Some(reader.identity.into()),
            checkpoint: Some(reader.checkpoint.into()),
        }
    }
}

impl TryFrom<control::CheckpointReader> for adx_core::CheckpointReader {
    type Error = Error;
    fn try_from(reader: control::CheckpointReader) -> Result<Self> {
        Ok(Self {
            node_id: reader.node_id,
            identity: reader
                .identity
                .ok_or_else(|| Error::Invalid("reader identity required".into()))?
                .try_into()?,
            checkpoint: reader
                .checkpoint
                .ok_or_else(|| Error::Invalid("reader checkpoint required".into()))?
                .try_into()?,
        })
    }
}
