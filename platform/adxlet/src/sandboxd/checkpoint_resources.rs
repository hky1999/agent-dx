use crate::checkpoint::CheckpointResources;
use adx_core::{Error, Result};
use std::path::Path;
use tokio::io::AsyncReadExt;

/// Validate a completed artifact's original resource condition without capturing again.
pub(super) async fn verify(path: &Path, resources: Option<&CheckpointResources>) -> Result<()> {
    if let Some(condition) = resources {
        condition.validate()?;
    }
    let file = tokio::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path.join("manifest.json"))
        .await
        .map_err(super::unavailable)?;
    let metadata = file.metadata().await.map_err(super::unavailable)?;
    if !metadata.is_file() || metadata.len() > 65_536 {
        return Err(Error::Invalid(
            "bounded ordinary checkpoint manifest required".into(),
        ));
    }
    let mut data = Vec::new();
    file.take(65_537)
        .read_to_end(&mut data)
        .await
        .map_err(super::unavailable)?;
    if data.len() > 65_536 {
        return Err(Error::Invalid("checkpoint manifest exceeds bound".into()));
    }
    let value: serde_json::Value = serde_json::from_slice(&data).map_err(super::unavailable)?;
    let revision = match value.get("sourceResourceRevision") {
        None => 0,
        Some(value) => value
            .as_u64()
            .ok_or_else(|| Error::Invalid("invalid captured resource revision".into()))?,
    };
    match resources {
        None if revision == 0 => Ok(()),
        Some(condition)
            if revision == condition.revision
                && value
                    .get("sourceGeneration")
                    .and_then(serde_json::Value::as_str)
                    == Some(condition.generation.as_str()) =>
        {
            Ok(())
        }
        _ => Err(Error::Conflict),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn original_resource_condition_is_required_on_every_replay() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("manifest.json"),
            br#"{"sourceGeneration":"original","sourceResourceRevision":7}"#,
        )
        .unwrap();
        let condition = CheckpointResources {
            generation: "original".into(),
            revision: 7,
        };
        verify(root.path(), Some(&condition)).await.unwrap();
        assert!(verify(root.path(), None).await.is_err());
        assert!(verify(
            root.path(),
            Some(&CheckpointResources {
                revision: 8,
                ..condition.clone()
            })
        )
        .await
        .is_err());
        assert!(verify(
            root.path(),
            Some(&CheckpointResources {
                generation: "replacement".into(),
                ..condition
            })
        )
        .await
        .is_err());
    }
    #[tokio::test]
    async fn missing_condition_cannot_be_added_to_legacy_artifact() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("manifest.json"), b"{}").unwrap();
        verify(root.path(), None).await.unwrap();
        assert!(verify(
            root.path(),
            Some(&CheckpointResources {
                generation: "original".into(),
                revision: 1
            })
        )
        .await
        .is_err());
    }
}
