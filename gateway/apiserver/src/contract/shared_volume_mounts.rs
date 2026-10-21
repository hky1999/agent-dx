//! Closed public mount shape; caller host paths/options are not accepted.
use super::*;
use adx_core::shared_volumes::{SharedVolumeAccess, SharedVolumeId, SharedVolumeSubpath};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct Mount {
    volume_id: String,
    #[serde(default)]
    subpath: String,
    target: String,
    access: String,
}
impl Mount {
    pub(super) fn encode(self) -> Result<adx_core::shared_volumes::SharedVolumeMount, Status> {
        let access = match self.access.as_str() {
            "read_only" => SharedVolumeAccess::ReadOnly,
            "read_write" => SharedVolumeAccess::ReadWrite,
            _ => return Err(invalid("explicit shared volume access required")),
        };
        Ok(adx_core::shared_volumes::SharedVolumeMount {
            volume_id: SharedVolumeId::parse(self.volume_id).map_err(adx_protocol::status)?,
            subpath: SharedVolumeSubpath::parse(self.subpath).map_err(adx_protocol::status)?,
            target: self.target,
            access,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn body() -> Value {
        json!({"image":"example/image:v1","sharedVolumes":[{
        "volumeId":"00000000-0000-4000-8000-000000000001",
        "target":"/shared","access":"read_write","subpath":"project"}]})
    }
    fn caller() -> pb::CallerContext {
        pb::CallerContext {
            tenant_id: "tenant".into(),
            administrator: false,
        }
    }
    #[test]
    fn shared_mount_input_reaches_the_typed_execution() {
        let spec: adx_core::EnvironmentSpec =
            create_spec(body(), &caller()).unwrap().try_into().unwrap();
        let mount = &spec.execution.shared_volumes[0];
        assert_eq!(mount.subpath.as_str(), "project");
        assert_eq!(mount.access, SharedVolumeAccess::ReadWrite);
        assert_eq!(mount.target, "/shared");
        assert_eq!(spec.tenant_id, "tenant");
    }
    #[test]
    fn shared_mount_input_rejects_privileged_source_and_options() {
        for (key, value) in [
            ("hostPath", json!("/etc")),
            ("source", json!("/etc")),
            ("options", json!(["rw", "bind"])),
            ("tenantId", json!("other")),
        ] {
            let mut value_body = body();
            value_body["sharedVolumes"][0][key] = value;
            assert!(create_spec(value_body, &caller()).is_err(), "{key}");
        }
    }
    #[test]
    fn shared_mount_input_requires_explicit_access_and_safe_paths() {
        for (key, value) in [
            ("access", json!("unknown")),
            ("subpath", json!("../escape")),
            ("target", json!("/dev/shared")),
            ("volumeId", json!("invalid")),
        ] {
            let mut value_body = body();
            value_body["sharedVolumes"][0][key] = value;
            assert!(create_spec(value_body, &caller()).is_err(), "{key}");
        }
        let mut value = body();
        value["sharedVolumes"][0]
            .as_object_mut()
            .unwrap()
            .remove("access");
        assert!(create_spec(value, &caller()).is_err());
    }
    #[test]
    fn shared_mount_input_does_not_silently_drop_duplicates_or_overlaps() {
        let mut value = body();
        let mut other = value["sharedVolumes"][0].clone();
        other["target"] = json!("/another");
        value["sharedVolumes"].as_array_mut().unwrap().push(other);
        assert!(create_spec(value, &caller()).is_err());
    }
}
