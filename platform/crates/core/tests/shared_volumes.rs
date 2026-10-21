use adx_core::shared_volumes::{
    SharedVolumeAccess as Access, SharedVolumeGrant as Grant, SharedVolumeId as VolumeId,
    SharedVolumeMount as Mount, SharedVolumeRecord as Volume, SharedVolumeSubpath as Subpath,
    MAX_SHARED_VOLUME_GRANTS,
};
use adx_core::Error;
use serde_json::json;

const ID: &str = "3ca91193-56c8-4309-b159-f08e11fa6267";
const OTHER_ID: &str = "db671c19-4aaf-4714-93c9-16fb2bf1d246";

fn volume() -> Volume {
    Volume::new(VolumeId::parse(ID).unwrap(), "owner".into()).unwrap()
}

fn subpath(value: &str) -> Subpath {
    Subpath::parse(value).unwrap()
}

fn mount(path: &str, access: Access) -> Mount {
    Mount {
        volume_id: VolumeId::parse(ID).unwrap(),
        subpath: subpath(path),
        target: "/shared".into(),
        access,
    }
}

fn grant(tenant: &str, path: &str, access: Access) -> Grant {
    Grant {
        tenant_id: tenant.into(),
        subpath: subpath(path),
        access,
    }
}

#[test]
fn volume_id_is_opaque_canonical_and_non_nil() {
    assert_eq!(VolumeId::parse(ID).unwrap().as_str(), ID);
    for invalid in [
        "".to_owned(),
        "/etc/passwd".to_owned(),
        "../volume".to_owned(),
        "00000000-0000-0000-0000-000000000000".to_owned(),
        ID.to_ascii_uppercase(),
        ID.replace('-', ""),
        format!(" {ID}"),
        format!("urn:uuid:{ID}"),
    ] {
        assert!(VolumeId::parse(invalid).is_err());
    }
}

#[test]
fn deserialization_cannot_bypass_id_or_subpath_validation() {
    assert!(serde_json::from_value::<VolumeId>(json!("../escape")).is_err());
    assert!(serde_json::from_value::<Subpath>(json!("a/../b")).is_err());
    let id: VolumeId = serde_json::from_value(json!(ID)).unwrap();
    assert_eq!(serde_json::to_value(id).unwrap(), json!(ID));
}

#[test]
fn subpaths_reject_lexical_escape_and_ambiguity() {
    assert_eq!(Subpath::default().as_str(), "");
    assert_eq!(subpath("team/project").as_str(), "team/project");
    for invalid in [
        ".", "..", "../x", "/x", "x/../y", "x/./y", "x//y", "x/", "x\\y", "x\0y", "x\ny",
    ] {
        assert!(Subpath::parse(invalid).is_err(), "{invalid:?}");
    }
    assert!(Subpath::parse("x".repeat(4097)).is_err());
}

#[test]
fn omitted_mount_access_is_read_only_and_subpath_is_root() {
    let value: Mount = serde_json::from_value(json!({
        "volume_id": ID, "target": "/shared"
    }))
    .unwrap();
    value.validate().unwrap();
    assert_eq!(value.access, Access::ReadOnly);
    assert_eq!(value.subpath.as_str(), "");
}

#[test]
fn mount_cannot_smuggle_an_arbitrary_host_path() {
    for field in ["host_path", "hostPath", "image_url", "s3_config"] {
        let mut value = json!({"volume_id": ID, "target": "/shared"});
        value[field] = json!("/etc");
        assert!(serde_json::from_value::<Mount>(value).is_err());
    }
}

#[test]
fn guest_targets_are_canonical_and_do_not_shadow_runtime_mounts() {
    for invalid in [
        "relative",
        "/",
        "//shared",
        "/shared/",
        "/shared//x",
        "/shared/./x",
        "/shared/../x",
        "/proc",
        "/proc/x",
        "/sys",
        "/dev/x",
        "/__adx",
        "/__adx/x",
        "/shared\\x",
        "/shared\nx",
    ] {
        let mut request = mount("", Access::ReadOnly);
        request.target = invalid.into();
        assert!(request.validate().is_err(), "{invalid:?}");
    }
    for valid in ["/shared", "/mnt/team", "/proc-not-reserved"] {
        let mut request = mount("", Access::ReadOnly);
        request.target = valid.into();
        request.validate().unwrap();
    }
}

#[test]
fn owner_has_permission_without_an_explicit_grant() {
    let record = volume();
    assert_eq!(record.id().as_str(), ID);
    assert_eq!(record.owner_tenant_id(), "owner");
    assert_eq!(record.revision(), 1);
    assert!(record.grants().is_empty());
    for access in [Access::ReadOnly, Access::ReadWrite] {
        record
            .check_mount_permission("owner", &mount("team/data", access))
            .unwrap();
    }
}

#[test]
fn stranger_cannot_read_or_modify_another_tenants_permissions() {
    let mut record = volume();
    let before = record.clone();
    assert_eq!(
        record.check_mount_permission("stranger", &mount("", Access::ReadOnly)),
        Err(Error::NotFound)
    );
    assert_eq!(
        record.grant("stranger", grant("stranger", "", Access::ReadWrite)),
        Err(Error::NotFound)
    );
    assert_eq!(
        record.revoke("stranger", "owner", &subpath("")),
        Err(Error::NotFound)
    );
    assert_eq!(record, before);
}

#[test]
fn read_only_grant_cannot_be_upgraded_by_mount_request() {
    let mut record = volume();
    record
        .grant("owner", grant("reader", "team", Access::ReadOnly))
        .unwrap();
    record
        .check_mount_permission("reader", &mount("team/data", Access::ReadOnly))
        .unwrap();
    assert_eq!(
        record.check_mount_permission("reader", &mount("team/data", Access::ReadWrite)),
        Err(Error::NotFound)
    );
    assert_eq!(
        record.check_mount_permission("other-reader", &mount("team/data", Access::ReadOnly)),
        Err(Error::NotFound)
    );
}

#[test]
fn subtree_grants_use_component_boundaries_not_string_prefixes() {
    let mut record = volume();
    record
        .grant("owner", grant("reader", "team", Access::ReadOnly))
        .unwrap();
    for path in ["team", "team/project", "team/project/file"] {
        record
            .check_mount_permission("reader", &mount(path, Access::ReadOnly))
            .unwrap();
    }
    for path in ["", "team2", "teammate/project", "other/team"] {
        assert_eq!(
            record.check_mount_permission("reader", &mount(path, Access::ReadOnly)),
            Err(Error::NotFound)
        );
    }
}

#[test]
fn multiple_subtree_grants_do_not_expand_each_others_write_scope() {
    let mut record = volume();
    record
        .grant("owner", grant("agent", "", Access::ReadOnly))
        .unwrap();
    record
        .grant("owner", grant("agent", "team/output", Access::ReadWrite))
        .unwrap();
    record
        .check_mount_permission("agent", &mount("team/output/result", Access::ReadWrite))
        .unwrap();
    assert_eq!(
        record.check_mount_permission("agent", &mount("team/input", Access::ReadWrite)),
        Err(Error::NotFound)
    );
    record
        .check_mount_permission("agent", &mount("team/input", Access::ReadOnly))
        .unwrap();
}

#[test]
fn permission_updates_are_versioned_and_idempotent() {
    let mut record = volume();
    let ro = grant("reader", "team", Access::ReadOnly);
    assert!(record.grant("owner", ro.clone()).unwrap());
    assert_eq!(record.revision(), 2);
    assert!(!record.grant("owner", ro).unwrap());
    assert_eq!(record.revision(), 2);
    assert!(record
        .grant("owner", grant("reader", "team", Access::ReadWrite))
        .unwrap());
    assert_eq!(record.revision(), 3);
    assert!(record.revoke("owner", "reader", &subpath("team")).unwrap());
    assert_eq!(record.revision(), 4);
    assert!(!record.revoke("owner", "reader", &subpath("team")).unwrap());
    assert_eq!(record.revision(), 4);
}

#[test]
fn saved_mount_does_not_keep_revoked_or_downgraded_permissions() {
    let mut record = volume();
    let saved = mount("team/data", Access::ReadWrite);
    record
        .grant("owner", grant("agent", "team", Access::ReadWrite))
        .unwrap();
    record.check_mount_permission("agent", &saved).unwrap();
    let saved_json = serde_json::to_value(saved).unwrap();
    record.revoke("owner", "agent", &subpath("team")).unwrap();
    let restored: Mount = serde_json::from_value(saved_json).unwrap();
    assert_eq!(
        record.check_mount_permission("agent", &restored),
        Err(Error::NotFound)
    );
    record
        .grant("owner", grant("agent", "team", Access::ReadOnly))
        .unwrap();
    assert_eq!(
        record.check_mount_permission("agent", &restored),
        Err(Error::NotFound)
    );
    record
        .check_mount_permission("agent", &mount("team/data", Access::ReadOnly))
        .unwrap();
}

#[test]
fn owner_permissions_are_not_a_revocable_grant_entry() {
    let mut record = volume();
    let before = record.clone();
    assert!(record
        .grant("owner", grant("owner", "", Access::ReadOnly))
        .is_err());
    assert_eq!(record, before);
    assert!(!record.revoke("owner", "owner", &subpath("")).unwrap());
    record
        .check_mount_permission("owner", &mount("", Access::ReadWrite))
        .unwrap();
}

#[test]
fn malformed_or_duplicate_persisted_permissions_fail_deserialization() {
    let original = serde_json::to_value(volume()).unwrap();
    for (key, value) in [
        ("owner_tenant_id", json!("")),
        ("owner_tenant_id", json!(" owner")),
        ("revision", json!(0)),
        ("host_path", json!("/etc")),
        (
            "grants",
            json!([
                grant("reader", "team", Access::ReadOnly),
                grant("reader", "team", Access::ReadWrite)
            ]),
        ),
        ("grants", json!([grant("owner", "", Access::ReadOnly)])),
    ] {
        let mut raw = original.clone();
        raw[key] = value;
        assert!(serde_json::from_value::<Volume>(raw).is_err(), "{key}");
    }
}

#[test]
fn revision_exhaustion_cannot_partially_change_permissions() {
    let mut initial = volume();
    initial
        .grant("owner", grant("reader", "team", Access::ReadOnly))
        .unwrap();
    let mut raw = serde_json::to_value(initial).unwrap();
    raw["revision"] = json!(u64::MAX);
    let mut record: Volume = serde_json::from_value(raw).unwrap();
    let before = record.clone();
    assert_eq!(
        record.grant("owner", grant("reader", "team", Access::ReadWrite)),
        Err(Error::Conflict)
    );
    assert_eq!(record, before);
    assert_eq!(
        record.revoke("owner", "reader", &subpath("team")),
        Err(Error::Conflict)
    );
    assert_eq!(record, before);
}

#[test]
fn grant_limit_bounds_records_without_blocking_an_existing_update() {
    let mut record = volume();
    for index in 0..MAX_SHARED_VOLUME_GRANTS {
        record
            .grant(
                "owner",
                grant(&format!("tenant-{index}"), "", Access::ReadOnly),
            )
            .unwrap();
    }
    let before = record.clone();
    assert!(record
        .grant("owner", grant("overflow", "", Access::ReadOnly))
        .is_err());
    assert_eq!(record, before);
    assert!(record
        .grant("owner", grant("tenant-0", "", Access::ReadWrite))
        .unwrap());
    assert_eq!(record.grants().len(), MAX_SHARED_VOLUME_GRANTS);
}

#[test]
fn permissions_are_bound_to_volume_and_explicit_access_modes() {
    let record = volume();
    let mut other = mount("", Access::ReadOnly);
    other.volume_id = VolumeId::parse(OTHER_ID).unwrap();
    assert_eq!(
        record.check_mount_permission("owner", &other),
        Err(Error::NotFound)
    );
    assert!(Access::ReadWrite.permits(Access::ReadOnly));
    assert!(Access::ReadWrite.permits(Access::ReadWrite));
    assert!(Access::ReadOnly.permits(Access::ReadOnly));
    assert!(!Access::ReadOnly.permits(Access::ReadWrite));
    assert!(serde_json::from_value::<Access>(json!("rw")).is_err());
}
