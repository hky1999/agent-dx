//! Structural execution contracts only, not real mounts or storage fencing.
use adx_core::{
    execution::{ExecutionOptions, Mount, StorageSource, MAX_SHARED_VOLUME_MOUNTS},
    shared_volumes::{SharedVolumeAccess, SharedVolumeId, SharedVolumeMount, SharedVolumeSubpath},
    Resources,
};

fn mount(number: u32, target: &str) -> SharedVolumeMount {
    SharedVolumeMount {
        volume_id: SharedVolumeId::parse(format!("00000000-0000-4000-8000-{number:012x}")).unwrap(),
        subpath: SharedVolumeSubpath::parse("project").unwrap(),
        target: target.into(),
        access: SharedVolumeAccess::ReadWrite,
    }
}

fn options(mounts: Vec<SharedVolumeMount>) -> ExecutionOptions {
    ExecutionOptions {
        shared_volumes: mounts,
        ..Default::default()
    }
}

#[test]
fn absent_shared_volumes_preserve_historical_serialized_execution_bytes() {
    let legacy = r#"{"readiness_probe":null,"healthcheck_probe":null,"init_timeout_seconds":0,"rootfs":null,"mounts":[],"network":null,"data_plane":{"tunnel":"Inherit","port_forward":"Inherit"},"ports":[],"failover":false,"inherit_entrypoint":false,"limits":{"cpu_millis":0,"memory_bytes":0,"disk_bytes":0},"extra_config":""}"#;
    let restored: ExecutionOptions = serde_json::from_str(legacy).unwrap();
    assert!(restored.shared_volumes.is_empty());
    assert_eq!(serde_json::to_string(&restored).unwrap(), legacy);
    assert_eq!(
        serde_json::to_string(&ExecutionOptions::default()).unwrap(),
        legacy
    );
}

#[test]
fn shared_volume_execution_roundtrip_keeps_explicit_access_and_identity() {
    for access in [SharedVolumeAccess::ReadOnly, SharedVolumeAccess::ReadWrite] {
        let mut value = mount(1, "/shared");
        value.access = access;
        let expected = options(vec![value]);
        expected.validate(&Resources::default()).unwrap();
        let encoded = serde_json::to_string(&expected).unwrap();
        assert_eq!(
            serde_json::from_str::<ExecutionOptions>(&encoded).unwrap(),
            expected
        );
    }
}

#[test]
fn duplicate_volume_cannot_hide_behind_different_targets() {
    assert!(options(vec![mount(1, "/first"), mount(1, "/second")])
        .validate(&Resources::default())
        .is_err());
}

#[test]
fn shared_mount_targets_cannot_overlap_in_either_direction() {
    for (left, right) in [
        ("/shared", "/shared"),
        ("/shared", "/shared/sub"),
        ("/shared/sub", "/shared"),
    ] {
        assert!(options(vec![mount(1, left), mount(2, right)])
            .validate(&Resources::default())
            .is_err());
    }
    options(vec![mount(1, "/shared"), mount(2, "/shared2")])
        .validate(&Resources::default())
        .unwrap();
}

#[test]
fn ordinary_mount_aliases_cannot_shadow_shared_mounts() {
    for ordinary in ["/", "/shared", "/shared/sub", "//shared/./sub", "/shared/."] {
        let mut execution = options(vec![mount(1, "/shared")]);
        execution.mounts.push(Mount {
            kind: "bind".into(),
            target: ordinary.into(),
            options: vec!["ro".into()],
            source: StorageSource::Image("registry.example/tools:v1".into()),
        });
        assert!(
            execution.validate(&Resources::default()).is_err(),
            "{ordinary}"
        );
    }
}

#[test]
fn malformed_shared_mount_target_is_not_accepted_via_execution() {
    for target in ["/", "/dev/private", "/__adx/private", "/shared/../escape"] {
        assert!(
            options(vec![mount(1, target)])
                .validate(&Resources::default())
                .is_err(),
            "{target}"
        );
    }
}

#[test]
fn execution_mount_count_has_a_bounded_admission_surface() {
    let mounts = (1..=MAX_SHARED_VOLUME_MOUNTS)
        .map(|number| mount(number as u32, &format!("/shared-{number}")))
        .collect();
    let mut execution = options(mounts);
    execution.validate(&Resources::default()).unwrap();
    execution.shared_volumes.push(mount(999, "/overflow"));
    assert!(execution.validate(&Resources::default()).is_err());
}
