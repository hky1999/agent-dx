#![cfg(target_os = "linux")]
use adx_core::{
    pressure::{PressureIdentity, PressureSource},
    Error,
};
use serde_json::{json, Value};

fn source(path: std::path::PathBuf) -> PressureSource {
    PressureSource {
        path,
        max_age_millis: 3000,
        identity: PressureIdentity {
            host_id: "host".into(),
            authority: "operator-generation-1".into(),
            boot_id: std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
                .unwrap()
                .trim()
                .to_owned(),
            parent_device: 30,
            parent_inode: 42,
            storage_identity_sha256: "a".repeat(64),
        },
    }
}
fn observation(source: &PressureSource) -> Value {
    json!({"schema":1,"identity":source.identity,"sequence":1,
        "observed_unix_millis":10000,"valid_until_unix_millis":13000,
        "accepting":true,"reason":"low_watermark","metrics":{
            "host_memory_per_mille":100,"parent_memory_per_mille":100,
            "host_memory_psi_per_mille":0,"parent_memory_psi_per_mille":0,
            "disk_used_per_mille":100,"parent_memory_current_bytes":100,
            "parent_memory_max_bytes":1000,"disk_available_bytes":900}})
}

#[test]
fn pressure_source_requires_fresh_complete_matching_authority() {
    let root = tempfile::tempdir().unwrap();
    let source = source(root.path().join("pressure.json"));
    assert_eq!(source.check(10000), Err(Error::NoCapacity));
    let good = observation(&source);
    std::fs::write(&source.path, good.to_string()).unwrap();
    assert_eq!(source.check(12000).unwrap().as_millis(), 1000);
    assert_eq!(source.check(13000), Err(Error::NoCapacity));
    assert_eq!(source.check(9999), Err(Error::NoCapacity));
    let faults = [
        ("/identity/authority", json!("other")),
        ("/identity/boot_id", json!("new-boot")),
        ("/identity/parent_inode", json!(43)),
        ("/identity/storage_identity_sha256", json!("b".repeat(64))),
        ("/schema", json!(2)),
        ("/sequence", json!(0)),
        ("/accepting", json!(false)),
        ("/reason", json!("warming")),
        ("/valid_until_unix_millis", json!(13001)),
        ("/metrics/parent_memory_per_mille", json!(1001)),
        ("/metrics/parent_memory_current_bytes", json!(1001)),
    ];
    for (pointer, value) in faults {
        let mut bad = good.clone();
        *bad.pointer_mut(pointer).unwrap() = value;
        std::fs::write(&source.path, bad.to_string()).unwrap();
        assert_eq!(source.check(10000), Err(Error::NoCapacity), "{pointer}");
    }
    for bytes in ["{}".into(), "{".into(), " ".repeat(8193)] {
        std::fs::write(&source.path, bytes).unwrap();
        assert_eq!(source.check(10000), Err(Error::NoCapacity));
    }
}

#[cfg(unix)]
#[test]
fn pressure_source_rejects_symlink_and_invalid_configuration() {
    let root = tempfile::tempdir().unwrap();
    let mut source = source(root.path().join("pressure.json"));
    let other = root.path().join("other.json");
    std::fs::write(&other, observation(&source).to_string()).unwrap();
    std::os::unix::fs::symlink(other, &source.path).unwrap();
    assert_eq!(source.check(10000), Err(Error::NoCapacity));
    source.max_age_millis = 0;
    assert!(matches!(source.validate(), Err(Error::Invalid(_))));
    source.max_age_millis = 3000;
    source.path = "relative".into();
    assert!(source.validate().is_err());
}

#[test]
fn pressure_source_rejects_matching_old_boot_file_before_its_expiry() {
    let root = tempfile::tempdir().unwrap();
    let mut source = source(root.path().join("pressure.json"));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let mut value = observation(&source);
    value["observed_unix_millis"] = now.into();
    value["valid_until_unix_millis"] = (now + 3000).into();
    std::fs::write(&source.path, value.to_string()).unwrap();
    assert!(source.check_now().is_ok());
    source.identity.boot_id = "previous-boot".into();
    value["identity"] = serde_json::to_value(&source.identity).unwrap();
    std::fs::write(&source.path, value.to_string()).unwrap();
    assert_eq!(source.check_now(), Err(Error::NoCapacity));
}
