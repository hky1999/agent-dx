use adx_core::{CheckpointReaders, EnvironmentRecord};
use adx_protocol::control;
use prost::Message;

fn record() -> EnvironmentRecord {
    serde_json::from_value(serde_json::json!({
        "spec":{"id":"e","tenant_id":"t","image":"image","runtime_class":"runsc",
            "resources":{"cpu_millis":1,"memory_bytes":1,"disk_bytes":1},"priority":0},
        "assignment":{"environment_id":"e","node_id":"n","shard_id":0,"generation":1},
        "state":"Running","revision":2,"runtime":{"id":"e-1-r1","ip":"127.0.0.1"},"resources_held":true
    })).unwrap()
}

#[test]
fn execution_reader_survives_protobuf_round_trip_with_distinct_latest_point() {
    let mut r = record();
    r.checkpoint_readers = CheckpointReaders::known_empty();
    let point = adx_core::RestorePoint {
        id: "a".into(),
        origin: None,
        source_runtime_id: "e-1".into(),
        expires_at_unix_seconds: None,
        artifact: adx_core::CheckpointArtifact {
            storage: "shared_fs".into(),
            location: "a".into(),
            size_bytes: 1,
        },
    };
    r.register_checkpoint_reader(point.clone()).unwrap();
    let mut latest = point;
    latest.id = "b".into();
    latest.artifact.location = "b".into();
    r.checkpoint = Some(latest);
    let bytes = control::EnvironmentRecord::try_from(r.clone())
        .unwrap()
        .encode_to_vec();
    let decoded: EnvironmentRecord = control::EnvironmentRecord::decode(bytes.as_slice())
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(decoded, r);
    assert_eq!(decoded.current_checkpoint_reader().unwrap().id, "a");
    assert_eq!(decoded.checkpoint.unwrap().id, "b");
}

#[test]
fn old_wire_omission_stays_incomplete_and_legacy_json_digest_shape_is_unchanged() {
    let r = record();
    let original = serde_json::to_value(&r).unwrap();
    assert!(original.get("checkpoint_readers").is_none());
    let mut wire = control::EnvironmentRecord::try_from(r.clone()).unwrap();
    wire.checkpoint_readers = None;
    let decoded: EnvironmentRecord = wire.try_into().unwrap();
    assert!(!decoded.checkpoint_readers.complete);
    assert_eq!(serde_json::to_value(decoded).unwrap(), original);
}

#[test]
fn missing_reader_identity_is_rejected_instead_of_dropping_reference() {
    let mut wire = control::EnvironmentRecord::try_from(record()).unwrap();
    wire.checkpoint_readers = Some(control::CheckpointReaders {
        complete: true,
        readers: vec![control::CheckpointReader::default()],
    });
    assert!(EnvironmentRecord::try_from(wire).is_err());
}
