use adx_core::memory::ElasticMemoryPolicy;
use serde_json::json;

#[test]
fn independent_minimum_keeps_the_original_birth_and_ceiling() {
    let policy: ElasticMemoryPolicy = serde_json::from_value(json!({
        "high_per_mille": 750,
        "minimum_bytes": 640_u64 << 20
    }))
    .expect("a whole-MiB shrink floor below the unchanged birth must be supported");
    policy.validate(2048 << 20, 2048 << 20).unwrap();
    assert_eq!(
        serde_json::to_value(policy).unwrap(),
        json!({"high_per_mille": 750, "minimum_bytes": 640_u64 << 20})
    );
}

#[test]
fn independent_minimum_rejects_invalid_and_unenforceable_bounds() {
    for minimum in [0_u64, (640 << 20) + 1, 2049 << 20] {
        let parsed = serde_json::from_value::<ElasticMemoryPolicy>(json!({
            "high_per_mille": 750, "minimum_bytes": minimum
        }));
        assert!(parsed.is_err() || parsed.unwrap().validate(2048 << 20, 2048 << 20).is_err());
    }
    let parsed = serde_json::from_value::<ElasticMemoryPolicy>(json!({
        "high_per_mille": 1, "minimum_bytes": 1 << 20
    }));
    assert!(parsed.is_err() || parsed.unwrap().validate(2048 << 20, 2048 << 20).is_err());
}

#[test]
fn omitted_minimum_preserves_the_original_serialized_policy() {
    let policy: ElasticMemoryPolicy =
        serde_json::from_value(json!({"high_per_mille": 750})).unwrap();
    policy.validate(2048 << 20, 2048 << 20).unwrap();
    assert_eq!(
        serde_json::to_value(policy).unwrap(),
        json!({"high_per_mille": 750})
    );
}

#[test]
fn independent_minimum_requires_the_explicit_known_node_contract() {
    let independent: ElasticMemoryPolicy = serde_json::from_value(json!({
        "high_per_mille":750, "minimum_bytes":640_u64 << 20
    }))
    .unwrap();
    let original: ElasticMemoryPolicy =
        serde_json::from_value(json!({"high_per_mille":750})).unwrap();
    for version in [0, 1, 2, 3, u32::MAX] {
        assert_eq!(independent.supports_node_version(version), version == 2);
        assert_eq!(
            original.supports_node_version(version),
            version == 1 || version == 2
        );
    }
    assert_eq!(independent.minimum(2048 << 20), 640 << 20);
    assert_eq!(original.minimum(2048 << 20), 2048 << 20);
}
