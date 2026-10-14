use adx_apiserver::contract::create_spec;
use adx_core::EnvironmentSpec;
use adx_protocol::control as pb;
use serde_json::{json, Value};

fn create(extra: Value) -> Result<EnvironmentSpec, Box<tonic::Status>> {
    let mut body = json!({"image":"fixture","cpu":500,"memory":128,"mem_limit":256});
    body.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    create_spec(
        body,
        &pb::CallerContext {
            tenant_id: "verified".into(),
            administrator: false,
        },
    )
    .map(|spec| spec.try_into().unwrap())
    .map_err(Box::new)
}

#[test]
fn elastic_memory_preserves_birth_floor_and_allowed_ceiling() {
    let spec = create(json!({"elastic_memory":{"high_per_mille":750}})).unwrap();
    assert_eq!(spec.sandbox.elastic_memory.unwrap().high_per_mille, 750);
    let spec = create(json!({"elastic_memory":{"high_per_mille":750}})).unwrap();
    assert_eq!(spec.execution_resources().memory_bytes, 128 * 1048576);
    assert_eq!(spec.execution_ceiling().memory_bytes, 256 * 1048576);
    assert_eq!(spec.execution_resources().cpu_millis, 500);
}

#[test]
fn elastic_memory_omission_or_null_keeps_fixed_execution() {
    for extra in [json!({}), json!({"elastic_memory":null})] {
        let spec = create(extra).unwrap();
        assert!(spec.sandbox.elastic_memory.is_none());
        assert_eq!(spec.execution_resources().memory_bytes, 256 * 1048576);
    }
}

#[test]
fn elastic_memory_rejects_invalid_policy_at_http_boundary() {
    for policy in [
        json!({}),
        json!({"high_per_mille":0}),
        json!({"high_per_mille":1000}),
        json!({"high_per_mille":true}),
        json!({"high_per_mille":750.5}),
        json!({"high_per_mille":750,"typo":1}),
        json!("750"),
    ] {
        let error = create(json!({"elastic_memory":policy})).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
}

#[test]
fn elastic_memory_rejects_unsupported_execution_and_bounds() {
    for extra in [
        json!({"runtime":"runc"}),
        json!({"runtime":"kata"}),
        json!({"snapshotId":"snapshot"}),
        json!({"cpu":9}),
        json!({"mem_limit":127}),
        json!({"mem_limit":8589934593_i64}),
        json!({"memory":1,"elastic_memory":{"high_per_mille":1}}),
    ] {
        let mut extra = extra;
        extra
            .as_object_mut()
            .unwrap()
            .entry("elastic_memory")
            .or_insert(json!({"high_per_mille":750}));
        let error = create(extra).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
    assert!(
        create(json!({"cpu":1,"cpu_limit":10,"elastic_memory":{"high_per_mille":750}})).is_ok()
    );
}
