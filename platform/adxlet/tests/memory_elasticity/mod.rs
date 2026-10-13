use super::*;
use serde_json::json;
const MIB: u64 = 1024 * 1024;
fn elastic_spec() -> EnvironmentSpec {
    let mut value = serde_json::to_value(spec()).unwrap();
    value["resources"]["cpu_millis"] = json!(500);
    value["resources"]["memory_bytes"] = json!(128 * MIB);
    value["sandbox"]["limits"]["memory_bytes"] = json!(512 * MIB);
    value["sandbox"]["elastic_memory"] = json!({"high_per_mille":750});
    serde_json::from_value(value).unwrap()
}
#[test]
fn elastic_birth_uses_initial_memory_and_persists_independent_ceiling() {
    let spec = elastic_spec();
    spec.validate().unwrap();
    let request = start_request(&spec, "i-1", 1, &[], &Config::default()).unwrap();
    assert_eq!(
        request.resources["Memory"], 128.0,
        "initial allocation must match the budget, not the ceiling"
    );
    assert_eq!(request.resources["CPU"], 500.0);
    let policy: serde_json::Value =
        serde_json::from_str(&request.labels["akernel.scheduler/elastic-resources"]).unwrap();
    assert_eq!(
        policy,
        json!({"version":1,"CPU":{"initial":500,"ceiling":500},"Memory":{"initial":128,"ceiling":512}})
    );
    assert_eq!(spec.execution_resources().memory_bytes, 128 * MIB);
    let roundtrip: adx_protocol::control::EnvironmentSpec = spec.clone().into();
    let decoded: EnvironmentSpec = roundtrip.try_into().unwrap();
    assert_eq!(
        serde_json::to_value(decoded).unwrap()["sandbox"]["elastic_memory"],
        json!({"high_per_mille":750})
    );
}
#[test]
fn elastic_birth_rejects_unenforceable_policy_before_dispatch() {
    let original = serde_json::to_value(elastic_spec()).unwrap();
    for case in 0..7 {
        let mut value = original.clone();
        match case {
            0 => value["sandbox"]["elastic_memory"]["high_per_mille"] = json!(0),
            1 => value["sandbox"]["elastic_memory"]["high_per_mille"] = json!(1000),
            2 => value["resources"]["memory_bytes"] = json!(128 * MIB + 4096),
            3 => value["sandbox"]["limits"]["memory_bytes"] = json!(512 * MIB + 4096),
            4 => value["runtime_class"] = json!("kata"),
            5 => value["snapshot_id"] = json!("snapshot"),
            _ => value["sandbox"]["limits"]["memory_bytes"] = json!((1u64 << 53) + MIB),
        }
        let rejected = match serde_json::from_value::<EnvironmentSpec>(value) {
            Ok(spec) => start_request(&spec, "i-1", 1, &[], &Config::default()).is_err(),
            Err(_) => true,
        };
        assert!(rejected, "case {case} must fail before sandboxd dispatch");
    }
}

#[test]
fn elastic_boundary_rejects_cpu_below_the_enforceable_quota() {
    let mut spec = elastic_spec();
    spec.resources.cpu_millis = 1;
    assert!(start_request(&spec, "i-1", 1, &[], &Config::default()).is_err());
}
#[test]
fn elastic_boundary_rejects_soft_limit_below_readback_alignment() {
    let mut spec = elastic_spec();
    spec.resources.memory_bytes = MIB;
    spec.sandbox.elastic_memory.as_mut().unwrap().high_per_mille = 1;
    assert!(start_request(&spec, "i-1", 1, &[], &Config::default()).is_err());
}
