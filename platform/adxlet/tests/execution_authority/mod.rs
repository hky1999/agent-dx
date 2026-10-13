use adx_core::{
    execution::{ExecutionAuthorization, ExecutionRequest},
    Result,
};

pub fn authorize(request: &ExecutionRequest) -> Result<ExecutionAuthorization> {
    let now = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap();
    Ok(ExecutionAuthorization {
        request: request.clone(),
        target: request.target()?,
        node_session_id: "test-node-session".into(),
        coordinator_epoch: 1,
        issued_unix_millis: now,
        deadline_unix_millis: now.checked_add(request.timeout_millis).unwrap(),
    })
}
