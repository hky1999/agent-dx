use crate::clients::{authorize, Clients};
use adx_observability::trace;
use adx_protocol::control as pb;
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tonic::{Code, Status};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Delete,
    Pause,
    Resume,
    Snapshot,
    Network,
    Reload,
}
impl Kind {
    fn code(self) -> i32 {
        (match self {
            Self::Pause => pb::LifecycleKind::Pause,
            Self::Resume => pb::LifecycleKind::Resume,
            Self::Snapshot => pb::LifecycleKind::Snapshot,
            Self::Network => pb::LifecycleKind::Network,
            Self::Reload => pb::LifecycleKind::Reload,
            Self::Delete => pb::LifecycleKind::Unspecified,
        }) as i32
    }
    fn state(self) -> i32 {
        (match self {
            Self::Delete => pb::EnvironmentState::Deleted,
            Self::Pause => pb::EnvironmentState::Paused,
            _ => pb::EnvironmentState::Running,
        }) as i32
    }
}
struct Operation {
    assignment: pb::Assignment,
    expected: u64,
    kind: Kind,
    body: Value,
}
type OperationKey = (String, String, String);
type PendingOperations = HashMap<OperationKey, Arc<Mutex<Operation>>>;
pub struct Operations {
    clients: Arc<Clients>,
    pending: Mutex<PendingOperations>,
}
impl Operations {
    pub fn new(clients: Arc<Clients>) -> Self {
        Self {
            clients,
            pending: Mutex::new(HashMap::new()),
        }
    }
    pub async fn execute(
        &self,
        kind: Kind,
        id: &str,
        request_id: &str,
        body: Value,
        caller: &pb::CallerContext,
    ) -> Result<Value, Status> {
        let ttl = number(&body, "ttlSeconds", 90000, u64::MAX)?;
        let seconds = number(&body, "timeoutSeconds", 300, 3600)?;
        let name = body
            .get("name")
            .map(|n| {
                n.as_str()
                    .ok_or_else(|| Status::invalid_argument("invalid snapshot name"))
            })
            .transpose()?
            .unwrap_or("")
            .trim();
        if body
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(|v| !v.is_empty() && name.is_empty())
        {
            return Err(Status::invalid_argument("snapshot name cannot be blank"));
        }
        let clients = &self.clients;
        // Workload checkpoint acknowledges publication before the directory watch
        // necessarily catches up. Reload must start from the committed revision.
        let mut owner = clients
            .owner(
                id,
                caller,
                kind == Kind::Reload || (kind == Kind::Resume && body.get("source").is_some()),
            )
            .await?;
        let record = owner
            .record
            .as_ref()
            .ok_or_else(|| Status::unavailable("environment directory returned no record"))?;
        validate_parking_source(kind, &body, record, request_id)?;
        if kind == Kind::Delete
            && record.state == pb::EnvironmentState::Deleted as i32
            && !record.resources_held
        {
            return Ok(Value::Null);
        }
        let key = (caller.tenant_id.clone(), id.into(), request_id.into());
        let operation = {
            let mut pending = self.pending.lock().await;
            if let Some(operation) = pending.get(&key) {
                operation.clone()
            } else {
                // Retained retry identities are not admission slots. Completed
                // cache capacity must not reject an independent lifecycle request.
                let expected = record
                    .last_operation
                    .as_ref()
                    .filter(|operation| operation.id == request_id && operation.kind == kind.code())
                    .map_or(record.revision, |operation| operation.expected_revision);
                let operation = Arc::new(Mutex::new(Operation {
                    assignment: record.assignment.clone().ok_or_else(|| {
                        Status::unavailable("environment record returned no assignment")
                    })?,
                    expected,
                    kind,
                    body: body.clone(),
                }));
                pending.insert(key.clone(), operation.clone());
                operation
            }
        };
        let operation = operation.lock().await;
        if operation.kind != kind
            || operation.body != body
            || Some(&operation.assignment)
                != owner
                    .record
                    .as_ref()
                    .and_then(|record| record.assignment.as_ref())
        {
            return Err(Status::failed_precondition(
                "operation target or arguments changed",
            ));
        }
        for attempt in 0..2 {
            validate_parking_source(
                kind,
                &body,
                owner
                    .record
                    .as_ref()
                    .ok_or_else(|| Status::unavailable("environment record missing"))?,
                request_id,
            )?;
            let mut node = pb::node_service_client::NodeServiceClient::new(
                clients.channel(&owner.node_address).await?,
            );
            let assignment = Some(operation.assignment.clone());
            let caller_context = Some(caller.clone());
            let mut snapshot = None;
            let result = match kind {
                Kind::Delete => {
                    clients
                        .rpc(
                            "apiserver.delete",
                            node.delete_environment(trace::inject(pb::DeleteEnvironmentRequest {
                                failed_revision: None,
                                node_session_id: String::new(),
                                assignment,
                                caller: caller_context,
                            })),
                        )
                        .await
                }
                Kind::Pause => {
                    clients
                        .rpc_with_timeout(
                            "apiserver.pause",
                            clients.config.timeout() + Duration::from_secs(seconds),
                            node.pause_environment(trace::inject(pb::PauseEnvironmentRequest {
                                assignment,
                                caller: caller_context,
                                operation_id: request_id.into(),
                                expected_revision: operation.expected,
                                ttl_seconds: ttl,
                                timeout_seconds: seconds,
                                resources: parking_resources(&body)?,
                            })),
                        )
                        .await
                }
                Kind::Resume => {
                    clients
                        .rpc(
                            "apiserver.resume",
                            node.resume_environment(trace::inject(pb::ResumeEnvironmentRequest {
                                assignment,
                                caller: caller_context,
                                operation_id: request_id.into(),
                                expected_revision: operation.expected,
                            })),
                        )
                        .await
                }
                Kind::Snapshot => match clients
                    .rpc_with_timeout(
                        "apiserver.snapshot",
                        clients.config.timeout() + Duration::from_secs(seconds),
                        node.create_snapshot(trace::inject(pb::CreateSnapshotRequest {
                            assignment,
                            caller: caller_context,
                            operation_id: request_id.into(),
                            expected_revision: operation.expected,
                            names: if name.is_empty() {
                                vec![]
                            } else {
                                vec![name.into()]
                            },
                            timeout_seconds: seconds,
                        })),
                    )
                    .await
                {
                    Ok(result) => {
                        snapshot = result.snapshot;
                        result
                            .environment
                            .ok_or_else(|| Status::data_loss("missing snapshot result"))
                    }
                    Err(error) => Err(error),
                },
                Kind::Network => {
                    let network =
                        crate::contract::network_policy(operation.body.clone())?.map(Into::into);
                    clients
                        .rpc(
                            "apiserver.update_network_policy",
                            node.update_network_policy(trace::inject(
                                pb::UpdateNetworkPolicyRequest {
                                    assignment,
                                    caller: caller_context,
                                    operation_id: request_id.into(),
                                    expected_revision: operation.expected,
                                    network,
                                },
                            )),
                        )
                        .await
                }
                Kind::Reload => {
                    clients
                        .rpc(
                            "apiserver.reload",
                            node.reload_environment(trace::inject(pb::ReloadEnvironmentRequest {
                                assignment,
                                caller: caller_context,
                                operation_id: request_id.into(),
                                expected_revision: operation.expected,
                            })),
                        )
                        .await
                }
            };
            match result {
                Ok(result) => {
                    let record = result
                        .record
                        .as_ref()
                        .ok_or_else(|| Status::unavailable("operation result missing"))?;
                    authorize(caller, Some(record))?;
                    if result.durability != pb::Durability::Published as i32
                        || record.assignment.as_ref() != Some(&operation.assignment)
                        || record.state != kind.state()
                    {
                        return Err(Status::unavailable("operation is not durably confirmed"));
                    }
                    if kind == Kind::Delete {
                        if record.resources_held {
                            return Err(Status::unavailable(
                                "deleted instance still holds resources",
                            ));
                        }
                    } else if record.last_operation.as_ref().is_none_or(|last| {
                        last.id != request_id
                            || last.kind != kind.code()
                            || last.expected_revision != operation.expected
                    }) {
                        return Err(Status::unavailable("operation result version mismatch"));
                    }
                    let value = match kind {
                        Kind::Delete => Value::Null,
                        Kind::Pause => {
                            let checkpoint = record
                                .checkpoint
                                .as_ref()
                                .filter(|checkpoint| {
                                    checkpoint.id == request_id
                                        && checkpoint.expires_at_unix_seconds > 0
                                })
                                .ok_or_else(|| Status::data_loss("invalid recovery point"))?;
                            let size = checkpoint
                                .artifact
                                .as_ref()
                                .filter(|artifact| {
                                    artifact.size_bytes > 0
                                        && artifact.size_bytes <= i64::MAX as u64
                                })
                                .ok_or_else(|| Status::data_loss("invalid checkpoint artifact"))?
                                .size_bytes;
                            json!({
                                "sandboxId": id,
                                "snapshotId": checkpoint.id,
                                "size": size,
                                "state": "paused",
                                "expiresAt": checkpoint.expires_at_unix_seconds,
                            })
                        }
                        Kind::Resume => {
                            if owner.relay_address.is_empty() {
                                return Err(Status::data_loss("missing node route"));
                            }
                            json!({
                                "sandboxId": id,
                                "state": "running",
                                "routeAddress": owner.relay_address,
                                "functionProxyId": operation.assignment.node_id,
                                "nodeId": operation.assignment.node_id,
                                "portMappings": {},
                            })
                        }
                        Kind::Snapshot => {
                            let snapshot = snapshot
                                .as_ref()
                                .filter(|snapshot| {
                                    snapshot.state == pb::SnapshotState::Ready as i32
                                        && snapshot.template == record.spec
                                })
                                .ok_or_else(|| Status::data_loss("invalid snapshot source"))?;
                            snapshot_value(snapshot, caller)?
                        }
                        Kind::Network => json!({"success": true}),
                        Kind::Reload => json!({"success": true}),
                    };
                    owner.record = result.record;
                    clients.put_owner(owner).await?;
                    self.pending.lock().await.remove(&key);
                    return Ok(value);
                }
                Err(error) => {
                    let retryable =
                        matches!(error.code(), Code::Unavailable | Code::DeadlineExceeded)
                            || (kind == Kind::Delete
                                && matches!(
                                    error.code(),
                                    Code::NotFound | Code::FailedPrecondition
                                ));
                    if !retryable
                        || (matches!(kind, Kind::Pause | Kind::Resume)
                            && body.get("source").is_some())
                    {
                        self.pending.lock().await.remove(&key);
                        return Err(error);
                    }
                    if attempt == 1 {
                        return Err(error);
                    }
                    owner = clients.owner(id, caller, true).await?;
                    if owner
                        .record
                        .as_ref()
                        .and_then(|record| record.assignment.as_ref())
                        != Some(&operation.assignment)
                    {
                        self.pending.lock().await.remove(&key);
                        return Err(Status::failed_precondition(
                            "ownership changed; original operation cannot be replayed",
                        ));
                    }
                }
            }
        }
        unreachable!()
    }
}
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ParkingSource {
    runtime_id: String,
    assignment_generation: u64,
    revision: u64,
    #[serde(default)]
    checkpoint_id: Option<String>,
    #[serde(default)]
    resource_generation: Option<String>,
    #[serde(default)]
    resource_revision: Option<u64>,
}

fn parking_resources(body: &Value) -> Result<Option<pb::CheckpointResources>, Status> {
    let Some(value) = body.get("source") else {
        return Ok(None);
    };
    let source: ParkingSource = serde_json::from_value(value.clone())
        .map_err(|_| Status::invalid_argument("complete parking source required"))?;
    match (source.resource_generation, source.resource_revision) {
        (None, None)
            if value.get("resourceGeneration").is_none()
                && value.get("resourceRevision").is_none() =>
        {
            Ok(None)
        }
        (Some(generation), Some(revision))
            if !generation.trim().is_empty() && generation.len() <= 256 && revision > 0 =>
        {
            Ok(Some(pb::CheckpointResources {
                generation,
                revision,
            }))
        }
        _ => Err(Status::invalid_argument(
            "complete positive original physical resource condition required",
        )),
    }
}

/// Explicit source fencing for the trusted parking controller. Existing lifecycle
/// clients may omit source; a supplied fence is never ignored or retargeted.
fn validate_parking_source(
    kind: Kind,
    body: &Value,
    record: &pb::EnvironmentRecord,
    request_id: &str,
) -> Result<(), Status> {
    let Some(value) = body.get("source") else {
        return Ok(());
    };
    if !matches!(kind, Kind::Pause | Kind::Resume) {
        return Err(Status::invalid_argument(
            "source fence only supports pause or resume",
        ));
    }
    let source: ParkingSource = serde_json::from_value(value.clone())
        .map_err(|_| Status::invalid_argument("complete parking source required"))?;
    if parking_resources(body)?.is_some() && kind != Kind::Pause {
        return Err(Status::invalid_argument(
            "physical capture condition only supports pause",
        ));
    }
    let replay = record.last_operation.as_ref().is_some_and(|op| {
        op.id == request_id && op.kind == kind.code() && op.expected_revision == source.revision
    });
    let resume_replay = kind == Kind::Resume
        && replay
        && record
            .checkpoint
            .as_ref()
            .is_some_and(|cp| cp.source_runtime_id == source.runtime_id)
        && source.revision.checked_add(1).is_some_and(|revision| {
            record.runtime.as_ref().is_some_and(|r| {
                r.id == format!(
                    "{}-{}-r{revision}",
                    record.spec.as_ref().map_or("", |s| s.id.as_str()),
                    source.assignment_generation
                )
            })
        });
    if kind == Kind::Resume
        && source.checkpoint_id.as_deref().is_none_or(|id| {
            record
                .checkpoint
                .as_ref()
                .is_none_or(|cp| cp.id != id || cp.source_runtime_id != source.runtime_id)
        })
    {
        return Err(Status::failed_precondition(
            "original recovery point changed",
        ));
    }
    if source.runtime_id.is_empty()
        || source.runtime_id.len() > 256
        || source.assignment_generation == 0
        || source.revision == 0
        || record
            .runtime
            .as_ref()
            .is_none_or(|r| r.id != source.runtime_id && !resume_replay)
        || record
            .assignment
            .as_ref()
            .is_none_or(|a| a.generation != source.assignment_generation)
        || (record.revision != source.revision && !replay)
    {
        return Err(Status::failed_precondition(
            "lifecycle source changed; refusing to retarget",
        ));
    }
    Ok(())
}

pub fn number(value: &Value, key: &str, default: u64, max: u64) -> Result<u64, Status> {
    let value = match value.get(key) {
        None | Some(Value::Null) => default,
        Some(value) => value
            .as_u64()
            .ok_or_else(|| Status::invalid_argument(format!("invalid {key}")))?,
    };
    let value = if value == 0 { default } else { value };
    if value > max {
        return Err(Status::invalid_argument(format!("invalid {key}")));
    }
    Ok(value)
}
pub fn snapshot_value(
    s: &pb::ReusableSnapshot,
    caller: &pb::CallerContext,
) -> Result<Value, Status> {
    let spec = s
        .template
        .as_ref()
        .ok_or_else(|| Status::data_loss("invalid snapshot metadata"))?;
    if s.id.is_empty() || spec.tenant_id.is_empty() {
        return Err(Status::data_loss("invalid snapshot identity"));
    }
    if !caller.administrator && spec.tenant_id != caller.tenant_id {
        return Err(Status::permission_denied(
            "snapshot belongs to another tenant",
        ));
    }
    Ok(json!({"snapshotId":s.id,"names":s.names}))
}

#[cfg(test)]
mod parking_source_tests {
    use super::*;
    #[test]
    fn source_pause_guard_rejects_runtime_assignment_revision_and_partial_identity() {
        let r = pb::EnvironmentRecord {
            runtime: Some(pb::Runtime {
                id: "runtime".into(),
                ..Default::default()
            }),
            assignment: Some(pb::Assignment {
                generation: 7,
                ..Default::default()
            }),
            revision: 9,
            ..Default::default()
        };
        let source = json!({"runtimeId":"runtime","assignmentGeneration":7,"revision":9});
        assert!(
            validate_parking_source(Kind::Pause, &json!({"source":source}), &r, "park").is_ok()
        );
        for bad in [
            json!({"runtimeId":"other","assignmentGeneration":7,"revision":9}),
            json!({"runtimeId":"runtime","assignmentGeneration":8,"revision":9}),
            json!({"runtimeId":"runtime","assignmentGeneration":7,"revision":10}),
            json!({"runtimeId":"runtime"}),
        ] {
            assert!(
                validate_parking_source(Kind::Pause, &json!({"source":bad}), &r, "park").is_err()
            );
        }
        assert!(
            validate_parking_source(Kind::Resume, &json!({"source":source}), &r, "park").is_err()
        );
        assert!(validate_parking_source(Kind::Pause, &json!({}), &r, "park").is_ok());
    }
}

#[cfg(test)]
mod recovery_source_tests {
    use super::*;
    #[test]
    fn source_fenced_resume_is_accepted_before_dispatch() {
        let record = pb::EnvironmentRecord {
            runtime: Some(pb::Runtime {
                id: "source".into(),
                ..Default::default()
            }),
            assignment: Some(pb::Assignment {
                generation: 7,
                ..Default::default()
            }),
            checkpoint: Some(pb::RestorePoint {
                source_resources: None,
                id: "park-original".into(),
                source_runtime_id: "source".into(),
                ..Default::default()
            }),
            revision: 9,
            ..Default::default()
        };
        let body = json!({"source":{"runtimeId":"source","assignmentGeneration":7,"revision":9,"checkpointId":"park-original"}});
        assert!(validate_parking_source(Kind::Resume, &body, &record, "resume-once").is_ok());
    }
}

#[cfg(test)]
mod parking_resource_condition_tests {
    use super::*;

    #[test]
    fn original_physical_resource_pair_is_accepted_and_partial_pairs_are_rejected() {
        let record = pb::EnvironmentRecord {
            runtime: Some(pb::Runtime {
                id: "source".into(),
                ..Default::default()
            }),
            assignment: Some(pb::Assignment {
                generation: 7,
                ..Default::default()
            }),
            revision: 9,
            ..Default::default()
        };
        let source = json!({"runtimeId":"source","assignmentGeneration":7,"revision":9,
                            "resourceGeneration":"original-physical","resourceRevision":2});
        assert!(
            validate_parking_source(Kind::Pause, &json!({"source":source}), &record, "park")
                .is_ok(),
            "a complete original physical resource condition must reach the node"
        );
        for bad in [
            json!({"runtimeId":"source","assignmentGeneration":7,"revision":9,"resourceGeneration":"original-physical"}),
            json!({"runtimeId":"source","assignmentGeneration":7,"revision":9,"resourceRevision":2}),
            json!({"runtimeId":"source","assignmentGeneration":7,"revision":9,"resourceGeneration":"","resourceRevision":2}),
            json!({"runtimeId":"source","assignmentGeneration":7,"revision":9,"resourceGeneration":"original-physical","resourceRevision":0}),
            json!({"runtimeId":"source","assignmentGeneration":7,"revision":9,"resourceGeneration":null,"resourceRevision":null}),
        ] {
            assert!(
                validate_parking_source(Kind::Pause, &json!({"source":bad}), &record, "park")
                    .is_err()
            );
        }
    }
}
