//! Private host administration; never expose the Redis configuration to tasks.
use adx_coordinator::storage::{OverheadReservation, RedisStore};
use adx_core::execution::{ExecutionKind, ExecutionRequest};
use serde::Deserialize;
use std::{
    io::Read,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
#[derive(Deserialize)]
struct Config {
    redis_url: String,
    namespace: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueuedResume {
    environment_id: String,
    source_runtime_id: String,
    assignment_generation: u64,
    source_revision: u64,
    checkpoint_id: String,
    operation_id: String,
    node_session_id: String,
    timeout_millis: u64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Command {
    #[serde(default)]
    queued_resume: Option<QueuedResume>,
    #[serde(default)]
    journal: Option<serde_json::Value>,
    #[serde(default)]
    expected_revision: u64,
    op: String,
    epoch: u64,
    #[serde(default)]
    id: String,
    #[serde(default)]
    reservation: Option<OverheadReservation>,
    #[serde(default)]
    owner_sha256: String,
    #[serde(default)]
    retirement_proof_sha256: String,
}
async fn execute() -> Result<(), Box<dyn std::error::Error>> {
    let config: Config = adx_process::read_config()?;
    let mut bytes = Vec::new();
    std::io::stdin()
        .take((3 << 20) + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 3 << 20 {
        return Err("bounded command required".into());
    }
    let command: Command = serde_json::from_slice(&bytes)?;
    let store =
        RedisStore::connect(&config.redis_url, &config.namespace, Duration::from_secs(5)).await?;
    let session = store.attach_overhead_session(command.epoch).await?;
    if command.op == "queued_reserve" {
        let queued = command
            .queued_resume
            .ok_or("queued resume identity required")?;
        let stored = session.get(&queued.environment_id).await?;
        let source = stored.result.ok_or("published paused source required")?;
        if source.runtime.id != queued.source_runtime_id
            || source.assignment.generation != queued.assignment_generation
            || source.revision != queued.source_revision
            || source.checkpoint.as_ref().is_none_or(|cp| {
                cp.id != queued.checkpoint_id || cp.source_runtime_id != queued.source_runtime_id
            })
        {
            return Err("original restore source differs".into());
        }
        let request = ExecutionRequest {
            source,
            kind: ExecutionKind::Resume,
            operation_id: queued.operation_id,
            expected_revision: queued.source_revision,
            timeout_millis: queued.timeout_millis,
        };
        let now = u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis())?;
        match session
            .authorize_execution_with_peak(
                request,
                &queued.node_session_id,
                now,
                &command.id,
                command.reservation.ok_or("peak reservation required")?,
            )
            .await
        {
            Ok(grant) => println!(
                "{}",
                serde_json::json!({"admitted":true,"authorization":{"operation":grant.request.operation_id,"source_runtime":grant.request.source.runtime.id,"source_revision":grant.request.expected_revision,"target_runtime":grant.target.runtime.id,"deadline_ms":grant.deadline_unix_millis,"peak_reservation_id":command.id}})
            ),
            Err(adx_core::Error::NoCapacity) => println!(
                "{}",
                serde_json::json!({"admitted":false,"reason":"budget","no_authorization_or_peak_committed":true})
            ),
            Err(error) => return Err(error.into()),
        }
        return Ok(());
    }
    if command.op == "journal_read" || command.op == "journal_write" {
        if command.op == "journal_write" {
            session
                .publish_parking_journal(
                    &command.id,
                    &command.owner_sha256,
                    command.expected_revision,
                    command.journal.ok_or("journal required")?,
                )
                .await?;
        }
        println!(
            "{}",
            serde_json::json!({"epoch":session.epoch(),"journal":session.parking_journal(&command.id).await?})
        );
        return Ok(());
    }
    match command.op.as_str() {
        "reserve" => {
            session
                .reserve_overhead(
                    &command.id,
                    command.reservation.ok_or("reservation required")?,
                )
                .await?
        }
        "retire" => {
            session
                .retire_overhead(
                    &command.id,
                    &command.owner_sha256,
                    &command.retirement_proof_sha256,
                )
                .await?
        }
        "inspect" => {}
        _ => return Err("unknown operation".into()),
    }
    let environment = if command.op == "inspect" && !command.id.is_empty() {
        let e = session.get(&command.id).await?;
        Some(
            serde_json::json!({"execution":e.execution.as_ref().map(|x|serde_json::json!({"pending":x.pending,"operation":x.authorization.request.operation_id,"source_runtime":x.authorization.request.source.runtime.id,"source_revision":x.authorization.request.expected_revision,"target_runtime":x.authorization.target.runtime.id,"deadline_ms":x.authorization.deadline_unix_millis,"peak":x.peak})),"assignment":e.assignment,"memory":e.memory.as_ref().map(|m| &m.confirmed),"result":e.result.as_ref().map(|r|serde_json::json!({"runtime":r.runtime,"state":r.state,"resources_held":r.resources_held,"revision":r.revision,"checkpoint":r.checkpoint,"last_operation":r.last_operation}))}),
        )
    } else {
        None
    };
    println!(
        "{}",
        serde_json::json!({"environment":environment,"epoch":session.epoch(),"hosts":session.host_budgets().await?,"reservations":session.overhead_reservations().await?})
    );
    Ok(())
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    if execute().await.is_err() {
        eprintln!("overhead budget request rejected; reservation outcome requires readback");
        std::process::exit(1);
    }
}
