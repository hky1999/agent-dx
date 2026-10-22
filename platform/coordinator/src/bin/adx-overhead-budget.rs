//! Private host administration; never expose the Redis configuration to tasks.
use adx_coordinator::storage::{OverheadReservation, RedisStore};
use serde::Deserialize;
use std::{io::Read, time::Duration};
#[derive(Deserialize)]
struct Config {
    redis_url: String,
    namespace: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Command {
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
            serde_json::json!({"assignment":e.assignment,"memory":e.memory.as_ref().map(|m| &m.confirmed),"result":e.result.as_ref().map(|r|serde_json::json!({"runtime":r.runtime,"state":r.state,"resources_held":r.resources_held,"revision":r.revision,"checkpoint":r.checkpoint,"last_operation":r.last_operation}))}),
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
