//! Connection failure isolation and operation deadlines against a dedicated Redis.
use super::*;
use std::time::Duration;
use tokio::time::timeout;

#[path = "../../tests/common/mod.rs"]
mod common;

async fn client_id(store: &RedisStore) -> u64 {
    let mut command = redis::cmd("CLIENT");
    command.arg("ID");
    store.query(command).await.unwrap()
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn connection_cache_wait_obeys_operation_deadline() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let original = client_id(&store).await;
    let guard = store.connection.current.lock().await;

    // No connection was borrowed. A blocked cache acquisition must time out
    // without waiting again for the same lock or invalidating its current value.
    let outcome = timeout(
        Duration::from_secs(2),
        store.query::<String>(redis::cmd("PING")),
    )
    .await;
    assert!(guard.is_some());
    drop(guard);
    assert!(
        matches!(outcome, Ok(Err(Error::Unavailable(_)))),
        "the configured operation timeout must include cache-lock contention"
    );
    assert_eq!(client_id(&store).await, original);
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn connection_error_returns_without_waiting_for_cache_cleanup() {
    let rig = common::Redis::new().await;
    let store = rig.store().await;
    let original = client_id(&store).await;
    let mut held = None;

    let outcome = timeout(
        Duration::from_secs(2),
        store.query_with_budget::<String>(redis::cmd("PING"), |_| {
            // Hold the cache lock exactly when a completed data read is rejected.
            // Cleanup must return the original failure without an unbounded wait.
            held = Some(store.connection.current.try_lock().unwrap());
            Err(Error::Conflict)
        }),
    )
    .await;
    drop(held);
    assert_eq!(
        outcome.expect("connection cleanup must remain bounded"),
        Err(Error::Conflict)
    );
    assert_ne!(
        client_id(&store).await,
        original,
        "a rejected borrowed connection must not be reused after the lock is released"
    );
}

#[tokio::test]
#[ignore = "requires dedicated real Redis; build/ci/run.py storage"]
async fn connection_old_failure_preserves_replacement_connection() {
    let mut rig = common::Redis::new().await;
    let store = rig.store().await;
    let replacement_store = rig.store().await;
    let original = client_id(&store).await;
    let replacement_id = client_id(&replacement_store).await;
    assert_ne!(original, replacement_id);
    let replacement = replacement_store
        .connection
        .current
        .lock()
        .await
        .take()
        .unwrap();

    let outcome = store
        .query_with_budget::<String>(redis::cmd("PING"), |_| {
            // A different operation has already installed a new real stream
            // when the old read produces its late failure.
            *store.connection.current.try_lock().unwrap() = Some(replacement);
            Err(Error::Conflict)
        })
        .await;
    assert_eq!(outcome, Err(Error::Conflict));
    assert!(
        store.connection.current.lock().await.is_some(),
        "an old operation must not evict a replacement stream"
    );
    assert_eq!(client_id(&store).await, replacement_id);
    rig.crash();
}
