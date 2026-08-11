//! PostgreSQL `Store` integration test.
//!
//! Runs ONLY when `TEST_DATABASE_URL` is set (it needs an external Postgres). When unset the test
//! prints a note and returns early — it never fails the default `cargo test` run, which stays
//! database-free. Spin up a throwaway Postgres and run:
//!
//! ```text
//! docker run --rm -d -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=delta \
//!   -p 127.0.0.1:55470:5432 postgres:18-alpine
//! TEST_DATABASE_URL=postgres://postgres:pw@127.0.0.1:55470/delta \
//!   cargo test --test pg_store -- --nocapture
//! ```
//!
//! The `Store` trait is async: each method `.await`s sqlx natively (no `block_in_place`), so it runs
//! on any Tokio scheduler — this test stays on `multi_thread` for parallel queries.

use std::sync::Arc;

use delta::store::{PgStore, Store, StoreError};
use sha2::{Digest, Sha256};

fn payload_hash(payload: &str) -> String {
    hex::encode(Sha256::digest(payload.as_bytes()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pg_store_full_integration() {
    let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
        eprintln!(
            "NOTE: TEST_DATABASE_URL not set — skipping Postgres integration test \
             (needs external Postgres). This is expected for the default test run."
        );
        return;
    };

    // --- connect / migrate (idempotent: run twice) -------------------------
    let pg = PgStore::connect(&url)
        .await
        .expect("connect TEST_DATABASE_URL");
    pg.migrate().await.expect("migrate");
    pg.migrate().await.expect("migrate is idempotent");
    let pg = Arc::new(pg);

    // --- append allocates monotonic global seq -----------------------------
    let a = pg
        .append("orders", "k1", "evt-pg-1", &payload_hash("p1"), "p1", 100)
        .await
        .expect("append a");
    let b = pg
        .append("billing", "", "evt-pg-2", &payload_hash("p2"), "p2", 101)
        .await
        .expect("append b");
    let c = pg
        .append("orders", "k3", "evt-pg-3", &payload_hash("p3"), "p3", 102)
        .await
        .expect("append c");
    assert_eq!((a.seq, b.seq, c.seq), (1, 2, 3));
    assert_eq!(pg.head_seq("orders").await, 3);
    assert_eq!(pg.head_seq("billing").await, 2);

    // --- event identity replay is stable and conflicting evidence is closed -
    let replay = pg
        .append("orders", "k1", "evt-pg-1", &payload_hash("p1"), "p1", 103)
        .await
        .expect("same event/hash replay");
    assert_eq!(replay.seq, a.seq);
    assert!(matches!(
        pg.append(
            "orders",
            "k1",
            "evt-pg-1",
            &payload_hash("changed"),
            "changed",
            104,
        )
        .await,
        Err(StoreError::Conflict)
    ));
    assert_eq!(pg.head_seq("orders").await, 3);

    // --- durable read by offset, filtered per stream -----------------------
    let got = pg.read_after("orders", 0, 100).await;
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].seq, 1);
    assert_eq!(got[1].seq, 3);
    assert_eq!(pg.read_after("orders", 1, 100).await[0].payload, "p3");
    assert_eq!(pg.read_after("orders", 0, 1).await.len(), 1);
    let queried = pg.query_events("orders", "k3", "p3", 0, 10).await;
    assert_eq!(queried.len(), 1);
    assert_eq!(queried[0].seq, 3);

    // --- cursor upsert is idempotent + lag derived -------------------------
    pg.commit_cursor("worker-1", "orders", 1, 200)
        .await
        .expect("commit 1");
    pg.commit_cursor("worker-1", "orders", 3, 210)
        .await
        .expect("commit 2");
    let repeated = pg
        .commit_cursor("worker-1", "orders", 1, 220)
        .await
        .expect("older commit is an idempotent no-op");
    assert_eq!(repeated.offset_seq, 3);
    assert_eq!(repeated.updated_at, 210);
    let cur = pg.get_cursor("worker-1", "orders").await.expect("cursor");
    assert_eq!(cur.offset_seq, 3);
    assert_eq!(cur.updated_at, 210);

    // --- console aggregates ------------------------------------------------
    let streams = pg.list_streams(100).await;
    assert!(streams
        .iter()
        .any(|s| s.stream == "orders" && s.count == 2 && s.head_seq == 3));
    let tail = pg.tail("orders", 10).await;
    assert_eq!(tail.first().map(|e| e.seq), Some(3)); // newest-first
    assert_eq!(pg.list_cursors().await.len(), 1);

    // --- duplicate delivery is atomic across independent service instances -
    let race_a = PgStore::connect(&url).await.expect("connect race store a");
    let race_b = PgStore::connect(&url).await.expect("connect race store b");
    let race_hash = payload_hash("race-payload");
    let (race_a, race_b) = tokio::join!(
        race_a.append(
            "replay-race",
            "aggregate:1",
            "evt-pg-race",
            &race_hash,
            "race-payload",
            250,
        ),
        race_b.append(
            "replay-race",
            "aggregate:1",
            "evt-pg-race",
            &race_hash,
            "race-payload",
            251,
        ),
    );
    let race_a = race_a.expect("first concurrent replay");
    let race_b = race_b.expect("second concurrent replay");
    assert_eq!(race_a.seq, race_b.seq);
    assert_eq!(pg.read_after("replay-race", 0, 10).await.len(), 1);

    // --- concurrent appends never collide on seq ---------------------------
    let mut handles = Vec::new();
    for i in 0..32 {
        let pg = pg.clone();
        handles.push(tokio::spawn(async move {
            let payload = format!("p{i}");
            pg.append(
                "concurrent",
                "",
                &format!("evt-pg-concurrent-{i}"),
                &payload_hash(&payload),
                &payload,
                300 + i,
            )
            .await
            .expect("concurrent append")
            .seq
        }));
    }
    let mut seqs = Vec::new();
    for h in handles {
        seqs.push(h.await.unwrap());
    }
    seqs.sort();
    seqs.dedup();
    assert_eq!(seqs.len(), 32, "every concurrent append got a distinct seq");
}
