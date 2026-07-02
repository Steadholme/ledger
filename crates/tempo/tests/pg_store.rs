//! PostgreSQL `Store` integration test.
//!
//! Runs ONLY when `TEST_DATABASE_URL` is set (it needs an external Postgres). When unset the test
//! prints a note and returns early — it never fails the default `cargo test` run, which stays
//! database-free. Spin up a throwaway Postgres and run:
//!
//! ```text
//! docker run --rm -d -e POSTGRES_PASSWORD=pw -e POSTGRES_DB=tempo \
//!   -p 127.0.0.1:55470:5432 postgres:18-alpine
//! TEST_DATABASE_URL=postgres://postgres:pw@127.0.0.1:55470/tempo \
//!   cargo test --test pg_store -- --nocapture
//! ```
//!
//! The `Store` trait is async: each method `.await`s sqlx natively (no `block_in_place`), so it runs
//! on any Tokio scheduler — this test stays on `multi_thread` for parallel queries.

use tempo::model::{Heartbeat, Job, Retry, Run, KIND_CRON, KIND_HEARTBEAT, STATUS_DLQ, STATUS_OK};
use tempo::store::{PgStore, Store, StoreError};

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

    // --- jobs: create / conflict / list / update / toggle / touch ----------
    let cron = Job {
        id: "job_pg_cron".to_string(),
        name: "pg cron".to_string(),
        kind: KIND_CRON.to_string(),
        schedule: "@every 5m".to_string(),
        target_url: "https://example.com/ping".to_string(),
        grace_secs: 300,
        enabled: true,
        last_run_at: 0,
        last_status: String::new(),
        created_at: 1000,
    };
    pg.create_job(&cron).await.expect("create cron");

    // Duplicate id -> Conflict.
    assert!(matches!(
        pg.create_job(&cron).await,
        Err(StoreError::Conflict(_))
    ));

    let hb_job = Job {
        id: "job_pg_hb".to_string(),
        name: "pg heartbeat".to_string(),
        kind: KIND_HEARTBEAT.to_string(),
        schedule: String::new(),
        target_url: String::new(),
        grace_secs: 60,
        enabled: true,
        last_run_at: 0,
        last_status: String::new(),
        created_at: 2000,
    };
    pg.create_job(&hb_job).await.expect("create hb job");

    let jobs = pg.list_jobs().await;
    assert!(jobs.len() >= 2);
    assert_eq!(jobs[0].id, "job_pg_hb", "newest first by created_at");

    // Edit mutable fields (touch fields preserved).
    let mut edited = cron.clone();
    edited.schedule = "@every 1h".to_string();
    edited.enabled = false;
    pg.update_job(&edited).await.expect("update");
    let fetched = pg.get_job("job_pg_cron").await.expect("get");
    assert_eq!(fetched.schedule, "@every 1h");
    assert!(!fetched.enabled);

    // Toggle + touch.
    assert!(pg.set_enabled("job_pg_cron", true).await.expect("toggle"));
    pg.touch_job("job_pg_cron", 12345, STATUS_OK)
        .await
        .expect("touch");
    let after = pg.get_job("job_pg_cron").await.expect("get2");
    assert!(after.enabled);
    assert_eq!(after.last_run_at, 12345);
    assert_eq!(after.last_status, STATUS_OK);
    // The edited schedule survived the touch (touch must not clobber editable fields).
    assert_eq!(after.schedule, "@every 1h");

    // --- runs: insert (idempotent on id) + newest-first --------------------
    for i in 0..3 {
        let run = Run {
            id: format!("run_pg_{i}"),
            job_id: "job_pg_cron".to_string(),
            started_at: i as i64,
            status: STATUS_OK.to_string(),
            detail: format!("GET ok {i}"),
        };
        pg.insert_run(&run).await.expect("insert run");
    }
    // Duplicate run id is a no-op (ON CONFLICT DO NOTHING).
    pg.insert_run(&Run {
        id: "run_pg_0".to_string(),
        job_id: "job_pg_cron".to_string(),
        started_at: 99,
        status: STATUS_OK.to_string(),
        detail: "dup".to_string(),
    })
    .await
    .expect("dup run no-op");
    let runs = pg.recent_runs().await;
    assert!(runs.len() >= 3);
    assert_eq!(runs[0].id, "run_pg_2", "newest first");
    assert_eq!(pg.get_run("run_pg_2").await.unwrap().status, STATUS_OK);
    pg.insert_run(&Run {
        id: "run_pg_dlq".to_string(),
        job_id: "job_pg_cron".to_string(),
        started_at: 500,
        status: STATUS_DLQ.to_string(),
        detail: "dead".to_string(),
    })
    .await
    .expect("insert dlq");
    let dlq_page = pg.list_runs(STATUS_DLQ, 10, 0).await;
    assert!(dlq_page.total >= 1);
    assert!(dlq_page.runs.iter().any(|r| r.id == "run_pg_dlq"));

    // --- retry queue: insert / due / delete -------------------------------
    let retry = Retry {
        id: "retry_pg_1".to_string(),
        job_id: "job_pg_cron".to_string(),
        source_run_id: "run_pg_dlq".to_string(),
        attempt: 1,
        max_attempts: 3,
        due_at: 600,
        created_at: 500,
    };
    pg.enqueue_retry(&retry).await.expect("enqueue retry");
    pg.enqueue_retry(&retry)
        .await
        .expect("enqueue retry idempotent");
    let due = pg.due_retries(700, 10).await;
    assert!(due.iter().any(|r| r.id == "retry_pg_1"));
    assert!(pg.delete_retry("retry_pg_1").await.expect("delete retry"));

    // --- heartbeats: create / get / touch / list ---------------------------
    let hb = Heartbeat {
        token: "tok_pg_abc".to_string(),
        job_id: "job_pg_hb".to_string(),
        last_beat_at: 0,
    };
    pg.create_heartbeat(&hb).await.expect("create hb");
    // Idempotent.
    pg.create_heartbeat(&hb)
        .await
        .expect("create hb idempotent");
    assert!(pg.get_heartbeat("tok_pg_abc").await.is_some());
    assert!(pg
        .touch_heartbeat("tok_pg_abc", 7777)
        .await
        .expect("touch hb"));
    assert_eq!(
        pg.get_heartbeat("tok_pg_abc").await.unwrap().last_beat_at,
        7777
    );
    assert!(!pg.touch_heartbeat("nope", 1).await.expect("touch unknown"));
    assert!(pg
        .list_heartbeats()
        .await
        .iter()
        .any(|h| h.token == "tok_pg_abc"));

    println!(
        "PG STORE INTEGRATION OK: migrate (idempotent) + jobs create/conflict/list/update/toggle/\
         touch + runs insert(conflict)/recent + heartbeats create/get/touch/list against real Postgres"
    );
}
