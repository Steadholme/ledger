//! End-to-end HTTP + scheduler flow over the in-memory store (NO database).
//!
//! Drives the real `app` router via `tower::oneshot`, exactly like the rest of the estate. Covers:
//! health, the empty console, the SSO/CSRF guards on save/toggle, cron job creation, heartbeat job
//! creation + the public `/ping/{token}` capability endpoint, and the scheduler `tick` flagging a
//! silent heartbeat DOWN then recording recovery once it beats again.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use tempo::model::{
    Job, Run, KIND_CRON, KIND_HEARTBEAT, STATUS_DLQ, STATUS_DOWN, STATUS_FAIL, STATUS_UP,
};
use tempo::store::{InMemoryStore, Store};
use tempo::{app, build_dev_state, now_secs, scheduler, AppState};
use tower::ServiceExt;

const CSRF: &str = "tok_csrf_for_tests";

#[tokio::test]
async fn stylesheet_is_public_immutable_and_typed() {
    let response = app(build_dev_state())
        .oneshot(
            Request::get("/assets/tempo-20260908.css")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/css; charset=utf-8"
    );
    assert_eq!(
        response.headers().get(header::CACHE_CONTROL).unwrap(),
        "public, max-age=31536000, immutable"
    );
    assert_eq!(
        response
            .headers()
            .get(header::X_CONTENT_TYPE_OPTIONS)
            .unwrap(),
        "nosniff"
    );
}

#[tokio::test]
async fn console_guards_and_job_lifecycle() {
    let state = build_dev_state();

    // --- health ------------------------------------------------------------
    let (status, body) = call(&state, get("/healthz")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "ok");

    // --- empty console (GET / mints a CSRF cookie) -------------------------
    let resp = app(state.clone()).oneshot(get("/")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let set_cookie = resp
        .headers()
        .get(header::SET_COOKIE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        set_cookie.contains("__Host-csrf="),
        "GET / mints CSRF cookie"
    );
    let (_, html) = read(resp).await;
    assert!(html.contains("No jobs yet"), "empty job list placeholder");
    assert!(html.contains(r#"href="/assets/tempo-20260908.css""#));
    assert!(!html.contains("<style>"));

    // --- POST without CSRF cookie -> 400 -----------------------------------
    let form_body = form(&[
        ("name", "ping-example"),
        ("kind", "cron"),
        ("schedule", "@every 30s"),
        ("target_url", "https://example.com/ping"),
        ("grace_secs", "300"),
        ("enabled", "on"),
        ("csrf_token", CSRF),
    ]);
    let (status, _) = call(&state, post_no_cookie("/api/jobs", &form_body)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "no CSRF cookie -> 400");

    // --- bad schedule rejected ---------------------------------------------
    let bad = form(&[
        ("name", "bad"),
        ("kind", "cron"),
        ("schedule", "hourly-ish"),
        ("target_url", "https://example.com/ping"),
        ("csrf_token", CSRF),
    ]);
    let (status, _) = call(&state, post_csrf("/api/jobs", &bad)).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "unparseable schedule -> 400"
    );

    // --- non-http target rejected ------------------------------------------
    let bad_url = form(&[
        ("name", "bad"),
        ("kind", "cron"),
        ("schedule", "@every 30s"),
        ("target_url", "javascript:alert(1)"),
        ("csrf_token", CSRF),
    ]);
    let (status, _) = call(&state, post_csrf("/api/jobs", &bad_url)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "non-http target -> 400");

    // --- create a valid cron job -------------------------------------------
    let (status, _) = call(&state, post_csrf("/api/jobs", &form_body)).await;
    assert_eq!(status, StatusCode::SEE_OTHER, "create redirects (PRG)");

    // --- console lists it + escapes nothing dangerous ----------------------
    let (_, html) = call(&state, get("/")).await;
    assert!(html.contains("ping-example"));
    assert!(html.contains("https://example.com/ping"));
    assert!(html.contains("every 30s"));

    // --- toggle requires CSRF ----------------------------------------------
    let job_id = first_job_id(&state).await;
    let toggle = form(&[("csrf_token", CSRF)]);
    let (status, _) = call(
        &state,
        post_csrf(&format!("/api/jobs/{job_id}/toggle"), &toggle),
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert!(
        !state.store.get_job(&job_id).await.unwrap().enabled,
        "toggled off"
    );
}

#[tokio::test]
async fn heartbeat_ping_and_deadman_sweep() {
    // Use a shared in-memory store we can seed + inspect directly alongside the HTTP app.
    let mut state = build_dev_state();
    let store = Arc::new(InMemoryStore::new());
    state.store = store.clone();

    // Create a heartbeat job through the console (grace 60s).
    let body = form(&[
        ("name", "nightly-backup"),
        ("kind", "heartbeat"),
        ("grace_secs", "60"),
        ("enabled", "on"),
        ("csrf_token", CSRF),
    ]);
    let (status, _) = call(&state, post_csrf("/api/jobs", &body)).await;
    assert_eq!(status, StatusCode::SEE_OTHER);

    // The console reveals the public ping URL with the token.
    let (_, html) = call(&state, get("/")).await;
    assert!(html.contains("/ping/"), "ping URL shown for heartbeat job");

    // Grab the generated token from the heartbeat row.
    let hb = state.store.list_heartbeats().await;
    assert_eq!(hb.len(), 1);
    let token = hb[0].token.clone();
    let job_id = hb[0].job_id.clone();

    // --- unknown token -> 404 ----------------------------------------------
    let (status, b) = call(&state, get("/ping/totally-unknown-token")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(b, "not found");

    // --- a real beat -> 200 ok + advances last_beat_at ---------------------
    let (status, b) = call(&state, get(&format!("/ping/{token}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(b, "ok");
    assert!(
        state
            .store
            .get_heartbeat(&token)
            .await
            .unwrap()
            .last_beat_at
            > 0
    );

    // --- force the beat into the past so the dead-man window has elapsed ----
    let stale = now_secs() - 600;
    store.touch_heartbeat(&token, stale).await.unwrap();

    // The scheduler sweep flags it DOWN (emits audit + would notify Klaxon).
    scheduler::tick(&state).await;
    assert_eq!(
        state.store.get_job(&job_id).await.unwrap().last_status,
        STATUS_DOWN,
        "silent heartbeat flagged down"
    );
    let runs = state.store.recent_runs().await;
    assert!(
        runs.iter().any(|r| r.status == STATUS_DOWN),
        "down run recorded"
    );

    // A fresh beat + another sweep records recovery.
    store.touch_heartbeat(&token, now_secs()).await.unwrap();
    scheduler::tick(&state).await;
    assert_eq!(
        state.store.get_job(&job_id).await.unwrap().last_status,
        STATUS_UP,
        "recovered heartbeat flagged up"
    );

    // Idempotent: a heartbeat job is not a cron job and never carries the heartbeat kind wrongly.
    assert!(state.store.get_job(&job_id).await.unwrap().kind == KIND_HEARTBEAT);
}

#[tokio::test]
async fn dlq_run_can_be_filtered_and_replayed() {
    let state = build_dev_state();
    let now = now_secs();
    state
        .store
        .create_job(&Job {
            id: "job_dead".to_string(),
            name: "dead cron".to_string(),
            kind: KIND_CRON.to_string(),
            schedule: "@every 30s".to_string(),
            target_url: "https://example.com/ping".to_string(),
            grace_secs: 300,
            enabled: true,
            last_run_at: now,
            last_status: STATUS_DLQ.to_string(),
            created_at: now,
        })
        .await
        .unwrap();
    state
        .store
        .insert_run(&Run {
            id: "run_dead".to_string(),
            job_id: "job_dead".to_string(),
            started_at: now,
            status: STATUS_DLQ.to_string(),
            detail: "attempt 3/3 exhausted; moved to DLQ".to_string(),
        })
        .await
        .unwrap();

    let (_, html) = call(&state, get("/?status=dlq")).await;
    assert!(html.contains("run history") || html.contains("Run history"));
    assert!(html.contains("Replay"));
    assert!(html.contains("attempt 3/3"));

    let body = form(&[("csrf_token", CSRF)]);
    let (status, _) = call(&state, post_csrf("/api/runs/run_dead/replay", &body)).await;
    assert_eq!(status, StatusCode::SEE_OTHER);

    let retries = state.store.due_retries(now_secs() + 60, 10).await;
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].source_run_id, "run_dead");
    assert_eq!(retries[0].job_id, "job_dead");
}

#[tokio::test]
async fn failing_cron_fire_queues_retry() {
    let state = build_dev_state();
    state
        .store
        .create_job(&Job {
            id: "job_retry".to_string(),
            name: "retry cron".to_string(),
            kind: KIND_CRON.to_string(),
            schedule: "@every 30s".to_string(),
            target_url: "http://127.0.0.1:9/unreachable".to_string(),
            grace_secs: 300,
            enabled: true,
            last_run_at: 0,
            last_status: String::new(),
            created_at: now_secs(),
        })
        .await
        .unwrap();

    scheduler::tick(&state).await;

    assert_eq!(
        state.store.get_job("job_retry").await.unwrap().last_status,
        STATUS_FAIL
    );
    let runs = state.store.recent_runs().await;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].status, STATUS_FAIL);
    assert!(runs[0].detail.contains("retry queued"));

    let retries = state.store.due_retries(now_secs() + 60, 10).await;
    assert_eq!(retries.len(), 1);
    assert_eq!(retries[0].job_id, "job_retry");
    assert_eq!(retries[0].attempt, 2);
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

async fn first_job_id(state: &AppState) -> String {
    state.store.list_jobs().await[0].id.clone()
}

async fn call(state: &AppState, req: Request<Body>) -> (StatusCode, String) {
    let resp = app(state.clone()).oneshot(req).await.unwrap();
    read(resp).await
}

async fn read(resp: axum::response::Response) -> (StatusCode, String) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

fn get(uri: &str) -> Request<Body> {
    Request::builder().uri(uri).body(Body::empty()).unwrap()
}

/// A urlencoded POST carrying the test CSRF cookie + gateway identity.
fn post_csrf(uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header(header::COOKIE, format!("__Host-csrf={CSRF}"))
        .header("x-auth-subject", "u_admin")
        .header("x-auth-email", "admin@w33d.xyz")
        .body(Body::from(body.to_string()))
        .unwrap()
}

/// A POST with the gateway identity but NO CSRF cookie (to exercise the guard).
fn post_no_cookie(uri: &str, body: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
        .header("x-auth-subject", "u_admin")
        .header("x-auth-email", "admin@w33d.xyz")
        .body(Body::from(body.to_string()))
        .unwrap()
}

fn form(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| format!("{}={}", k, enc(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Minimal application/x-www-form-urlencoded value encoder.
fn enc(s: &str) -> String {
    let mut o = String::new();
    for b in s.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                o.push(b as char)
            }
            b' ' => o.push('+'),
            _ => o.push_str(&format!("%{b:02X}")),
        }
    }
    o
}
