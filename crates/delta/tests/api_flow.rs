//! End-to-end API + console flow over the real router (in-memory store, no DB, no port bind).
//!
//! Drives [`delta::app`] via `tower::ServiceExt::oneshot`, exactly like the rest of the estate.
//! Covers: bearer auth on `/api`, append -> offset read, cursor commit/read + lag, and the SSO
//! console render.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use delta::config::DEFAULT_SERVICE_TOKEN;
use delta::{app, build_dev_state};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

const TOKEN: &str = DEFAULT_SERVICE_TOKEN;

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn json_of(s: &str) -> serde_json::Value {
    serde_json::from_str(s).unwrap()
}

fn append_body(key: Option<&str>, event_id: &str, payload: &str) -> String {
    serde_json::json!({
        "key": key,
        "event_id": event_id,
        "payload_hash": hex::encode(Sha256::digest(payload.as_bytes())),
        "payload": payload
    })
    .to_string()
}

#[tokio::test]
async fn healthz_is_open_and_plain_ok() {
    let resp = app(build_dev_state())
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_string(resp).await, "ok");
}

#[tokio::test]
async fn stylesheet_is_public_immutable_and_typed() {
    let response = app(build_dev_state())
        .oneshot(
            Request::get("/assets/delta-20260908.css")
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
async fn api_requires_bearer() {
    // No Authorization header -> 401 with WWW-Authenticate.
    let resp = app(build_dev_state())
        .oneshot(
            Request::post("/api/streams/orders/events")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"payload":"x"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert!(resp.headers().contains_key(header::WWW_AUTHENTICATE));

    // Wrong token -> 401.
    let resp = app(build_dev_state())
        .oneshot(
            Request::post("/api/streams/orders/events")
                .header(header::AUTHORIZATION, "Bearer nope")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"payload":"x"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn append_then_read_by_offset() {
    let app = app(build_dev_state());

    // Append three events to two streams.
    for (index, (stream, payload)) in [("orders", "a"), ("orders", "b"), ("billing", "c")]
        .into_iter()
        .enumerate()
    {
        let resp = app
            .clone()
            .oneshot(
                Request::post(format!("/api/streams/{stream}/events"))
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(append_body(
                        None,
                        &format!("evt-append-{index}"),
                        payload,
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    // Read orders from offset 0 -> two events with global seqs 1 and 2.
    let resp = app
        .clone()
        .oneshot(
            Request::get("/api/streams/orders/events?after=0&limit=10")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_of(&body_string(resp).await);
    assert_eq!(v["count"], 2);
    assert_eq!(v["head"], 2);
    assert_eq!(v["events"][0]["seq"], 1);
    assert_eq!(v["events"][0]["payload"], "a");
    assert_eq!(v["events"][1]["seq"], 2);
    assert_eq!(v["next_after"], 2);

    // Durable read past offset 1 -> only the second orders event.
    let resp = app
        .clone()
        .oneshot(
            Request::get("/api/streams/orders/events?after=1")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let v = json_of(&body_string(resp).await);
    assert_eq!(v["count"], 1);
    assert_eq!(v["events"][0]["payload"], "b");
}

#[tokio::test]
async fn read_filters_by_key_and_payload_fragment() {
    let app = app(build_dev_state());

    for (index, (key, payload)) in [
        ("created", "alice paid"),
        ("updated", "bob refunded"),
        ("created", "alice shipped"),
    ]
    .into_iter()
    .enumerate()
    {
        let resp = app
            .clone()
            .oneshot(
                Request::post("/api/streams/orders/events")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(append_body(
                        Some(key),
                        &format!("evt-query-{index}"),
                        payload,
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    let resp = app
        .clone()
        .oneshot(
            Request::get("/api/streams/orders/events?key=created&contains=shipped")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_of(&body_string(resp).await);
    assert_eq!(v["count"], 1);
    assert_eq!(v["events"][0]["key"], "created");
    assert_eq!(v["events"][0]["payload"], "alice shipped");
}

#[tokio::test]
async fn append_rejects_empty_payload() {
    let resp = app(build_dev_state())
        .oneshot(
            Request::post("/api/streams/orders/events")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"key":"k"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn append_replay_returns_original_seq_and_hash_conflict_is_409() {
    let app = app(build_dev_state());
    let first = app
        .clone()
        .oneshot(
            Request::post("/api/streams/orders/events")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(append_body(
                    Some("order:1"),
                    "evt-replay",
                    "payload-a",
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::CREATED);
    let first_seq = json_of(&body_string(first).await)["seq"].clone();

    let replay = app
        .clone()
        .oneshot(
            Request::post("/api/streams/orders/events")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(append_body(
                    Some("order:1"),
                    "evt-replay",
                    "payload-a",
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::CREATED);
    assert_eq!(json_of(&body_string(replay).await)["seq"], first_seq);

    let conflict = app
        .oneshot(
            Request::post("/api/streams/orders/events")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(append_body(
                    Some("order:1"),
                    "evt-replay",
                    "payload-b",
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    let conflict = json_of(&body_string(conflict).await);
    assert_eq!(conflict["error"]["type"], "event_conflict");
}

#[tokio::test]
async fn cursor_commit_read_and_lag() {
    let app = app(build_dev_state());

    // Append 5 to orders.
    for i in 0..5 {
        app.clone()
            .oneshot(
                Request::post("/api/streams/orders/events")
                    .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(append_body(
                        None,
                        &format!("evt-cursor-{i}"),
                        &format!("e{i}"),
                    )))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    // Fresh consumer reads as offset 0, lag = head = 5.
    let resp = app
        .clone()
        .oneshot(
            Request::get("/api/cursors/worker-1/orders")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let v = json_of(&body_string(resp).await);
    assert_eq!(v["offset"], 0);
    assert_eq!(v["head"], 5);
    assert_eq!(v["lag"], 5);

    // Commit offset 3 -> lag becomes 2.
    let resp = app
        .clone()
        .oneshot(
            Request::post("/api/cursors/worker-1/orders")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"offset":3}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_of(&body_string(resp).await);
    assert_eq!(v["offset"], 3);
    assert_eq!(v["lag"], 2);

    // An out-of-order retry cannot move the durable acknowledgement backward,
    // and the response reports the effective cursor rather than the stale input.
    let resp = app
        .clone()
        .oneshot(
            Request::post("/api/cursors/worker-1/orders")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"offset":1}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = json_of(&body_string(resp).await);
    assert_eq!(v["offset"], 3);
    assert_eq!(v["lag"], 2);

    // Re-read confirms the durable commit.
    let resp = app
        .clone()
        .oneshot(
            Request::get("/api/cursors/worker-1/orders")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let v = json_of(&body_string(resp).await);
    assert_eq!(v["offset"], 3);
    assert_eq!(v["lag"], 2);
}

#[tokio::test]
async fn console_renders_streams_and_escapes() {
    let app = app(build_dev_state());

    // Append an event whose payload contains HTML metacharacters.
    app.clone()
        .oneshot(
            Request::post("/api/streams/orders/events")
                .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(append_body(
                    None,
                    "evt-console",
                    "<script>x</script>",
                )))
                .unwrap(),
        )
        .await
        .unwrap();

    let resp = app
        .clone()
        .oneshot(
            Request::get("/")
                .header("x-auth-email", "ops@w33d.xyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let html = body_string(resp).await;
    assert!(html.contains("Steadholme"));
    assert!(html.contains(r#"href="/assets/delta-20260908.css""#));
    assert!(!html.contains("<style>"));
    assert!(html.contains("orders"));
    assert!(html.contains("ops@w33d.xyz"));
    // The payload metacharacters are escaped, never injected raw.
    assert!(html.contains("&lt;script&gt;"));
    assert!(!html.contains("<script>x</script>"));

    let resp = app
        .clone()
        .oneshot(
            Request::get("/?stream=orders&contains=script")
                .header("x-auth-email", "ops@w33d.xyz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let html = body_string(resp).await;
    assert!(html.contains("Event query"));
    assert!(html.contains("&lt;script&gt;"));
}
