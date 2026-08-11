//! The producer/consumer event-log API (`/api/...`) — Delta's OWN bearer-token auth surface.
//!
//! This subtree is `auth=public` at the Sluice gateway (a backend producer/consumer speaks neither
//! the browser OIDC nor cookie SSO), so Delta authenticates every `/api` call itself against the
//! shared `DELTA_SERVICE_TOKEN` (`Authorization: Bearer <token>`, constant-time compared).
//! Missing/invalid tokens get a `401` JSON error envelope.
//!
//! Endpoints:
//! - `POST /api/streams/{stream}/events`
//!   `{ "key"?, "event_id", "payload_hash", "payload" }` -> `{ "seq" }` (idempotent append).
//! - `GET  /api/streams/{stream}/events?after={seq}&limit={n}` -> `{ events: [...] }` (durable read).
//! - `POST /api/cursors/{consumer}/{stream}`  `{ "offset" }` -> the committed cursor (+ head/lag).
//! - `GET  /api/cursors/{consumer}/{stream}` -> the committed cursor (+ head/lag).
//!
//! `payload` is opaque text (the producer encodes JSON). A SAMPLED `delta.stream.append` audit
//! event is emitted per append — non-blocking, payload-free.

use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::audit::AuditEvent;
use crate::config::MAX_READ_LIMIT;
use crate::store::Event;
use crate::{now_secs, AppState};

/// Append body: `payload` is required (opaque text); `key` is an optional routing key.
#[derive(Debug, Deserialize)]
pub struct AppendBody {
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub event_id: Option<String>,
    #[serde(default)]
    pub payload_hash: Option<String>,
    #[serde(default)]
    pub payload: Option<String>,
}

/// Cursor-commit body: the offset (last processed seq) the consumer is committing.
#[derive(Debug, Deserialize)]
pub struct CommitBody {
    #[serde(default)]
    pub offset: Option<i64>,
}

/// Query string for the durable read (`?after={seq}&limit={n}`).
#[derive(Debug, Deserialize)]
pub struct ReadQuery {
    #[serde(default)]
    pub after: Option<i64>,
    #[serde(default)]
    pub limit: Option<i64>,
    #[serde(default)]
    pub key: Option<String>,
    #[serde(default)]
    pub contains: Option<String>,
}

// ===========================================================================
// POST /api/streams/{stream}/events — append
// ===========================================================================

pub async fn append(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(stream): Path<String>,
    body: Result<Json<AppendBody>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if let Err(resp) = authorize(&state, &headers) {
        return resp;
    }
    let stream = stream.trim();
    if stream.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "stream is required.",
        );
    }
    let Json(body) = match body {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("Malformed JSON body: {e}"),
            )
        }
    };
    let payload = body.payload.unwrap_or_default();
    if payload.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "`payload` is required and must be non-empty.",
        );
    }
    let key = body.key.unwrap_or_default();
    let event_id = body.event_id.unwrap_or_default();
    if event_id.is_empty() || event_id.len() > 256 || event_id.contains(['\n', '\r', '\0']) {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "`event_id` is required and must be a safe identifier.",
        );
    }
    let payload_hash = body.payload_hash.unwrap_or_default();
    if payload_hash.len() != 64
        || !payload_hash
            .bytes()
            .all(|value| value.is_ascii_digit() || (b'a'..=b'f').contains(&value))
        || hex::encode(Sha256::digest(payload.as_bytes())) != payload_hash
    {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "`payload_hash` must be the lowercase SHA-256 of `payload`.",
        );
    }

    let event = match state
        .store
        .append(stream, &key, &event_id, &payload_hash, &payload, now_secs())
        .await
    {
        Ok(ev) => ev,
        Err(crate::store::StoreError::Conflict) => {
            return error_response(
                StatusCode::CONFLICT,
                "event_conflict",
                "The event identity is already sealed with a different payload.",
            )
        }
        Err(e) => {
            tracing::error!(error = %e, stream, "append failed");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "Append failed.",
            );
        }
    };

    // Sampled, non-blocking audit (payload never carried). Emit one in every `audit_divisor`.
    if event.seq % state.config.audit_divisor() == 0 {
        state.audit.emit(AuditEvent::info(
            "delta.stream.append",
            "service",
            stream,
            &format!("seq={}", event.seq),
        ));
    }

    tracing::debug!(stream, seq = event.seq, "event appended");
    (StatusCode::CREATED, Json(json!({ "seq": event.seq }))).into_response()
}

// ===========================================================================
// GET /api/streams/{stream}/events?after=&limit= — durable read by offset
// ===========================================================================

pub async fn read(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(stream): Path<String>,
    Query(q): Query<ReadQuery>,
) -> Response {
    if let Err(resp) = authorize(&state, &headers) {
        return resp;
    }
    let stream = stream.trim();
    if stream.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "stream is required.",
        );
    }
    let after = q.after.unwrap_or(0).max(0);
    let limit = match q.limit {
        Some(n) if n > 0 => n.min(MAX_READ_LIMIT),
        Some(_) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "`limit` must be a positive integer.",
            )
        }
        None => state.config.read_limit,
    };

    let key = q.key.unwrap_or_default();
    let contains = q.contains.unwrap_or_default();
    let events = if key.trim().is_empty() && contains.trim().is_empty() {
        state.store.read_after(stream, after, limit).await
    } else {
        state
            .store
            .query_events(stream, key.trim(), contains.trim(), after, limit)
            .await
    };
    let head = state.store.head_seq(stream).await;
    let next_after = events.last().map(|e| e.seq).unwrap_or(after);

    Json(json!({
        "stream": stream,
        "after": after,
        "limit": limit,
        "key": key.trim(),
        "contains": contains.trim(),
        "count": events.len(),
        "head": head,
        "next_after": next_after,
        "events": events.iter().map(event_json).collect::<Vec<_>>(),
    }))
    .into_response()
}

// ===========================================================================
// POST /api/cursors/{consumer}/{stream} — commit an offset
// ===========================================================================

pub async fn commit_cursor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((consumer, stream)): Path<(String, String)>,
    body: Result<Json<CommitBody>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if let Err(resp) = authorize(&state, &headers) {
        return resp;
    }
    let (consumer, stream) = (consumer.trim(), stream.trim());
    if consumer.is_empty() || stream.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "consumer and stream are required.",
        );
    }
    let Json(body) = match body {
        Ok(b) => b,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                &format!("Malformed JSON body: {e}"),
            )
        }
    };
    let offset = match body.offset {
        Some(n) if n >= 0 => n,
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "`offset` is required and must be a non-negative integer.",
            )
        }
    };

    let cursor = match state
        .store
        .commit_cursor(consumer, stream, offset, now_secs())
        .await
    {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, consumer, stream, "cursor commit failed");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                "Cursor commit failed.",
            );
        }
    };
    let head = state.store.head_seq(stream).await;
    tracing::debug!(consumer, stream, offset, "cursor committed");
    Json(cursor_json(
        &cursor.consumer,
        &cursor.stream,
        cursor.offset_seq,
        cursor.updated_at,
        head,
    ))
    .into_response()
}

// ===========================================================================
// GET /api/cursors/{consumer}/{stream} — read an offset
// ===========================================================================

pub async fn read_cursor(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((consumer, stream)): Path<(String, String)>,
) -> Response {
    if let Err(resp) = authorize(&state, &headers) {
        return resp;
    }
    let (consumer, stream) = (consumer.trim(), stream.trim());
    if consumer.is_empty() || stream.is_empty() {
        return error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "consumer and stream are required.",
        );
    }
    let head = state.store.head_seq(stream).await;
    match state.store.get_cursor(consumer, stream).await {
        Some(c) => Json(cursor_json(
            &c.consumer,
            &c.stream,
            c.offset_seq,
            c.updated_at,
            head,
        ))
        .into_response(),
        // An uncommitted cursor reads as offset 0 (a fresh consumer starts at the beginning).
        None => Json(cursor_json(consumer, stream, 0, 0, head)).into_response(),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Verify the `Authorization: Bearer <token>` header against the configured service token. Returns
/// `Ok(())` on success; on failure returns a ready `401` JSON error response.
fn authorize(state: &AppState, headers: &HeaderMap) -> Result<(), Response> {
    if crate::auth::verify_service_token(headers, &state.config.service_token) {
        Ok(())
    } else {
        Err(unauthorized(
            "Missing or invalid bearer token. Pass `Authorization: Bearer <DELTA_SERVICE_TOKEN>`.",
        ))
    }
}

/// JSON shape for one event in a read response.
fn event_json(e: &Event) -> serde_json::Value {
    json!({
        "seq": e.seq,
        "stream": e.stream,
        "key": e.key,
        "payload": e.payload,
        "created_at": e.created_at,
    })
}

/// JSON shape for a cursor, with the stream `head` and the derived `lag` (head - offset, clamped).
fn cursor_json(
    consumer: &str,
    stream: &str,
    offset: i64,
    updated_at: i64,
    head: i64,
) -> serde_json::Value {
    json!({
        "consumer": consumer,
        "stream": stream,
        "offset": offset,
        "head": head,
        "lag": (head - offset).max(0),
        "updated_at": updated_at,
    })
}

/// A `401` with the `WWW-Authenticate: Bearer` challenge + JSON error envelope.
fn unauthorized(message: &str) -> Response {
    let mut resp = error_response(StatusCode::UNAUTHORIZED, "unauthorized", message);
    resp.headers_mut()
        .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    resp
}

/// Build a `{ "error": { message, type } }` JSON response at `status`.
fn error_response(status: StatusCode, type_: &str, message: &str) -> Response {
    (
        status,
        Json(json!({ "error": { "message": message, "type": type_ } })),
    )
        .into_response()
}
