//! The public dead-man heartbeat endpoint (`GET /ping/{token}`).
//!
//! This is the URL an EXTERNAL cron job `curl`s on every successful run ("I am alive"). It is
//! `auth=public` at the Sluice gateway (longer-prefix `/ping/` wins over the SSO root), reachable by
//! a client with no session: the unguessable `{token}` IS the capability/credential.
//!
//! It does exactly one thing — advance the heartbeat's `last_beat_at` to now — and answers in plain
//! text: `200 ok` for a known token, `404 not found` for an unknown one. It NEVER touches Watchtower
//! or Klaxon (the dead-man evaluation + alerting all happens off the hot path in the scheduler), so
//! this endpoint stays trivially fast and cannot be blocked by a slow downstream.

use axum::extract::{Path, State};
use axum::http::StatusCode;

use crate::{now_secs, AppState};

/// `GET /ping/{token}` — record a beat. Public; the token in the path is the only credential.
pub async fn ping(State(state): State<AppState>, Path(token): Path<String>) -> (StatusCode, &'static str) {
    match state.store.touch_heartbeat(&token, now_secs()).await {
        Ok(true) => (StatusCode::OK, "ok"),
        Ok(false) => (StatusCode::NOT_FOUND, "not found"),
        Err(e) => {
            tracing::warn!(error = %e, "heartbeat touch failed");
            (StatusCode::INTERNAL_SERVER_ERROR, "error")
        }
    }
}
