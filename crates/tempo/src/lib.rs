//! Tempo — durable cron scheduler, dead-man heartbeats, and synthetic pings for the HOLDFAST stack.
//!
//! Library root: defines [`AppState`], wires the routes via [`app`], and provides
//! [`build_dev_state`] (in-memory store, no Klaxon, audit off) and [`build_state_from_env`]
//! (env-selected store + Watchtower audit + optional Klaxon). Integration tests consume [`app`]
//! directly via `tower::oneshot`, exactly like the rest of the estate.
//!
//! Tempo serves TWO surfaces on one subdomain (`jobs.w33d.xyz`), split at the Sluice gateway
//! (the cellar/loom precedent):
//!
//! - The WEB console at `/` is `auth=sso` (gateway-injected `X-Auth-*`): list/create/edit/toggle
//!   jobs, see recent runs and live heartbeat status. Tempo is internal-only and trusts the injected
//!   identity headers.
//! - The dead-man heartbeat URL `GET /ping/{token}` is `auth=public` at the gateway (longer prefix
//!   wins) — an external cron `curl`s it with no session. It carries its OWN capability auth: the
//!   unguessable `{token}` IS the credential. It touches `last_beat_at` and answers `200 ok`.
//!
//! A background tokio loop ([`scheduler`]) ticks every `TEMPO_TICK_SECS` (default 30s): it fires due
//! cron jobs (GET/webhook the `target_url`, record a run, audit + Klaxon on failure) and flags
//! heartbeats whose grace window has elapsed as DOWN.

pub mod audit;
pub mod auth;
pub mod config;
pub mod error;
pub mod handlers;
pub mod model;
pub mod schedule;
pub mod scheduler;
pub mod store;

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::routing::{get, post};
use axum::Router;
use rand::rngs::OsRng;
use rand::RngCore;

use crate::audit::AuditSink;
use crate::config::{env_nonempty, env_truthy, Config};
use crate::store::{InMemoryStore, PgStore, Store};

/// Shared application state. Cheap to clone (everything behind `Arc` / cloneable handles).
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub store: Arc<dyn Store>,
    pub http: reqwest::Client,
    pub audit: AuditSink,
}

/// Build the router wiring all endpoints onto `state`.
///
/// The console routes sit at the service root (Sluice forwards them unmodified); the more-specific
/// `/ping/{token}` path is the public heartbeat surface (Tempo's own capability-token auth there).
pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(handlers::health::healthz))
        // --- SSO web console ---
        .route("/", get(handlers::console::index))
        .route("/api/jobs", post(handlers::console::save_job))
        .route("/api/jobs/{id}/toggle", post(handlers::console::toggle_job))
        // --- public dead-man heartbeat (own capability-token auth) ---
        .route("/ping/{token}", get(handlers::ping::ping))
        .with_state(state)
}

/// Construct dev state: dev [`Config`], an empty [`InMemoryStore`], a reqwest client, and a disabled
/// audit sink. Tests reuse this shape. The scheduler loop is NOT spawned here — `main` owns that, so
/// tests drive [`scheduler::tick`] deterministically.
pub fn build_dev_state() -> AppState {
    AppState {
        config: Arc::new(Config::dev()),
        store: Arc::new(InMemoryStore::new()),
        http: build_http_client(),
        audit: AuditSink::disabled(),
    }
}

/// Build runtime state from the environment.
///
/// [`Config`] comes from [`Config::from_env`]. The store is selected by `TEMPO_STORE`:
/// - `memory` (default): empty [`InMemoryStore`] — no database required.
/// - `postgres`: connect `TEMPO_DATABASE_URL` (or `DATABASE_URL`), run the idempotent migration,
///   wire [`PgStore`].
///
/// The audit sink is enabled by `AUDIT_ENABLED` + `WATCHTOWER_URL` + `AUDIT_INGEST_TOKEN`. Returns
/// an error string on misconfiguration so `main` can fail loudly.
pub async fn build_state_from_env() -> Result<AppState, String> {
    let config = Config::from_env();

    let store_kind = env_nonempty("TEMPO_STORE").unwrap_or_else(|| "memory".to_string());
    let store: Arc<dyn Store> = match store_kind.as_str() {
        "postgres" => {
            let database_url = env_nonempty("TEMPO_DATABASE_URL")
                .or_else(|| env_nonempty("DATABASE_URL"))
                .ok_or_else(|| "TEMPO_STORE=postgres requires TEMPO_DATABASE_URL".to_string())?;
            tracing::info!("TEMPO_STORE=postgres — connecting to database");
            let pg = PgStore::connect(&database_url)
                .await
                .map_err(|e| format!("connect postgres: {e}"))?;
            pg.migrate()
                .await
                .map_err(|e| format!("run migration: {e}"))?;
            tracing::info!("postgres store ready (migrated)");
            Arc::new(pg)
        }
        "memory" => Arc::new(InMemoryStore::new()),
        other => return Err(format!("unknown TEMPO_STORE={other} (use memory|postgres)")),
    };

    let audit = AuditSink::start(
        env_truthy("AUDIT_ENABLED"),
        &env_nonempty("WATCHTOWER_URL").unwrap_or_default(),
        env_nonempty("AUDIT_INGEST_TOKEN").as_deref(),
    );

    match &config.klaxon {
        Some(k) => tracing::info!(url = %k.url, "klaxon notify enabled"),
        None => tracing::info!("klaxon notify disabled (KLAXON_URL/_INGEST_TOKEN/_NOTIFY_EMAIL unset)"),
    }

    Ok(AppState {
        config: Arc::new(config),
        store,
        http: build_http_client(),
        audit,
    })
}

/// The shared reqwest client used for firing cron targets + the Klaxon notify (rustls; bounded
/// timeouts so a slow target cannot hang the scheduler tick indefinitely).
fn build_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .build()
        .expect("failed to build reqwest client")
}

/// Current wall-clock time in epoch seconds.
pub fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_secs() as i64
}

/// Generate a random URL-safe alphanumeric string of `len` characters from a 62-symbol alphabet, via
/// the OS CSPRNG. Used for job/run ids and the heartbeat capability token. The modulo over 62 is a
/// negligible bias irrelevant for tokens of this size.
pub fn random_alnum(len: usize) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut bytes = vec![0u8; len];
    OsRng.fill_bytes(&mut bytes);
    bytes
        .iter()
        .map(|b| ALPHABET[*b as usize % ALPHABET.len()] as char)
        .collect()
}
