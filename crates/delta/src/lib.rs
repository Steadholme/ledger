//! Delta — durable, offset-addressed event log / lightweight message queue for the Steadholme stack.
//!
//! Library root: defines [`AppState`], wires the routes via [`app`], and provides
//! [`build_dev_state`] (in-memory store, audit off) and [`build_state_from_env`] (env-selected
//! store + Watchtower audit). Integration tests consume [`app`] directly via `tower::oneshot`,
//! exactly like the rest of the estate.
//!
//! Delta replaces Kafka/Redis-streams with one append-only, offset-addressed event log on Postgres:
//! producers append, consumers poll by offset, durable + replayable + at-least-once. It serves TWO
//! surfaces on one subdomain (`events.w33d.xyz`), split at the Sluice gateway (the relay/loom/cellar
//! precedent):
//!
//! - The WEB console at `/` is `auth=sso` (gateway-injected `X-Auth-*`): a read-only dashboard of
//!   streams, per-stream tail, and consumer cursors + lag. Delta is internal-only and trusts the
//!   injected identity headers (display only).
//! - The producer/consumer API under `/api/` is `auth=public` at the gateway — a backend service
//!   speaks neither browser OIDC nor cookie SSO — so Delta does its OWN bearer auth there against
//!   the shared `DELTA_SERVICE_TOKEN`.
//!
//! Boots zero-config: the default in-memory store + dev service token make the whole service usable
//! with NO database and NO configuration.

pub mod audit;
pub mod auth;
pub mod config;
pub mod error;
pub mod handlers;
pub mod store;

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::routing::{get, post};
use axum::Router;

use crate::audit::AuditSink;
use crate::config::{env_nonempty, env_truthy, Config};
use crate::store::{InMemoryStore, PgStore, Store};

/// Shared application state. Cheap to clone (everything behind `Arc` / cloneable handles).
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub store: Arc<dyn Store>,
    pub audit: AuditSink,
}

/// Build the router wiring all endpoints onto `state`.
///
/// The console route sits at the service root (Sluice forwards it unmodified); the `/api/*` subtree
/// is the public producer/consumer API (Delta enforces its own bearer auth there).
pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(handlers::health::healthz))
        // --- SSO web console ---
        .route("/", get(handlers::console::index))
        // --- public producer/consumer API (own bearer auth) ---
        .route(
            "/api/streams/{stream}/events",
            post(handlers::api::append).get(handlers::api::read),
        )
        .route(
            "/api/cursors/{consumer}/{stream}",
            post(handlers::api::commit_cursor).get(handlers::api::read_cursor),
        )
        .with_state(state)
}

/// Construct dev state: dev [`Config`], an empty [`InMemoryStore`], and a disabled audit sink.
/// Used by `main`'s memory mode and the integration tests, so they need no database.
pub fn build_dev_state() -> AppState {
    AppState {
        config: Arc::new(Config::dev()),
        store: Arc::new(InMemoryStore::new()),
        audit: AuditSink::disabled(),
    }
}

/// Build runtime state from the environment.
///
/// [`Config`] comes from [`Config::from_env`]. The store is selected by `DELTA_STORE`:
/// - `memory` (default): empty [`InMemoryStore`] — no database required.
/// - `postgres`: connect `DELTA_DATABASE_URL` (or `DATABASE_URL`), run the idempotent migration,
///   wire [`PgStore`].
///
/// The audit sink is enabled by `AUDIT_ENABLED` + `WATCHTOWER_URL` + `AUDIT_INGEST_TOKEN`. Returns
/// an error string on misconfiguration so `main` can fail loudly.
pub async fn build_state_from_env() -> Result<AppState, String> {
    let config = Config::from_env();

    let store_kind = env_nonempty("DELTA_STORE").unwrap_or_else(|| "memory".to_string());
    let store: Arc<dyn Store> = match store_kind.as_str() {
        "postgres" => {
            let database_url = env_nonempty("DELTA_DATABASE_URL")
                .or_else(|| env_nonempty("DATABASE_URL"))
                .ok_or_else(|| "DELTA_STORE=postgres requires DELTA_DATABASE_URL".to_string())?;
            tracing::info!("DELTA_STORE=postgres — connecting to database");
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
        other => return Err(format!("unknown DELTA_STORE={other} (use memory|postgres)")),
    };

    let audit = AuditSink::start(
        env_truthy("AUDIT_ENABLED"),
        &env_nonempty("WATCHTOWER_URL").unwrap_or_default(),
        env_nonempty("AUDIT_INGEST_TOKEN").as_deref(),
    );

    Ok(AppState {
        config: Arc::new(config),
        store,
        audit,
    })
}

/// Current wall-clock time in epoch seconds (the event `created_at` / cursor `updated_at`).
pub fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before UNIX epoch")
        .as_secs() as i64
}
