//! Ledger — one container hosting the Steadholme data surfaces (events / jobs).
//!
//! Each surface is its OWN library crate (Delta/Tempo), reused verbatim: same schema, same routes,
//! same templates, same OWN database, same subdomain, same background work. This binary only adds a
//! **Host-based vhost demux** plus a **dual listener** so the estate runs ONE deployable instead of
//! two — and so the existing internal addresses keep resolving unchanged:
//!
//! - `events.w33d.xyz` / internal `delta:9150`  -> Delta (durable event log).
//! - `jobs.w33d.xyz`   / internal `tempo:9160`  -> Tempo (cron + heartbeats + pings).
//!
//! Both ports (`:9150` and `:9160`) serve the SAME demux router, which dispatches by the `Host`
//! header's leading label. So a producer POSTing to `http://delta:9150/api/...` and a service
//! `curl`ing `http://tempo:9160/ping/...` both keep working, because matching is by host label, not
//! by which port the request landed on. An unknown host is a 404.
//!
//! Ark (the backup god-credential surface) is intentionally NOT part of Ledger, so the runtime image
//! stays pure-glibc with no `pg_dump`.
//!
//! Each surface's `AppState` is built EXPLICITLY (NOT via `build_state_from_env`, which falls back to
//! the bare `DATABASE_URL` — a collision when two surfaces share one process): Delta connects
//! `DELTA_DATABASE_URL` and Tempo connects `TEMPO_DATABASE_URL`, each migrating its OWN schema. Tempo
//! additionally keeps its OWN reqwest client, its OWN audit sink, its OWN Klaxon notify config, and
//! its background scheduler loop (explicitly spawned here).
//!
//! `healthcheck` subcommand: a dependency-free loopback `GET /healthz` (host-agnostic) used as the
//! container HEALTHCHECK, so the image needs no curl.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use tower::ServiceExt;

/// Primary listen address — the events/`delta` port. Sluice fronts `events.w33d.xyz` here, and the
/// internal `delta:9150` callers reach it directly. The container HEALTHCHECK probes this port.
const DEFAULT_BIND_PRIMARY: &str = "0.0.0.0:9150";
/// Secondary listen address — the jobs/`tempo` port. Sluice fronts `jobs.w33d.xyz` here, and the
/// internal `tempo:9160` callers reach it directly. Serves the SAME demux router as the primary.
const DEFAULT_BIND_SECONDARY: &str = "0.0.0.0:9160";

/// The two composed per-surface routers, dispatched by Host. Cheap to clone (each `Router` is
/// `Arc`-backed internally).
#[derive(Clone)]
struct Vhosts {
    events: Router,
    jobs: Router,
}

#[tokio::main]
async fn main() {
    // Container HEALTHCHECK path — handled before any setup, exits the process.
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        std::process::exit(run_healthcheck());
    }

    tracing_subscriber::fmt::init();

    let bind_primary =
        std::env::var("BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND_PRIMARY.to_string());
    let bind_secondary =
        std::env::var("BIND_ADDR_SECONDARY").unwrap_or_else(|_| DEFAULT_BIND_SECONDARY.to_string());

    // Each surface connects to its OWN database and migrates idempotently — exactly what the
    // standalone service did. A failure here is fatal (the surface cannot serve without its DB).
    let events = build_events()
        .await
        .unwrap_or_else(|e| fatal("events (delta)", e));
    let jobs = build_jobs()
        .await
        .unwrap_or_else(|e| fatal("jobs (tempo)", e));

    let app = Router::new()
        // Host-agnostic liveness for the container HEALTHCHECK + estate probes (answers on both ports).
        .route("/healthz", get(|| async { "ok" }))
        .fallback(dispatch)
        .with_state(Vhosts { events, jobs });

    let primary: SocketAddr = bind_primary.parse().expect("invalid BIND_ADDR");
    let secondary: SocketAddr = bind_secondary.parse().expect("invalid BIND_ADDR_SECONDARY");

    let l_primary = tokio::net::TcpListener::bind(primary)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {primary}: {e}"));
    let l_secondary = tokio::net::TcpListener::bind(secondary)
        .await
        .unwrap_or_else(|e| panic!("failed to bind {secondary}: {e}"));

    tracing::info!(%primary, %secondary, "Ledger listening (events/jobs vhost demux, dual listener)");

    // Serve the SAME demux router on both ports. Either task ending is fatal.
    let app_secondary = app.clone();
    let primary_task = tokio::spawn(async move {
        axum::serve(l_primary, app)
            .await
            .expect("primary server error");
    });
    let secondary_task = tokio::spawn(async move {
        axum::serve(l_secondary, app_secondary)
            .await
            .expect("secondary server error");
    });

    let _ = tokio::try_join!(primary_task, secondary_task);
}

/// Dispatch one request to the surface matching its `Host` header. The full request (headers + body)
/// is forwarded, so each surface's own auth (Delta's `/api` bearer, Tempo's `/ping/{token}`
/// capability) still applies. An unknown host is a 404.
async fn dispatch(State(v): State<Vhosts>, req: Request) -> Response {
    let host = req
        .headers()
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    // Match on the leading label, ignoring any port. Accept the gateway subdomain label
    // (`events`/`jobs`) AND the bare internal service-name label (`delta`/`tempo`) callers use.
    let label = host
        .split(':')
        .next()
        .unwrap_or("")
        .split('.')
        .next()
        .unwrap_or("");
    let router = match label {
        "events" | "delta" => v.events,
        "jobs" | "tempo" => v.jobs,
        _ => return (StatusCode::NOT_FOUND, "unknown data host").into_response(),
    };
    // `Router` is a tower `Service` (the exact `app(state).oneshot(req)` path the surfaces' own
    // tests use); its error type is `Infallible`.
    match router.oneshot(req).await {
        Ok(resp) => resp,
        Err(e) => match e {},
    }
}

/// Build the events (Delta) surface router against `DELTA_DATABASE_URL`, with Delta's OWN audit sink.
///
/// State is built EXPLICITLY (not via `delta::build_state_from_env`) so this single process holds
/// both surfaces under one set of binds WITHOUT the bare-`DATABASE_URL` fallback collision: connect +
/// migrate Delta's OWN database, start its OWN audit emitter exactly as Delta does (`AUDIT_ENABLED` +
/// `WATCHTOWER_URL` + `AUDIT_INGEST_TOKEN`), and assemble the `AppState` Delta's `app()` expects. The
/// config is `Config::from_env()`, so `DELTA_SERVICE_TOKEN` still guards the `/api/` surface.
async fn build_events() -> Result<Router, String> {
    let dsn = require_env("DELTA_DATABASE_URL")?;
    let pg = delta::store::PgStore::connect(&dsn)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    pg.migrate().await.map_err(|e| format!("migrate: {e}"))?;
    tracing::info!("events (delta) store ready");
    let audit = delta::audit::AuditSink::start(
        env_truthy("AUDIT_ENABLED"),
        &delta::config::env_nonempty("WATCHTOWER_URL").unwrap_or_default(),
        delta::config::env_nonempty("AUDIT_INGEST_TOKEN").as_deref(),
    );
    let state = delta::AppState {
        config: Arc::new(delta::config::Config::from_env()),
        store: Arc::new(pg),
        audit,
    };
    Ok(delta::app(state))
}

/// Build the jobs (Tempo) surface router against `TEMPO_DATABASE_URL`, with Tempo's OWN reqwest
/// client, audit sink and Klaxon notify config — and spawn its background scheduler loop.
///
/// Same explicit construction as [`build_events`], against Tempo's OWN database. `Config::from_env()`
/// resolves the Klaxon notify target from the `KLAXON_URL` / `KLAXON_INGEST_TOKEN` /
/// `KLAXON_NOTIFY_EMAIL` triple (notify degrades off if incomplete), so the cron-failure alert path
/// is preserved. The standalone Tempo spawned `scheduler::run` in its own `main`; the demux composes
/// only the HTTP router, so we spawn it here so `jobs.w33d.xyz` keeps firing due cron jobs and
/// flagging dead-man heartbeats.
async fn build_jobs() -> Result<Router, String> {
    let dsn = require_env("TEMPO_DATABASE_URL")?;
    let pg = tempo::store::PgStore::connect(&dsn)
        .await
        .map_err(|e| format!("connect: {e}"))?;
    pg.migrate().await.map_err(|e| format!("migrate: {e}"))?;
    tracing::info!("jobs (tempo) store ready");
    let audit = tempo::audit::AuditSink::start(
        env_truthy("AUDIT_ENABLED"),
        &tempo::config::env_nonempty("WATCHTOWER_URL").unwrap_or_default(),
        tempo::config::env_nonempty("AUDIT_INGEST_TOKEN").as_deref(),
    );
    let config = tempo::config::Config::from_env();
    match &config.klaxon {
        Some(k) => tracing::info!(url = %k.url, "tempo klaxon notify enabled"),
        None => tracing::info!(
            "tempo klaxon notify disabled (KLAXON_URL/_INGEST_TOKEN/_NOTIFY_EMAIL unset)"
        ),
    }
    let state = tempo::AppState {
        config: Arc::new(config),
        store: Arc::new(pg),
        http: build_tempo_http_client(),
        audit,
    };

    // Preserve Tempo's background scheduler (fires due cron jobs + flags dead-man heartbeats). The
    // standalone Tempo ran it alongside its HTTP server (`tokio::spawn(scheduler::run(state))`); spawn
    // it here so the jobs surface keeps its periodic work.
    tokio::spawn(tempo::scheduler::run(state.clone()));
    tracing::info!(
        tick_secs = state.config.tick_secs,
        "tempo scheduler started"
    );

    Ok(tempo::app(state))
}

/// Rebuild the exact reqwest client Tempo's private `build_http_client` constructs (rustls; bounded
/// timeouts so a slow target cannot hang a scheduler tick). Tempo's `AppState.http` requires a
/// `reqwest::Client` value, and the helper is private, so it is replicated here verbatim.
fn build_tempo_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .build()
        .expect("failed to build reqwest client")
}

/// Interpret a boolean-ish env var (`on` / `true` / `1` / `yes`, case-insensitive). Mirrors the
/// `env_truthy` each surface uses in its own `build_state_from_env`, so audit is gated identically.
fn env_truthy(key: &str) -> bool {
    matches!(
        std::env::var(key)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "on" | "true" | "1" | "yes"
    )
}

/// Read a required env var, returning a descriptive error when unset/empty.
fn require_env(key: &str) -> Result<String, String> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => Ok(v),
        _ => Err(format!("{key} is required")),
    }
}

/// Log a fatal startup error for one surface and exit.
fn fatal(surface: &str, err: String) -> ! {
    tracing::error!(surface, error = %err, "failed to build data surface");
    std::process::exit(1);
}

/// GET `/healthz` over a raw TCP socket on the loopback. Returns the process exit code. Probes the
/// primary (events) port, which always answers the host-agnostic `/healthz`.
fn run_healthcheck() -> i32 {
    let bind_addr = std::env::var("BIND_ADDR").unwrap_or_else(|_| DEFAULT_BIND_PRIMARY.to_string());
    let port = bind_addr.rsplit(':').next().unwrap_or("9150");
    let target = format!("127.0.0.1:{port}");
    match healthcheck_once(&target) {
        Ok(true) => 0,
        Ok(false) => {
            eprintln!("healthcheck: {target} did not return 200");
            1
        }
        Err(e) => {
            eprintln!("healthcheck: {target} error: {e}");
            1
        }
    }
}

fn healthcheck_once(target: &str) -> std::io::Result<bool> {
    let addr: SocketAddr = target
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, format!("{e}")))?;
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    stream.write_all(b"GET /healthz HTTP/1.0\r\nHost: localhost\r\nConnection: close\r\n\r\n")?;
    let mut buf = String::new();
    stream.read_to_string(&mut buf)?;
    Ok(buf.lines().next().unwrap_or("").contains("200"))
}
