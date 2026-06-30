//! Server configuration, env-driven with working dev defaults.
//!
//! Every value keeps its dev default when the corresponding env var is unset/empty, so the
//! in-memory dev path boots with NO configuration, NO database, NO audit, and NO Klaxon — exactly
//! like relay/loom/sanctum. Production overrides each via the environment. The audit ingest token
//! and the Klaxon ingest token are resolved in [`crate::build_state_from_env`], not stored here in
//! plaintext config that ends up in logs.

/// Default listen address (all interfaces, internal-only port 9160).
pub const DEFAULT_BIND_ADDR: &str = "0.0.0.0:9160";

/// Public base URL of this service (used to render the heartbeat ping URLs in the console).
pub const DEFAULT_PUBLIC_BASE_URL: &str = "https://jobs.w33d.xyz";

/// Default scheduler tick interval, seconds (`TEMPO_TICK_SECS`).
pub const DEFAULT_TICK_SECS: u64 = 30;

/// Hard cap on how many run-log rows the console renders.
pub const RUN_LIMIT: usize = 50;

/// Optional Klaxon notify target. Present only when `KLAXON_URL` + `KLAXON_INGEST_TOKEN` +
/// `KLAXON_NOTIFY_EMAIL` are ALL configured — otherwise notify silently degrades to off.
#[derive(Clone, Debug)]
pub struct Klaxon {
    /// Base URL of the Klaxon ingest service (e.g. `http://klaxon:9050`). `/api/notify` is appended.
    pub url: String,
    /// Bearer token presented to Klaxon's internal ingest API (`KLAXON_INGEST_TOKEN`).
    pub token: String,
    /// The recipient address every Tempo alert is delivered to (`KLAXON_NOTIFY_EMAIL`).
    pub notify_email: String,
}

/// Runtime configuration. Cheap to clone; shared read-only behind `Arc`.
#[derive(Clone, Debug)]
pub struct Config {
    /// Listen address (`BIND_ADDR`).
    pub bind_addr: String,
    /// Public base URL (`PUBLIC_BASE_URL`), trailing slash trimmed.
    pub public_base_url: String,
    /// Scheduler tick interval in seconds (`TEMPO_TICK_SECS`).
    pub tick_secs: u64,
    /// Configured Klaxon notify target, or `None` (notify disabled).
    pub klaxon: Option<Klaxon>,
}

impl Config {
    /// Default development configuration (in-memory, no database, no Klaxon).
    pub fn dev() -> Self {
        Config {
            bind_addr: DEFAULT_BIND_ADDR.to_string(),
            public_base_url: DEFAULT_PUBLIC_BASE_URL.to_string(),
            tick_secs: DEFAULT_TICK_SECS,
            klaxon: None,
        }
    }

    /// Configuration with the dev defaults overridden by environment variables.
    pub fn from_env() -> Self {
        let mut config = Config::dev();
        if let Some(v) = env_nonempty("BIND_ADDR") {
            config.bind_addr = v;
        }
        if let Some(v) = env_nonempty("PUBLIC_BASE_URL") {
            config.public_base_url = v.trim_end_matches('/').to_string();
        }
        if let Some(v) = env_nonempty("TEMPO_TICK_SECS").and_then(|v| v.parse::<u64>().ok()) {
            if v > 0 {
                config.tick_secs = v;
            }
        }
        // Klaxon notify is enabled only when the full triple is present; otherwise it degrades off.
        config.klaxon = match (
            env_nonempty("KLAXON_URL"),
            env_nonempty("KLAXON_INGEST_TOKEN"),
            env_nonempty("KLAXON_NOTIFY_EMAIL"),
        ) {
            (Some(url), Some(token), Some(notify_email)) => Some(Klaxon {
                url: url.trim_end_matches('/').to_string(),
                token,
                notify_email,
            }),
            _ => None,
        };
        config
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::dev()
    }
}

/// Read an env var, returning `None` when unset OR empty (empty never clobbers a default).
pub fn env_nonempty(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() => Some(v),
        _ => None,
    }
}

/// Interpret a boolean-ish env var (`on` / `true` / `1` / `yes`, case-insensitive).
pub fn env_truthy(key: &str) -> bool {
    matches!(
        std::env::var(key)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "on" | "true" | "1" | "yes"
    )
}
