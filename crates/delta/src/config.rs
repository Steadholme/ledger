//! Server configuration, env-driven with working dev defaults.
//!
//! Every value keeps its dev default when the corresponding env var is unset/empty, so the
//! in-memory dev path boots with NO configuration and NO database — exactly like
//! relay/loom/sanctum. Production overrides each via the environment.

/// Default listen address (all interfaces, internal-only port 9150).
pub const DEFAULT_BIND_ADDR: &str = "0.0.0.0:9150";

/// Dev/default service bearer token for the `/api/` surface. Producers/consumers present
/// `Authorization: Bearer <token>`; production overrides via `DELTA_SERVICE_TOKEN`. A dev default
/// keeps the service fully usable with zero configuration.
pub const DEFAULT_SERVICE_TOKEN: &str = "dev-delta-token";

/// Default upper bound on how many events a single durable read returns (a client `limit` is
/// clamped to this). Keeps an unbounded poll bounded.
pub const DEFAULT_READ_LIMIT: i64 = 100;
/// Hard ceiling on the read `limit`, regardless of what a client asks for.
pub const MAX_READ_LIMIT: i64 = 1000;

/// How many latest events the console renders per stream (the per-stream "tail").
pub const TAIL_LIMIT: i64 = 8;
/// How many streams the console lists.
pub const STREAM_LIST_LIMIT: usize = 200;

/// Default append-audit sampling: emit one `delta.stream.append` event every Nth append (so a
/// firehose stream does not flood Watchtower). `1` audits every append; `0` is treated as `1`.
pub const DEFAULT_AUDIT_SAMPLE: i64 = 64;

/// Runtime configuration. Cheap to clone; shared read-only behind `Arc`.
#[derive(Clone, Debug)]
pub struct Config {
    /// Listen address (`BIND_ADDR`).
    pub bind_addr: String,
    /// Bearer token guarding the `/api/` producer/consumer surface (`DELTA_SERVICE_TOKEN`).
    pub service_token: String,
    /// Default read page size (`DELTA_READ_LIMIT`).
    pub read_limit: i64,
    /// Append-audit sampling factor (`DELTA_AUDIT_SAMPLE`).
    pub audit_sample: i64,
}

impl Config {
    /// Default development configuration (in-memory friendly, no database).
    pub fn dev() -> Self {
        Config {
            bind_addr: DEFAULT_BIND_ADDR.to_string(),
            service_token: DEFAULT_SERVICE_TOKEN.to_string(),
            read_limit: DEFAULT_READ_LIMIT,
            audit_sample: DEFAULT_AUDIT_SAMPLE,
        }
    }

    /// Configuration with the dev defaults overridden by environment variables.
    pub fn from_env() -> Self {
        let mut config = Config::dev();
        if let Some(v) = env_nonempty("BIND_ADDR") {
            config.bind_addr = v;
        }
        if let Some(v) = env_nonempty("DELTA_SERVICE_TOKEN") {
            config.service_token = v;
        }
        if let Some(v) = env_nonempty("DELTA_READ_LIMIT").and_then(|v| v.parse::<i64>().ok()) {
            if v > 0 {
                config.read_limit = v.min(MAX_READ_LIMIT);
            }
        }
        if let Some(v) = env_nonempty("DELTA_AUDIT_SAMPLE").and_then(|v| v.parse::<i64>().ok()) {
            if v >= 0 {
                config.audit_sample = v;
            }
        }
        config
    }

    /// Effective sampling divisor (never zero, so `seq % divisor` is always defined).
    pub fn audit_divisor(&self) -> i64 {
        if self.audit_sample <= 0 {
            1
        } else {
            self.audit_sample
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dev_defaults_are_usable() {
        let c = Config::dev();
        assert_eq!(c.bind_addr, DEFAULT_BIND_ADDR);
        assert_eq!(c.service_token, DEFAULT_SERVICE_TOKEN);
        assert_eq!(c.read_limit, DEFAULT_READ_LIMIT);
        assert_eq!(c.audit_divisor(), DEFAULT_AUDIT_SAMPLE);
    }

    #[test]
    fn audit_divisor_never_zero() {
        let mut c = Config::dev();
        c.audit_sample = 0;
        assert_eq!(c.audit_divisor(), 1);
        c.audit_sample = -5;
        assert_eq!(c.audit_divisor(), 1);
        c.audit_sample = 10;
        assert_eq!(c.audit_divisor(), 10);
    }
}
