//! Domain rows: scheduled jobs, run-log entries, dead-man heartbeats.
//!
//! Each struct maps 1:1 to a table row (see [`crate::store`]). Field types are deliberately the
//! portable set the PostgreSQL layer uses (`String` -> TEXT, `i64` -> BIGINT, `bool` -> BOOLEAN), so
//! the in-memory and Postgres stores share one shape and the SQL stays FusionDB-portable.

/// Job kind: a scheduled outbound cron ping/webhook.
pub const KIND_CRON: &str = "cron";
/// Job kind: a Healthchecks-style dead-man heartbeat (an external cron curls a ping URL).
pub const KIND_HEARTBEAT: &str = "heartbeat";

/// Run status: the fired cron target returned a 2xx.
pub const STATUS_OK: &str = "ok";
/// Run status: the fired cron target failed (non-2xx, or the request errored).
pub const STATUS_FAIL: &str = "fail";
/// Run status / job status: a heartbeat went silent past its grace window.
pub const STATUS_DOWN: &str = "down";
/// Run status / job status: a previously-down heartbeat started beating again.
pub const STATUS_UP: &str = "up";
/// Run status: all retry attempts for a cron fire were exhausted; the run is in the DLQ.
pub const STATUS_DLQ: &str = "dlq";

/// Default number of attempts for a cron fire before it is dead-lettered.
pub const DEFAULT_MAX_ATTEMPTS: i64 = 3;
/// Default delay before a failed cron fire is retried by the scheduler.
pub const DEFAULT_RETRY_DELAY_SECS: i64 = 30;

/// A scheduled job (maps 1:1 to a `jobs` row). For `kind = cron` the `schedule` + `target_url`
/// drive the firing; for `kind = heartbeat` the `grace_secs` window drives the dead-man check and a
/// separate [`Heartbeat`] row holds the capability token an external job curls.
#[derive(Clone, Debug)]
pub struct Job {
    pub id: String,
    pub name: String,
    /// [`KIND_CRON`] or [`KIND_HEARTBEAT`].
    pub kind: String,
    /// Schedule spec (cron jobs only): `@every Ns/Nm/Nh` or `HH:MM` daily. See [`crate::schedule`].
    pub schedule: String,
    /// Target URL fired on each cron tick (cron jobs only).
    pub target_url: String,
    /// Dead-man grace window in seconds (heartbeats); also the per-run leniency for display.
    pub grace_secs: i64,
    /// Disabled jobs are skipped by the scheduler.
    pub enabled: bool,
    /// Epoch seconds of the last fire / last dead-man transition (`0` = never).
    pub last_run_at: i64,
    /// Last outcome label ([`STATUS_OK`] / [`STATUS_FAIL`] / [`STATUS_DOWN`] / [`STATUS_UP`] / "").
    pub last_status: String,
    /// Epoch seconds the job was created.
    pub created_at: i64,
}

impl Job {
    /// True when this is a dead-man heartbeat job (vs. an outbound cron job).
    pub fn is_heartbeat(&self) -> bool {
        self.kind == KIND_HEARTBEAT
    }
}

/// One recorded run (maps 1:1 to a `runs` row): a cron fire outcome, or a heartbeat
/// down/recovered transition.
#[derive(Clone, Debug)]
pub struct Run {
    pub id: String,
    pub job_id: String,
    pub started_at: i64,
    pub status: String,
    pub detail: String,
}

/// One queued retry for a failed cron run. The row is durable so a restart does not lose retry work.
#[derive(Clone, Debug)]
pub struct Retry {
    pub id: String,
    pub job_id: String,
    pub source_run_id: String,
    pub attempt: i64,
    pub max_attempts: i64,
    pub due_at: i64,
    pub created_at: i64,
}

/// One page of run history for the console.
#[derive(Clone, Debug)]
pub struct RunPage {
    pub runs: Vec<Run>,
    pub total: i64,
}

/// A dead-man heartbeat (maps 1:1 to a `heartbeats` row). `token` is the capability secret embedded
/// in the public `/ping/{token}` URL the external cron curls; `last_beat_at` advances on each beat.
#[derive(Clone, Debug)]
pub struct Heartbeat {
    pub token: String,
    pub job_id: String,
    pub last_beat_at: i64,
}
