//! The background scheduler loop: fire due cron jobs, and flag silent heartbeats.
//!
//! A single detached tokio task ([`run`]) wakes every `tick_secs` and calls [`tick`]. Each tick:
//!
//! - (a) For every enabled `cron` job whose [`crate::schedule`] is due, GET its `target_url`
//!   (reqwest + rustls, bounded timeout), record a `runs` row, and update the job's
//!   `last_run_at` / `last_status`. A non-2xx or a transport error is a FAILURE: it emits a
//!   non-blocking `tempo.job.fail` audit event and (when configured) POSTs a Klaxon notify.
//! - (b) For every enabled `heartbeat` job, compare its heartbeat's `last_beat_at + grace_secs` to
//!   now. A job that has beaten at least once but is now past its grace window is flagged DOWN
//!   (a `runs` row + `tempo.heartbeat.miss` audit + optional Klaxon); a previously-down job that
//!   has resumed beating is recorded as recovered (`up`).
//!
//! Watchtower and Klaxon being slow/down NEVER affects the request path: audit is the bounded
//! fire-and-forget sink, and the Klaxon POST happens here on the background task under the shared
//! reqwest client's timeout — never on the `/ping` hot path.

use std::time::Duration;

use serde_json::json;

use crate::audit::AuditEvent;
use crate::config::RETRY_BATCH_LIMIT;
use crate::model::{
    Heartbeat, Job, Retry, Run, DEFAULT_MAX_ATTEMPTS, DEFAULT_RETRY_DELAY_SECS, STATUS_DLQ,
    STATUS_DOWN, STATUS_FAIL, STATUS_OK, STATUS_UP,
};
use crate::{now_secs, random_alnum, schedule, AppState};

/// Run the scheduler forever. Spawned detached from `main`; never returns.
pub async fn run(state: AppState) {
    let mut ticker = tokio::time::interval(Duration::from_secs(state.config.tick_secs.max(1)));
    // If a tick runs long (a slow target), skip the missed beats rather than bursting.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tracing::info!(tick_secs = state.config.tick_secs, "scheduler loop started");
    loop {
        ticker.tick().await;
        tick(&state).await;
    }
}

/// One scheduler sweep. Public so integration tests can drive it deterministically.
pub async fn tick(state: &AppState) {
    let now = now_secs();
    process_due_retries(state, now).await;

    let jobs = state.store.list_jobs().await;
    let heartbeats = state.store.list_heartbeats().await;

    for job in &jobs {
        if !job.enabled {
            continue;
        }
        if job.is_heartbeat() {
            check_heartbeat(state, job, &heartbeats, now).await;
        } else if let Some(sched) = schedule::parse(&job.schedule) {
            if sched.is_due(job.last_run_at, now) {
                fire_cron(state, job, now).await;
            }
        }
    }
}

/// Process durable retry rows that are due. Retry rows are deleted before firing so a scheduler
/// crash never double-fires a completed retry; a failed attempt enqueues the next row if needed.
async fn process_due_retries(state: &AppState, now: i64) {
    let retries = state.store.due_retries(now, RETRY_BATCH_LIMIT).await;
    for retry in retries {
        let Some(job) = state.store.get_job(&retry.job_id).await else {
            if let Err(e) = state.store.delete_retry(&retry.id).await {
                tracing::warn!(retry = %retry.id, error = %e, "delete retry for missing job failed");
            }
            continue;
        };
        if job.is_heartbeat() || !job.enabled {
            if let Err(e) = state.store.delete_retry(&retry.id).await {
                tracing::warn!(retry = %retry.id, error = %e, "delete skipped retry failed");
            }
            continue;
        }
        match state.store.delete_retry(&retry.id).await {
            Ok(true) => {
                fire_cron_attempt(
                    state,
                    &job,
                    now,
                    retry.attempt.max(1),
                    retry.max_attempts.max(1),
                    &retry.source_run_id,
                )
                .await;
            }
            Ok(false) => tracing::debug!(retry = %retry.id, "retry row already gone"),
            Err(e) => tracing::warn!(retry = %retry.id, error = %e, "delete retry failed"),
        }
    }
}

/// Fire one cron job: GET its target, record the run, update last_run/last_status, and on failure
/// either enqueue a retry or move the run into the DLQ.
async fn fire_cron(state: &AppState, job: &Job, now: i64) {
    fire_cron_attempt(state, job, now, 1, DEFAULT_MAX_ATTEMPTS, "").await;
}

async fn fire_cron_attempt(
    state: &AppState,
    job: &Job,
    now: i64,
    attempt: i64,
    max_attempts: i64,
    source_run_id: &str,
) {
    let (status, detail) = match state.http.get(&job.target_url).send().await {
        Ok(resp) => {
            let code = resp.status();
            let detail = format!("GET {} -> HTTP {}", job.target_url, code.as_u16());
            if code.is_success() {
                (STATUS_OK, detail)
            } else {
                (STATUS_FAIL, detail)
            }
        }
        Err(e) => (
            STATUS_FAIL,
            format!("GET {} -> error: {}", job.target_url, e),
        ),
    };

    if status == STATUS_OK {
        let detail = attempt_detail(&detail, attempt, max_attempts);
        record_run(state, &job.id, now, STATUS_OK, &detail).await;
        if let Err(e) = state.store.touch_job(&job.id, now, STATUS_OK).await {
            tracing::warn!(job = %job.id, error = %e, "touch_job failed after fire");
        }
        tracing::info!(job = %job.id, attempt, "cron job fired ok");
        return;
    }

    let exhausted = attempt >= max_attempts.max(1);
    let run_status = if exhausted { STATUS_DLQ } else { STATUS_FAIL };
    let detail = if exhausted {
        format!(
            "{}; attempt {}/{} exhausted; moved to DLQ",
            detail,
            attempt,
            max_attempts.max(1)
        )
    } else {
        format!(
            "{}; attempt {}/{} failed; retry queued in {}s",
            detail,
            attempt,
            max_attempts.max(1),
            DEFAULT_RETRY_DELAY_SECS
        )
    };
    let run_id = record_run(state, &job.id, now, run_status, &detail).await;
    if let Err(e) = state.store.touch_job(&job.id, now, run_status).await {
        tracing::warn!(job = %job.id, error = %e, "touch_job failed after fire");
    }

    if exhausted {
        tracing::warn!(job = %job.id, detail = %detail, "cron job moved to DLQ");
        state.audit.emit(AuditEvent::warning(
            "tempo.job.dlq",
            "scheduler",
            &job.name,
            &detail,
        ));
        klaxon_notify(
            state,
            &format!("Cron job dead-lettered: {}", job.name),
            &detail,
        )
        .await;
    } else {
        let source = if source_run_id.is_empty() {
            run_id.clone()
        } else {
            source_run_id.to_string()
        };
        let retry = Retry {
            id: format!("retry_{}", random_alnum(20)),
            job_id: job.id.clone(),
            source_run_id: source,
            attempt: attempt + 1,
            max_attempts: max_attempts.max(1),
            due_at: now + DEFAULT_RETRY_DELAY_SECS,
            created_at: now,
        };
        if let Err(e) = state.store.enqueue_retry(&retry).await {
            tracing::warn!(job = %job.id, error = %e, "enqueue_retry failed");
        }
        tracing::warn!(job = %job.id, retry = %retry.id, detail = %detail, "cron job retry queued");
        state.audit.emit(AuditEvent::warning(
            "tempo.job.retry",
            "scheduler",
            &job.name,
            &detail,
        ));
    }
}

/// Evaluate one heartbeat job's liveness and record a transition when it crosses the grace window.
async fn check_heartbeat(state: &AppState, job: &Job, heartbeats: &[Heartbeat], now: i64) {
    let Some(hb) = heartbeats.iter().find(|h| h.job_id == job.id) else {
        return;
    };
    // No baseline yet (never beaten) — nothing to declare down.
    if hb.last_beat_at == 0 {
        return;
    }
    let alive = now <= hb.last_beat_at + job.grace_secs;

    if !alive && job.last_status != STATUS_DOWN {
        let silent_for = now.saturating_sub(hb.last_beat_at);
        let detail = format!("no heartbeat for {silent_for}s (grace {}s)", job.grace_secs);
        record_run(state, &job.id, now, STATUS_DOWN, &detail).await;
        if let Err(e) = state.store.touch_job(&job.id, now, STATUS_DOWN).await {
            tracing::warn!(job = %job.id, error = %e, "touch_job failed on heartbeat miss");
        }
        tracing::warn!(job = %job.id, detail = %detail, "heartbeat missed");
        state.audit.emit(AuditEvent::warning(
            "tempo.heartbeat.miss",
            "scheduler",
            &job.name,
            &detail,
        ));
        klaxon_notify(state, &format!("Heartbeat missed: {}", job.name), &detail).await;
    } else if alive && job.last_status == STATUS_DOWN {
        let detail = "heartbeat resumed".to_string();
        record_run(state, &job.id, now, STATUS_UP, &detail).await;
        if let Err(e) = state.store.touch_job(&job.id, now, STATUS_UP).await {
            tracing::warn!(job = %job.id, error = %e, "touch_job failed on heartbeat recover");
        }
        tracing::info!(job = %job.id, "heartbeat recovered");
        state.audit.emit(AuditEvent::info(
            "tempo.heartbeat.recover",
            "scheduler",
            &job.name,
            &detail,
        ));
    }
}

/// Append a run row (best-effort: a failed insert only warns — it never aborts the sweep).
async fn record_run(
    state: &AppState,
    job_id: &str,
    started_at: i64,
    status: &str,
    detail: &str,
) -> String {
    let id = format!("run_{}", random_alnum(20));
    let run = Run {
        id: id.clone(),
        job_id: job_id.to_string(),
        started_at,
        status: status.to_string(),
        detail: detail.to_string(),
    };
    if let Err(e) = state.store.insert_run(&run).await {
        tracing::warn!(job = %job_id, error = %e, "insert_run failed");
    }
    id
}

fn attempt_detail(detail: &str, attempt: i64, max_attempts: i64) -> String {
    if max_attempts <= 1 {
        detail.to_string()
    } else {
        format!("{detail}; attempt {attempt}/{max_attempts}")
    }
}

/// Best-effort Klaxon notify. No-op when Klaxon is not configured; a slow/down Klaxon is bounded by
/// the shared reqwest client timeout and never propagates an error.
async fn klaxon_notify(state: &AppState, title: &str, body: &str) {
    let Some(k) = &state.config.klaxon else {
        return;
    };
    let url = format!("{}/api/notify", k.url);
    let payload = json!({
        "user_email": k.notify_email,
        "source": "tempo",
        "title": title,
        "body": body,
        "url": state.config.public_base_url,
    });
    match state
        .http
        .post(&url)
        .bearer_auth(&k.token)
        .json(&payload)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            tracing::debug!("klaxon notify delivered")
        }
        Ok(resp) => tracing::warn!(status = %resp.status(), "klaxon notify rejected"),
        Err(e) => tracing::warn!(error = %e, "klaxon notify failed"),
    }
}
