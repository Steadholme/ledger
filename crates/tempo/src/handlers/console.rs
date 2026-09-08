//! The SSO web console (`jobs.w33d.xyz/`): schedule jobs, watch runs + heartbeat liveness.
//!
//! Mounted behind the gateway `auth=sso` route — the identity is taken from the injected
//! `X-Auth-Subject` / `X-Auth-Email` (Tempo trusts these; it is internal-only). Jobs are a single
//! shared estate-wide schedule (no per-user ownership in the data model). Every POST is double-submit
//! CSRF protected; all producer-supplied text is HTML-escaped on render.

use crate::handlers::theme_of;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::Form;
use serde::Deserialize;

use crate::audit::AuditEvent;
use crate::auth::{self, Identity};
use crate::config::RUN_PAGE_SIZE;
use crate::error::AppError;
use crate::handlers::{esc, fmt_ago, fmt_ts, fmt_until, html_with_csrf, page, redirect, short};
use crate::model::{
    Heartbeat, Job, Retry, RunPage, DEFAULT_MAX_ATTEMPTS, KIND_CRON, KIND_HEARTBEAT, STATUS_DLQ,
    STATUS_DOWN, STATUS_FAIL, STATUS_OK, STATUS_UP,
};
use crate::{now_secs, random_alnum, schedule, AppState};

const JOB_ID_LEN: usize = 16;
const HEARTBEAT_TOKEN_LEN: usize = 32;
const DEFAULT_GRACE_SECS: i64 = 300;

// ===========================================================================
// GET / — console dashboard (optionally pre-filling the form for ?edit=<id>)
// ===========================================================================

#[derive(Debug, Deserialize)]
pub struct IndexQuery {
    #[serde(default)]
    pub edit: Option<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub page: Option<i64>,
}

pub async fn index(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<IndexQuery>,
) -> Response {
    let who = auth::identity(&headers);
    let csrf = auth::new_csrf_token();

    // Resolve the job to edit (if the ?edit=<id> target still exists).
    let edit_job = match q.edit.as_deref() {
        Some(id) if !id.is_empty() => state.store.get_job(id).await,
        _ => None,
    };

    let run_status = normalize_run_status(q.status.as_deref());
    let run_page = q.page.unwrap_or(1).max(1);
    let body = build_dashboard(
        &state,
        &csrf,
        edit_job.as_ref(),
        None,
        &run_status,
        run_page,
    )
    .await;
    html_with_csrf(
        StatusCode::OK,
        page("Jobs", "/", theme_of(&headers), Some(&who.email), &body),
        &csrf,
    )
}

// ===========================================================================
// POST /api/jobs — create or edit a job
// ===========================================================================

#[derive(Debug, Deserialize)]
pub struct JobForm {
    #[serde(default)]
    pub csrf_token: String,
    /// Empty -> create; present -> edit that job id.
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub schedule: String,
    #[serde(default)]
    pub target_url: String,
    #[serde(default)]
    pub grace_secs: String,
    /// Checkbox: present (`"on"`) when ticked.
    #[serde(default)]
    pub enabled: Option<String>,
}

pub async fn save_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    Form(form): Form<JobForm>,
) -> Result<Response, AppError> {
    if !auth::verify_csrf(&headers, &form.csrf_token) {
        return Err(AppError::BadRequest(
            "Your session token expired. Reload the page and submit again.".to_string(),
        ));
    }
    let who = auth::identity(&headers);

    let name = form.name.trim();
    if name.is_empty() {
        return Err(AppError::BadRequest("Job name is required.".to_string()));
    }
    let enabled = form.enabled.is_some();
    let grace_secs = form
        .grace_secs
        .trim()
        .parse::<i64>()
        .ok()
        .filter(|n| *n >= 0)
        .unwrap_or(DEFAULT_GRACE_SECS);

    if form.id.trim().is_empty() {
        create_job(&state, &who, name, &form, enabled, grace_secs).await
    } else {
        edit_job(
            &state,
            &who,
            form.id.trim(),
            name,
            &form,
            enabled,
            grace_secs,
        )
        .await
    }
}

/// Create a brand-new job (and, for heartbeats, its capability token row).
async fn create_job(
    state: &AppState,
    who: &Identity,
    name: &str,
    form: &JobForm,
    enabled: bool,
    grace_secs: i64,
) -> Result<Response, AppError> {
    let kind = normalize_kind(&form.kind);
    let (schedule_val, target_url) = validate_cron_fields(&kind, form)?;

    let id = format!("job_{}", random_alnum(JOB_ID_LEN));
    let now = now_secs();
    let job = Job {
        id: id.clone(),
        name: name.chars().take(120).collect(),
        kind: kind.clone(),
        schedule: schedule_val,
        target_url,
        grace_secs,
        enabled,
        last_run_at: 0,
        last_status: String::new(),
        created_at: now,
    };
    state.store.create_job(&job).await?;

    if kind == KIND_HEARTBEAT {
        let hb = Heartbeat {
            token: random_alnum(HEARTBEAT_TOKEN_LEN),
            job_id: id.clone(),
            last_beat_at: 0,
        };
        state.store.create_heartbeat(&hb).await?;
    }

    tracing::info!(job = %id, kind = %kind, "job created");
    state.audit.emit(AuditEvent::notice(
        "tempo.job.create",
        &who.email,
        &job.name,
        &format!("kind={kind}"),
    ));
    Ok(redirect("/"))
}

/// Edit an existing job's mutable fields. The `kind` is fixed at creation (so heartbeat tokens stay
/// stable); only name/schedule/target_url/grace/enabled change.
async fn edit_job(
    state: &AppState,
    who: &Identity,
    id: &str,
    name: &str,
    form: &JobForm,
    enabled: bool,
    grace_secs: i64,
) -> Result<Response, AppError> {
    let existing = state
        .store
        .get_job(id)
        .await
        .ok_or_else(|| AppError::NotFound("no such job".to_string()))?;

    let (schedule_val, target_url) = validate_cron_fields(&existing.kind, form)?;
    let updated = Job {
        name: name.chars().take(120).collect(),
        schedule: schedule_val,
        target_url,
        grace_secs,
        enabled,
        ..existing.clone()
    };
    state.store.update_job(&updated).await?;

    tracing::info!(job = %id, "job updated");
    state.audit.emit(AuditEvent::notice(
        "tempo.job.update",
        &who.email,
        &updated.name,
        "job edited",
    ));
    Ok(redirect("/"))
}

/// Validate + normalize the cron-specific fields for the given `kind`. For a cron job the schedule
/// must parse and the target URL must be an http(s) URL; for a heartbeat both are ignored (the
/// schedule is stored empty and the target left as supplied/blank).
fn validate_cron_fields(kind: &str, form: &JobForm) -> Result<(String, String), AppError> {
    if kind == KIND_HEARTBEAT {
        return Ok((String::new(), form.target_url.trim().to_string()));
    }
    let schedule_val = form.schedule.trim().to_string();
    if schedule::parse(&schedule_val).is_none() {
        return Err(AppError::BadRequest(
            "Unrecognized schedule. Use \"@every 30s\" / \"@every 5m\" / \"@every 2h\" or \"HH:MM\" (daily, UTC)."
                .to_string(),
        ));
    }
    let target_url = form.target_url.trim().to_string();
    if !is_http_url(&target_url) {
        return Err(AppError::BadRequest(
            "Target URL must be an http:// or https:// address.".to_string(),
        ));
    }
    Ok((schedule_val, target_url))
}

/// Normalize the submitted kind to one of the two known kinds (defaulting to cron).
fn normalize_kind(raw: &str) -> String {
    if raw.trim() == KIND_HEARTBEAT {
        KIND_HEARTBEAT.to_string()
    } else {
        KIND_CRON.to_string()
    }
}

fn normalize_run_status(raw: Option<&str>) -> String {
    match raw.unwrap_or_default().trim() {
        STATUS_OK => STATUS_OK.to_string(),
        STATUS_FAIL => STATUS_FAIL.to_string(),
        STATUS_DLQ => STATUS_DLQ.to_string(),
        STATUS_DOWN => STATUS_DOWN.to_string(),
        STATUS_UP => STATUS_UP.to_string(),
        _ => String::new(),
    }
}

/// Scheme-allowlist for fired targets (no `javascript:`/`file:`/relative).
fn is_http_url(url: &str) -> bool {
    let u = url.to_ascii_lowercase();
    u.starts_with("http://") || u.starts_with("https://")
}

// ===========================================================================
// POST /api/jobs/{id}/toggle — enable/disable a job
// ===========================================================================

#[derive(Debug, Deserialize)]
pub struct ToggleForm {
    #[serde(default)]
    pub csrf_token: String,
}

pub async fn toggle_job(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<ToggleForm>,
) -> Result<Response, AppError> {
    if !auth::verify_csrf(&headers, &form.csrf_token) {
        return Err(AppError::BadRequest(
            "Your session token expired. Reload the page and try again.".to_string(),
        ));
    }
    let who = auth::identity(&headers);
    let job = state
        .store
        .get_job(&id)
        .await
        .ok_or_else(|| AppError::NotFound("no such job".to_string()))?;

    let now_enabled = !job.enabled;
    state.store.set_enabled(&id, now_enabled).await?;
    state.audit.emit(AuditEvent::notice(
        "tempo.job.toggle",
        &who.email,
        &job.name,
        if now_enabled { "enabled" } else { "disabled" },
    ));
    Ok(redirect("/"))
}

// ===========================================================================
// POST /api/runs/{id}/replay — manually replay one DLQ run
// ===========================================================================

#[derive(Debug, Deserialize)]
pub struct ReplayForm {
    #[serde(default)]
    pub csrf_token: String,
}

pub async fn replay_run(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Form(form): Form<ReplayForm>,
) -> Result<Response, AppError> {
    if !auth::verify_csrf(&headers, &form.csrf_token) {
        return Err(AppError::BadRequest(
            "Your session token expired. Reload the page and try again.".to_string(),
        ));
    }
    let who = auth::identity(&headers);
    let run = state
        .store
        .get_run(&id)
        .await
        .ok_or_else(|| AppError::NotFound("no such run".to_string()))?;
    if run.status != STATUS_DLQ {
        return Err(AppError::BadRequest(
            "Only dead-lettered runs can be replayed.".to_string(),
        ));
    }
    let job = state
        .store
        .get_job(&run.job_id)
        .await
        .ok_or_else(|| AppError::NotFound("no such job".to_string()))?;
    if job.is_heartbeat() || !job.enabled {
        return Err(AppError::BadRequest(
            "Enable the cron job before replaying its DLQ run.".to_string(),
        ));
    }

    let now = now_secs();
    let retry = Retry {
        id: format!("retry_{}", random_alnum(20)),
        job_id: job.id.clone(),
        source_run_id: run.id.clone(),
        attempt: 1,
        max_attempts: DEFAULT_MAX_ATTEMPTS,
        due_at: now,
        created_at: now,
    };
    state.store.enqueue_retry(&retry).await?;
    state.audit.emit(AuditEvent::notice(
        "tempo.run.replay",
        &who.email,
        &job.name,
        &format!("run={}", run.id),
    ));
    Ok(redirect("/?status=dlq"))
}

// ===========================================================================
// Rendering
// ===========================================================================

async fn build_dashboard(
    state: &AppState,
    csrf: &str,
    edit_job: Option<&Job>,
    _banner: Option<&str>,
    run_status: &str,
    run_page_num: i64,
) -> String {
    let now = now_secs();
    let jobs = state.store.list_jobs().await;
    let runs = state
        .store
        .list_runs(
            run_status,
            RUN_PAGE_SIZE,
            (run_page_num - 1).saturating_mul(RUN_PAGE_SIZE),
        )
        .await;
    let heartbeats = state.store.list_heartbeats().await;

    let stat_grid = render_stats(&jobs, now);
    let job_list = render_jobs(&jobs, &heartbeats, &state.config.public_base_url, csrf, now);
    let run_table = render_runs(&runs, now, csrf, run_status, run_page_num);
    let form = render_form(csrf, edit_job);

    format!(
        r##"<div class="console__head">
  <h1>Jobs</h1>
  <p class="sub">Scheduler sweep every {tick}s</p>
</div>
{stat_grid}
<div class="layout">
  <section class="card">
    <div class="card__head"><h2>Run history</h2>{run_filter}</div>
    <div class="card__body--list">{run_table}</div>
  </section>
  <div>
    <section class="card">
      <div class="card__head"><h2>Jobs</h2></div>
      <div class="card__body">
        <ul class="job-list">{job_list}</ul>
      </div>
    </section>
    {form}
  </div>
</div>"##,
        tick = state.config.tick_secs,
        stat_grid = stat_grid,
        run_filter = render_run_filter(run_status),
        run_table = run_table,
        job_list = job_list,
        form = form,
    )
}

fn render_stats(jobs: &[Job], _now: i64) -> String {
    let total = jobs.len();
    let enabled = jobs.iter().filter(|j| j.enabled).count();
    let heartbeats = jobs.iter().filter(|j| j.is_heartbeat()).count();
    let down = jobs
        .iter()
        .filter(|j| j.last_status == STATUS_DOWN || j.last_status == STATUS_FAIL)
        .count();
    let (down_class, _) = if down > 0 {
        ("stat__val--warn", "")
    } else {
        ("stat__val--ok", "")
    };
    format!(
        r##"<div class="stat-grid">
  <div class="stat"><div class="stat__num">{total}</div><div class="stat__label">Jobs</div></div>
  <div class="stat"><div class="stat__num">{enabled}</div><div class="stat__label">Enabled</div></div>
  <div class="stat"><div class="stat__num">{heartbeats}</div><div class="stat__label">Heartbeats</div></div>
  <div class="stat"><div class="stat__num {down_class}">{down}</div><div class="stat__label">Alerting</div></div>
</div>"##,
    )
}

fn render_jobs(
    jobs: &[Job],
    heartbeats: &[Heartbeat],
    base_url: &str,
    csrf: &str,
    now: i64,
) -> String {
    if jobs.is_empty() {
        return "<li class=\"job-item job-item--empty\">No jobs yet. Add a cron ping or a dead-man heartbeat below.</li>".to_string();
    }
    jobs.iter()
        .map(|j| render_job_row(j, heartbeats, base_url, csrf, now))
        .collect::<Vec<_>>()
        .join("")
}

fn render_job_row(
    job: &Job,
    heartbeats: &[Heartbeat],
    base_url: &str,
    csrf: &str,
    now: i64,
) -> String {
    let enabled_badge = if job.enabled {
        "<span class=\"state-badge state-badge--open\">enabled</span>"
    } else {
        "<span class=\"state-badge state-badge--closed\">disabled</span>"
    };
    let kind_badge = if job.is_heartbeat() {
        "<span class=\"kind-badge kind-badge--hb\">heartbeat</span>"
    } else {
        "<span class=\"kind-badge kind-badge--cron\">cron</span>"
    };

    // The detail line differs by kind.
    let detail = if job.is_heartbeat() {
        let hb = heartbeats.iter().find(|h| h.job_id == job.id);
        let (live_badge, beat_line) = match hb {
            Some(h) if h.last_beat_at == 0 => (
                "<span class=\"status-badge status-badge--wait\">waiting</span>".to_string(),
                "no beats yet".to_string(),
            ),
            Some(h) => {
                let alive = now <= h.last_beat_at + job.grace_secs;
                let badge = if alive {
                    "<span class=\"status-badge status-badge--ok\">live</span>"
                } else {
                    "<span class=\"status-badge status-badge--down\">overdue</span>"
                };
                (
                    badge.to_string(),
                    format!("last beat {}", fmt_ago(h.last_beat_at, now)),
                )
            }
            None => (
                "<span class=\"status-badge status-badge--wait\">waiting</span>".to_string(),
                "no beats yet".to_string(),
            ),
        };
        let ping_url = match hb {
            Some(h) => format!("{}/ping/{}", base_url.trim_end_matches('/'), h.token),
            None => "(no token)".to_string(),
        };
        format!(
            r##"<div class="job-item__line">{live_badge} <span class="muted">grace {grace}s · {beat_line}</span></div>
  <div class="job-item__url">curl <code>{ping_url}</code></div>"##,
            grace = job.grace_secs,
            beat_line = esc(&beat_line),
            ping_url = esc(&ping_url),
        )
    } else {
        let (sched, next_due, next_ts) = match schedule::parse(&job.schedule) {
            Some(s) => {
                let due = s.next_due_at(job.last_run_at, now);
                (s.describe(), fmt_until(due, now), fmt_ts(due))
            }
            None => (
                format!("unparsed: {}", job.schedule),
                "unknown".to_string(),
                String::new(),
            ),
        };
        format!(
            r##"<div class="job-item__line">{status} <span class="muted">{sched} · next run <span title="{next_ts}">{next_due}</span> · last run {last}</span></div>
  <div class="job-item__url">GET <code>{target}</code></div>"##,
            status = status_badge(&job.last_status),
            sched = esc(&sched),
            next_due = esc(&next_due),
            next_ts = esc(&next_ts),
            last = esc(&fmt_ago(job.last_run_at, now)),
            target = esc(&job.target_url),
        )
    };

    let toggle_label = if job.enabled { "Disable" } else { "Enable" };
    format!(
        r##"<li class="job-item">
  <div class="job-item__head">
    <span class="job-item__name">{name}</span>
    {kind_badge}
    {enabled_badge}
  </div>
  {detail}
  <div class="job-item__actions">
    <a class="btn btn-ghost btn-sm" href="/?edit={id}">Edit</a>
    <form class="inline-form" method="post" action="/api/jobs/{id}/toggle">
      <input type="hidden" name="csrf_token" value="{csrf}">
      <button class="btn btn-ghost btn-sm" type="submit">{toggle_label}</button>
    </form>
  </div>
</li>"##,
        name = esc(&job.name),
        kind_badge = kind_badge,
        enabled_badge = enabled_badge,
        detail = detail,
        id = esc(&job.id),
        csrf = esc(csrf),
        toggle_label = toggle_label,
    )
}

/// A status pill for a job/run status label.
fn status_badge(status: &str) -> String {
    let (class, label) = match status {
        STATUS_OK | STATUS_UP => ("status-badge--ok", status),
        STATUS_FAIL | STATUS_DOWN | STATUS_DLQ => ("status-badge--down", status),
        "" => ("status-badge--wait", "pending"),
        other => ("status-badge--wait", other),
    };
    format!("<span class=\"status-badge {class}\">{}</span>", esc(label))
}

fn render_run_filter(status: &str) -> String {
    let options = [
        ("", "all"),
        (STATUS_OK, STATUS_OK),
        (STATUS_FAIL, STATUS_FAIL),
        (STATUS_DLQ, STATUS_DLQ),
        (STATUS_DOWN, STATUS_DOWN),
        (STATUS_UP, STATUS_UP),
    ]
    .iter()
    .map(|(value, label)| {
        let selected = if *value == status { "selected" } else { "" };
        format!(
            "<option value=\"{}\" {selected}>{}</option>",
            esc(value),
            esc(label)
        )
    })
    .collect::<Vec<_>>()
    .join("");
    format!(
        r##"<form class="inline-form run-filter" method="get" action="/">
  <select name="status" aria-label="Run status">{options}</select>
  <button class="btn btn-ghost btn-sm" type="submit">Filter</button>
</form>"##,
    )
}

fn render_runs(page: &RunPage, now: i64, csrf: &str, status: &str, page_num: i64) -> String {
    if page.runs.is_empty() {
        return "<div class=\"log-empty\">No runs yet. Fired cron jobs and heartbeat transitions appear here.</div>".to_string();
    }
    let rows = page
        .runs
        .iter()
        .map(|r| {
            let action = if r.status == STATUS_DLQ {
                format!(
                    r##"<form class="inline-form" method="post" action="/api/runs/{id}/replay">
  <input type="hidden" name="csrf_token" value="{csrf}">
  <button class="btn btn-danger btn-sm" type="submit">Replay</button>
</form>"##,
                    id = esc(&r.id),
                    csrf = esc(csrf),
                )
            } else {
                String::new()
            };
            format!(
                r##"<tr>
  <td class="log__when" title="{ts}">{ago}</td>
  <td>{badge}</td>
  <td class="log__detail">{detail}</td>
  <td class="log__hash"><code>{job}</code></td>
  <td>{action}</td>
</tr>"##,
                ts = esc(&fmt_ts(r.started_at)),
                ago = esc(&fmt_ago(r.started_at, now)),
                badge = status_badge(&r.status),
                detail = esc(&r.detail),
                job = esc(&short(&r.job_id, 16)),
                action = action,
            )
        })
        .collect::<Vec<_>>()
        .join("");
    let pager = render_run_pager(status, page_num, page.total);
    format!(
        r##"<table class="log-table">
  <thead><tr><th>When</th><th>Status</th><th>Detail</th><th>Job</th><th>Action</th></tr></thead>
  <tbody>{rows}</tbody>
</table>{pager}"##,
    )
}

fn render_run_pager(status: &str, page_num: i64, total: i64) -> String {
    let pages = ((total + RUN_PAGE_SIZE - 1) / RUN_PAGE_SIZE).max(1);
    if pages <= 1 {
        return String::new();
    }
    let prev = if page_num > 1 {
        format!(
            "<a class=\"btn btn-ghost btn-sm\" href=\"{}\">Previous</a>",
            esc(&run_page_href(status, page_num - 1))
        )
    } else {
        "<span class=\"btn btn-ghost btn-sm btn-disabled\">Previous</span>".to_string()
    };
    let next = if page_num < pages {
        format!(
            "<a class=\"btn btn-ghost btn-sm\" href=\"{}\">Next</a>",
            esc(&run_page_href(status, page_num + 1))
        )
    } else {
        "<span class=\"btn btn-ghost btn-sm btn-disabled\">Next</span>".to_string()
    };
    format!(
        r##"<div class="pager">{prev}<span class="muted">Page {page} of {pages} · {total} runs</span>{next}</div>"##,
        prev = prev,
        page = page_num,
        pages = pages,
        total = total,
        next = next,
    )
}

fn run_page_href(status: &str, page: i64) -> String {
    if status.is_empty() {
        format!("/?page={page}")
    } else {
        format!("/?status={}&page={page}", status)
    }
}

/// The create/edit job form. When `edit` is `Some`, it is pre-filled and titled "Edit job".
fn render_form(csrf: &str, edit: Option<&Job>) -> String {
    let (
        heading,
        id_value,
        name_value,
        kind_value,
        schedule_value,
        target_value,
        grace_value,
        enabled,
    ) = match edit {
        Some(j) => (
            "Edit job",
            j.id.as_str(),
            j.name.clone(),
            j.kind.clone(),
            j.schedule.clone(),
            j.target_url.clone(),
            j.grace_secs.to_string(),
            j.enabled,
        ),
        None => (
            "Add job",
            "",
            String::new(),
            KIND_CRON.to_string(),
            String::new(),
            String::new(),
            DEFAULT_GRACE_SECS.to_string(),
            true,
        ),
    };

    let kind_disabled = if edit.is_some() {
        // Kind is fixed once a job (and its heartbeat token) exists.
        "disabled"
    } else {
        ""
    };
    let cron_selected = if kind_value == KIND_CRON {
        "selected"
    } else {
        ""
    };
    let hb_selected = if kind_value == KIND_HEARTBEAT {
        "selected"
    } else {
        ""
    };
    let enabled_checked = if enabled { "checked" } else { "" };
    let cancel = if edit.is_some() {
        r#"<a class="btn btn-ghost" href="/">Cancel</a>"#
    } else {
        ""
    };

    format!(
        r##"<section class="card">
  <div class="card__head"><h2>{heading}</h2></div>
  <div class="card__body">
    <form method="post" action="/api/jobs">
      <input type="hidden" name="csrf_token" value="{csrf}">
      <input type="hidden" name="id" value="{id_value}">
      <div class="field">
        <label for="name">Name</label>
        <input type="text" id="name" name="name" maxlength="120" placeholder="e.g. nightly-backup" autocomplete="off" required value="{name_value}">
      </div>
      <div class="field">
        <label for="kind">Kind</label>
        <select id="kind" name="kind" {kind_disabled}>
          <option value="cron" {cron_selected}>cron — fire an HTTP ping/webhook on a schedule</option>
          <option value="heartbeat" {hb_selected}>heartbeat — alert if an external job stops curling</option>
        </select>
      </div>
      <div class="field">
        <label for="schedule">Schedule <span class="muted">(cron only)</span></label>
        <input type="text" id="schedule" name="schedule" maxlength="64" placeholder="@every 5m  ·  or  09:30" autocomplete="off" value="{schedule_value}">
      </div>
      <div class="field">
        <label for="target_url">Target URL <span class="muted">(cron: fired · heartbeat: optional)</span></label>
        <input type="text" id="target_url" name="target_url" maxlength="400" placeholder="https://example.com/cron/ping" autocomplete="off" value="{target_value}">
      </div>
      <div class="field">
        <label for="grace_secs">Grace seconds <span class="muted">(heartbeat dead-man window)</span></label>
        <input type="text" id="grace_secs" name="grace_secs" maxlength="12" inputmode="numeric" placeholder="300" autocomplete="off" value="{grace_value}">
      </div>
      <div class="field field--check">
        <label class="check"><input type="checkbox" name="enabled" {enabled_checked}> Enabled</label>
      </div>
      <div class="actions">
        <button class="btn btn-primary" type="submit">Save job</button>
        {cancel}
      </div>
    </form>
  </div>
</section>"##,
        heading = heading,
        csrf = esc(csrf),
        id_value = esc(id_value),
        name_value = esc(&name_value),
        kind_disabled = kind_disabled,
        cron_selected = cron_selected,
        hb_selected = hb_selected,
        schedule_value = esc(&schedule_value),
        target_value = esc(&target_value),
        grace_value = esc(&grace_value),
        enabled_checked = enabled_checked,
        cancel = cancel,
    )
}
