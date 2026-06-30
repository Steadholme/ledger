//! Storage: jobs, runs, heartbeats.
//!
//! `Store` is a small async trait with an in-memory and a PostgreSQL implementation, mirroring the
//! keystone/relay/sanctum seam: handlers + the scheduler depend only on the trait, so a
//! FusionDB-backed store can drop in later. The PostgreSQL layer uses ONLY portable standard SQL
//! (TEXT/BIGINT/BOOLEAN, PRIMARY KEY/UNIQUE/NOT NULL/DEFAULT, parameterized queries,
//! `INSERT .. ON CONFLICT`, plain indexes) and runtime queries (no compile-time macros), so the
//! build needs NO database and the same statements later run unchanged on FusionDB over pgwire.
//! There is NO JSONB/array/SERIAL/extension/vendor type anywhere.
//!
//! The trait is async: the axum handlers + the scheduler `.await` it directly on the serving
//! runtime, and `PgStore` drives sqlx natively — there is NO `block_in_place` and NO sync-over-async
//! bridge. There is no read-modify-write chain that needs an in-process serializer (the single
//! scheduler task owns the touch path; the DB enforces PK/UNIQUE), so no extra `Mutex` is required.

use std::sync::Mutex;

use async_trait::async_trait;
use thiserror::Error;

use crate::config::RUN_LIMIT;
use crate::model::{Heartbeat, Job, Run};

/// Storage failure surfaced to the handler layer.
#[derive(Debug, Error)]
pub enum StoreError {
    /// A job with this id already exists (the PRIMARY KEY guard).
    #[error("job already exists: {0}")]
    Conflict(String),
    /// Backend I/O failure (mapped to a 500).
    #[error("store error: {0}")]
    Backend(String),
}

/// Pluggable store.
#[async_trait]
pub trait Store: Send + Sync {
    // --- jobs ----------------------------------------------------------------
    /// All jobs, newest-first (for the console + the scheduler sweep).
    async fn list_jobs(&self) -> Vec<Job>;
    /// One job by id.
    async fn get_job(&self, id: &str) -> Option<Job>;
    /// Insert a new job. Errors with [`StoreError::Conflict`] if the id is taken.
    async fn create_job(&self, job: &Job) -> Result<(), StoreError>;
    /// Update a job's editable fields (name/kind/schedule/target_url/grace_secs/enabled) by id.
    async fn update_job(&self, job: &Job) -> Result<(), StoreError>;
    /// Flip a job's `enabled` flag by id. Returns whether a row changed.
    async fn set_enabled(&self, id: &str, enabled: bool) -> Result<bool, StoreError>;
    /// Record a fire / dead-man transition outcome: set `last_run_at` + `last_status` by id.
    async fn touch_job(&self, id: &str, last_run_at: i64, last_status: &str)
        -> Result<(), StoreError>;

    // --- runs ----------------------------------------------------------------
    /// Append one run row.
    async fn insert_run(&self, run: &Run) -> Result<(), StoreError>;
    /// The most recent runs across all jobs, newest-first, capped at [`RUN_LIMIT`].
    async fn recent_runs(&self) -> Vec<Run>;

    // --- heartbeats ----------------------------------------------------------
    /// Create the heartbeat row for a dead-man job (first writer wins).
    async fn create_heartbeat(&self, hb: &Heartbeat) -> Result<(), StoreError>;
    /// Look up a heartbeat by its capability token (the `/ping/{token}` path).
    async fn get_heartbeat(&self, token: &str) -> Option<Heartbeat>;
    /// Record a beat: set `last_beat_at` by token. Returns whether the token was known.
    async fn touch_heartbeat(&self, token: &str, at: i64) -> Result<bool, StoreError>;
    /// All heartbeat rows (for the scheduler dead-man sweep + the console status panel).
    async fn list_heartbeats(&self) -> Vec<Heartbeat>;
}

// --------------------------------------------------------------------------------------
// In-memory store (the default; keeps the whole service database-free for dev + tests).
// --------------------------------------------------------------------------------------

/// In-memory `Store`. Each `Mutex<Vec<_>>` critical section is fully synchronous (no `.await` held
/// across the guard), so the std `Mutex` is correct here.
#[derive(Default)]
pub struct InMemoryStore {
    jobs: Mutex<Vec<Job>>,
    runs: Mutex<Vec<Run>>,
    heartbeats: Mutex<Vec<Heartbeat>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Store for InMemoryStore {
    async fn list_jobs(&self) -> Vec<Job> {
        let mut v: Vec<Job> = self.jobs.lock().expect("jobs lock poisoned").clone();
        v.sort_by(|a, b| {
            b.created_at
                .cmp(&a.created_at)
                .then_with(|| b.id.cmp(&a.id))
        });
        v
    }

    async fn get_job(&self, id: &str) -> Option<Job> {
        self.jobs
            .lock()
            .expect("jobs lock poisoned")
            .iter()
            .find(|j| j.id == id)
            .cloned()
    }

    async fn create_job(&self, job: &Job) -> Result<(), StoreError> {
        let mut jobs = self.jobs.lock().expect("jobs lock poisoned");
        if jobs.iter().any(|j| j.id == job.id) {
            return Err(StoreError::Conflict(job.id.clone()));
        }
        jobs.push(job.clone());
        Ok(())
    }

    async fn update_job(&self, job: &Job) -> Result<(), StoreError> {
        let mut jobs = self.jobs.lock().expect("jobs lock poisoned");
        match jobs.iter_mut().find(|j| j.id == job.id) {
            Some(existing) => {
                existing.name = job.name.clone();
                existing.kind = job.kind.clone();
                existing.schedule = job.schedule.clone();
                existing.target_url = job.target_url.clone();
                existing.grace_secs = job.grace_secs;
                existing.enabled = job.enabled;
                Ok(())
            }
            None => Err(StoreError::Backend(format!("no job with id {}", job.id))),
        }
    }

    async fn set_enabled(&self, id: &str, enabled: bool) -> Result<bool, StoreError> {
        let mut jobs = self.jobs.lock().expect("jobs lock poisoned");
        match jobs.iter_mut().find(|j| j.id == id) {
            Some(j) => {
                j.enabled = enabled;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn touch_job(
        &self,
        id: &str,
        last_run_at: i64,
        last_status: &str,
    ) -> Result<(), StoreError> {
        let mut jobs = self.jobs.lock().expect("jobs lock poisoned");
        if let Some(j) = jobs.iter_mut().find(|j| j.id == id) {
            j.last_run_at = last_run_at;
            j.last_status = last_status.to_string();
        }
        Ok(())
    }

    async fn insert_run(&self, run: &Run) -> Result<(), StoreError> {
        self.runs.lock().expect("runs lock poisoned").push(run.clone());
        Ok(())
    }

    async fn recent_runs(&self) -> Vec<Run> {
        let mut v: Vec<Run> = self.runs.lock().expect("runs lock poisoned").clone();
        v.sort_by(|a, b| {
            b.started_at
                .cmp(&a.started_at)
                .then_with(|| b.id.cmp(&a.id))
        });
        v.truncate(RUN_LIMIT);
        v
    }

    async fn create_heartbeat(&self, hb: &Heartbeat) -> Result<(), StoreError> {
        let mut hbs = self.heartbeats.lock().expect("heartbeats lock poisoned");
        if !hbs.iter().any(|h| h.token == hb.token) {
            hbs.push(hb.clone());
        }
        Ok(())
    }

    async fn get_heartbeat(&self, token: &str) -> Option<Heartbeat> {
        self.heartbeats
            .lock()
            .expect("heartbeats lock poisoned")
            .iter()
            .find(|h| h.token == token)
            .cloned()
    }

    async fn touch_heartbeat(&self, token: &str, at: i64) -> Result<bool, StoreError> {
        let mut hbs = self.heartbeats.lock().expect("heartbeats lock poisoned");
        match hbs.iter_mut().find(|h| h.token == token) {
            Some(h) => {
                h.last_beat_at = at;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    async fn list_heartbeats(&self) -> Vec<Heartbeat> {
        self.heartbeats.lock().expect("heartbeats lock poisoned").clone()
    }
}

// --------------------------------------------------------------------------------------
// PostgreSQL-backed store (portable: standard SQL, runtime queries, no macros).
// --------------------------------------------------------------------------------------
//
// Selected at runtime by `TEMPO_STORE=postgres`. Each method drives sqlx natively and the callers
// `.await` it on the serving runtime — NO `block_in_place`, NO sync-over-async. The DB enforces the
// PRIMARY KEY constraints; `ON CONFLICT` handles a duplicate run id / heartbeat token.

use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::Row;

/// PostgreSQL-backed [`Store`]. Holds just a `PgPool`.
pub struct PgStore {
    pool: PgPool,
}

impl PgStore {
    /// Open a pooled connection. Async; call from within a Tokio runtime.
    pub async fn connect(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect(database_url)
            .await?;
        Ok(Self::from_pool(pool))
    }

    /// Construct from an existing pool (used by tests that share a pool).
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Idempotent, portable migration. Standard SQL only — safe to run on every startup.
    pub async fn migrate(&self) -> Result<(), sqlx::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS jobs (\
                 id TEXT PRIMARY KEY, \
                 name TEXT NOT NULL, \
                 kind TEXT NOT NULL DEFAULT 'cron', \
                 schedule TEXT NOT NULL DEFAULT '', \
                 target_url TEXT NOT NULL DEFAULT '', \
                 grace_secs BIGINT NOT NULL DEFAULT 300, \
                 enabled BOOLEAN NOT NULL DEFAULT TRUE, \
                 last_run_at BIGINT NOT NULL DEFAULT 0, \
                 last_status TEXT NOT NULL DEFAULT '', \
                 created_at BIGINT\
             )",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS runs (\
                 id TEXT PRIMARY KEY, \
                 job_id TEXT NOT NULL, \
                 started_at BIGINT, \
                 status TEXT NOT NULL, \
                 detail TEXT NOT NULL DEFAULT ''\
             )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_runs_job ON runs (job_id, started_at)")
            .execute(&self.pool)
            .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS heartbeats (\
                 token TEXT PRIMARY KEY, \
                 job_id TEXT NOT NULL, \
                 last_beat_at BIGINT NOT NULL DEFAULT 0\
             )",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    fn job_from_row(row: &PgRow) -> Result<Job, sqlx::Error> {
        Ok(Job {
            id: row.try_get("id")?,
            name: row.try_get("name")?,
            kind: row.try_get("kind")?,
            schedule: row.try_get("schedule")?,
            target_url: row.try_get("target_url")?,
            grace_secs: row.try_get("grace_secs")?,
            enabled: row.try_get("enabled")?,
            last_run_at: row.try_get("last_run_at")?,
            last_status: row.try_get("last_status")?,
            // created_at is nullable in the schema; read defensively.
            created_at: row.try_get::<Option<i64>, _>("created_at")?.unwrap_or(0),
        })
    }

    fn run_from_row(row: &PgRow) -> Result<Run, sqlx::Error> {
        Ok(Run {
            id: row.try_get("id")?,
            job_id: row.try_get("job_id")?,
            started_at: row.try_get::<Option<i64>, _>("started_at")?.unwrap_or(0),
            status: row.try_get("status")?,
            detail: row.try_get("detail")?,
        })
    }

    fn heartbeat_from_row(row: &PgRow) -> Result<Heartbeat, sqlx::Error> {
        Ok(Heartbeat {
            token: row.try_get("token")?,
            job_id: row.try_get("job_id")?,
            last_beat_at: row.try_get("last_beat_at")?,
        })
    }
}

/// True when a sqlx error is a UNIQUE/PK violation (Postgres SQLSTATE 23505) — the id clash.
fn is_unique_violation(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(db) if db.code().as_deref() == Some("23505"))
}

#[async_trait]
impl Store for PgStore {
    async fn list_jobs(&self) -> Vec<Job> {
        let rows = sqlx::query(
            "SELECT id, name, kind, schedule, target_url, grace_secs, enabled, last_run_at, \
                    last_status, created_at \
             FROM jobs ORDER BY created_at DESC, id DESC",
        )
        .fetch_all(&self.pool)
        .await;
        match rows {
            Ok(rows) => rows
                .iter()
                .filter_map(|r| Self::job_from_row(r).ok())
                .collect(),
            Err(e) => {
                tracing::error!(error = %e, "pg list_jobs failed");
                Vec::new()
            }
        }
    }

    async fn get_job(&self, id: &str) -> Option<Job> {
        let row = sqlx::query(
            "SELECT id, name, kind, schedule, target_url, grace_secs, enabled, last_run_at, \
                    last_status, created_at \
             FROM jobs WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await;
        match row {
            Ok(Some(r)) => Self::job_from_row(&r).ok(),
            Ok(None) => None,
            Err(e) => {
                tracing::error!(error = %e, "pg get_job failed");
                None
            }
        }
    }

    async fn create_job(&self, job: &Job) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO jobs \
                 (id, name, kind, schedule, target_url, grace_secs, enabled, last_run_at, \
                  last_status, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
        )
        .bind(&job.id)
        .bind(&job.name)
        .bind(&job.kind)
        .bind(&job.schedule)
        .bind(&job.target_url)
        .bind(job.grace_secs)
        .bind(job.enabled)
        .bind(job.last_run_at)
        .bind(&job.last_status)
        .bind(job.created_at)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            if is_unique_violation(&e) {
                StoreError::Conflict(job.id.clone())
            } else {
                StoreError::Backend(e.to_string())
            }
        })?;
        Ok(())
    }

    async fn update_job(&self, job: &Job) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE jobs SET name = $1, kind = $2, schedule = $3, target_url = $4, \
                    grace_secs = $5, enabled = $6 \
             WHERE id = $7",
        )
        .bind(&job.name)
        .bind(&job.kind)
        .bind(&job.schedule)
        .bind(&job.target_url)
        .bind(job.grace_secs)
        .bind(job.enabled)
        .bind(&job.id)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn set_enabled(&self, id: &str, enabled: bool) -> Result<bool, StoreError> {
        let result = sqlx::query("UPDATE jobs SET enabled = $1 WHERE id = $2")
            .bind(enabled)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(result.rows_affected() > 0)
    }

    async fn touch_job(
        &self,
        id: &str,
        last_run_at: i64,
        last_status: &str,
    ) -> Result<(), StoreError> {
        sqlx::query("UPDATE jobs SET last_run_at = $1, last_status = $2 WHERE id = $3")
            .bind(last_run_at)
            .bind(last_status)
            .bind(id)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn insert_run(&self, run: &Run) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO runs (id, job_id, started_at, status, detail) \
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT (id) DO NOTHING",
        )
        .bind(&run.id)
        .bind(&run.job_id)
        .bind(run.started_at)
        .bind(&run.status)
        .bind(&run.detail)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn recent_runs(&self) -> Vec<Run> {
        let rows = sqlx::query(
            "SELECT id, job_id, started_at, status, detail \
             FROM runs ORDER BY started_at DESC, id DESC LIMIT $1",
        )
        .bind(RUN_LIMIT as i64)
        .fetch_all(&self.pool)
        .await;
        match rows {
            Ok(rows) => rows
                .iter()
                .filter_map(|r| Self::run_from_row(r).ok())
                .collect(),
            Err(e) => {
                tracing::error!(error = %e, "pg recent_runs failed");
                Vec::new()
            }
        }
    }

    async fn create_heartbeat(&self, hb: &Heartbeat) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO heartbeats (token, job_id, last_beat_at) \
             VALUES ($1, $2, $3) ON CONFLICT (token) DO NOTHING",
        )
        .bind(&hb.token)
        .bind(&hb.job_id)
        .bind(hb.last_beat_at)
        .execute(&self.pool)
        .await
        .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(())
    }

    async fn get_heartbeat(&self, token: &str) -> Option<Heartbeat> {
        let row = sqlx::query("SELECT token, job_id, last_beat_at FROM heartbeats WHERE token = $1")
            .bind(token)
            .fetch_optional(&self.pool)
            .await;
        match row {
            Ok(Some(r)) => Self::heartbeat_from_row(&r).ok(),
            Ok(None) => None,
            Err(e) => {
                tracing::error!(error = %e, "pg get_heartbeat failed");
                None
            }
        }
    }

    async fn touch_heartbeat(&self, token: &str, at: i64) -> Result<bool, StoreError> {
        let result = sqlx::query("UPDATE heartbeats SET last_beat_at = $1 WHERE token = $2")
            .bind(at)
            .bind(token)
            .execute(&self.pool)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(result.rows_affected() > 0)
    }

    async fn list_heartbeats(&self) -> Vec<Heartbeat> {
        let rows = sqlx::query("SELECT token, job_id, last_beat_at FROM heartbeats")
            .fetch_all(&self.pool)
            .await;
        match rows {
            Ok(rows) => rows
                .iter()
                .filter_map(|r| Self::heartbeat_from_row(r).ok())
                .collect(),
            Err(e) => {
                tracing::error!(error = %e, "pg list_heartbeats failed");
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{KIND_CRON, KIND_HEARTBEAT};

    fn job(id: &str, kind: &str) -> Job {
        Job {
            id: id.to_string(),
            name: format!("job {id}"),
            kind: kind.to_string(),
            schedule: "@every 30s".to_string(),
            target_url: "http://example.test/ping".to_string(),
            grace_secs: 300,
            enabled: true,
            last_run_at: 0,
            last_status: String::new(),
            created_at: 100,
        }
    }

    #[tokio::test]
    async fn job_create_conflict_update_toggle_touch() {
        let store = InMemoryStore::new();
        store.create_job(&job("j1", KIND_CRON)).await.unwrap();
        // Duplicate id rejected.
        assert!(matches!(
            store.create_job(&job("j1", KIND_CRON)).await,
            Err(StoreError::Conflict(_))
        ));

        // Edit mutable fields.
        let mut edited = job("j1", KIND_CRON);
        edited.schedule = "@every 5m".to_string();
        store.update_job(&edited).await.unwrap();
        assert_eq!(store.get_job("j1").await.unwrap().schedule, "@every 5m");

        // Toggle.
        assert!(store.set_enabled("j1", false).await.unwrap());
        assert!(!store.get_job("j1").await.unwrap().enabled);
        assert!(!store.set_enabled("missing", true).await.unwrap());

        // Touch (scheduler path) does not clobber the schedule.
        store.touch_job("j1", 999, "ok").await.unwrap();
        let j = store.get_job("j1").await.unwrap();
        assert_eq!(j.last_run_at, 999);
        assert_eq!(j.last_status, "ok");
        assert_eq!(j.schedule, "@every 5m");
    }

    #[tokio::test]
    async fn runs_are_newest_first() {
        let store = InMemoryStore::new();
        for i in 0..3 {
            store
                .insert_run(&Run {
                    id: format!("run_{i}"),
                    job_id: "j1".to_string(),
                    started_at: i as i64,
                    status: "ok".to_string(),
                    detail: String::new(),
                })
                .await
                .unwrap();
        }
        let runs = store.recent_runs().await;
        assert_eq!(runs.len(), 3);
        assert_eq!(runs[0].id, "run_2");
    }

    #[tokio::test]
    async fn heartbeat_create_touch_lookup() {
        let store = InMemoryStore::new();
        store.create_job(&job("j2", KIND_HEARTBEAT)).await.unwrap();
        store
            .create_heartbeat(&Heartbeat {
                token: "tok_abc".to_string(),
                job_id: "j2".to_string(),
                last_beat_at: 0,
            })
            .await
            .unwrap();
        assert!(store.get_heartbeat("tok_abc").await.is_some());
        assert!(store.touch_heartbeat("tok_abc", 1234).await.unwrap());
        assert_eq!(
            store.get_heartbeat("tok_abc").await.unwrap().last_beat_at,
            1234
        );
        // Unknown token -> false.
        assert!(!store.touch_heartbeat("nope", 1).await.unwrap());
        assert_eq!(store.list_heartbeats().await.len(), 1);
    }
}
