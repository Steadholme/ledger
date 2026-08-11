//! Event-log storage: the append-only `events` log + the consumer `cursors`.
//!
//! `Store` is a small async trait with an in-memory and a PostgreSQL implementation, mirroring the
//! keystone/relay/watchtower seam: handlers depend only on the trait, so a FusionDB-backed store
//! can drop in later. The PostgreSQL layer uses ONLY portable standard SQL
//! (TEXT/BIGINT/DOUBLE PRECISION, PK/UNIQUE/NOT NULL/DEFAULT, `INSERT .. ON CONFLICT`, parameterized
//! queries, `CREATE INDEX`) and runtime queries (no compile-time macros), so the build needs NO
//! database and the same statements later run unchanged on FusionDB over pgwire.
//!
//! INVARIANT — append-only & monotonic seq: there is no update or delete of an event. The only
//! event mutation is [`Store::append`], which is SERIALIZED (the in-memory store behind its
//! `Mutex`; the Postgres store behind a process-wide `tokio::sync::Mutex` serial guard + a DB
//! transaction). Each append allocates `seq = max(seq) + 1`, so seq is strictly monotonic and
//! gap-free without relying on a vendor SERIAL/sequence — it stays portable to FusionDB.
//!
//! The methods are `async`: the axum handlers `.await` them directly on the serving runtime, and
//! `PgStore` drives sqlx natively — there is NO `block_in_place` and NO sync-over-async bridge, so
//! a DB round-trip never blocks a worker thread.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use thiserror::Error;

/// One durable event (maps 1:1 to an `events` row). `seq` is the global offset; reads address the
/// log by `seq` within a `stream`.
#[derive(Clone, Debug)]
pub struct Event {
    pub seq: i64,
    pub stream: String,
    pub key: String,
    pub event_id: Option<String>,
    pub payload_hash: Option<String>,
    pub payload: String,
    pub created_at: i64,
}

/// One consumer cursor (maps 1:1 to a `cursors` row): the last offset a `consumer` committed for a
/// `stream` — the at-least-once durable read position.
#[derive(Clone, Debug)]
pub struct Cursor {
    pub consumer: String,
    pub stream: String,
    pub offset_seq: i64,
    pub updated_at: i64,
}

/// Aggregate stats for one stream, for the console (count + head offset + last activity).
#[derive(Clone, Debug)]
pub struct StreamStat {
    pub stream: String,
    pub count: i64,
    pub head_seq: i64,
    pub last_at: i64,
}

/// Storage failure surfaced to the handler layer.
#[derive(Debug, Error)]
pub enum StoreError {
    /// The same event identity was already sealed with different payload evidence.
    #[error("event identity conflicts with the sealed payload")]
    Conflict,
    /// Backend I/O failure (mapped to a 500).
    #[error("store error: {0}")]
    Backend(String),
}

impl From<sqlx::Error> for StoreError {
    fn from(error: sqlx::Error) -> Self {
        Self::Backend(error.to_string())
    }
}

/// Pluggable event-log store.
#[async_trait]
pub trait Store: Send + Sync {
    /// Append one event to `stream`, allocating the next monotonic `seq`. Serialized so two
    /// concurrent appends can never collide on a `seq`. Returns the sealed [`Event`].
    async fn append(
        &self,
        stream: &str,
        key: &str,
        event_id: &str,
        payload_hash: &str,
        payload: &str,
        now: i64,
    ) -> Result<Event, StoreError>;

    /// Durable read by offset: events of `stream` with `seq > after`, ascending, capped at `limit`.
    async fn read_after(&self, stream: &str, after: i64, limit: i64) -> Vec<Event>;

    /// The current head (max `seq`) of `stream`, or `0` when the stream is empty.
    async fn head_seq(&self, stream: &str) -> i64;

    /// Commit (upsert) a consumer's offset for a stream. Idempotent on `(consumer, stream)`.
    async fn commit_cursor(
        &self,
        consumer: &str,
        stream: &str,
        offset_seq: i64,
        now: i64,
    ) -> Result<Cursor, StoreError>;

    /// Read one consumer's committed cursor for a stream, if any.
    async fn get_cursor(&self, consumer: &str, stream: &str) -> Option<Cursor>;

    // ---- console (read-only aggregates) -----------------------------------

    /// Per-stream stats, ordered by stream name, capped at `limit` streams.
    async fn list_streams(&self, limit: usize) -> Vec<StreamStat>;

    /// The latest `limit` events of `stream`, newest-first (the console "tail").
    async fn tail(&self, stream: &str, limit: i64) -> Vec<Event>;
    /// Filterable event query, ascending by `seq`. Empty `stream`, `key`, or `contains` means no
    /// filter for that dimension.
    async fn query_events(
        &self,
        stream: &str,
        key: &str,
        contains: &str,
        after: i64,
        limit: i64,
    ) -> Vec<Event>;

    /// All consumer cursors, ordered by `(consumer, stream)`.
    async fn list_cursors(&self) -> Vec<Cursor>;
}

// --------------------------------------------------------------------------------------
// In-memory store (the default; keeps the whole service database-free for dev + tests).
// --------------------------------------------------------------------------------------

#[derive(Default)]
pub struct InMemoryStore {
    /// Append-only, always ordered by `seq` ascending. The `Mutex` IS the serial guard.
    events: Mutex<Vec<Event>>,
    /// Consumer cursors keyed by `(consumer, stream)`.
    cursors: Mutex<HashMap<(String, String), Cursor>>,
}

impl InMemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Store for InMemoryStore {
    // The std `Mutex` is fine throughout: each critical section is fully synchronous (no `.await`
    // inside), so a guard is never held across a yield point.
    async fn append(
        &self,
        stream: &str,
        key: &str,
        event_id: &str,
        payload_hash: &str,
        payload: &str,
        now: i64,
    ) -> Result<Event, StoreError> {
        let mut events = self.events.lock().expect("events lock poisoned");
        if let Some(existing) = events
            .iter()
            .find(|event| event.stream == stream && event.event_id.as_deref() == Some(event_id))
        {
            return if existing.payload_hash.as_deref() == Some(payload_hash) {
                Ok(existing.clone())
            } else {
                Err(StoreError::Conflict)
            };
        }
        // Monotonic seq from the current head (the Mutex serializes the read+push).
        let seq = events.last().map(|e| e.seq + 1).unwrap_or(1);
        let event = Event {
            seq,
            stream: stream.to_string(),
            key: key.to_string(),
            event_id: Some(event_id.to_string()),
            payload_hash: Some(payload_hash.to_string()),
            payload: payload.to_string(),
            created_at: now,
        };
        events.push(event.clone());
        Ok(event)
    }

    async fn read_after(&self, stream: &str, after: i64, limit: i64) -> Vec<Event> {
        let events = self.events.lock().expect("events lock poisoned");
        events
            .iter()
            .filter(|e| e.stream == stream && e.seq > after)
            .take(limit.max(0) as usize)
            .cloned()
            .collect()
    }

    async fn head_seq(&self, stream: &str) -> i64 {
        let events = self.events.lock().expect("events lock poisoned");
        events
            .iter()
            .filter(|e| e.stream == stream)
            .map(|e| e.seq)
            .max()
            .unwrap_or(0)
    }

    async fn commit_cursor(
        &self,
        consumer: &str,
        stream: &str,
        offset_seq: i64,
        now: i64,
    ) -> Result<Cursor, StoreError> {
        let mut cursors = self.cursors.lock().expect("cursors lock poisoned");
        let key = (consumer.to_string(), stream.to_string());
        if let Some(cursor) = cursors.get_mut(&key) {
            // A committed cursor is a durable acknowledgement boundary. Retried
            // or out-of-order consumers may repeat an older commit, but they must
            // never move that boundary backward.
            if offset_seq > cursor.offset_seq {
                cursor.offset_seq = offset_seq;
                cursor.updated_at = now;
            }
            return Ok(cursor.clone());
        }
        let cursor = Cursor {
            consumer: consumer.to_string(),
            stream: stream.to_string(),
            offset_seq,
            updated_at: now,
        };
        cursors.insert(key, cursor.clone());
        Ok(cursor)
    }

    async fn get_cursor(&self, consumer: &str, stream: &str) -> Option<Cursor> {
        self.cursors
            .lock()
            .expect("cursors lock poisoned")
            .get(&(consumer.to_string(), stream.to_string()))
            .cloned()
    }

    async fn list_streams(&self, limit: usize) -> Vec<StreamStat> {
        let events = self.events.lock().expect("events lock poisoned");
        let mut by_stream: HashMap<&str, StreamStat> = HashMap::new();
        for e in events.iter() {
            let stat = by_stream.entry(e.stream.as_str()).or_insert(StreamStat {
                stream: e.stream.clone(),
                count: 0,
                head_seq: 0,
                last_at: 0,
            });
            stat.count += 1;
            stat.head_seq = stat.head_seq.max(e.seq);
            stat.last_at = stat.last_at.max(e.created_at);
        }
        let mut v: Vec<StreamStat> = by_stream.into_values().collect();
        v.sort_by(|a, b| a.stream.cmp(&b.stream));
        v.truncate(limit);
        v
    }

    async fn tail(&self, stream: &str, limit: i64) -> Vec<Event> {
        let events = self.events.lock().expect("events lock poisoned");
        let mut v: Vec<Event> = events
            .iter()
            .filter(|e| e.stream == stream)
            .cloned()
            .collect();
        v.sort_by(|a, b| b.seq.cmp(&a.seq));
        v.truncate(limit.max(0) as usize);
        v
    }

    async fn query_events(
        &self,
        stream: &str,
        key: &str,
        contains: &str,
        after: i64,
        limit: i64,
    ) -> Vec<Event> {
        let events = self.events.lock().expect("events lock poisoned");
        let mut v: Vec<Event> = events
            .iter()
            .filter(|e| stream.is_empty() || e.stream == stream)
            .filter(|e| e.seq > after)
            .filter(|e| key.is_empty() || e.key == key)
            .filter(|e| contains.is_empty() || e.payload.contains(contains))
            .cloned()
            .collect();
        v.sort_by(|a, b| a.seq.cmp(&b.seq));
        v.truncate(limit.max(0) as usize);
        v
    }

    async fn list_cursors(&self) -> Vec<Cursor> {
        let mut v: Vec<Cursor> = self
            .cursors
            .lock()
            .expect("cursors lock poisoned")
            .values()
            .cloned()
            .collect();
        v.sort_by(|a, b| {
            a.consumer
                .cmp(&b.consumer)
                .then_with(|| a.stream.cmp(&b.stream))
        });
        v
    }
}

// --------------------------------------------------------------------------------------
// PostgreSQL-backed store (portable: standard SQL, runtime queries, no macros).
// --------------------------------------------------------------------------------------
//
// Selected at runtime by `DELTA_STORE=postgres`. Each method drives sqlx natively and the handlers
// `.await` it on the serving runtime — NO `block_in_place`, NO sync-over-async. Appends are
// serialized by an in-process `tokio::sync::Mutex<()>` guard held across the DB transaction so two
// appends can never race for the same `seq`; reads never take the guard, so an append burst never
// starves concurrent reads.

use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

/// PostgreSQL-backed [`Store`]. Holds a `PgPool` and the async append serializer.
pub struct PgStore {
    pool: PgPool,
    /// Process-wide append serializer. A `tokio::sync::Mutex` so it can be held across the DB
    /// transaction `.await` without blocking a worker thread (waiting on it yields).
    append_guard: tokio::sync::Mutex<()>,
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
        Self {
            pool,
            append_guard: tokio::sync::Mutex::new(()),
        }
    }

    /// Idempotent, portable migration. Standard SQL only — safe to run on every startup.
    pub async fn migrate(&self) -> Result<(), sqlx::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS events (\
                 seq BIGINT PRIMARY KEY, \
                 stream TEXT NOT NULL, \
                 key TEXT NOT NULL DEFAULT '', \
                 event_id TEXT, \
                 payload_hash TEXT, \
                 payload TEXT NOT NULL, \
                 created_at BIGINT NOT NULL\
             )",
        )
        .execute(&self.pool)
        .await?;
        sqlx::query("ALTER TABLE events ADD COLUMN IF NOT EXISTS event_id TEXT")
            .execute(&self.pool)
            .await?;
        sqlx::query("ALTER TABLE events ADD COLUMN IF NOT EXISTS payload_hash TEXT")
            .execute(&self.pool)
            .await?;
        sqlx::query(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_events_stream_event_id \
             ON events (stream, event_id) WHERE event_id IS NOT NULL",
        )
        .execute(&self.pool)
        .await?;
        // Backs the durable offset read (`WHERE stream = $1 AND seq > $2 ORDER BY seq`).
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_stream_seq ON events (stream, seq)")
            .execute(&self.pool)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS cursors (\
                 consumer TEXT NOT NULL, \
                 stream TEXT NOT NULL, \
                 offset_seq BIGINT NOT NULL DEFAULT 0, \
                 updated_at BIGINT NOT NULL DEFAULT 0, \
                 PRIMARY KEY (consumer, stream)\
             )",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    fn event_from_row(row: &sqlx::postgres::PgRow) -> Result<Event, sqlx::Error> {
        Ok(Event {
            seq: row.try_get("seq")?,
            stream: row.try_get("stream")?,
            key: row.try_get("key")?,
            event_id: row.try_get("event_id")?,
            payload_hash: row.try_get("payload_hash")?,
            payload: row.try_get("payload")?,
            created_at: row.try_get("created_at")?,
        })
    }

    fn cursor_from_row(row: &sqlx::postgres::PgRow) -> Result<Cursor, sqlx::Error> {
        Ok(Cursor {
            consumer: row.try_get("consumer")?,
            stream: row.try_get("stream")?,
            offset_seq: row.try_get("offset_seq")?,
            updated_at: row.try_get("updated_at")?,
        })
    }

    async fn append_async(
        &self,
        stream: &str,
        key: &str,
        event_id: &str,
        payload_hash: &str,
        payload: &str,
        now: i64,
    ) -> Result<Event, StoreError> {
        let mut tx = self.pool.begin().await?;
        if let Some(row) = sqlx::query(
            "SELECT seq,stream,key,event_id,payload_hash,payload,created_at \
             FROM events WHERE stream=$1 AND event_id=$2",
        )
        .bind(stream)
        .bind(event_id)
        .fetch_optional(&mut *tx)
        .await?
        {
            let existing_hash: Option<String> = row.try_get("payload_hash")?;
            if existing_hash.as_deref() != Some(payload_hash) {
                return Err(StoreError::Conflict);
            }
            let event = Self::event_from_row(&row)?;
            tx.commit().await?;
            return Ok(event);
        }
        // Read the current head inside the transaction (the serial guard already prevents a
        // concurrent appender in-process; the transaction bounds the read+write atomically).
        let head: Option<i64> = sqlx::query("SELECT max(seq) AS m FROM events")
            .fetch_one(&mut *tx)
            .await?
            .try_get("m")?;
        let seq = head
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| StoreError::Backend("event sequence exhausted".to_string()))?;
        let inserted = sqlx::query(
            "INSERT INTO events (seq,stream,key,event_id,payload_hash,payload,created_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7)",
        )
        .bind(seq)
        .bind(stream)
        .bind(key)
        .bind(event_id)
        .bind(payload_hash)
        .bind(payload)
        .bind(now)
        .execute(&mut *tx)
        .await;
        if let Err(error) = inserted {
            let unique_violation = error
                .as_database_error()
                .and_then(|database| database.code())
                .is_some_and(|code| code == "23505");
            tx.rollback().await?;
            if unique_violation {
                if let Some(row) = sqlx::query(
                    "SELECT seq,stream,key,event_id,payload_hash,payload,created_at \
                     FROM events WHERE stream=$1 AND event_id=$2",
                )
                .bind(stream)
                .bind(event_id)
                .fetch_optional(&self.pool)
                .await?
                {
                    let existing_hash: Option<String> = row.try_get("payload_hash")?;
                    return if existing_hash.as_deref() == Some(payload_hash) {
                        Ok(Self::event_from_row(&row)?)
                    } else {
                        Err(StoreError::Conflict)
                    };
                }
            }
            return Err(StoreError::Backend(error.to_string()));
        }
        tx.commit().await?;
        Ok(Event {
            seq,
            stream: stream.to_string(),
            key: key.to_string(),
            event_id: Some(event_id.to_string()),
            payload_hash: Some(payload_hash.to_string()),
            payload: payload.to_string(),
            created_at: now,
        })
    }

    async fn read_after_async(
        &self,
        stream: &str,
        after: i64,
        limit: i64,
    ) -> Result<Vec<Event>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT seq,stream,key,event_id,payload_hash,payload,created_at \
             FROM events WHERE stream = $1 AND seq > $2 ORDER BY seq ASC LIMIT $3",
        )
        .bind(stream)
        .bind(after)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(Self::event_from_row).collect()
    }

    async fn head_seq_async(&self, stream: &str) -> Result<i64, sqlx::Error> {
        let head: Option<i64> = sqlx::query("SELECT max(seq) AS m FROM events WHERE stream = $1")
            .bind(stream)
            .fetch_one(&self.pool)
            .await?
            .try_get("m")?;
        Ok(head.unwrap_or(0))
    }

    async fn commit_cursor_async(
        &self,
        consumer: &str,
        stream: &str,
        offset_seq: i64,
        now: i64,
    ) -> Result<Cursor, sqlx::Error> {
        let row = sqlx::query(
            "INSERT INTO cursors (consumer, stream, offset_seq, updated_at) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (consumer, stream) \
             DO UPDATE SET \
                 offset_seq = GREATEST(cursors.offset_seq, EXCLUDED.offset_seq), \
                 updated_at = CASE \
                     WHEN EXCLUDED.offset_seq > cursors.offset_seq THEN EXCLUDED.updated_at \
                     ELSE cursors.updated_at \
                 END \
             RETURNING consumer, stream, offset_seq, updated_at",
        )
        .bind(consumer)
        .bind(stream)
        .bind(offset_seq)
        .bind(now)
        .fetch_one(&self.pool)
        .await?;
        Self::cursor_from_row(&row)
    }

    async fn get_cursor_async(
        &self,
        consumer: &str,
        stream: &str,
    ) -> Result<Option<Cursor>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT consumer, stream, offset_seq, updated_at \
             FROM cursors WHERE consumer = $1 AND stream = $2",
        )
        .bind(consumer)
        .bind(stream)
        .fetch_optional(&self.pool)
        .await?;
        match row {
            Some(r) => Ok(Some(Self::cursor_from_row(&r)?)),
            None => Ok(None),
        }
    }

    async fn list_streams_async(&self, limit: i64) -> Result<Vec<StreamStat>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT stream, count(*) AS cnt, max(seq) AS head, max(created_at) AS last_at \
             FROM events GROUP BY stream ORDER BY stream ASC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter()
            .map(|r| {
                Ok(StreamStat {
                    stream: r.try_get("stream")?,
                    count: r.try_get("cnt")?,
                    head_seq: r.try_get::<Option<i64>, _>("head")?.unwrap_or(0),
                    last_at: r.try_get::<Option<i64>, _>("last_at")?.unwrap_or(0),
                })
            })
            .collect()
    }

    async fn tail_async(&self, stream: &str, limit: i64) -> Result<Vec<Event>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT seq,stream,key,event_id,payload_hash,payload,created_at \
             FROM events WHERE stream = $1 ORDER BY seq DESC LIMIT $2",
        )
        .bind(stream)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(Self::event_from_row).collect()
    }

    async fn query_events_async(
        &self,
        stream: &str,
        key: &str,
        contains: &str,
        after: i64,
        limit: i64,
    ) -> Result<Vec<Event>, sqlx::Error> {
        let pattern = like_contains_pattern(contains);
        let rows = sqlx::query(
            "SELECT seq,stream,key,event_id,payload_hash,payload,created_at \
             FROM events \
             WHERE ($1 = '' OR stream = $1) \
               AND seq > $2 \
               AND ($3 = '' OR key = $3) \
               AND ($4 = '' OR payload LIKE $5 ESCAPE '\\') \
             ORDER BY seq ASC LIMIT $6",
        )
        .bind(stream)
        .bind(after)
        .bind(key)
        .bind(contains)
        .bind(pattern)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(Self::event_from_row).collect()
    }

    async fn list_cursors_async(&self) -> Result<Vec<Cursor>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT consumer, stream, offset_seq, updated_at \
             FROM cursors ORDER BY consumer ASC, stream ASC",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(Self::cursor_from_row).collect()
    }
}

#[async_trait]
impl Store for PgStore {
    async fn append(
        &self,
        stream: &str,
        key: &str,
        event_id: &str,
        payload_hash: &str,
        payload: &str,
        now: i64,
    ) -> Result<Event, StoreError> {
        // Serial guard: only one append runs the read-head -> insert sequence at a time. The tokio
        // `Mutex` is held across the transaction `.await` without blocking a worker thread, and
        // reads never take it — so an append burst can never starve concurrent reads.
        let _guard = self.append_guard.lock().await;
        self.append_async(stream, key, event_id, payload_hash, payload, now)
            .await
    }

    async fn read_after(&self, stream: &str, after: i64, limit: i64) -> Vec<Event> {
        self.read_after_async(stream, after, limit)
            .await
            .unwrap_or_else(|e| {
                tracing::error!(error = %e, "pg read_after failed");
                Vec::new()
            })
    }

    async fn head_seq(&self, stream: &str) -> i64 {
        self.head_seq_async(stream).await.unwrap_or_else(|e| {
            tracing::error!(error = %e, "pg head_seq failed");
            0
        })
    }

    async fn commit_cursor(
        &self,
        consumer: &str,
        stream: &str,
        offset_seq: i64,
        now: i64,
    ) -> Result<Cursor, StoreError> {
        self.commit_cursor_async(consumer, stream, offset_seq, now)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))
    }

    async fn get_cursor(&self, consumer: &str, stream: &str) -> Option<Cursor> {
        self.get_cursor_async(consumer, stream)
            .await
            .unwrap_or_else(|e| {
                tracing::error!(error = %e, "pg get_cursor failed");
                None
            })
    }

    async fn list_streams(&self, limit: usize) -> Vec<StreamStat> {
        self.list_streams_async(limit as i64)
            .await
            .unwrap_or_else(|e| {
                tracing::error!(error = %e, "pg list_streams failed");
                Vec::new()
            })
    }

    async fn tail(&self, stream: &str, limit: i64) -> Vec<Event> {
        self.tail_async(stream, limit).await.unwrap_or_else(|e| {
            tracing::error!(error = %e, "pg tail failed");
            Vec::new()
        })
    }

    async fn query_events(
        &self,
        stream: &str,
        key: &str,
        contains: &str,
        after: i64,
        limit: i64,
    ) -> Vec<Event> {
        self.query_events_async(stream, key, contains, after, limit.max(0))
            .await
            .unwrap_or_else(|e| {
                tracing::error!(error = %e, "pg query_events failed");
                Vec::new()
            })
    }

    async fn list_cursors(&self) -> Vec<Cursor> {
        self.list_cursors_async().await.unwrap_or_else(|e| {
            tracing::error!(error = %e, "pg list_cursors failed");
            Vec::new()
        })
    }
}

fn like_contains_pattern(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('%');
    for c in s.chars() {
        if matches!(c, '%' | '_' | '\\') {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('%');
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::{Digest, Sha256};

    async fn append_test(
        store: &InMemoryStore,
        stream: &str,
        key: &str,
        event_id: &str,
        payload: &str,
        now: i64,
    ) -> Result<Event, StoreError> {
        let payload_hash = hex::encode(Sha256::digest(payload.as_bytes()));
        store
            .append(stream, key, event_id, &payload_hash, payload, now)
            .await
    }

    #[tokio::test]
    async fn append_allocates_monotonic_seq_across_streams() {
        let s = InMemoryStore::new();
        let a = append_test(&s, "orders", "k1", "evt-1", "p1", 100)
            .await
            .unwrap();
        let b = append_test(&s, "billing", "k2", "evt-2", "p2", 101)
            .await
            .unwrap();
        let c = append_test(&s, "orders", "", "evt-3", "p3", 102)
            .await
            .unwrap();
        assert_eq!((a.seq, b.seq, c.seq), (1, 2, 3));
        assert_eq!(s.head_seq("orders").await, 3);
        assert_eq!(s.head_seq("billing").await, 2);
        assert_eq!(s.head_seq("missing").await, 0);
    }

    #[tokio::test]
    async fn read_after_filters_by_stream_and_offset() {
        let s = InMemoryStore::new();
        append_test(&s, "orders", "", "evt-read-1", "p1", 1)
            .await
            .unwrap();
        append_test(&s, "billing", "", "evt-read-2", "x", 1)
            .await
            .unwrap();
        append_test(&s, "orders", "", "evt-read-3", "p2", 1)
            .await
            .unwrap();
        let got = s.read_after("orders", 0, 100).await;
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].seq, 1);
        assert_eq!(got[1].seq, 3);
        // After offset 1 -> only the later orders event.
        let got = s.read_after("orders", 1, 100).await;
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].payload, "p2");
        // Limit is honored.
        assert_eq!(s.read_after("orders", 0, 1).await.len(), 1);
    }

    #[tokio::test]
    async fn query_events_filters_by_stream_key_payload_and_offset() {
        let s = InMemoryStore::new();
        append_test(&s, "orders", "created", "evt-query-1", "alice paid", 1)
            .await
            .unwrap();
        append_test(&s, "orders", "updated", "evt-query-2", "bob refunded", 2)
            .await
            .unwrap();
        append_test(&s, "billing", "created", "evt-query-3", "alice invoice", 3)
            .await
            .unwrap();

        let got = s.query_events("orders", "created", "alice", 0, 10).await;
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].payload, "alice paid");

        let got = s.query_events("", "created", "alice", 1, 10).await;
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].stream, "billing");
    }

    #[test]
    fn like_contains_pattern_escapes_wildcards() {
        assert_eq!(like_contains_pattern("a%b_c\\d"), "%a\\%b\\_c\\\\d%");
    }

    #[tokio::test]
    async fn append_replay_returns_original_seq_and_hash_conflict_is_closed() {
        let store = InMemoryStore::new();
        let first = append_test(&store, "orders", "order:1", "evt-replay", "payload-a", 10)
            .await
            .unwrap();
        let replay = append_test(&store, "orders", "order:1", "evt-replay", "payload-a", 20)
            .await
            .unwrap();
        assert_eq!(replay.seq, first.seq);
        assert_eq!(store.read_after("orders", 0, 10).await.len(), 1);
        assert!(matches!(
            append_test(&store, "orders", "order:1", "evt-replay", "payload-b", 30).await,
            Err(StoreError::Conflict)
        ));
    }

    #[tokio::test]
    async fn cursor_commit_is_monotonic_idempotent_upsert() {
        let s = InMemoryStore::new();
        assert!(s.get_cursor("c1", "orders").await.is_none());
        s.commit_cursor("c1", "orders", 5, 10).await.unwrap();
        s.commit_cursor("c1", "orders", 9, 20).await.unwrap();
        let repeated = s.commit_cursor("c1", "orders", 4, 30).await.unwrap();
        assert_eq!(repeated.offset_seq, 9);
        assert_eq!(repeated.updated_at, 20);
        let cur = s.get_cursor("c1", "orders").await.unwrap();
        assert_eq!(cur.offset_seq, 9);
        assert_eq!(cur.updated_at, 20);
        assert_eq!(s.list_cursors().await.len(), 1);
    }

    #[tokio::test]
    async fn concurrent_appends_never_collide_on_seq() {
        use std::sync::Arc;
        let s = Arc::new(InMemoryStore::new());
        let mut handles = Vec::new();
        for i in 0..64 {
            let s = s.clone();
            handles.push(tokio::spawn(async move {
                append_test(
                    &s,
                    "orders",
                    "",
                    &format!("evt-concurrent-{i}"),
                    &format!("p{i}"),
                    i as i64,
                )
                .await
                .unwrap()
                .seq
            }));
        }
        let mut seqs = Vec::new();
        for h in handles {
            seqs.push(h.await.unwrap());
        }
        seqs.sort();
        let expected: Vec<i64> = (1..=64).collect();
        assert_eq!(
            seqs, expected,
            "seqs must be a gap-free 1..=64 with no duplicates"
        );
    }
}
