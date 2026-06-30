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
    /// Backend I/O failure (mapped to a 500).
    #[error("store error: {0}")]
    Backend(String),
}

/// Pluggable event-log store.
#[async_trait]
pub trait Store: Send + Sync {
    /// Append one event to `stream`, allocating the next monotonic `seq`. Serialized so two
    /// concurrent appends can never collide on a `seq`. Returns the sealed [`Event`].
    async fn append(&self, stream: &str, key: &str, payload: &str, now: i64)
        -> Result<Event, StoreError>;

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
        payload: &str,
        now: i64,
    ) -> Result<Event, StoreError> {
        let mut events = self.events.lock().expect("events lock poisoned");
        // Monotonic seq from the current head (the Mutex serializes the read+push).
        let seq = events.last().map(|e| e.seq + 1).unwrap_or(1);
        let event = Event {
            seq,
            stream: stream.to_string(),
            key: key.to_string(),
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
        let cursor = Cursor {
            consumer: consumer.to_string(),
            stream: stream.to_string(),
            offset_seq,
            updated_at: now,
        };
        cursors.insert((consumer.to_string(), stream.to_string()), cursor.clone());
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
                 payload TEXT NOT NULL, \
                 created_at BIGINT NOT NULL\
             )",
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
        payload: &str,
        now: i64,
    ) -> Result<Event, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        // Read the current head inside the transaction (the serial guard already prevents a
        // concurrent appender in-process; the transaction bounds the read+write atomically).
        let head: Option<i64> = sqlx::query("SELECT max(seq) AS m FROM events")
            .fetch_one(&mut *tx)
            .await?
            .try_get("m")?;
        let seq = head.unwrap_or(0) + 1;
        sqlx::query(
            "INSERT INTO events (seq, stream, key, payload, created_at) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(seq)
        .bind(stream)
        .bind(key)
        .bind(payload)
        .bind(now)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Event {
            seq,
            stream: stream.to_string(),
            key: key.to_string(),
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
            "SELECT seq, stream, key, payload, created_at \
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
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO cursors (consumer, stream, offset_seq, updated_at) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (consumer, stream) \
             DO UPDATE SET offset_seq = EXCLUDED.offset_seq, updated_at = EXCLUDED.updated_at",
        )
        .bind(consumer)
        .bind(stream)
        .bind(offset_seq)
        .bind(now)
        .execute(&self.pool)
        .await?;
        Ok(())
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
            "SELECT seq, stream, key, payload, created_at \
             FROM events WHERE stream = $1 ORDER BY seq DESC LIMIT $2",
        )
        .bind(stream)
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
        payload: &str,
        now: i64,
    ) -> Result<Event, StoreError> {
        // Serial guard: only one append runs the read-head -> insert sequence at a time. The tokio
        // `Mutex` is held across the transaction `.await` without blocking a worker thread, and
        // reads never take it — so an append burst can never starve concurrent reads.
        let _guard = self.append_guard.lock().await;
        self.append_async(stream, key, payload, now)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))
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
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(Cursor {
            consumer: consumer.to_string(),
            stream: stream.to_string(),
            offset_seq,
            updated_at: now,
        })
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

    async fn list_cursors(&self) -> Vec<Cursor> {
        self.list_cursors_async().await.unwrap_or_else(|e| {
            tracing::error!(error = %e, "pg list_cursors failed");
            Vec::new()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn append_allocates_monotonic_seq_across_streams() {
        let s = InMemoryStore::new();
        let a = s.append("orders", "k1", "p1", 100).await.unwrap();
        let b = s.append("billing", "k2", "p2", 101).await.unwrap();
        let c = s.append("orders", "", "p3", 102).await.unwrap();
        assert_eq!((a.seq, b.seq, c.seq), (1, 2, 3));
        assert_eq!(s.head_seq("orders").await, 3);
        assert_eq!(s.head_seq("billing").await, 2);
        assert_eq!(s.head_seq("missing").await, 0);
    }

    #[tokio::test]
    async fn read_after_filters_by_stream_and_offset() {
        let s = InMemoryStore::new();
        s.append("orders", "", "p1", 1).await.unwrap();
        s.append("billing", "", "x", 1).await.unwrap();
        s.append("orders", "", "p2", 1).await.unwrap();
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
    async fn cursor_commit_is_idempotent_upsert() {
        let s = InMemoryStore::new();
        assert!(s.get_cursor("c1", "orders").await.is_none());
        s.commit_cursor("c1", "orders", 5, 10).await.unwrap();
        s.commit_cursor("c1", "orders", 9, 20).await.unwrap();
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
                s.append("orders", "", &format!("p{i}"), i as i64)
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
        assert_eq!(seqs, expected, "seqs must be a gap-free 1..=64 with no duplicates");
    }
}
