# Delta — durable event log / lightweight message queue

Delta is the Steadholme event spine: one append-only, offset-addressed event log on Postgres that
replaces Kafka / Redis-streams. Producers append, consumers poll by offset, durable + replayable +
at-least-once.

- **Subdomain:** `events.w33d.xyz` · **internal port:** `9150` · **db:** `delta`
- **Route split** (at the Sluice gateway):
  - `/` — `auth=sso` read-only console (streams, per-stream tail, consumer cursors + lag).
  - `/api/` — `auth=public`; Delta does its OWN bearer auth against `DELTA_SERVICE_TOKEN`.

## API

All `/api` routes require `Authorization: Bearer $DELTA_SERVICE_TOKEN`. `payload` is opaque text
(the producer encodes JSON).

| Method | Path | Body | Returns |
|--------|------|------|---------|
| POST | `/api/streams/{stream}/events` | `{ "key"?, "payload" }` | `{ "seq" }` |
| GET  | `/api/streams/{stream}/events?after={seq}&limit={n}` | — | `{ events: [...], head, next_after }` |
| POST | `/api/cursors/{consumer}/{stream}` | `{ "offset" }` | committed cursor `{ offset, head, lag }` |
| GET  | `/api/cursors/{consumer}/{stream}` | — | cursor `{ offset, head, lag }` |
| GET  | `/healthz` | — | `200 "ok"` (no auth) |

`seq` is a global monotonic offset (gap-free, allocated via a serialized `max(seq)+1` — no vendor
`SERIAL`). A durable read returns events of one `stream` with `seq > after`. At-least-once: a
consumer commits the last processed `seq` as its cursor and resumes from there.

## Configuration (all optional — boots zero-config in-memory)

| Env | Default | Purpose |
|-----|---------|---------|
| `BIND_ADDR` | `0.0.0.0:9150` | Listen address. |
| `DELTA_STORE` | `memory` | `memory` or `postgres`. |
| `DELTA_DATABASE_URL` / `DATABASE_URL` | — | Required when `DELTA_STORE=postgres`. |
| `DELTA_SERVICE_TOKEN` | `dev-delta-token` | Bearer token for `/api`. |
| `DELTA_READ_LIMIT` | `100` | Default read page size (capped at 1000). |
| `DELTA_AUDIT_SAMPLE` | `64` | Emit one `delta.stream.append` audit per N appends. |
| `AUDIT_ENABLED` / `WATCHTOWER_URL` / `AUDIT_INGEST_TOKEN` | off | Non-blocking Watchtower audit. |

## Build / test

```sh
CARGO_BUILD_JOBS=2 cargo check --all-targets
cargo test            # database-free (in-memory); pg_store test skips without TEST_DATABASE_URL
```
