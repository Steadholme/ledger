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

## Frontend (v2, 2026-09-08)

The console follows the shared Steadholme v2 system implemented from the Figma
file `ljE8aFvLz8Q0Wtd5TmdyhQ` (Ledger, cobalt accent). `/assets/delta-20260908.css`
is `crates/odyssey`'s canonical layer, then this crate's `static/service.css`;
bump the date in `src/handlers/mod.rs` and in the tests together when the CSS
changes.

Every page renders through `page(title, active, theme, email, body)`, which
fills the theme attributes, the stylesheet, the footer and the app bar — the app
bar last, so the one caller-supplied chrome value can never be re-scanned as a
template marker. The app bar carries both Ledger surfaces, so Delta and Tempo
cross-link from the same nav.

The stylesheet adds the domain families the design calls for: job kinds (cron
cobalt, heartbeat teal), the violet stream-key chip, the lag bar with its value
beside it, and the dark log surface shared by payload blocks and ping strips.
That surface stays dark in both themes, because it reads as a terminal.

Vocabulary follows the estate rule that every visible string is a name, a value
or an action. The intro paragraphs are gone: the titles name the pages and the
scheduler sweep interval is stated as a value.

`src/bin/delta_fixture.rs` serves the console from the in-memory store with no
database and no egress.
