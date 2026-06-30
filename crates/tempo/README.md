# Tempo

Durable **cron on owned metal** for the HOLDFAST estate: scheduled HTTP pings/webhooks and
Healthchecks-style **dead-man heartbeats** for external cron jobs. Built on `axum` + a background
`tokio` scheduler loop, with the same async `Store` seam (in-memory default + portable PostgreSQL)
as the rest of the estate.

Lives at `jobs.w33d.xyz`, internal-only behind the Sluice gateway.

## Surfaces (split at the gateway)

- **SSO console (`/`)** — `auth=sso`. The gateway injects `X-Auth-Subject` / `X-Auth-Email`; Tempo
  trusts them (it is never publicly reachable). List / create / edit / enable jobs, watch recent
  runs, and see live heartbeat status. Every POST is double-submit CSRF protected (`__Host-csrf`).
- **Dead-man heartbeat (`GET /ping/{token}`)** — `auth=public` (longer prefix wins over the SSO
  root). An external cron `curl`s this on every successful run; the unguessable `{token}` IS the
  capability. It advances `last_beat_at` and answers plain `200 ok` (`404 not found` for an unknown
  token). It never touches Watchtower/Klaxon, so it cannot be blocked by a slow downstream.

## Endpoints

| Method + path              | Auth          | Purpose                                            |
|----------------------------|---------------|----------------------------------------------------|
| `GET  /healthz`            | none          | Liveness (container HEALTHCHECK + gateway).         |
| `GET  /`                   | sso           | Console: jobs, recent runs, heartbeat status.       |
| `POST /api/jobs`           | sso + CSRF    | Create or edit a job (hidden `id` => edit).          |
| `POST /api/jobs/{id}/toggle` | sso + CSRF  | Enable/disable a job.                               |
| `GET  /ping/{token}`       | public (token)| Dead-man heartbeat beat.                            |

## Scheduler

A single detached `tokio` task wakes every `TEMPO_TICK_SECS` (default 30s) and:

1. **Fires due cron jobs** — GETs `target_url` (reqwest + rustls), records a `runs` row, updates
   `last_run_at`/`last_status`. A non-2xx or transport error is a failure: it emits a non-blocking
   `tempo.job.fail` audit event to Watchtower and (when configured) POSTs a Klaxon notify.
2. **Flags silent heartbeats** — a heartbeat job that has beaten at least once but is now past
   `last_beat_at + grace_secs` is flagged DOWN (a `runs` row + `tempo.heartbeat.miss` audit +
   optional Klaxon). A previously-down job that resumes beating is recorded as recovered.

### Schedule syntax (cron jobs)

Deliberately minimal — not a full crontab:

- `@every <N><unit>` — fixed interval; `unit` is `s`/`m`/`h` (e.g. `@every 30s`, `@every 5m`,
  `@every 2h`).
- `HH:MM` — once per day at that UTC wall-clock time (e.g. `09:30`).

An unrecognized schedule is rejected at save time, so a job can never silently never-fire.

## Configuration

All values have working dev defaults; the service boots zero-config with an in-memory store.

| Env var                | Default               | Notes                                              |
|------------------------|-----------------------|----------------------------------------------------|
| `BIND_ADDR`            | `0.0.0.0:9160`        | Listen address.                                     |
| `PUBLIC_BASE_URL`      | `https://jobs.w33d.xyz` | Used to render the heartbeat ping URLs.           |
| `TEMPO_TICK_SECS`      | `30`                  | Scheduler sweep interval (seconds).                 |
| `TEMPO_STORE`          | `memory`              | `memory` or `postgres`.                             |
| `TEMPO_DATABASE_URL`   | —                     | Required when `TEMPO_STORE=postgres` (or `DATABASE_URL`). |
| `AUDIT_ENABLED`        | off                   | `on`/`true`/`1`/`yes` to emit to Watchtower.        |
| `WATCHTOWER_URL`       | —                     | e.g. `http://watchtower:8500`.                      |
| `AUDIT_INGEST_TOKEN`   | —                     | Bearer token for Watchtower ingest.                 |
| `KLAXON_URL`           | —                     | e.g. `http://klaxon:9050`. Notify degrades off if unset. |
| `KLAXON_INGEST_TOKEN`  | —                     | Bearer token for Klaxon `/api/notify`.              |
| `KLAXON_NOTIFY_EMAIL`  | —                     | Recipient address for Tempo failure/miss alerts.    |

Klaxon notify is enabled only when all three `KLAXON_*` vars are present.

## Data model (portable standard SQL)

`jobs(id, name, kind, schedule, target_url, grace_secs, enabled, last_run_at, last_status,
created_at)`, `runs(id, job_id, started_at, status, detail)` with `INDEX(job_id, started_at)`, and
`heartbeats(token, job_id, last_beat_at)`. Only `TEXT`/`BIGINT`/`BOOLEAN`, `PRIMARY KEY`/`NOT NULL`/
`DEFAULT`, `INSERT .. ON CONFLICT`, and plain `CREATE INDEX` — no JSONB/arrays/SERIAL/extensions, so
the same statements run unchanged on FusionDB over pgwire. `migrate()` runs `CREATE TABLE IF NOT
EXISTS` on startup.

## Build & test

```sh
CARGO_BUILD_JOBS=2 cargo check --all-targets
cargo test                       # DB-free: in-memory flow + unit tests
# optional Postgres integration:
TEST_DATABASE_URL=postgres://… cargo test --test pg_store -- --nocapture
```
