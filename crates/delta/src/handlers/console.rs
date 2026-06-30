//! The SSO web console (`events.w33d.xyz/`): the event spine at a glance.
//!
//! Mounted behind the gateway `auth=sso` route — the operator identity is taken from the injected
//! `X-Auth-Email` (Delta trusts it; it is internal-only) for display only. The console is READ-ONLY
//! (no state-changing POST, hence no CSRF surface): it renders the streams list, each stream's tail
//! (latest events), and the consumer cursors with their lag behind each stream head.
//!
//! All operator-supplied text (stream names, routing keys, payloads) is HTML-escaped on render, and
//! payloads are truncated to a compact preview — the console injects NO raw HTML.

use std::collections::HashMap;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::Response;

use crate::auth;
use crate::config::{STREAM_LIST_LIMIT, TAIL_LIMIT};
use crate::handlers::{esc, fmt_ts, html, page, truncate};
use crate::store::{Cursor, Event, StreamStat};
use crate::AppState;

/// Payload preview length on the console tail.
const PAYLOAD_PREVIEW: usize = 80;

pub async fn index(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let email = auth::operator_email(&headers);

    let streams = state.store.list_streams(STREAM_LIST_LIMIT).await;
    let cursors = state.store.list_cursors().await;

    // Stream head map, for cursor lag.
    let heads: HashMap<&str, i64> = streams
        .iter()
        .map(|s| (s.stream.as_str(), s.head_seq))
        .collect();

    let total_events: i64 = streams.iter().map(|s| s.count).sum();
    let max_lag = cursors
        .iter()
        .map(|c| (heads.get(c.stream.as_str()).copied().unwrap_or(0) - c.offset_seq).max(0))
        .max()
        .unwrap_or(0);

    let mut body = String::new();
    body.push_str(&render_header());
    body.push_str(&render_stats(
        total_events,
        streams.len(),
        cursors.len(),
        max_lag,
    ));

    // Per-stream cards with the tail.
    body.push_str("<section class=\"card\"><div class=\"card__head\"><h2>Streams</h2></div>");
    if streams.is_empty() {
        body.push_str(
            "<div class=\"card__body\"><p class=\"empty\">No streams yet. Producers append via \
             <code>POST /api/streams/{stream}/events</code>.</p></div>",
        );
    } else {
        body.push_str("<div class=\"card__body card__body--streams\">");
        for s in &streams {
            let tail = state.store.tail(&s.stream, TAIL_LIMIT).await;
            body.push_str(&render_stream(s, &tail));
        }
        body.push_str("</div>");
    }
    body.push_str("</section>");

    // Consumer cursors + lag.
    body.push_str(&render_cursors(&cursors, &heads));

    html(page("Delta", email.as_deref(), &body))
}

fn render_header() -> String {
    r#"<div class="console__head">
  <h1>Event spine</h1>
  <p class="sub">Durable, offset-addressed append-only log. Producers append; consumers poll by offset and commit a durable cursor.</p>
</div>"#
        .to_string()
}

fn render_stats(events: i64, streams: usize, consumers: usize, max_lag: i64) -> String {
    let lag_class = if max_lag > 0 { "stat__val--warn" } else { "stat__val--ok" };
    format!(
        r#"<div class="stat-grid">
  <div class="stat"><div class="stat__num">{events}</div><div class="stat__label">Events</div></div>
  <div class="stat"><div class="stat__num">{streams}</div><div class="stat__label">Streams</div></div>
  <div class="stat"><div class="stat__num">{consumers}</div><div class="stat__label">Cursors</div></div>
  <div class="stat"><div class="stat__num {lag_class}">{max_lag}</div><div class="stat__label">Max lag</div></div>
</div>"#,
    )
}

/// One stream block: name + head/count meta, then a small tail table (latest events first).
fn render_stream(s: &StreamStat, tail: &[Event]) -> String {
    let mut rows = String::new();
    if tail.is_empty() {
        rows.push_str("<tr><td colspan=\"4\" class=\"log-empty\">No events.</td></tr>");
    } else {
        for e in tail {
            let key = if e.key.is_empty() {
                "<span class=\"muted\">—</span>".to_string()
            } else {
                format!("<code>{}</code>", esc(&truncate(&e.key, 32)))
            };
            rows.push_str(&format!(
                r#"<tr>
  <td class="seq">{seq}</td>
  <td class="key">{key}</td>
  <td class="payload">{payload}</td>
  <td class="when">{when}</td>
</tr>"#,
                seq = e.seq,
                key = key,
                payload = esc(&truncate(&e.payload, PAYLOAD_PREVIEW)),
                when = esc(&fmt_ts(e.created_at)),
            ));
        }
    }
    format!(
        r#"<div class="stream">
  <div class="stream__head">
    <span class="stream__name">{name}</span>
    <span class="stream__meta">head <code>{head}</code> · {count} events</span>
  </div>
  <table class="log-table">
    <thead><tr><th>Seq</th><th>Key</th><th>Payload</th><th>When</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</div>"#,
        name = esc(&s.stream),
        head = s.head_seq,
        count = s.count,
        rows = rows,
    )
}

/// The consumer cursors card: consumer | stream | committed offset | head | lag badge.
fn render_cursors(cursors: &[Cursor], heads: &HashMap<&str, i64>) -> String {
    let mut rows = String::new();
    if cursors.is_empty() {
        rows.push_str(
            "<tr><td colspan=\"6\" class=\"log-empty\">No consumer cursors committed yet.</td></tr>",
        );
    } else {
        for c in cursors {
            let head = heads.get(c.stream.as_str()).copied().unwrap_or(0);
            let lag = (head - c.offset_seq).max(0);
            let (badge_class, badge) = if lag == 0 {
                ("cache-badge--hit", "caught up".to_string())
            } else {
                ("cache-badge--miss", format!("{lag} behind"))
            };
            rows.push_str(&format!(
                r#"<tr>
  <td class="consumer">{consumer}</td>
  <td class="stream-cell"><code>{stream}</code></td>
  <td class="seq">{offset}</td>
  <td class="seq">{head}</td>
  <td><span class="cache-badge {badge_class}">{badge}</span></td>
  <td class="when">{when}</td>
</tr>"#,
                consumer = esc(&c.consumer),
                stream = esc(&c.stream),
                offset = c.offset_seq,
                head = head,
                badge_class = badge_class,
                badge = esc(&badge),
                when = esc(&fmt_ts(c.updated_at)),
            ));
        }
    }
    format!(
        r#"<section class="card">
  <div class="card__head"><h2>Consumer cursors</h2></div>
  <div class="card__body card__body--list">
    <table class="log-table">
      <thead><tr><th>Consumer</th><th>Stream</th><th>Offset</th><th>Head</th><th>Lag</th><th>Committed</th></tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</section>"#,
        rows = rows,
    )
}
