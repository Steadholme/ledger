//! HTTP handlers + shared server-render helpers.
//!
//! - [`health`] — unauthenticated liveness probe (`/healthz`).
//! - [`console`] — the SSO web console: streams list + per-stream tail + consumer cursors + lag.
//! - [`api`] — the producer/consumer event-log API (`/api/...`) with Delta's OWN bearer auth.
//!
//! The shared design tokens / CSS are embedded and served as one immutable stylesheet for every
//! page, matching the Steadholme enterprise brand. All operator-supplied text (stream names, keys,
//! payloads) is HTML-escaped on render — the console injects NO raw HTML.

pub mod api;
pub mod console;
pub mod health;

use axum::http::{header, HeaderValue, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use std::sync::OnceLock;

/// Delta-only CSS layered after Odyssey's canonical font, tokens, and components.
pub const SERVICE_CSS: &str = include_str!("../../static/service.css");
pub const APP_CSS_PATH: &str = "/assets/delta-20260908.css";

static APP_CSS: OnceLock<String> = OnceLock::new();

/// Embedded design system (Odyssey canonical CSS + Delta service CSS).
pub fn app_css() -> &'static str {
    APP_CSS
        .get_or_init(|| {
            let mut css = String::with_capacity(odyssey::APP_CSS.len() + SERVICE_CSS.len());
            css.push_str(odyssey::APP_CSS);
            css.push_str(SERVICE_CSS);
            css
        })
        .as_str()
}

pub async fn app_css_asset() -> Response {
    let mut response = app_css().into_response();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/css; charset=utf-8"),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

/// Cross-subdomain SSO logout (terminated at the Keystone IdP behind the gateway).
pub const LOGOUT_URL: &str = "https://sso.w33d.xyz/_gw/auth/logout";

/// Branded page + error shells.
const PAGE_HTML: &str = include_str!("../../templates/page.html");
const ERROR_HTML: &str = include_str!("../../templates/error.html");

/// Format epoch seconds as a compact UTC timestamp `YYYY-MM-DD HH:MM:SSZ`.
pub fn fmt_ts(secs: i64) -> String {
    match time::OffsetDateTime::from_unix_timestamp(secs) {
        Ok(dt) => format!(
            "{:04}-{:02}-{:02} {:02}:{:02}:{:02}Z",
            dt.year(),
            dt.month() as u8,
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second()
        ),
        Err(_) => secs.to_string(),
    }
}

/// First `n` characters of a string, with an ellipsis when truncated (compact payload preview).
pub fn truncate(s: &str, n: usize) -> String {
    let mut out: String = s.chars().take(n).collect();
    if s.chars().count() > n {
        out.push('\u{2026}');
    }
    out
}

/// Icons used across the console chrome (inline so no asset request is needed).
pub const ICON_MARK: &str = r##"<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M12 7v14"/><path d="M3 18a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1h5a4 4 0 0 1 4 4 4 4 0 0 1 4-4h5a1 1 0 0 1 1 1v13a1 1 0 0 1-1 1h-6a3 3 0 0 0-3 3 3 3 0 0 0-3-3z"/></svg>"##;
pub const ICON_GRID: &str = r##"<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/></svg>"##;

/// The console pages, in app-bar order.
pub const NAV: [(&str, &str); 2] = [("/", "Streams"), ("https://jobs.w33d.xyz/", "Tempo")];

/// Render the app bar: brand lockup + host + page pills; All apps, identity and Log out.
pub fn app_bar(active: &str, email: Option<&str>) -> String {
    let mut pills = String::new();
    for (href, label) in NAV {
        pills.push_str(&format!(
            r#"<a class="surf{state}" href="{href}"{aria}>{label}</a>"#,
            state = if href == active { " is-active" } else { "" },
            href = href,
            aria = if href == active {
                r#" aria-current="page""#
            } else {
                ""
            },
            label = label,
        ));
    }
    let chip = match email {
        Some(value) if !value.is_empty() && value != "—" => {
            let initial = value
                .chars()
                .next()
                .map(|c| c.to_uppercase().to_string())
                .unwrap_or_else(|| "S".to_string());
            format!(
                r#"<span class="userchip"><span class="userchip__avatar" aria-hidden="true">{initial}</span><span class="user-email">{email}</span></span>"#,
                initial = esc(&initial),
                email = esc(value),
            )
        }
        _ => {
            r#"<span class="user-email user-email--none">— (no gateway session)</span>"#.to_string()
        }
    };
    format!(
        r#"<header class="suitebar">
  <a class="suitebar__brand" href="/">
    <span class="brand-tile" aria-hidden="true">{mark}</span>
    <span class="suitebar__name"><b>Steadholme</b><span>Ledger · Delta</span></span>
  </a>
  <span class="suitebar__host">events.w33d.xyz</span>
  <nav class="surfaces" aria-label="Delta pages">{pills}</nav>
  <span class="suitebar__spacer"></span>
  <div class="suitebar__right">
    <a class="allapps" href="https://w33d.xyz">{grid}<span>All apps</span></a>
    {chip}
    <a class="btn btn-ghost btn-sm" href="{logout}">Log out</a>
  </div>
</header>"#,
        mark = ICON_MARK,
        pills = pills,
        grid = ICON_GRID,
        chip = chip,
        logout = LOGOUT_URL,
    )
}

/// The shared page footer.
pub const FOOTER: &str = r##"<footer class="v2-foot">
  <span class="v2-foot__lead">Steadholme · Ledger · events.w33d.xyz</span>
  <a href="https://events.w33d.xyz">Delta</a>
  <a href="https://jobs.w33d.xyz">Tempo</a>
  <a href="https://audit.w33d.xyz">Watchtower</a>
  <a href="https://status.w33d.xyz">Status</a>
  <a href="https://w33d.xyz">All apps</a>
</footer>"##;

/// Resolve the viewer's theme from the cookie header.
pub fn theme_of(headers: &axum::http::HeaderMap) -> &'static str {
    odyssey::resolve_theme(
        headers
            .get(header::COOKIE)
            .and_then(|value| value.to_str().ok()),
    )
}

/// Render the shared HTML page shell: theme attributes, the app bar with `active` marked, the
/// body, and the footer. `body` is already-escaped main content HTML.
pub fn page(title: &str, active: &str, theme: &str, email: Option<&str>, body: &str) -> String {
    PAGE_HTML
        .replace("{{THEME_ATTR}}", odyssey::html_theme_attr(theme))
        .replace("{{COLOR_SCHEME}}", odyssey::color_scheme_meta(theme))
        .replace("{{CSS_PATH}}", APP_CSS_PATH)
        .replace("{{TITLE}}", &esc(title))
        .replace("{{APPBAR}}", &app_bar(active, email))
        .replace("{{FOOTER}}", FOOTER)
        .replace("{{BODY}}", body)
}

/// Wrap a rendered page body in an HTML 200 response.
pub fn html(body: String) -> Response {
    (StatusCode::OK, Html(body)).into_response()
}

/// Render the branded error page as one status tile.
pub fn render_error(
    status: StatusCode,
    heading: &str,
    message: &str,
    email: Option<&str>,
) -> (StatusCode, Html<String>) {
    let body = ERROR_HTML
        .replace("{{THEME_ATTR}}", "")
        .replace("{{COLOR_SCHEME}}", "light dark")
        .replace("{{CSS_PATH}}", APP_CSS_PATH)
        .replace("{{APPBAR}}", &app_bar("/", email))
        .replace("{{FOOTER}}", FOOTER)
        .replace("{{STATUS}}", &status.as_u16().to_string())
        .replace("{{HEADING}}", &esc(heading))
        .replace("{{MESSAGE}}", &esc(message));
    (status, Html(body))
}

/// Minimal HTML escaping for text/attribute interpolation (defense-in-depth on every field).
pub fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#x27;"),
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_html_metacharacters() {
        assert_eq!(esc("<script>&\"'"), "&lt;script&gt;&amp;&quot;&#x27;");
    }

    #[test]
    fn truncate_adds_ellipsis_when_clipped() {
        assert_eq!(truncate("abcdef", 3), "abc\u{2026}");
        assert_eq!(truncate("abc", 8), "abc");
    }
}
