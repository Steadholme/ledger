//! Gateway-injected identity + double-submit CSRF for the SSO console.
//!
//! Tempo does NO login of its own. The console (`jobs.w33d.xyz/`) sits behind a Sluice `auth=sso`
//! route: the gateway runs the OIDC browser login against Keystone, STRIPS any inbound `X-Auth-*`,
//! and injects the verified `X-Auth-Subject` / `X-Auth-Email`. Tempo is internal-only, so it TRUSTS
//! those headers as the authenticated operator (falling back to a dev identity only when none are
//! present, so the service still runs DB-free locally and in tests).
//!
//! State-changing POSTs (save job / toggle) are double-submit CSRF protected: a random token lives
//! in a JS-readable `__Host-csrf` cookie AND in a hidden form field; the POST is accepted only when
//! the two match.
//!
//! The public `/ping/{token}` endpoint deliberately uses NONE of this — it is a capability URL whose
//! secret IS the token in the path, reachable by an external cron with no session.

use axum::http::{header, HeaderMap};

use crate::random_alnum;

pub const HEADER_SUBJECT: &str = "x-auth-subject";
pub const HEADER_EMAIL: &str = "x-auth-email";

/// Dev/test fallback identity used ONLY when no gateway headers are present (local `cargo run` or the
/// DB-free test suite). In production every console request arrives with `X-Auth-*` injected.
pub const DEV_SUBJECT: &str = "dev-user";
pub const DEV_EMAIL: &str = "dev@tempo.local";

/// Double-submit CSRF cookie. `__Host-` prefix => Secure + Path=/ + no Domain, so the browser only
/// ever returns it over TLS to this exact host.
pub const CSRF_COOKIE: &str = "__Host-csrf";
/// CSRF cookie lifetime, seconds.
const CSRF_TTL: u64 = 3600;
/// CSRF token length (characters from the 62-symbol alphabet).
const CSRF_LEN: usize = 40;

/// The authenticated console operator. Subject is the stable id; email is display-only.
#[derive(Clone, Debug)]
pub struct Identity {
    pub subject: String,
    pub email: String,
}

/// Resolve the current console operator from the gateway-injected headers, falling back to the dev
/// identity when none are present.
pub fn identity(headers: &HeaderMap) -> Identity {
    Identity {
        subject: header_value(headers, HEADER_SUBJECT).unwrap_or_else(|| DEV_SUBJECT.to_string()),
        email: header_value(headers, HEADER_EMAIL).unwrap_or_else(|| DEV_EMAIL.to_string()),
    }
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

// ---------------------------------------------------------------------------
// CSRF (double-submit)
// ---------------------------------------------------------------------------

/// Mint a fresh CSRF token (same value goes in the cookie and the form field).
pub fn new_csrf_token() -> String {
    random_alnum(CSRF_LEN)
}

/// `Set-Cookie` value for the (JS-readable) CSRF cookie.
pub fn csrf_cookie(value: &str) -> String {
    format!("{CSRF_COOKIE}={value}; Path=/; Secure; SameSite=Lax; Max-Age={CSRF_TTL}")
}

/// Double-submit check: the `submitted` form token must equal the `__Host-csrf` cookie.
pub fn verify_csrf(headers: &HeaderMap, submitted: &str) -> bool {
    match get_cookie(headers, CSRF_COOKIE) {
        Some(cookie) if !cookie.is_empty() => ct_eq(cookie.as_bytes(), submitted.as_bytes()),
        _ => false,
    }
}

/// Read a single cookie value from the request's `Cookie` header(s).
pub fn get_cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    for hv in headers.get_all(header::COOKIE).iter() {
        let Ok(raw) = hv.to_str() else { continue };
        for pair in raw.split(';') {
            let pair = pair.trim();
            if let Some((k, v)) = pair.split_once('=') {
                if k.trim() == name {
                    return Some(v.trim().to_string());
                }
            }
        }
    }
    None
}

/// Length-checked constant-time byte comparison (no early return on the first differing byte).
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn identity_falls_back_to_dev() {
        let id = identity(&HeaderMap::new());
        assert_eq!(id.subject, DEV_SUBJECT);
        assert_eq!(id.email, DEV_EMAIL);
    }

    #[test]
    fn identity_reads_gateway_headers() {
        let mut h = HeaderMap::new();
        h.insert(HEADER_SUBJECT, HeaderValue::from_static("u_admin"));
        h.insert(HEADER_EMAIL, HeaderValue::from_static("a@w33d.xyz"));
        let id = identity(&h);
        assert_eq!(id.subject, "u_admin");
        assert_eq!(id.email, "a@w33d.xyz");
    }

    #[test]
    fn csrf_double_submit() {
        let token = new_csrf_token();
        assert_eq!(token.len(), CSRF_LEN);
        let mut h = HeaderMap::new();
        h.insert(header::COOKIE, format!("{CSRF_COOKIE}={token}").parse().unwrap());
        assert!(verify_csrf(&h, &token));
        assert!(!verify_csrf(&h, "not-the-token"));
        assert!(!verify_csrf(&HeaderMap::new(), &token));
    }
}
