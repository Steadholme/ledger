//! Two authentication surfaces, deliberately split at the Sluice gateway.
//!
//! - The WEB console (`events.w33d.xyz/`) is `auth=sso`: the gateway runs the OIDC browser login
//!   against Keystone, STRIPS any inbound `X-Auth-*`, and injects the verified `X-Auth-Subject` /
//!   `X-Auth-Email` / `X-Auth-Scope`. Delta is internal-only, so it TRUSTS those headers as the
//!   signed-in operator (display-only — the console is read-only, so there are no state-changing
//!   POSTs and thus no CSRF surface). Delta re-emits NO `X-Auth-*` headers downstream.
//!
//! - The producer/consumer API (`events.w33d.xyz/api/...`) is `auth=public` at the gateway — a
//!   backend service speaks neither the browser OIDC nor cookie SSO — so Delta does its OWN bearer
//!   auth there against the shared `DELTA_SERVICE_TOKEN` (`Authorization: Bearer <token>`),
//!   compared in constant time.

use axum::http::{header, HeaderMap};

pub const HEADER_SUBJECT: &str = "x-auth-subject";
pub const HEADER_EMAIL: &str = "x-auth-email";
pub const HEADER_SCOPE: &str = "x-auth-scope";

/// The signed-in console operator's email, if the gateway injected one (display-only).
pub fn operator_email(headers: &HeaderMap) -> Option<String> {
    header_value(headers, HEADER_EMAIL)
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
// API bearer auth (/api surface)
// ---------------------------------------------------------------------------

/// Parse the token from an `Authorization: Bearer <token>` header, if present and non-empty.
pub fn parse_bearer(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let token = raw
        .strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))?
        .trim();
    if token.is_empty() {
        None
    } else {
        Some(token.to_string())
    }
}

/// Verify the presented bearer token against the configured service token, in constant time.
pub fn verify_service_token(headers: &HeaderMap, expected: &str) -> bool {
    match parse_bearer(headers) {
        Some(token) => ct_eq(token.as_bytes(), expected.as_bytes()),
        None => false,
    }
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

    #[test]
    fn bearer_parses_token() {
        let mut h = HeaderMap::new();
        h.insert(header::AUTHORIZATION, "Bearer abc123".parse().unwrap());
        assert_eq!(parse_bearer(&h).as_deref(), Some("abc123"));
        assert!(parse_bearer(&HeaderMap::new()).is_none());
        let mut empty = HeaderMap::new();
        empty.insert(header::AUTHORIZATION, "Bearer ".parse().unwrap());
        assert!(parse_bearer(&empty).is_none());
    }

    #[test]
    fn service_token_constant_time_match() {
        let mut h = HeaderMap::new();
        h.insert(header::AUTHORIZATION, "Bearer s3cr3t".parse().unwrap());
        assert!(verify_service_token(&h, "s3cr3t"));
        assert!(!verify_service_token(&h, "wrong"));
        assert!(!verify_service_token(&HeaderMap::new(), "s3cr3t"));
    }

    #[test]
    fn operator_email_reads_gateway_header() {
        let mut h = HeaderMap::new();
        h.insert(HEADER_EMAIL, "ops@w33d.xyz".parse().unwrap());
        assert_eq!(operator_email(&h).as_deref(), Some("ops@w33d.xyz"));
        assert!(operator_email(&HeaderMap::new()).is_none());
    }
}
