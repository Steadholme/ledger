//! Application errors for the WEB console, rendered as branded HTML pages.
//!
//! Tempo's console surface is browser-facing, so a failure renders the enterprise error page (same
//! app-bar + design tokens) rather than a JSON envelope. The public `/ping/{token}` endpoint does
//! NOT use this type — it answers in plain text (`ok` / `not found`).

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    /// Malformed/rejected request (e.g. CSRF mismatch, empty name, unparseable schedule).
    #[error("bad_request: {0}")]
    BadRequest(String),

    /// No such resource (job id, page).
    #[error("not_found: {0}")]
    NotFound(String),

    /// Unexpected internal failure (store I/O).
    #[error("server_error: {0}")]
    Internal(String),
}

impl AppError {
    /// Map to `(status, heading, message)` for the rendered error page.
    fn parts(&self) -> (StatusCode, &'static str, String) {
        match self {
            AppError::BadRequest(d) => (StatusCode::BAD_REQUEST, "Request rejected", d.clone()),
            AppError::NotFound(d) => (StatusCode::NOT_FOUND, "Not found", d.clone()),
            AppError::Internal(d) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Something went wrong",
                d.clone(),
            ),
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, heading, message) = self.parts();
        crate::handlers::render_error(status, heading, &message, None).into_response()
    }
}

/// Store failures collapse to their HTTP shape: a job-id conflict is a 409, everything else a 500.
impl From<crate::store::StoreError> for AppError {
    fn from(e: crate::store::StoreError) -> Self {
        match e {
            crate::store::StoreError::Conflict(m) => AppError::BadRequest(m),
            crate::store::StoreError::Backend(m) => AppError::Internal(m),
        }
    }
}
