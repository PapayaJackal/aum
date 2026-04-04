//! Unified API error type that maps domain errors to HTTP responses.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use aum_core::auth::AuthError;
use aum_core::search::types::SearchError;

/// A JSON error response body.
#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

/// Unified error type for all API endpoints.
///
/// Implements [`IntoResponse`] so handlers can use `?` to propagate errors.
#[derive(Debug)]
pub enum ApiError {
    /// 400 Bad Request.
    BadRequest(String),
    /// 401 Unauthorized.
    Unauthorized(String),
    /// 403 Forbidden (permission denied).
    Forbidden(String),
    /// 404 Not Found.
    NotFound(String),
    /// 409 Conflict (e.g. username taken).
    Conflict(String),
    /// 415 Unsupported Media Type.
    UnsupportedMediaType(String),
    /// 422 Unprocessable Entity.
    UnprocessableEntity(String),
    /// 429 Too Many Requests.
    RateLimited(String),
    /// 500 Internal Server Error.
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            Self::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg),
            Self::Forbidden(msg) => (StatusCode::FORBIDDEN, msg),
            Self::NotFound(msg) => (StatusCode::NOT_FOUND, msg),
            Self::Conflict(msg) => (StatusCode::CONFLICT, msg),
            Self::UnsupportedMediaType(msg) => (StatusCode::UNSUPPORTED_MEDIA_TYPE, msg),
            Self::UnprocessableEntity(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg),
            Self::RateLimited(msg) => (StatusCode::TOO_MANY_REQUESTS, msg),
            Self::Internal(msg) => (StatusCode::INTERNAL_SERVER_ERROR, msg),
        };

        let body = axum::Json(ErrorBody { error: message });
        (status, body).into_response()
    }
}

impl From<AuthError> for ApiError {
    fn from(err: AuthError) -> Self {
        match err {
            AuthError::InvalidCredentials => {
                Self::Unauthorized("Invalid username or password".into())
            }
            AuthError::NoLocalPassword => Self::Unauthorized("No local password set".into()),
            AuthError::RateLimited => {
                Self::RateLimited("Too many failed login attempts. Try again later.".into())
            }
            AuthError::PasswordPolicy(msg) => Self::BadRequest(msg),
            AuthError::InvalidInvitation => Self::NotFound("Invalid or expired invitation".into()),
            AuthError::UsernameTaken(name) => {
                Self::Conflict(format!("Username '{name}' is already taken"))
            }
            AuthError::UserNotFound(name) => Self::NotFound(format!("User '{name}' not found")),
            AuthError::InvalidSession => Self::Unauthorized("Session expired or invalid".into()),
            AuthError::Db(e) => {
                tracing::error!(error = %e, "database error in auth");
                Self::Internal("Internal server error".into())
            }
        }
    }
}

impl From<SearchError> for ApiError {
    fn from(err: SearchError) -> Self {
        tracing::error!(error = %err, "search backend error");
        Self::Internal("Search backend error".into())
    }
}
