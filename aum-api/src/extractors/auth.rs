//! Session-based authentication extractors.

use axum::extract::FromRequestParts;
use axum::http::header;
use axum::http::request::Parts;

use aum_core::auth::User;

use crate::error::ApiError;
use crate::state::AppState;

/// Extract the raw session token from the `Authorization: Bearer <token>` header.
fn extract_bearer_token(parts: &Parts) -> Option<&str> {
    let value = parts
        .headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())?;
    // RFC 7235: auth-scheme is case-insensitive.
    if value.len() > 7 && value[..7].eq_ignore_ascii_case("Bearer ") {
        Some(&value[7..])
    } else {
        None
    }
}

/// Extractor that requires a valid authenticated user.
///
/// Reads the `Authorization: Bearer <token>` header, validates the session,
/// and returns the associated [`User`]. Returns 401 if the token is missing
/// or invalid.
pub struct AuthenticatedUser(pub User);

impl FromRequestParts<AppState> for AuthenticatedUser {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let token = extract_bearer_token(parts)
            .ok_or_else(|| ApiError::Unauthorized("Missing authorization header".into()))?;

        let user = state
            .auth
            .validate_session(token)
            .await
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError::Unauthorized("Invalid or expired session".into()))?;

        Ok(Self(user))
    }
}

/// Extractor for endpoints that support both authenticated and anonymous access.
///
/// In public mode (`config.auth.public_mode = true`), anonymous requests (no
/// `Authorization` header) are allowed and return `None`. When a token is
/// present it is always validated.
///
/// In non-public mode, a valid session is required and this behaves like
/// [`AuthenticatedUser`].
pub struct OptionalUser(pub Option<User>);

impl FromRequestParts<AppState> for OptionalUser {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let Some(token) = extract_bearer_token(parts) else {
            // No token provided.
            if state.config.auth.public_mode {
                return Ok(Self(None));
            }
            return Err(ApiError::Unauthorized(
                "Missing authorization header".into(),
            ));
        };

        // Token present — always validate.
        let user = state
            .auth
            .validate_session(token)
            .await
            .map_err(ApiError::from)?
            .ok_or_else(|| ApiError::Unauthorized("Invalid or expired session".into()))?;

        Ok(Self(Some(user)))
    }
}
