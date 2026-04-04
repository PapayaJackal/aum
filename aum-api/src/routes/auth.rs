//! Authentication and invitation API routes.

use std::net::SocketAddr;

use axum::extract::{ConnectInfo, Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use tracing::info;

use crate::dto::{
    InviteValidationResponse, LoginRequest, ProvidersResponse, RedeemInviteRequest,
    SessionTokenResponse,
};
use crate::error::ApiError;
use crate::extractors::auth::AuthenticatedUser;
use crate::state::AppState;

/// Build the auth router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/auth/login", post(login))
        .route("/api/auth/logout", post(logout))
        .route("/api/auth/providers", get(list_providers))
        .route("/api/auth/invite/{token}", get(validate_invite))
        .route("/api/auth/invite/{token}/redeem", post(redeem_invite))
}

/// Authenticate with username and password, returning a session token.
///
/// # Errors
///
/// Returns 429 if rate-limited, 401 if credentials are invalid, or 500 on
/// internal failures.
#[utoipa::path(
    post,
    path = "/api/auth/login",
    request_body = LoginRequest,
    responses(
        (status = 200, body = SessionTokenResponse),
        (status = 401, description = "Invalid credentials"),
        (status = 429, description = "Rate limited"),
    )
)]
pub async fn login(
    State(state): State<AppState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(req): Json<LoginRequest>,
) -> Result<Json<SessionTokenResponse>, ApiError> {
    let client_ip = addr.ip().to_string();
    state.auth.check_rate_limit(&client_ip)?;

    let user = match state.auth.authenticate(&req.username, &req.password).await {
        Ok(user) => user,
        Err(e) => {
            state.auth.record_login_failure(&client_ip);
            tracing::warn!(username = %req.username, client_ip, "login failed");
            return Err(e.into());
        }
    };

    let token = state.auth.create_session(&user).await?;
    info!(username = %user.username, is_admin = user.is_admin, "login successful");

    Ok(Json(SessionTokenResponse {
        session_token: token,
        token_type: "bearer".into(),
    }))
}

/// Invalidate the current session (logout).
///
/// # Errors
///
/// Returns 401 if the session token is missing or invalid.
#[utoipa::path(
    post,
    path = "/api/auth/logout",
    responses((status = 204, description = "Session deleted")),
    security(("bearer" = []))
)]
pub async fn logout(
    State(state): State<AppState>,
    auth: AuthenticatedUser,
) -> Result<axum::http::StatusCode, ApiError> {
    // The extractor already validated the session; we need to extract the raw
    // token again to delete it.  Re-read from the request isn't available here,
    // so we delete all sessions for the user. A per-token delete would require
    // passing the token through; for now, single-session logout via user ID.
    let _ = state.auth.delete_user_sessions(auth.0.id).await;
    Ok(axum::http::StatusCode::NO_CONTENT)
}

/// List available authentication providers and mode information.
#[utoipa::path(
    get,
    path = "/api/auth/providers",
    responses((status = 200, body = ProvidersResponse))
)]
pub async fn list_providers(State(state): State<AppState>) -> Json<ProvidersResponse> {
    Json(ProvidersResponse {
        providers: vec![],
        public_mode: state.config.auth.public_mode,
    })
}

/// Validate an invitation token.
///
/// # Errors
///
/// Returns 404 if the token is invalid or expired, or 500 on database errors.
#[utoipa::path(
    get,
    path = "/api/auth/invite/{token}",
    params(("token" = String, Path, description = "Invitation token")),
    responses(
        (status = 200, body = InviteValidationResponse),
        (status = 404, description = "Invalid or expired invitation"),
    )
)]
pub async fn validate_invite(
    State(state): State<AppState>,
    Path(token): Path<String>,
) -> Result<Json<InviteValidationResponse>, ApiError> {
    let invitation = state
        .auth
        .validate_invitation(&token)
        .await?
        .ok_or_else(|| ApiError::NotFound("Invalid or expired invitation".into()))?;

    Ok(Json(InviteValidationResponse {
        username: invitation.username,
        valid: true,
    }))
}

/// Redeem an invitation to create a new user account.
///
/// # Errors
///
/// Returns 404 if the invitation is invalid, 400 if the password violates
/// policy, 409 if the username is already taken, or 500 on database errors.
#[utoipa::path(
    post,
    path = "/api/auth/invite/{token}/redeem",
    params(("token" = String, Path, description = "Invitation token")),
    request_body = RedeemInviteRequest,
    responses(
        (status = 200, body = SessionTokenResponse),
        (status = 400, description = "Bad request"),
        (status = 404, description = "Invalid or expired invitation"),
    )
)]
pub async fn redeem_invite(
    State(state): State<AppState>,
    Path(token): Path<String>,
    Json(req): Json<RedeemInviteRequest>,
) -> Result<Json<SessionTokenResponse>, ApiError> {
    let user = state.auth.redeem_invitation(&token, &req.password).await?;

    let session_token = state.auth.create_session(&user).await?;
    info!(username = %user.username, "invitation redeemed via API");

    Ok(Json(SessionTokenResponse {
        session_token,
        token_type: "bearer".into(),
    }))
}
