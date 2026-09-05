//! HTTP authentication and authorization regression tests.
use std::{net::SocketAddr, sync::Arc};

use aum_api::{
    build_router, extractors::auth::AuthenticatedSession, routes::auth::logout, state::AppState,
};
use aum_core::{
    auth::AuthService, config::AumConfig, db::SqlxIndexEmbeddingRepository, search::AumBackend,
};
use axum::{
    body::{Body, to_bytes},
    extract::{ConnectInfo, State},
    http::{Request, StatusCode},
    response::IntoResponse,
};
use tower::ServiceExt;

#[tokio::test]
async fn login_logout_revokes_only_current_session() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut config = AumConfig::default();
    config.data.dir = dir.path().to_owned();
    let pool = aum_core::bootstrap_db(&config).await;
    let auth = AuthService::new(pool.clone(), &config.auth);
    let user = auth.create_user("reader", "Test1234!", false).await?;
    let other_token = auth.create_session(&user).await?;
    let state = AppState {
        backend: Arc::new(AumBackend::from_config(&config)?),
        config: Arc::new(config),
        auth,
        embeddings_repo: Arc::new(SqlxIndexEmbeddingRepository::new(pool.clone())),
    };
    let app = build_router(&state);
    let response = app
        .clone()
        .oneshot(
            Request::post("/api/auth/login")
                .header("content-type", "application/json")
                .extension(ConnectInfo("127.0.0.1:12345".parse::<SocketAddr>()?))
                .body(Body::from(
                    r#"{"username":"reader","password":"Test1234!"}"#,
                ))?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(response.into_body(), 4096).await?)?;
    let token = body["session_token"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("missing token"))?;

    // Authentication and per-index permissions apply to every document surface.
    for path in [
        "/api/search?q=test&index=private",
        "/api/documents/doc?index=private",
        "/api/documents/doc/preview?index=private",
        "/api/documents/doc/download?index=private",
    ] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty())?)
            .await?;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
        let response = app
            .clone()
            .oneshot(
                Request::get(path)
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
    }
    let response = app
        .clone()
        .oneshot(
            Request::post("/api/auth/logout")
                .header("authorization", format!("bEaReR {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(state.auth.validate_session(token).await?.is_none());
    assert!(state.auth.validate_session(&other_token).await?.is_some());
    let response = app
        .clone()
        .oneshot(
            Request::post("/api/auth/logout")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

    // A storage failure must not claim successful revocation.
    pool.close().await;
    let response = logout(State(state), AuthenticatedSession(other_token))
        .await
        .into_response();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    Ok(())
}
