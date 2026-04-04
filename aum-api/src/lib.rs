//! aum HTTP API server library.
//!
//! Exposes [`serve`] for use by `aum-api` binary and `aum serve` CLI command.

// utoipa's OpenApi derive macro generates `for_each` calls we cannot control.
#![allow(clippy::needless_for_each)]

pub mod dto;
pub mod email_preview;
pub mod error;
pub mod extractors;
pub mod routes;
pub mod state;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use tower_http::cors::{Any, CorsLayer};
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::TraceLayer;
use tracing::info;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use aum_core::auth::AuthService;
use aum_core::config::AumConfig;
use aum_core::db::SqlxIndexEmbeddingRepository;
use aum_core::search::AumBackend;

use state::AppState;

/// `OpenAPI` documentation definition.
#[derive(OpenApi)]
#[openapi(
    info(title = "aum", description = "Document search engine API"),
    paths(
        routes::auth::login,
        routes::auth::logout,
        routes::auth::list_providers,
        routes::auth::validate_invite,
        routes::auth::redeem_invite,
        routes::indices::list_indices,
        routes::search::search,
        routes::search::get_document,
        routes::search::download_document,
        routes::search::preview_document,
    ),
    components(schemas(
        dto::LoginRequest,
        dto::SessionTokenResponse,
        dto::ProvidersResponse,
        dto::InviteValidationResponse,
        dto::RedeemInviteRequest,
        dto::IndicesResponse,
        dto::IndexInfo,
        dto::SearchResponse,
        dto::SearchResultResponse,
        dto::DocumentResponse,
        dto::AttachmentResponse,
        dto::ExtractedFromResponse,
        dto::ThreadMessageResponse,
    ))
)]
struct ApiDoc;

/// Build the Axum router with all routes and middleware.
pub fn build_router(state: &AppState) -> Router {
    let api = Router::new()
        .merge(routes::auth::router())
        .merge(routes::indices::router())
        .merge(routes::search::router())
        .with_state(state.clone());

    let mut app = Router::new().merge(api);

    // OpenAPI docs
    if state.config.server.enable_docs {
        app = app.merge(SwaggerUi::new("/api/docs").url("/api/openapi.json", ApiDoc::openapi()));
    }

    // CORS
    if !state.config.server.cors_origins.is_empty() {
        let origins: Vec<_> = state
            .config
            .server
            .cors_origins
            .iter()
            .filter_map(|o| match o.parse() {
                Ok(origin) => Some(origin),
                Err(e) => {
                    tracing::warn!(origin = %o, error = %e, "ignoring invalid CORS origin");
                    None
                }
            })
            .collect();
        let cors = CorsLayer::new()
            .allow_origin(origins)
            .allow_methods(Any)
            .allow_headers(Any)
            .allow_credentials(true);
        app = app.layer(cors);
    }

    // Security headers
    app = app
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::X_FRAME_OPTIONS,
            axum::http::HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::X_CONTENT_TYPE_OPTIONS,
            axum::http::HeaderValue::from_static("nosniff"),
        ));

    // Request tracing
    app = app.layer(TraceLayer::new_for_http());

    // SPA static file serving
    app = attach_frontend(app);

    app
}

/// Attach the SPA frontend to the router.
///
/// When compiled with `bundle-frontend`, the built frontend assets are embedded
/// in the binary and served from memory. Otherwise, falls back to serving from
/// the `frontend/dist` directory on disk (for development).
#[cfg(feature = "bundle-frontend")]
fn attach_frontend(app: Router) -> Router {
    use axum::body::Body;
    use axum::http::{StatusCode, Uri, header};
    use axum::response::{IntoResponse, Response};

    #[derive(rust_embed::Embed)]
    #[folder = "../frontend/dist/"]
    struct FrontendAssets;

    async fn serve_embedded(uri: Uri) -> Response {
        let path = uri.path().trim_start_matches('/');

        // Try the exact path first, then fall back to index.html (SPA routing).
        let is_exact = FrontendAssets::get(path).is_some();
        let file = if is_exact {
            FrontendAssets::get(path)
        } else {
            FrontendAssets::get("index.html")
        };

        match file {
            Some(content) => {
                let mime = mime_guess::from_path(if is_exact { path } else { "index.html" })
                    .first_or_octet_stream();

                Response::builder()
                    .header(header::CONTENT_TYPE, mime.as_ref())
                    .body(Body::from(content.data.to_vec()))
                    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
            }
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    info!("serving bundled frontend assets");
    app.fallback(serve_embedded)
}

#[cfg(not(feature = "bundle-frontend"))]
fn attach_frontend(app: Router) -> Router {
    use std::path::Path;
    use tower_http::services::{ServeDir, ServeFile};

    let frontend_dist = Path::new("frontend/dist");
    if frontend_dist.is_dir() {
        info!(path = %frontend_dist.display(), "serving frontend static files");
        let serve_dir = ServeDir::new(frontend_dist)
            .not_found_service(ServeFile::new(frontend_dist.join("index.html")));
        app.fallback_service(serve_dir)
    } else {
        app
    }
}

/// Start the HTTP API server with the given configuration.
///
/// # Errors
///
/// Returns an error if the server cannot bind to the configured address or
/// if the search backend cannot be constructed.
pub async fn serve(config: AumConfig) -> anyhow::Result<()> {
    let pool = aum_core::bootstrap_db(&config).await;
    let auth = AuthService::new(pool.clone(), &config.auth);
    let backend = Arc::new(AumBackend::from_config(&config)?);
    let embeddings_repo = Arc::new(SqlxIndexEmbeddingRepository::new(pool));

    let config = Arc::new(config);
    let state = AppState {
        config: config.clone(),
        auth,
        backend,
        embeddings_repo,
    };

    let app = build_router(&state);

    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port).parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "aum-api listening");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;

    Ok(())
}
