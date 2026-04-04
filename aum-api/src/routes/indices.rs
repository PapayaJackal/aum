//! Index listing API route.

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use futures::future::join_all;

use aum_core::auth::ADMIN_ALL_INDICES;
use aum_core::db::IndexEmbeddingRepository as _;
use aum_core::search::backend::SearchBackend;

use crate::dto::{IndexInfo, IndicesResponse};
use crate::error::ApiError;
use crate::extractors::auth::OptionalUser;
use crate::state::AppState;

/// Build the indices router.
pub fn router() -> Router<AppState> {
    Router::new().route("/api/indices", get(list_indices))
}

/// List all available search indices with embedding status.
///
/// Non-admin users only see indices they have been granted access to.
/// Anonymous users in public mode see all indices.
///
/// # Errors
///
/// Returns 401 if authentication is required and missing, 403 if access is
/// denied, or 500 on backend/database errors.
#[utoipa::path(
    get,
    path = "/api/indices",
    responses((status = 200, body = IndicesResponse)),
)]
pub async fn list_indices(
    State(state): State<AppState>,
    OptionalUser(user): OptionalUser,
) -> Result<Json<IndicesResponse>, ApiError> {
    let all_indices = state.backend.list_indices().await?;

    // Filter by permissions.
    let visible: Vec<String> = match &user {
        Some(u) if u.is_admin => all_indices,
        Some(u) => {
            let permitted = state.auth.list_user_indices(u).await?;
            if permitted.iter().any(|p| p == ADMIN_ALL_INDICES) {
                all_indices
            } else {
                all_indices
                    .into_iter()
                    .filter(|idx| permitted.contains(idx))
                    .collect()
            }
        }
        None => all_indices, // Public mode
    };

    let embedding_checks: Vec<_> = visible
        .iter()
        .map(|name| state.embeddings_repo.get_embedding_model(name))
        .collect();
    let results = join_all(embedding_checks).await;
    let indices: Vec<IndexInfo> = visible
        .into_iter()
        .zip(results)
        .map(|(name, result)| IndexInfo {
            has_embeddings: result.ok().flatten().is_some(),
            name,
        })
        .collect();

    Ok(Json(IndicesResponse { indices }))
}
