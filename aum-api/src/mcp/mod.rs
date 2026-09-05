//! Model Context Protocol server, mounted at `/mcp`.
//!
//! Exposes the same corpus the web UI searches to MCP-speaking agents, over
//! streamable HTTP. Authentication and per-index access control are shared with
//! the REST API: callers present a session token as `Authorization: Bearer
//! <token>`, and in public mode anonymous callers are allowed through. Issue a
//! long-lived token for an agent with `aum user token <username> --days N`.

pub mod schema;
pub mod tools;

use std::sync::Arc;

use axum::Router;
use axum::http::request::Parts;
use rmcp::model::{Extensions, Implementation, ProtocolVersion, ServerCapabilities, ServerInfo};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{ErrorData, tool_handler};

use aum_core::auth::{ADMIN_ALL_INDICES, User};

use crate::error::ApiError;
use crate::state::AppState;

/// Instructions handed to agents on `initialize`.
const INSTRUCTIONS: &str = "\
aum indexes a corpus of documents (PDFs, Office files, emails and their \
attachments, archives) and exposes it for search.

A productive loop looks like: `list_indices` to see what you can read, \
`search_documents` to find candidates, then `get_document` on the few that \
matter. Search returns snippets only — never assume a snippet is the whole \
story before fetching the document.

Filtering beats paging. If a search returns thousands of hits, call \
`list_facets` to see the exact values available for file type, author, year, \
and email participants, then re-search with `filters` set. Facet values must \
match exactly, so read them from `list_facets` rather than guessing.

`search_type: \"text\"` is keyword matching: use it for names, identifiers, \
quoted phrases, and anything spelled a specific way. `search_type: \"hybrid\"` \
blends in vector similarity and is better for conceptual questions, but only \
works on indices where `list_indices` reports `has_embeddings: true`.

Emails are indexed per message. When a message matters, call \
`get_email_thread` for the surrounding conversation and check the document's \
`attachments` — the substance is often in the attachment, not the email body.";

/// The MCP server handler.
///
/// Holds the same [`AppState`] as the REST handlers, so search, permissions,
/// and embeddings all go through one code path.
#[derive(Clone)]
pub struct AumMcp {
    /// Shared server state.
    state: AppState,
}

impl AumMcp {
    /// Create a handler over the given application state.
    #[must_use]
    pub const fn new(state: AppState) -> Self {
        Self { state }
    }
}

// The macro generates `async fn` trait impls whose bodies are sometimes purely
// synchronous; we do not control the expansion.
#[allow(clippy::unused_async_trait_impl)]
#[tool_handler(router = Self::tool_router())]
impl rmcp::ServerHandler for AumMcp {
    fn get_info(&self) -> ServerInfo {
        let implementation = Implementation::new("aum", env!("CARGO_PKG_VERSION"))
            .with_title("aum document search")
            .with_description("Search a corpus of documents and emails");

        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_protocol_version(ProtocolVersion::LATEST)
            .with_server_info(implementation)
            .with_instructions(INSTRUCTIONS)
    }
}

/// Map an [`ApiError`] onto the closest MCP error.
///
/// Client mistakes stay descriptive; internal failures are flattened so the
/// agent does not see backend detail it cannot act on.
fn to_mcp_error(err: ApiError) -> ErrorData {
    match err {
        ApiError::BadRequest(msg)
        | ApiError::UnprocessableEntity(msg)
        | ApiError::UnsupportedMediaType(msg) => ErrorData::invalid_params(msg, None),
        ApiError::NotFound(msg) => ErrorData::resource_not_found(msg, None),
        ApiError::Unauthorized(msg) | ApiError::Forbidden(msg) | ApiError::Conflict(msg) => {
            ErrorData::invalid_request(msg, None)
        }
        ApiError::RateLimited(msg) => ErrorData::invalid_request(msg, None),
        ApiError::Internal(msg) => {
            tracing::error!(error = %msg, "mcp internal error");
            ErrorData::internal_error("Internal server error", None)
        }
    }
}

/// Resolve the caller from the MCP request's originating HTTP headers.
///
/// Mirrors [`crate::extractors::auth::OptionalUser`]: a bearer token is always
/// validated when present, and its absence is only tolerated in public mode.
async fn authenticate(
    state: &AppState,
    extensions: &Extensions,
) -> Result<Option<User>, ErrorData> {
    let token = extensions
        .get::<Parts>()
        .and_then(|parts| {
            parts
                .headers
                .get(axum::http::header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
        })
        .and_then(|value| {
            // RFC 7235: auth-scheme is case-insensitive.
            (value.len() > 7 && value[..7].eq_ignore_ascii_case("Bearer "))
                .then(|| value[7..].to_owned())
        });

    let Some(token) = token else {
        if state.config.auth.public_mode {
            return Ok(None);
        }
        return Err(ErrorData::invalid_request(
            "Missing Authorization header. Pass a session token as \
             'Authorization: Bearer <token>'; generate one with \
             'aum user token <username>'.",
            None,
        ));
    };

    let user = state
        .auth
        .validate_session(&token)
        .await
        .map_err(|e| to_mcp_error(ApiError::from(e)))?
        .ok_or_else(|| ErrorData::invalid_request("Session expired or invalid", None))?;

    Ok(Some(user))
}

/// Narrow a list of index names to those the caller may read.
///
/// Anonymous callers in public mode see everything, matching
/// [`crate::routes::indices::list_indices`].
async fn visible_indices(
    state: &AppState,
    user: Option<&User>,
    all: Vec<String>,
) -> Result<Vec<String>, ErrorData> {
    let Some(u) = user else { return Ok(all) };
    if u.is_admin {
        return Ok(all);
    }
    let permitted = state
        .auth
        .list_user_indices(u)
        .await
        .map_err(|e| to_mcp_error(ApiError::from(e)))?;
    if permitted.iter().any(|p| p == ADMIN_ALL_INDICES) {
        return Ok(all);
    }
    Ok(all
        .into_iter()
        .filter(|idx| permitted.contains(idx))
        .collect())
}

/// Host values always accepted, regardless of configuration.
const LOOPBACK_HOSTS: &[&str] = &["localhost", "127.0.0.1", "::1", "[::1]"];

/// Build the `Host` allowlist for the MCP endpoint.
///
/// MCP servers validate `Host` to block DNS rebinding against locally running
/// servers. Loopback plus the authority of `server.base_url` covers the normal
/// deployment; `server.mcp_allowed_hosts` adds any other name the server is
/// reached by.
fn allowed_hosts(config: &aum_core::config::AumConfig) -> Vec<String> {
    let mut hosts: Vec<String> = LOOPBACK_HOSTS.iter().map(|h| (*h).to_owned()).collect();

    // Take the authority out of base_url without pulling in a URL parser.
    let authority = config
        .server
        .base_url
        .split_once("://")
        .map_or(config.server.base_url.as_str(), |(_, rest)| rest)
        .split('/')
        .next()
        .unwrap_or_default()
        .trim();
    if !authority.is_empty() {
        hosts.push(authority.to_owned());
        // Accept the bare hostname too, so a base_url with an explicit port
        // still matches requests arriving on the default port.
        if let Some((host, _port)) = authority.rsplit_once(':')
            && !host.is_empty()
            && !host.ends_with(']')
        {
            hosts.push(host.to_owned());
        }
    }

    hosts.extend(config.server.mcp_allowed_hosts.iter().cloned());
    hosts.sort();
    hosts.dedup();
    hosts
}

/// Mount the MCP endpoint at `/mcp` on the given router.
pub fn attach(app: Router, state: &AppState) -> Router {
    let hosts = allowed_hosts(&state.config);
    tracing::info!(?hosts, "serving MCP endpoint at /mcp");

    let handler_state = state.clone();
    let service = StreamableHttpService::new(
        move || Ok(AumMcp::new(handler_state.clone())),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default().with_allowed_hosts(hosts),
    );

    app.nest_service("/mcp", service)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aum_core::config::AumConfig;

    fn config_with(base_url: &str, extra: &[&str]) -> AumConfig {
        let mut config = AumConfig::default();
        config.server.base_url = base_url.to_owned();
        config.server.mcp_allowed_hosts = extra.iter().map(|s| (*s).to_owned()).collect();
        config
    }

    #[test]
    fn allowed_hosts_always_include_loopback() {
        let hosts = allowed_hosts(&config_with("https://aum.example.com", &[]));
        for expected in LOOPBACK_HOSTS {
            assert!(
                hosts.contains(&(*expected).to_owned()),
                "missing {expected}"
            );
        }
    }

    #[test]
    fn allowed_hosts_takes_authority_from_base_url() {
        let hosts = allowed_hosts(&config_with("https://aum.example.com/search/", &[]));
        assert!(hosts.contains(&"aum.example.com".to_owned()));
        assert!(!hosts.iter().any(|h| h.contains('/')));
    }

    #[test]
    fn allowed_hosts_keeps_both_forms_of_a_ported_authority() {
        let hosts = allowed_hosts(&config_with("http://aum.internal:8000", &[]));
        assert!(hosts.contains(&"aum.internal:8000".to_owned()));
        assert!(hosts.contains(&"aum.internal".to_owned()));
    }

    #[test]
    fn allowed_hosts_appends_configured_extras() {
        let hosts = allowed_hosts(&config_with("http://localhost:8000", &["aum.lan"]));
        assert!(hosts.contains(&"aum.lan".to_owned()));
    }
}
