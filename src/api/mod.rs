pub mod dto;
mod handlers;

use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::cache::Caches;
use crate::config::Config;
use crate::node::{proto, NodeClient};

pub struct AppInner {
    pub cfg: Config,
    pub node: NodeClient,
    pub caches: Caches,
    pub http: reqwest::Client,
    /// Present only when the indexer is enabled (`--indexer`). Surfaced via
    /// `/health`; from M3 it also backs the indexed read endpoints.
    pub indexer: Option<crate::indexer::IndexerHandle>,
}

pub type AppState = Arc<AppInner>;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/info", get(handlers::info))
        .route(
            "/api/v1/addresses/{address}",
            get(handlers::address_history),
        )
        .route(
            "/api/v1/addresses/{address}/balance",
            get(handlers::balance),
        )
        .route("/api/v1/addresses/{address}/utxos", get(handlers::utxos))
        .route(
            "/api/v1/addresses/{address}/utxos/count",
            get(handlers::utxo_count),
        )
        .route("/api/v1/broadcast", post(handlers::broadcast))
        // Indexed reads (phase 2). 404 when the indexer is disabled.
        .route("/api/v1/transactions/{id}", get(handlers::transaction))
        .route(
            "/api/v1/outpoints/{txid}/{index}/spend",
            get(handlers::outpoint_spend),
        )
        .route("/api/v1/market", get(handlers::market))
        // AI-inference oracle (phase 2c) — served from the indexer, else [].
        .route("/api/v1/capabilities", get(handlers::capabilities))
        .route("/api/v1/infer", get(handlers::infer))
        .route("/api/v1/challenges", get(handlers::challenges))
        .route("/ipfs/{cid}", get(handlers::ipfs))
        .route("/health", get(handlers::health))
        // CORS is load-bearing for the wallet's custom-host setup, not just dev
        // tools: the default host lives in the extension's host_permissions and
        // is fetched with extension privileges (no CORS), but a host set via the
        // wallet's Settings → Network is NOT in host_permissions, so its fetch
        // falls back to ordinary CORS and works only because we answer with
        // `Access-Control-Allow-Origin: *`. `allow_private_network` answers
        // Chrome's Private Network Access preflight so a loopback shim
        // (http://127.0.0.1:8787) keeps working as that check tightens.
        .layer(CorsLayer::permissive().allow_private_network(true))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// One-shot startup probe so obvious node misconfiguration lands in the logs
/// instead of surfacing only as failing wallet requests.
pub async fn startup_probe(app: AppState) {
    for _ in 0..12 {
        match app
            .node
            .get_server_info(proto::GetServerInfoRequestMessage {})
            .await
        {
            Ok(si) => {
                if !si.has_utxo_index {
                    tracing::warn!(
                        "keryxd is running WITHOUT --utxoindex — balance and UTXO endpoints will fail"
                    );
                }
                if !si.is_synced {
                    tracing::warn!("keryxd is not synced yet — responses may lag the network");
                }
                tracing::info!(
                    version = %si.server_version,
                    network = %si.network_id,
                    synced = si.is_synced,
                    utxoindex = si.has_utxo_index,
                    "node reachable"
                );
                return;
            }
            Err(_) => tokio::time::sleep(std::time::Duration::from_secs(5)).await,
        }
    }
    tracing::warn!("could not reach keryxd during the startup probe; retrying in the background");
}
