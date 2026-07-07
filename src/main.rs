mod api;
mod cache;
mod config;
mod error;
#[cfg(test)]
mod e2e_test;
mod node;

use std::sync::Arc;
use std::time::Duration;

use clap::Parser;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = config::Config::parse();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "keryx_api_shim=info,tower_http=warn".into()),
        )
        .init();

    // Fail fast on an unparseable node URL rather than inside the reconnect loop.
    tonic::transport::Endpoint::from_shared(cfg.node_grpc.clone())
        .map_err(|e| format!("invalid --node-grpc url {:?}: {e}", cfg.node_grpc))?;

    let node = node::NodeClient::spawn(
        cfg.node_grpc.clone(),
        Duration::from_secs(cfg.node_timeout_secs),
    );
    let listen = cfg.listen;
    let state: api::AppState = Arc::new(api::AppInner {
        node,
        caches: cache::Caches::default(),
        http: reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?,
        cfg,
    });

    tokio::spawn(api::startup_probe(state.clone()));

    let app = api::router(state);
    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!("keryx-api-shim listening on http://{listen}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutting down");
}
