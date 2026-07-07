use std::net::SocketAddr;

use clap::Parser;

/// Fast, small REST shim between the Keryx Wallet Extension and keryxd.
/// Serves the wallet's `/api/v1` contract straight from the node's gRPC
/// interface — deliberately not an indexer.
#[derive(Parser, Debug, Clone)]
#[command(name = "keryx-api-shim", version, about)]
pub struct Config {
    /// Socket address the shim listens on.
    #[arg(long, env = "KERYX_SHIM_LISTEN", default_value = "127.0.0.1:8787")]
    pub listen: SocketAddr,

    /// keryxd gRPC endpoint (node must run with --utxoindex).
    #[arg(
        long,
        env = "KERYX_SHIM_NODE_GRPC",
        default_value = "http://127.0.0.1:22110"
    )]
    pub node_grpc: String,

    /// Per-request timeout towards the node, in seconds. Keep well under the
    /// wallet extension's hard 15 s fetch timeout.
    #[arg(long, env = "KERYX_SHIM_NODE_TIMEOUT_SECS", default_value_t = 10)]
    pub node_timeout_secs: u64,

    /// Base URL of an IPFS gateway (e.g. http://127.0.0.1:8080) used to serve
    /// GET /ipfs/{cid}. Off when unset — the endpoint then returns 404.
    #[arg(long, env = "KERYX_SHIM_IPFS_GATEWAY")]
    pub ipfs_gateway: Option<String>,

    /// Upstream URL whose JSON is served (cached) as GET /api/v1/market.
    /// Off when unset — the endpoint then returns 404 and the wallet degrades
    /// gracefully (no USD prices).
    #[arg(long, env = "KERYX_SHIM_MARKET_UPSTREAM")]
    pub market_upstream: Option<String>,

    /// Hard cap for the `limit` query parameter on the UTXO endpoint.
    #[arg(long, env = "KERYX_SHIM_MAX_UTXO_LIMIT", default_value_t = 10_000)]
    pub max_utxo_limit: usize,

    /// Enable the windowed indexer (phase 2). Off by default; requires keryxd
    /// with `--retention-period-days` >= the indexer window for gapless
    /// backfill. See docs/PHASE2.md.
    #[arg(long, env = "KERYX_SHIM_INDEXER", default_value_t = false)]
    pub indexer: bool,

    /// Rolling retention window for indexed history, in days. Sized for the
    /// swap app's recovery buffer, not just lock time.
    #[arg(long, env = "KERYX_SHIM_INDEXER_WINDOW_DAYS", default_value_t = 7)]
    pub indexer_window_days: u64,
}
