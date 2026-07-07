use axum::extract::rejection::JsonRejection;
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use super::{dto, AppState};
use crate::error::ApiError;
use crate::node::proto;

const IPFS_MAX_BYTES: usize = 1024 * 1024;

// --- /api/v1/info ---------------------------------------------------------------

pub async fn info(State(app): State<AppState>) -> Result<Json<dto::InfoResponse>, ApiError> {
    let app2 = app.clone();
    let info = app
        .caches
        .info
        .get_or_try_init(move || build_info(app2))
        .await?;
    Ok(Json(info))
}

async fn build_info(app: AppState) -> Result<dto::InfoResponse, ApiError> {
    let (dag, supply) = tokio::try_join!(
        app.node
            .get_block_dag_info(proto::GetBlockDagInfoRequestMessage {}),
        app.node
            .get_coin_supply(proto::GetCoinSupplyRequestMessage {}),
    )?;

    // Best-effort extras on their own longer TTLs: /info must not fail (the
    // wallet uses it as its online/offline probe) just because these do.
    let node = app.node.clone();
    let hashrate_hps = app
        .caches
        .hashrate
        .get_or_init(move || async move {
            node.estimate_network_hashes_per_second(
                proto::EstimateNetworkHashesPerSecondRequestMessage {
                    window_size: 1000,
                    start_hash: String::new(),
                },
            )
            .await
            .map(|r| r.network_hashes_per_second as f64)
            .unwrap_or(0.0)
        })
        .await;

    let node = app.node.clone();
    let sink = dag.sink.clone();
    let block_reward_krx = app
        .caches
        .block_reward
        .get_or_init(move || async move {
            match node
                .get_block(proto::GetBlockRequestMessage {
                    hash: sink,
                    include_transactions: true,
                })
                .await
            {
                Ok(resp) => coinbase_reward_krx(&resp).unwrap_or(0.0),
                Err(_) => 0.0,
            }
        })
        .await;

    let circulating = supply.circulating_sompi as f64 / dto::SOMPI_PER_KRX;
    let max_supply = supply.max_sompi as f64 / dto::SOMPI_PER_KRX;
    Ok(dto::InfoResponse {
        network: dag.network_name,
        last_daa_score: dag.virtual_daa_score,
        block_reward_krx,
        total_supply_krx: circulating,
        max_supply_krx: max_supply,
        hashrate_hps,
        total_blocks: dag.block_count,
        // These four need the address/tx indexer (future phase); zeroed until then.
        total_txs: 0,
        burned_krx: 0.0,
        total_escrow_krx: 0.0,
        total_real_inferences: 0,
        mined_pct: if max_supply > 0.0 {
            circulating / max_supply * 100.0
        } else {
            0.0
        },
    })
}

/// A block's first transaction is its coinbase; in a merged DAG it carries one
/// output per merged blue block, so the median output is the closest
/// single-block reward figure available without an indexer.
fn coinbase_reward_krx(resp: &proto::GetBlockResponseMessage) -> Option<f64> {
    let coinbase = resp.block.as_ref()?.transactions.first()?;
    let mut amounts: Vec<u64> = coinbase
        .outputs
        .iter()
        .map(|o| o.amount)
        .filter(|&a| a > 0)
        .collect();
    if amounts.is_empty() {
        return None;
    }
    amounts.sort_unstable();
    Some(amounts[amounts.len() / 2] as f64 / dto::SOMPI_PER_KRX)
}

// --- /api/v1/addresses/{address}/... ----------------------------------------------

pub async fn balance(
    State(app): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<dto::BalanceResponse>, ApiError> {
    dto::validate_address(&address).map_err(ApiError::BadRequest)?;
    let resp = app
        .node
        .get_balance_by_address(proto::GetBalanceByAddressRequestMessage {
            address: address.clone(),
        })
        .await?;
    Ok(Json(dto::BalanceResponse {
        address,
        balance_sompi: resp.balance,
    }))
}

#[derive(Deserialize)]
pub struct UtxoQuery {
    limit: Option<usize>,
}

pub async fn utxos(
    State(app): State<AppState>,
    Path(address): Path<String>,
    Query(query): Query<UtxoQuery>,
) -> Result<Json<Vec<dto::UtxoDto>>, ApiError> {
    dto::validate_address(&address).map_err(ApiError::BadRequest)?;
    let limit = query
        .limit
        .unwrap_or(400)
        .clamp(1, app.cfg.max_utxo_limit.max(1));
    let mut utxos = fetch_utxos(&app, address).await?;
    // Largest-first, so a truncated page is still the most useful set for the
    // wallet's greedy coin selection.
    utxos.sort_unstable_by(|a, b| {
        b.amount_sompi
            .cmp(&a.amount_sompi)
            .then_with(|| a.transaction_id.cmp(&b.transaction_id))
            .then_with(|| a.index.cmp(&b.index))
    });
    utxos.truncate(limit);
    Ok(Json(utxos))
}

pub async fn utxo_count(
    State(app): State<AppState>,
    Path(address): Path<String>,
) -> Result<Json<dto::UtxoCountResponse>, ApiError> {
    dto::validate_address(&address).map_err(ApiError::BadRequest)?;
    let utxos = fetch_utxos(&app, address).await?;
    Ok(Json(dto::UtxoCountResponse { count: utxos.len() }))
}

async fn fetch_utxos(app: &AppState, address: String) -> Result<Vec<dto::UtxoDto>, ApiError> {
    let resp = app
        .node
        .get_utxos_by_addresses(proto::GetUtxosByAddressesRequestMessage {
            addresses: vec![address],
        })
        .await?;
    Ok(resp.entries.into_iter().filter_map(dto::utxo_dto).collect())
}

#[derive(Deserialize)]
pub struct HistoryQuery {
    #[allow(dead_code)]
    limit: Option<usize>,
    #[allow(dead_code)]
    offset: Option<usize>,
}

/// Phase 1: the shim is deliberately not an indexer, and a bare keryxd keeps
/// no per-address transaction history. A well-formed empty page keeps the
/// wallet's dashboard and history views functional; the indexer phase will
/// replace this handler.
pub async fn address_history(
    Path(address): Path<String>,
    Query(_query): Query<HistoryQuery>,
) -> Result<Json<dto::AddressHistoryResponse>, ApiError> {
    dto::validate_address(&address).map_err(ApiError::BadRequest)?;
    Ok(Json(dto::AddressHistoryResponse {
        address,
        total_received_sompi: 0,
        total_tx_count: 0,
        transactions: vec![],
    }))
}

// --- /api/v1/broadcast --------------------------------------------------------------

pub async fn broadcast(
    State(app): State<AppState>,
    payload: Result<Json<dto::TxJson>, JsonRejection>,
) -> Result<Json<dto::BroadcastResponse>, ApiError> {
    let Json(tx) = payload.map_err(|e| {
        ApiError::BadRequest(format!("invalid transaction JSON: {}", e.body_text()))
    })?;
    tx.validate().map_err(ApiError::BadRequest)?;
    let resp = app
        .node
        .submit_transaction(proto::SubmitTransactionRequestMessage {
            transaction: Some(tx.into_proto()),
            allow_orphan: false,
        })
        .await?;
    Ok(Json(dto::BroadcastResponse {
        transaction_id: resp.transaction_id,
    }))
}

// --- /api/v1/market -------------------------------------------------------------------

pub async fn market(State(app): State<AppState>) -> Result<Json<Value>, ApiError> {
    let Some(url) = app.cfg.market_upstream.clone() else {
        // The wallet catch-guards this call and simply hides USD values.
        return Err(ApiError::NotFound(
            "market data is not configured on this shim".into(),
        ));
    };
    let http = app.http.clone();
    let value = app
        .caches
        .market
        .get_or_try_init(move || async move {
            let resp = http
                .get(&url)
                .send()
                .await
                .map_err(|e| ApiError::Upstream(format!("market upstream: {e}")))?;
            if !resp.status().is_success() {
                return Err(ApiError::Upstream(format!(
                    "market upstream returned {}",
                    resp.status()
                )));
            }
            resp.json::<Value>()
                .await
                .map_err(|e| ApiError::Upstream(format!("market upstream: {e}")))
        })
        .await?;
    Ok(Json(value))
}

// --- inference endpoints (indexer phase) --------------------------------------------

/// `/capabilities`, `/infer` and `/challenges` describe the AI-inference
/// oracle layer, which requires indexing AiRequest subnetwork transactions —
/// future phase. Empty lists are the honest, well-formed "no data" answer the
/// wallet's inference screen already handles.
pub async fn empty_list() -> Json<Value> {
    Json(json!([]))
}

// --- /ipfs/{cid} ------------------------------------------------------------------------

fn is_cid_v0(s: &str) -> bool {
    // base58btc: digits/letters minus 0, O, I, l
    s.len() == 46
        && s.starts_with("Qm")
        && s.bytes().all(|b| {
            matches!(b, b'1'..=b'9' | b'A'..=b'H' | b'J'..=b'N' | b'P'..=b'Z' | b'a'..=b'k' | b'm'..=b'z')
        })
}

pub async fn ipfs(
    State(app): State<AppState>,
    Path(cid): Path<String>,
) -> Result<Response, ApiError> {
    if !is_cid_v0(&cid) {
        return Err(ApiError::BadRequest("invalid IPFS CID".into()));
    }
    let Some(gateway) = &app.cfg.ipfs_gateway else {
        return Err(ApiError::NotFound(
            "no IPFS gateway is configured on this shim".into(),
        ));
    };
    let url = format!("{}/ipfs/{cid}", gateway.trim_end_matches('/'));
    let mut resp = app
        .http
        .get(&url)
        .send()
        .await
        .map_err(|e| ApiError::Upstream(format!("ipfs gateway: {e}")))?;
    if !resp.status().is_success() {
        return Err(ApiError::Upstream(format!(
            "ipfs gateway returned {}",
            resp.status()
        )));
    }
    let mut body: Vec<u8> = Vec::new();
    while let Some(chunk) = resp
        .chunk()
        .await
        .map_err(|e| ApiError::Upstream(format!("ipfs gateway: {e}")))?
    {
        if body.len() + chunk.len() > IPFS_MAX_BYTES {
            return Err(ApiError::Upstream(
                "ipfs object exceeds the 1 MiB proxy limit".into(),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
        body,
    )
        .into_response())
}

// --- /health --------------------------------------------------------------------------

pub async fn health(State(app): State<AppState>) -> Result<Json<Value>, ApiError> {
    let si = app
        .node
        .get_server_info(proto::GetServerInfoRequestMessage {})
        .await?;
    let indexer = match &app.indexer {
        Some(handle) => handle.json(),
        None => json!("disabled"),
    };
    Ok(Json(json!({
        "status": "ok",
        "node": {
            "server_version": si.server_version,
            "network_id": si.network_id,
            "is_synced": si.is_synced,
            "has_utxo_index": si.has_utxo_index,
            "virtual_daa_score": si.virtual_daa_score,
        },
        "indexer": indexer,
    })))
}

#[cfg(test)]
mod tests {
    use super::is_cid_v0;

    #[test]
    fn cid_v0_validation() {
        assert!(is_cid_v0("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG"));
        assert!(!is_cid_v0("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbd")); // 45 chars
        assert!(!is_cid_v0(
            "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi"
        )); // CIDv1
        assert!(!is_cid_v0("QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdO")); // 'O' not base58
        assert!(!is_cid_v0("../../../etc/passwd")); // traversal attempt
    }
}
