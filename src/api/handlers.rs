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
    // total_txs comes from the indexer's counter when enabled (it counts only
    // transactions seen since the indexer started, not the whole chain's
    // history); 0 otherwise.
    let total_txs = app
        .indexer
        .as_ref()
        .and_then(|h| h.store())
        .and_then(|s| s.total_txs().ok())
        .unwrap_or(0);
    Ok(dto::InfoResponse {
        network: dag.network_name,
        last_daa_score: dag.virtual_daa_score,
        block_reward_krx,
        total_supply_krx: circulating,
        max_supply_krx: max_supply,
        hashrate_hps,
        total_blocks: dag.block_count,
        total_txs,
        // These three still need deeper indexing (escrow/burn accounting); 0 for now.
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
    limit: Option<usize>,
    offset: Option<usize>,
}

const HISTORY_MAX_LIMIT: usize = 1000;

/// Per-address transaction history. Served from the indexer's window when it is
/// enabled; otherwise a well-formed empty page (a bare keryxd keeps no
/// per-address history), which keeps the wallet's dashboard and history views
/// functional. `history_since_daa` lets a client distinguish "no transactions"
/// from "none within the retention window".
pub async fn address_history(
    State(app): State<AppState>,
    Path(address): Path<String>,
    Query(query): Query<HistoryQuery>,
) -> Result<Json<dto::AddressHistoryResponse>, ApiError> {
    dto::validate_address(&address).map_err(ApiError::BadRequest)?;

    let Some(indexer) = app.indexer.as_ref().filter(|h| h.store().is_some()) else {
        return Ok(Json(dto::AddressHistoryResponse {
            address,
            total_received_sompi: 0,
            total_tx_count: 0,
            transactions: vec![],
            history_since_daa: None,
        }));
    };
    let store = indexer.store().unwrap();

    let limit = query.limit.unwrap_or(10).clamp(1, HISTORY_MAX_LIMIT);
    let offset = query.offset.unwrap_or(0);
    let rows = store.address_history(&address, limit, offset)?;
    let (total_received_sompi, total_tx_count) = store.address_totals(&address)?;
    let history_since_daa = store.window_low_daa()?;

    // Prepend unconfirmed (mempool) rows on the first page only, tagged
    // pending with daa_score 0, so the wallet shows relayed txs immediately.
    let mut transactions: Vec<dto::HistoryTx> = Vec::new();
    if offset == 0 {
        for p in indexer.mempool().history(&address) {
            transactions.push(dto::HistoryTx {
                tx_id: p.tx_id,
                amount_sompi: p.amount_sompi,
                is_spend: p.is_spend,
                daa_score: 0,
                block_hash: String::new(),
                address: address.clone(),
                pending: true,
            });
        }
    }
    transactions.extend(rows.into_iter().map(|r| dto::HistoryTx {
        tx_id: r.tx_id,
        amount_sompi: r.amount_sompi,
        is_spend: r.is_spend,
        daa_score: r.daa_score,
        block_hash: r.block_hash,
        address: address.clone(),
        pending: false,
    }));
    Ok(Json(dto::AddressHistoryResponse {
        address,
        total_received_sompi,
        total_tx_count,
        transactions,
        history_since_daa,
    }))
}

// --- /api/v1/transactions/{id} and /api/v1/outpoints/{txid}/{index}/spend ------------

fn is_txid(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Render an indexed transaction in the wallet's wire shape plus acceptance
/// metadata. `payload` is included verbatim (swap-recovery reads redeem-script
/// hints from funding-tx payloads).
fn tx_to_json(itx: &crate::indexer::store::IndexedTx) -> Value {
    Value::Object(serde_json::Map::from_iter([
        ("tx_id".into(), json!(itx.tx_id)),
        ("version".into(), json!(itx.version)),
        (
            "inputs".into(),
            Value::Array(
                itx.inputs
                    .iter()
                    .map(|i| {
                        json!({
                            "transaction_id": i.previous_tx_id,
                            "index": i.previous_index,
                            "signature_script": i.signature_script,
                            // u64::MAX exceeds JS safe ints; match the wallet and emit a string.
                            "sequence": i.sequence.to_string(),
                            "sig_op_count": i.sig_op_count,
                        })
                    })
                    .collect(),
            ),
        ),
        (
            "outputs".into(),
            Value::Array(
                itx.outputs
                    .iter()
                    .map(|o| {
                        json!({
                            "amount": o.amount,
                            "script_version": o.script_version,
                            "script_public_key": o.script_public_key,
                        })
                    })
                    .collect(),
            ),
        ),
        ("lock_time".into(), json!(itx.lock_time)),
        ("subnetwork_id".into(), json!(itx.subnetwork_id)),
        ("gas".into(), json!(itx.gas)),
        ("payload".into(), json!(itx.payload)),
        ("block_hash".into(), json!(itx.accepting_block_hash)),
        ("daa_score".into(), json!(itx.accepting_daa)),
        ("is_coinbase".into(), json!(itx.is_coinbase)),
    ]))
}

fn require_index(app: &AppState) -> Result<&crate::indexer::store::Store, ApiError> {
    app.indexer
        .as_ref()
        .and_then(|h| h.store())
        .ok_or_else(|| ApiError::NotFound("transaction index is not enabled on this shim".into()))
}

pub async fn transaction(
    State(app): State<AppState>,
    Path(tx_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    if !is_txid(&tx_id) {
        return Err(ApiError::BadRequest(
            "transaction id must be 64 hex chars".into(),
        ));
    }
    let store = require_index(&app)?;
    match store.tx_by_id(&tx_id)? {
        Some(itx) => Ok(Json(tx_to_json(&itx))),
        None => Err(ApiError::NotFound(
            "transaction not found within the indexer window".into(),
        )),
    }
}

/// The transaction that spent a given outpoint — the HTLC preimage-extraction
/// path (the preimage is a push in the spending tx's `signature_script`).
pub async fn outpoint_spend(
    State(app): State<AppState>,
    Path((tx_id, index)): Path<(String, u32)>,
) -> Result<Json<Value>, ApiError> {
    if !is_txid(&tx_id) {
        return Err(ApiError::BadRequest(
            "transaction id must be 64 hex chars".into(),
        ));
    }
    let indexer = app
        .indexer
        .as_ref()
        .filter(|h| h.store().is_some())
        .ok_or_else(|| {
            ApiError::NotFound("transaction index is not enabled on this shim".into())
        })?;

    // Mempool first: an HTLC claim's preimage is visible at relay time, before
    // it is mined.
    if let Some(pending) = indexer.mempool().spend_of(&tx_id, index) {
        return Ok(Json(json!({ "status": "mempool", "transaction": pending })));
    }
    match indexer.store().unwrap().spend_of(&tx_id, index)? {
        Some(itx) => Ok(Json(json!({
            "status": "accepted",
            "transaction": tx_to_json(&itx),
        }))),
        None => Err(ApiError::NotFound(
            "outpoint has no known spending transaction in the indexer window".into(),
        )),
    }
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

// --- inference endpoints (phase 2c) -------------------------------------------------
//
// The AI-inference oracle (`/capabilities`, `/infer`, `/challenges`),
// reconstructed from the AI subnetwork transactions (03/04/05) and coinbase
// capability markers by the indexer. Each degrades to `[]` when the indexer is
// off or on any store error — the wallet's inference screen catch-guards these.

fn inference_store(app: &AppState) -> Option<&crate::indexer::store::Store> {
    app.indexer.as_ref().and_then(|h| h.store())
}

fn model_name(model_id: &str) -> String {
    crate::indexer::inference::model_key(model_id)
        .map(String::from)
        .unwrap_or_else(|| model_id.to_string())
}

pub async fn capabilities(State(app): State<AppState>) -> Json<Value> {
    let Some(store) = inference_store(&app) else {
        return Json(json!([]));
    };
    let caps = store.capabilities().unwrap_or_default();
    Json(Value::Array(
        caps.into_iter()
            .map(|c| {
                json!({
                    "model": model_name(&c.model_id),
                    "model_id_hex": c.model_id,
                    "miner_count": c.miner_pubkeys.len(),
                    "last_seen_daa": c.last_seen_daa,
                    "miner_pubkeys": c.miner_pubkeys,
                })
            })
            .collect(),
    ))
}

#[derive(Deserialize)]
pub struct InferQuery {
    limit: Option<usize>,
    offset: Option<usize>,
}

pub async fn infer(State(app): State<AppState>, Query(q): Query<InferQuery>) -> Json<Value> {
    let Some(store) = inference_store(&app) else {
        return Json(json!([]));
    };
    let limit = q.limit.unwrap_or(20).clamp(1, 200);
    let offset = q.offset.unwrap_or(0);
    let feed = store.inference_feed(limit, offset).unwrap_or_default();
    Json(Value::Array(
        feed.into_iter()
            .map(|(req, resp)| {
                json!({
                    "tx_id": req.tx_id,
                    "model": model_name(&req.model_id),
                    "prompt": req.prompt,
                    "max_tokens": req.max_tokens,
                    "inference_reward": req.inference_reward,
                    "priority_fee": req.priority_fee,
                    "daa_score": req.daa_score,
                    "block_hash": req.block_hash,
                    // first 16 hex of request_hash — what the wallet matches
                    // against a challenge's request_hash_hex[..16].
                    "payload_prefix": req.request_hash.chars().take(16).collect::<String>(),
                    "result": resp.as_ref().map(|r| r.cid.clone()),
                    // off-chain; the wallet fetches it via GET /ipfs/{cid}.
                    "result_text": Value::Null,
                    "result_block_hash": resp.as_ref().map(|r| r.result_block_hash.clone()),
                })
            })
            .collect(),
    ))
}

#[derive(Deserialize)]
pub struct ChallengeQuery {
    limit: Option<usize>,
}

pub async fn challenges(
    State(app): State<AppState>,
    Query(q): Query<ChallengeQuery>,
) -> Json<Value> {
    let Some(store) = inference_store(&app) else {
        return Json(json!([]));
    };
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    let list = store.challenges(limit).unwrap_or_default();
    Json(Value::Array(
        list.into_iter()
            .map(|c| {
                json!({
                    "tx_id": c.tx_id,
                    "request_hash_hex": c.request_hash,
                    // keryx-node removed on-chain slashing in v1.2.3, so no
                    // fraud is provable from consensus state — always false.
                    "fraud_proven": false,
                })
            })
            .collect(),
    ))
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
