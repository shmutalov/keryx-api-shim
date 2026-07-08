//! Durable, crash-safe indexer store (redb) — M2.
//!
//! One redb file holds the windowed ledger. Writes are transactional: an
//! entire `VirtualChainChanged` (or a whole backfill range) is applied in a
//! single write transaction, so a crash leaves the checkpoint and all derived
//! rows mutually consistent — restart re-backfills from the committed
//! checkpoint and the idempotent apply (keyed by chain-block hash) makes any
//! overlap a no-op.
//!
//! Retention is a bounded sweep: rows below the DAA cutoff are deleted, but the
//! monotonic counters (`total_txs`, per-address totals) are left intact so the
//! wallet's pagination math stays truthful across expiry. Reorgs unwind via the
//! per-chain-block `accepted_by` list and *do* reverse the counters, since
//! those transactions never really happened.

use std::collections::HashMap;
use std::path::Path;

use redb::{Database, ReadableTable, ReadableTableMetadata, TableDefinition};
use serde::{Deserialize, Serialize};

use super::address::script_to_address;
use super::inference;

// u64 keyed
const TX: TableDefinition<u64, &[u8]> = TableDefinition::new("tx");
const TX_BY_ID: TableDefinition<&str, u64> = TableDefinition::new("tx_by_id");
const APPLIED: TableDefinition<&str, u64> = TableDefinition::new("applied_blocks");
// &[u8] keyed (composite)
const ADDR_HISTORY: TableDefinition<&[u8], &[u8]> = TableDefinition::new("addr_history");
const OUTPOINT_SPEND: TableDefinition<&[u8], u64> = TableDefinition::new("outpoint_spend");
const CHAIN_BY_DAA: TableDefinition<&[u8], &str> = TableDefinition::new("chain_by_daa");
// &str keyed
const ADDR_TOTALS: TableDefinition<&str, &[u8]> = TableDefinition::new("addr_totals");
const ACCEPTED_BY: TableDefinition<&str, &[u8]> = TableDefinition::new("accepted_by");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta_u64");
const META_STR: TableDefinition<&str, &str> = TableDefinition::new("meta_str");
// AI inference subsystem (phase 2c)
const AI_REQ: TableDefinition<u64, &[u8]> = TableDefinition::new("ai_req"); // req_seq -> AiRequestRec
const AI_REQ_BY_HASH: TableDefinition<&str, u64> = TableDefinition::new("ai_req_by_hash"); // request_hash -> seq
const AI_RESP: TableDefinition<&str, &[u8]> = TableDefinition::new("ai_resp"); // request_hash -> AiResponseRec
const AI_RESP_HASH: TableDefinition<&str, &str> = TableDefinition::new("ai_resp_hash"); // response_hash -> request_hash
const AI_CHAL: TableDefinition<u64, &[u8]> = TableDefinition::new("ai_chal"); // chal_seq -> AiChallengeRec
const AI_CAPS: TableDefinition<&[u8], u64> = TableDefinition::new("ai_caps"); // model_id||FF||pubkey -> last_seen_daa

const SCHEMA_VERSION: u64 = 1;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("index db: {0}")]
    Db(String),
    #[error("index decode: {0}")]
    Decode(String),
}

macro_rules! db_err {
    ($($t:ty),+) => {$(
        impl From<$t> for StoreError {
            fn from(e: $t) -> Self { StoreError::Db(e.to_string()) }
        }
    )+};
}
db_err!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError
);

// --- plain data (bincode in redb values) --------------------------------------

/// The wire fields of one accepted transaction, as the indexer hands them in.
#[derive(Clone)]
pub struct RawTx {
    pub tx_id: String,
    pub is_coinbase: bool,
    pub version: u32,
    pub lock_time: u64,
    pub subnetwork_id: String,
    pub gas: u64,
    pub payload: String,
    pub inputs: Vec<RawIn>,
    pub outputs: Vec<RawOut>,
}

#[derive(Clone)]
pub struct RawIn {
    pub previous_tx_id: String,
    pub previous_index: u32,
    pub signature_script: String,
    pub sequence: u64,
    pub sig_op_count: u32,
}

#[derive(Clone)]
pub struct RawOut {
    pub amount: u64,
    pub script_version: u32,
    pub script_public_key: String,
}

/// All transactions accepted by one chain block.
pub struct AcceptedGroup {
    pub chain_block_hash: String,
    pub daa_score: u64,
    pub txs: Vec<RawTx>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StoredIn {
    pub previous_tx_id: String,
    pub previous_index: u32,
    pub signature_script: String,
    pub sequence: u64,
    pub sig_op_count: u32,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct StoredOut {
    pub amount: u64,
    pub script_version: u32,
    pub script_public_key: String,
}

/// One address's net effect from a transaction; kept on the tx so expiry and
/// unwind can find every derived row (and reverse the totals) without needing
/// the — possibly already-expired — funding transactions.
#[derive(Serialize, Deserialize, Clone)]
pub struct LedgerEntry {
    pub address: String,
    pub credit: u64,
    pub debit: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct IndexedTx {
    pub tx_num: u64,
    pub tx_id: String,
    pub accepting_block_hash: String,
    pub accepting_daa: u64,
    pub is_coinbase: bool,
    pub version: u32,
    pub lock_time: u64,
    pub subnetwork_id: String,
    pub gas: u64,
    pub payload: String,
    pub inputs: Vec<StoredIn>,
    pub outputs: Vec<StoredOut>,
    pub ledger: Vec<LedgerEntry>,
}

/// One row of an address's history (newest-first when read).
#[derive(Serialize, Deserialize, Clone)]
pub struct HistRow {
    pub tx_id: String,
    pub amount_sompi: u64,
    pub is_spend: bool,
    pub daa_score: u64,
    pub block_hash: String,
}

/// An indexed AI inference request (subnetwork 03), joined to its response
/// (if seen) at read time.
#[derive(Serialize, Deserialize, Clone)]
pub struct AiRequestRec {
    pub tx_id: String,
    pub model_id: String,
    pub max_tokens: u32,
    pub inference_reward: u64,
    pub priority_fee: u64,
    pub prompt: String,
    pub daa_score: u64,
    pub block_hash: String,
    pub request_hash: String,
    pub miner_pubkey: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AiResponseRec {
    pub cid: String,
    pub result_block_hash: String,
    pub response_length: u32,
    pub daa_score: u64,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AiChallengeRec {
    pub tx_id: String,
    pub request_hash: String,
    pub daa_score: u64,
}

/// One row of `/capabilities`: a model and the miners that declared it.
pub struct Capability {
    pub model_id: String,
    pub miner_pubkeys: Vec<String>,
    pub last_seen_daa: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    pub chain_block_hash: String,
    pub daa_score: u64,
}

#[derive(Default, Debug, Clone, Copy)]
pub struct ApplyStats {
    pub applied_blocks: u64,
    pub applied_txs: u64,
    pub total_txs: u64,
    pub checkpoint_daa: u64,
}

// --- key encoding -------------------------------------------------------------
// Addresses/txids are bech32/hex (no 0xFF byte), so 0xFF is a safe separator.

fn outpoint_key(txid: &str, index: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(txid.len() + 5);
    k.extend_from_slice(txid.as_bytes());
    k.push(0xFF);
    k.extend_from_slice(&index.to_be_bytes());
    k
}

fn hist_key(addr: &str, tx_num: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(addr.len() + 9);
    k.extend_from_slice(addr.as_bytes());
    k.push(0xFF);
    k.extend_from_slice(&tx_num.to_be_bytes());
    k
}

fn hist_bounds(addr: &str) -> (Vec<u8>, Vec<u8>) {
    let mut lo = Vec::with_capacity(addr.len() + 9);
    lo.extend_from_slice(addr.as_bytes());
    lo.push(0xFF);
    lo.extend_from_slice(&[0u8; 8]);
    let mut hi = Vec::with_capacity(addr.len() + 9);
    hi.extend_from_slice(addr.as_bytes());
    hi.push(0xFF);
    hi.extend_from_slice(&[0xFFu8; 8]);
    (lo, hi)
}

fn chain_key(daa: u64, hash: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(8 + hash.len());
    k.extend_from_slice(&daa.to_be_bytes());
    k.extend_from_slice(hash.as_bytes());
    k
}

fn caps_key(model_id: &str, pubkey: &str) -> Vec<u8> {
    let mut k = Vec::with_capacity(model_id.len() + 1 + pubkey.len());
    k.extend_from_slice(model_id.as_bytes());
    k.push(0xFF);
    k.extend_from_slice(pubkey.as_bytes());
    k
}

fn enc<T: Serialize>(v: &T) -> Vec<u8> {
    bincode::serialize(v).expect("bincode encode is infallible for our types")
}

fn dec<T: serde::de::DeserializeOwned>(b: &[u8]) -> Result<T, StoreError> {
    bincode::deserialize(b).map_err(|e| StoreError::Decode(e.to_string()))
}

// --- store --------------------------------------------------------------------

pub struct Store {
    db: Database,
    prefix: String,
}

impl Store {
    /// Open (creating if needed) the index db under `dir`, attributing
    /// addresses with `network_prefix` (e.g. "keryxsim").
    pub fn open(dir: &Path, network_prefix: &str) -> Result<Self, StoreError> {
        std::fs::create_dir_all(dir).map_err(|e| StoreError::Db(e.to_string()))?;
        let db = Database::create(dir.join("index.redb"))?;
        // Create every table up front so read transactions never miss one, and
        // stamp the schema version.
        let write = db.begin_write()?;
        {
            write.open_table(TX)?;
            write.open_table(TX_BY_ID)?;
            write.open_table(APPLIED)?;
            write.open_table(ADDR_HISTORY)?;
            write.open_table(OUTPOINT_SPEND)?;
            write.open_table(CHAIN_BY_DAA)?;
            write.open_table(ADDR_TOTALS)?;
            write.open_table(ACCEPTED_BY)?;
            let mut meta = write.open_table(META)?;
            if meta.get("schema")?.is_none() {
                meta.insert("schema", SCHEMA_VERSION)?;
            }
            write.open_table(META_STR)?;
            write.open_table(AI_REQ)?;
            write.open_table(AI_REQ_BY_HASH)?;
            write.open_table(AI_RESP)?;
            write.open_table(AI_RESP_HASH)?;
            write.open_table(AI_CHAL)?;
            write.open_table(AI_CAPS)?;
        }
        write.commit()?;
        Ok(Self {
            db,
            prefix: network_prefix.to_string(),
        })
    }

    // --- writes ---

    /// Apply accepted groups (low-to-high) in one transaction. Idempotent per
    /// chain-block hash.
    pub fn apply(&self, groups: &[AcceptedGroup]) -> Result<ApplyStats, StoreError> {
        let mut stats = ApplyStats::default();
        let write = self.db.begin_write()?;
        {
            let mut tx_t = write.open_table(TX)?;
            let mut txid_t = write.open_table(TX_BY_ID)?;
            let mut hist_t = write.open_table(ADDR_HISTORY)?;
            let mut totals_t = write.open_table(ADDR_TOTALS)?;
            let mut spend_t = write.open_table(OUTPOINT_SPEND)?;
            let mut applied_t = write.open_table(APPLIED)?;
            let mut chain_t = write.open_table(CHAIN_BY_DAA)?;
            let mut acc_t = write.open_table(ACCEPTED_BY)?;
            let mut meta = write.open_table(META)?;
            let mut meta_str = write.open_table(META_STR)?;
            let mut ai_req_t = write.open_table(AI_REQ)?;
            let mut ai_req_hash_t = write.open_table(AI_REQ_BY_HASH)?;
            let mut ai_resp_t = write.open_table(AI_RESP)?;
            let mut ai_resp_hash_t = write.open_table(AI_RESP_HASH)?;
            let mut ai_chal_t = write.open_table(AI_CHAL)?;
            let mut ai_caps_t = write.open_table(AI_CAPS)?;

            let mut next_tx_num = meta.get("next_tx_num")?.map(|g| g.value()).unwrap_or(0);
            let mut total_txs = meta.get("total_txs")?.map(|g| g.value()).unwrap_or(0);
            let mut ai_req_seq = meta.get("ai_req_seq")?.map(|g| g.value()).unwrap_or(0);
            let mut ai_chal_seq = meta.get("ai_chal_seq")?.map(|g| g.value()).unwrap_or(0);
            let mut checkpoint: Option<(String, u64)> = None;

            for group in groups {
                if applied_t.get(group.chain_block_hash.as_str())?.is_some() {
                    continue; // already applied — idempotent
                }
                let mut tx_nums = Vec::with_capacity(group.txs.len());
                for raw in &group.txs {
                    let tx_num = next_tx_num;
                    next_tx_num += 1;

                    let ledger = compute_ledger(raw, &self.prefix, &txid_t, &tx_t)?;
                    let itx = build_indexed(raw, tx_num, group, ledger);

                    tx_t.insert(tx_num, enc(&itx).as_slice())?;
                    txid_t.insert(raw.tx_id.as_str(), tx_num)?;

                    if !raw.is_coinbase {
                        for inp in &raw.inputs {
                            spend_t.insert(
                                outpoint_key(&inp.previous_tx_id, inp.previous_index).as_slice(),
                                tx_num,
                            )?;
                        }
                    }

                    for e in &itx.ledger {
                        let net = e.credit as i64 - e.debit as i64;
                        let row = HistRow {
                            tx_id: raw.tx_id.clone(),
                            amount_sompi: net.unsigned_abs(),
                            is_spend: net < 0,
                            daa_score: group.daa_score,
                            block_hash: group.chain_block_hash.clone(),
                        };
                        hist_t.insert(
                            hist_key(&e.address, tx_num).as_slice(),
                            enc(&row).as_slice(),
                        )?;
                        let (mut recv, mut cnt) = read_totals(&totals_t, &e.address)?;
                        recv += e.credit;
                        cnt += 1;
                        totals_t.insert(e.address.as_str(), enc(&(recv, cnt)).as_slice())?;
                    }

                    // AI inference subsystem (phase 2c): extract from the same
                    // pass. Gated by the per-block idempotency check above, so a
                    // replay of an applied block never double-indexes.
                    match raw.subnetwork_id.as_str() {
                        inference::SUBNET_AI_REQUEST => {
                            if let Some(bytes) = inference::from_hex(&raw.payload) {
                                if let Some(req) = inference::decode_request(&bytes) {
                                    let rhash = inference::request_hash(&bytes);
                                    if ai_req_hash_t.get(rhash.as_str())?.is_none() {
                                        let miner_pubkey = raw.outputs.get(1).and_then(|o| {
                                            inference::csv_escrow_pubkey(&o.script_public_key)
                                        });
                                        let rec = AiRequestRec {
                                            tx_id: raw.tx_id.clone(),
                                            model_id: req.model_id,
                                            max_tokens: req.max_tokens,
                                            inference_reward: req.inference_reward,
                                            priority_fee: req.priority_fee,
                                            prompt: req.prompt,
                                            daa_score: group.daa_score,
                                            block_hash: group.chain_block_hash.clone(),
                                            request_hash: rhash.clone(),
                                            miner_pubkey,
                                        };
                                        ai_req_t.insert(ai_req_seq, enc(&rec).as_slice())?;
                                        ai_req_hash_t.insert(rhash.as_str(), ai_req_seq)?;
                                        ai_req_seq += 1;
                                    }
                                }
                            }
                        }
                        inference::SUBNET_AI_RESPONSE => {
                            if let Some(bytes) = inference::from_hex(&raw.payload) {
                                if let Some(resp) = inference::decode_response(&bytes) {
                                    let resp_hash = inference::request_hash(&bytes);
                                    let rec = AiResponseRec {
                                        cid: resp.cid,
                                        result_block_hash: group.chain_block_hash.clone(),
                                        response_length: resp.response_length,
                                        daa_score: group.daa_score,
                                    };
                                    ai_resp_t
                                        .insert(resp.request_hash.as_str(), enc(&rec).as_slice())?;
                                    ai_resp_hash_t
                                        .insert(resp_hash.as_str(), resp.request_hash.as_str())?;
                                }
                            }
                        }
                        inference::SUBNET_AI_CHALLENGE => {
                            if let Some(bytes) = inference::from_hex(&raw.payload) {
                                if let Some(chal) = inference::decode_challenge(&bytes) {
                                    let request_hash = chal
                                        .request_hash
                                        .or_else(|| {
                                            ai_resp_hash_t
                                                .get(chal.response_hash.as_str())
                                                .ok()
                                                .flatten()
                                                .map(|g| g.value().to_string())
                                        })
                                        .unwrap_or_default();
                                    let rec = AiChallengeRec {
                                        tx_id: raw.tx_id.clone(),
                                        request_hash,
                                        daa_score: group.daa_score,
                                    };
                                    ai_chal_t.insert(ai_chal_seq, enc(&rec).as_slice())?;
                                    ai_chal_seq += 1;
                                }
                            }
                        }
                        _ => {}
                    }
                    if raw.is_coinbase {
                        if let Some(bytes) = inference::from_hex(&raw.payload) {
                            let models = inference::parse_ai_caps(&bytes);
                            if !models.is_empty() {
                                let pubkey = inference::parse_escrow_pubkey(&bytes).or_else(|| {
                                    raw.outputs
                                        .first()
                                        .and_then(|o| inference::p2pk_pubkey(&o.script_public_key))
                                });
                                if let Some(pk) = pubkey {
                                    for model_id in models {
                                        ai_caps_t.insert(
                                            caps_key(&model_id, &pk).as_slice(),
                                            group.daa_score,
                                        )?;
                                    }
                                }
                            }
                        }
                    }

                    tx_nums.push(tx_num);
                    total_txs += 1;
                    stats.applied_txs += 1;
                }

                acc_t.insert(group.chain_block_hash.as_str(), enc(&tx_nums).as_slice())?;
                applied_t.insert(group.chain_block_hash.as_str(), group.daa_score)?;
                chain_t.insert(
                    chain_key(group.daa_score, &group.chain_block_hash).as_slice(),
                    group.chain_block_hash.as_str(),
                )?;
                checkpoint = Some((group.chain_block_hash.clone(), group.daa_score));
                stats.applied_blocks += 1;
            }

            meta.insert("next_tx_num", next_tx_num)?;
            meta.insert("total_txs", total_txs)?;
            meta.insert("ai_req_seq", ai_req_seq)?;
            meta.insert("ai_chal_seq", ai_chal_seq)?;
            if let Some((hash, daa)) = checkpoint {
                meta_str.insert("checkpoint_hash", hash.as_str())?;
                meta.insert("checkpoint_daa", daa)?;
                stats.checkpoint_daa = daa;
            }
            stats.total_txs = total_txs;
        }
        write.commit()?;
        Ok(stats)
    }

    /// Reverse a chain block removed by a reorg: delete its transactions and
    /// their derived rows, and roll the counters back (these transactions did
    /// not really happen).
    pub fn unwind_chain_block(&self, hash: &str) -> Result<u64, StoreError> {
        let mut removed = 0u64;
        let write = self.db.begin_write()?;
        {
            let mut tx_t = write.open_table(TX)?;
            let mut txid_t = write.open_table(TX_BY_ID)?;
            let mut hist_t = write.open_table(ADDR_HISTORY)?;
            let mut totals_t = write.open_table(ADDR_TOTALS)?;
            let mut spend_t = write.open_table(OUTPOINT_SPEND)?;
            let mut applied_t = write.open_table(APPLIED)?;
            let mut chain_t = write.open_table(CHAIN_BY_DAA)?;
            let mut acc_t = write.open_table(ACCEPTED_BY)?;
            let mut meta = write.open_table(META)?;

            let tx_nums: Vec<u64> = match acc_t.get(hash)? {
                Some(g) => dec(g.value())?,
                None => return Ok(0), // never applied
            };
            let mut total_txs = meta.get("total_txs")?.map(|g| g.value()).unwrap_or(0);
            let daa = applied_t.get(hash)?.map(|g| g.value());

            for tx_num in tx_nums.iter().rev() {
                let itx: IndexedTx = match tx_t.get(tx_num)? {
                    Some(g) => dec(g.value())?,
                    None => continue,
                };
                for e in &itx.ledger {
                    hist_t.remove(hist_key(&e.address, *tx_num).as_slice())?;
                    let (recv, cnt) = read_totals(&totals_t, &e.address)?;
                    let recv = recv.saturating_sub(e.credit);
                    let cnt = cnt.saturating_sub(1);
                    if recv == 0 && cnt == 0 {
                        totals_t.remove(e.address.as_str())?;
                    } else {
                        totals_t.insert(e.address.as_str(), enc(&(recv, cnt)).as_slice())?;
                    }
                }
                if !itx.is_coinbase {
                    for inp in &itx.inputs {
                        spend_t.remove(
                            outpoint_key(&inp.previous_tx_id, inp.previous_index).as_slice(),
                        )?;
                    }
                }
                tx_t.remove(tx_num)?;
                txid_t.remove(itx.tx_id.as_str())?;
                total_txs = total_txs.saturating_sub(1);
                removed += 1;
            }

            acc_t.remove(hash)?;
            applied_t.remove(hash)?;
            if let Some(daa) = daa {
                chain_t.remove(chain_key(daa, hash).as_slice())?;
            }
            meta.insert("total_txs", total_txs)?;
        }
        write.commit()?;
        Ok(removed)
    }

    /// Roll the checkpoint back to `(hash, daa)` after an unwind that removed
    /// the current checkpoint block (used when a reorg has no replacement
    /// blocks in the same notification — vanishingly rare on a live DAG, but
    /// kept so the recovery path exists).
    #[allow(dead_code)]
    pub fn set_checkpoint(&self, hash: &str, daa: u64) -> Result<(), StoreError> {
        let write = self.db.begin_write()?;
        {
            let mut meta = write.open_table(META)?;
            let mut meta_str = write.open_table(META_STR)?;
            meta_str.insert("checkpoint_hash", hash)?;
            meta.insert("checkpoint_daa", daa)?;
        }
        write.commit()?;
        Ok(())
    }

    /// Delete all rows for transactions whose accepting DAA is below `cutoff`.
    /// Monotonic counters are intentionally left untouched. Returns the number
    /// of transactions dropped.
    pub fn expire_below(&self, cutoff_daa: u64) -> Result<u64, StoreError> {
        let mut removed = 0u64;
        let write = self.db.begin_write()?;
        {
            let mut tx_t = write.open_table(TX)?;
            let mut txid_t = write.open_table(TX_BY_ID)?;
            let mut hist_t = write.open_table(ADDR_HISTORY)?;
            let mut spend_t = write.open_table(OUTPOINT_SPEND)?;
            let mut applied_t = write.open_table(APPLIED)?;
            let mut chain_t = write.open_table(CHAIN_BY_DAA)?;
            let mut acc_t = write.open_table(ACCEPTED_BY)?;

            // Collect expired txs. tx_num is monotonic with accepting DAA (groups
            // apply low-to-high and the cutoff sits far below any reorg depth),
            // so the first tx at/above the cutoff ends the scan.
            let mut expired: Vec<(u64, IndexedTx)> = Vec::new();
            {
                for item in tx_t.iter()? {
                    let (k, v) = item?;
                    let itx: IndexedTx = dec(v.value())?;
                    if itx.accepting_daa >= cutoff_daa {
                        break;
                    }
                    expired.push((k.value(), itx));
                }
            }
            for (tx_num, itx) in &expired {
                for e in &itx.ledger {
                    hist_t.remove(hist_key(&e.address, *tx_num).as_slice())?;
                }
                if !itx.is_coinbase {
                    for inp in &itx.inputs {
                        spend_t.remove(
                            outpoint_key(&inp.previous_tx_id, inp.previous_index).as_slice(),
                        )?;
                    }
                }
                tx_t.remove(tx_num)?;
                txid_t.remove(itx.tx_id.as_str())?;
                removed += 1;
            }

            // Prune chain-block metadata below the cutoff (covers empty blocks too).
            let mut expired_chain: Vec<(Vec<u8>, String)> = Vec::new();
            {
                for item in chain_t.iter()? {
                    let (k, v) = item?;
                    let key = k.value().to_vec();
                    let daa = u64::from_be_bytes(key[..8].try_into().unwrap());
                    if daa >= cutoff_daa {
                        break;
                    }
                    expired_chain.push((key, v.value().to_string()));
                }
            }
            for (key, hash) in &expired_chain {
                chain_t.remove(key.as_slice())?;
                applied_t.remove(hash.as_str())?;
                acc_t.remove(hash.as_str())?;
            }

            // Inference tables (low volume) — drop entries below the cutoff.
            let mut ai_req_t = write.open_table(AI_REQ)?;
            let mut ai_req_hash_t = write.open_table(AI_REQ_BY_HASH)?;
            let mut ai_resp_t = write.open_table(AI_RESP)?;
            let mut ai_resp_hash_t = write.open_table(AI_RESP_HASH)?;
            let mut ai_chal_t = write.open_table(AI_CHAL)?;
            let mut ai_caps_t = write.open_table(AI_CAPS)?;

            let mut exp_req: Vec<(u64, String)> = Vec::new();
            {
                for item in ai_req_t.iter()? {
                    let (k, v) = item?;
                    let rec: AiRequestRec = dec(v.value())?;
                    if rec.daa_score < cutoff_daa {
                        exp_req.push((k.value(), rec.request_hash));
                    }
                }
            }
            for (seq, rhash) in &exp_req {
                ai_req_t.remove(seq)?;
                ai_req_hash_t.remove(rhash.as_str())?;
                ai_resp_t.remove(rhash.as_str())?;
            }
            // Response-hash mappings whose request is gone.
            let mut exp_rh: Vec<String> = Vec::new();
            {
                for item in ai_resp_hash_t.iter()? {
                    let (k, v) = item?;
                    if ai_req_hash_t.get(v.value())?.is_none() {
                        exp_rh.push(k.value().to_string());
                    }
                }
            }
            for rh in &exp_rh {
                ai_resp_hash_t.remove(rh.as_str())?;
            }
            let mut exp_chal: Vec<u64> = Vec::new();
            {
                for item in ai_chal_t.iter()? {
                    let (k, v) = item?;
                    let rec: AiChallengeRec = dec(v.value())?;
                    if rec.daa_score < cutoff_daa {
                        exp_chal.push(k.value());
                    }
                }
            }
            for seq in &exp_chal {
                ai_chal_t.remove(seq)?;
            }
            let mut exp_caps: Vec<Vec<u8>> = Vec::new();
            {
                for item in ai_caps_t.iter()? {
                    let (k, v) = item?;
                    if v.value() < cutoff_daa {
                        exp_caps.push(k.value().to_vec());
                    }
                }
            }
            for key in &exp_caps {
                ai_caps_t.remove(key.as_slice())?;
            }
        }
        write.commit()?;
        Ok(removed)
    }

    // --- reads ---

    pub fn checkpoint(&self) -> Result<Option<Checkpoint>, StoreError> {
        let read = self.db.begin_read()?;
        let meta = read.open_table(META)?;
        let meta_str = read.open_table(META_STR)?;
        let hash = meta_str
            .get("checkpoint_hash")?
            .map(|g| g.value().to_string());
        let daa = meta.get("checkpoint_daa")?.map(|g| g.value());
        Ok(match (hash, daa) {
            (Some(chain_block_hash), Some(daa_score)) => Some(Checkpoint {
                chain_block_hash,
                daa_score,
            }),
            _ => None,
        })
    }

    /// Whether a chain block has already been folded in (idempotency probe).
    /// Used by tests and available for future reconciliation logic.
    #[allow(dead_code)]
    pub fn is_applied(&self, hash: &str) -> Result<bool, StoreError> {
        let read = self.db.begin_read()?;
        Ok(read.open_table(APPLIED)?.get(hash)?.is_some())
    }

    pub fn total_txs(&self) -> Result<u64, StoreError> {
        let read = self.db.begin_read()?;
        Ok(read
            .open_table(META)?
            .get("total_txs")?
            .map(|g| g.value())
            .unwrap_or(0))
    }

    pub fn chain_blocks(&self) -> Result<u64, StoreError> {
        let read = self.db.begin_read()?;
        Ok(read.open_table(APPLIED)?.len()?)
    }

    /// Oldest accepting DAA still stored — the low edge of the retention window,
    /// so a client can tell "no transactions" from "none within the window".
    pub fn window_low_daa(&self) -> Result<Option<u64>, StoreError> {
        let read = self.db.begin_read()?;
        let tx_t = read.open_table(TX)?;
        let mut low = None;
        {
            let mut iter = tx_t.iter()?;
            if let Some(item) = iter.next() {
                let (_k, v) = item?;
                low = Some(dec::<IndexedTx>(v.value())?.accepting_daa);
            }
        }
        Ok(low)
    }

    pub fn address_totals(&self, addr: &str) -> Result<(u64, u64), StoreError> {
        let read = self.db.begin_read()?;
        read_totals(&read.open_table(ADDR_TOTALS)?, addr)
    }

    pub fn address_history(
        &self,
        addr: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<HistRow>, StoreError> {
        let read = self.db.begin_read()?;
        let t = read.open_table(ADDR_HISTORY)?;
        let (lo, hi) = hist_bounds(addr);
        let mut out = Vec::new();
        let mut skipped = 0usize;
        for item in t.range(lo.as_slice()..=hi.as_slice())?.rev() {
            let (_k, v) = item?;
            if skipped < offset {
                skipped += 1;
                continue;
            }
            if out.len() >= limit {
                break;
            }
            out.push(dec::<HistRow>(v.value())?);
        }
        Ok(out)
    }

    pub fn tx_by_id(&self, tx_id: &str) -> Result<Option<IndexedTx>, StoreError> {
        let read = self.db.begin_read()?;
        let txid_t = read.open_table(TX_BY_ID)?;
        let Some(tx_num) = txid_t.get(tx_id)?.map(|g| g.value()) else {
            return Ok(None);
        };
        let tx_t = read.open_table(TX)?;
        match tx_t.get(tx_num)? {
            Some(g) => Ok(Some(dec(g.value())?)),
            None => Ok(None),
        }
    }

    /// The transaction that spent a given outpoint, if the shim has seen it.
    pub fn spend_of(&self, txid: &str, index: u32) -> Result<Option<IndexedTx>, StoreError> {
        let read = self.db.begin_read()?;
        let spend_t = read.open_table(OUTPOINT_SPEND)?;
        let Some(tx_num) = spend_t
            .get(outpoint_key(txid, index).as_slice())?
            .map(|g| g.value())
        else {
            return Ok(None);
        };
        let tx_t = read.open_table(TX)?;
        match tx_t.get(tx_num)? {
            Some(g) => Ok(Some(dec(g.value())?)),
            None => Ok(None),
        }
    }

    // --- AI inference reads (phase 2c) ---

    /// Newest-first inference requests, each joined with its response (if the
    /// miner has published one yet).
    pub fn inference_feed(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<(AiRequestRec, Option<AiResponseRec>)>, StoreError> {
        let read = self.db.begin_read()?;
        let req_t = read.open_table(AI_REQ)?;
        let resp_t = read.open_table(AI_RESP)?;
        let mut out = Vec::new();
        let mut skipped = 0usize;
        for item in req_t.iter()?.rev() {
            let (_k, v) = item?;
            if skipped < offset {
                skipped += 1;
                continue;
            }
            if out.len() >= limit {
                break;
            }
            let req: AiRequestRec = dec(v.value())?;
            let resp = match resp_t.get(req.request_hash.as_str())? {
                Some(g) => Some(dec::<AiResponseRec>(g.value())?),
                None => None,
            };
            out.push((req, resp));
        }
        Ok(out)
    }

    /// Newest-first fraud challenges.
    pub fn challenges(&self, limit: usize) -> Result<Vec<AiChallengeRec>, StoreError> {
        let read = self.db.begin_read()?;
        let chal_t = read.open_table(AI_CHAL)?;
        let mut out = Vec::new();
        for item in chal_t.iter()?.rev() {
            if out.len() >= limit {
                break;
            }
            let (_k, v) = item?;
            out.push(dec::<AiChallengeRec>(v.value())?);
        }
        Ok(out)
    }

    /// Declared model capabilities, aggregated from coinbase markers.
    pub fn capabilities(&self) -> Result<Vec<Capability>, StoreError> {
        let read = self.db.begin_read()?;
        let caps_t = read.open_table(AI_CAPS)?;
        let mut by_model: HashMap<String, (Vec<String>, u64)> = HashMap::new();
        for item in caps_t.iter()? {
            let (k, v) = item?;
            let key = k.value();
            let Some(sep) = key.iter().position(|&b| b == 0xFF) else {
                continue;
            };
            let model_id = String::from_utf8_lossy(&key[..sep]).into_owned();
            let pubkey = String::from_utf8_lossy(&key[sep + 1..]).into_owned();
            let last_seen = v.value();
            let entry = by_model.entry(model_id).or_insert_with(|| (Vec::new(), 0));
            entry.0.push(pubkey);
            entry.1 = entry.1.max(last_seen);
        }
        let mut out: Vec<Capability> = by_model
            .into_iter()
            .map(|(model_id, (mut pubkeys, last_seen_daa))| {
                pubkeys.sort();
                pubkeys.dedup();
                Capability {
                    model_id,
                    miner_pubkeys: pubkeys,
                    last_seen_daa,
                }
            })
            .collect();
        out.sort_by_key(|c| std::cmp::Reverse(c.last_seen_daa));
        Ok(out)
    }
}

fn read_totals(
    t: &impl ReadableTable<&'static str, &'static [u8]>,
    addr: &str,
) -> Result<(u64, u64), StoreError> {
    match t.get(addr)? {
        Some(g) => dec(g.value()),
        None => Ok((0, 0)),
    }
}

fn build_indexed(
    raw: &RawTx,
    tx_num: u64,
    group: &AcceptedGroup,
    ledger: Vec<LedgerEntry>,
) -> IndexedTx {
    IndexedTx {
        tx_num,
        tx_id: raw.tx_id.clone(),
        accepting_block_hash: group.chain_block_hash.clone(),
        accepting_daa: group.daa_score,
        is_coinbase: raw.is_coinbase,
        version: raw.version,
        lock_time: raw.lock_time,
        subnetwork_id: raw.subnetwork_id.clone(),
        gas: raw.gas,
        payload: raw.payload.clone(),
        inputs: raw
            .inputs
            .iter()
            .map(|i| StoredIn {
                previous_tx_id: i.previous_tx_id.clone(),
                previous_index: i.previous_index,
                signature_script: i.signature_script.clone(),
                sequence: i.sequence,
                sig_op_count: i.sig_op_count,
            })
            .collect(),
        outputs: raw
            .outputs
            .iter()
            .map(|o| StoredOut {
                amount: o.amount,
                script_version: o.script_version,
                script_public_key: o.script_public_key.clone(),
            })
            .collect(),
        ledger,
    }
}

/// Per-address credit/debit for one transaction. Debits are resolved by looking
/// up each input's funding transaction in the index; a funding tx outside the
/// window is simply not attributed (the debit is skipped) — this self-heals
/// after one full window of operation, and never affects spend detection.
fn compute_ledger(
    raw: &RawTx,
    prefix: &str,
    txid_t: &impl ReadableTable<&'static str, u64>,
    tx_t: &impl ReadableTable<u64, &'static [u8]>,
) -> Result<Vec<LedgerEntry>, StoreError> {
    let mut acc: HashMap<String, (u64, u64)> = HashMap::new();
    for out in &raw.outputs {
        if let Some(addr) = script_to_address(&out.script_public_key, prefix) {
            acc.entry(addr).or_default().0 += out.amount;
        }
    }
    if !raw.is_coinbase {
        for inp in &raw.inputs {
            let Some(tx_num) = txid_t.get(inp.previous_tx_id.as_str())?.map(|g| g.value()) else {
                continue;
            };
            let Some(bytes) = tx_t.get(tx_num)?.map(|g| g.value().to_vec()) else {
                continue;
            };
            let funding: IndexedTx = dec(&bytes)?;
            if let Some(o) = funding.outputs.get(inp.previous_index as usize) {
                if let Some(addr) = script_to_address(&o.script_public_key, prefix) {
                    acc.entry(addr).or_default().1 += o.amount;
                }
            }
        }
    }
    Ok(acc
        .into_iter()
        .map(|(address, (credit, debit))| LedgerEntry {
            address,
            credit,
            debit,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    const PK1: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const PK2: &str = "2222222222222222222222222222222222222222222222222222222222222222";

    fn temp_store() -> Store {
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "keryx-shim-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        Store::open(&dir, "keryxsim").unwrap()
    }

    fn p2pk(pk: &str) -> String {
        format!("20{pk}ac")
    }

    fn coinbase(tx_id: &str, to_pk: &str, amount: u64) -> RawTx {
        RawTx {
            tx_id: tx_id.into(),
            is_coinbase: true,
            version: 0,
            lock_time: 0,
            subnetwork_id: "01".repeat(20),
            gas: 0,
            payload: String::new(),
            inputs: vec![],
            outputs: vec![RawOut {
                amount,
                script_version: 0,
                script_public_key: p2pk(to_pk),
            }],
        }
    }

    fn spend(tx_id: &str, prev: &str, prev_idx: u32, to_pk: &str, amount: u64) -> RawTx {
        RawTx {
            tx_id: tx_id.into(),
            is_coinbase: false,
            version: 0,
            lock_time: 0,
            subnetwork_id: "00".repeat(20),
            gas: 0,
            payload: String::new(),
            inputs: vec![RawIn {
                previous_tx_id: prev.into(),
                previous_index: prev_idx,
                signature_script: "41aa".into(),
                sequence: u64::MAX,
                sig_op_count: 1,
            }],
            outputs: vec![RawOut {
                amount,
                script_version: 0,
                script_public_key: p2pk(to_pk),
            }],
        }
    }

    fn group(hash: &str, daa: u64, txs: Vec<RawTx>) -> AcceptedGroup {
        AcceptedGroup {
            chain_block_hash: hash.into(),
            daa_score: daa,
            txs,
        }
    }

    fn addr(pk: &str) -> String {
        crate::indexer::address::script_to_address(&format!("20{pk}ac"), "keryxsim").unwrap()
    }

    // Ingest throughput sanity check (run with: cargo test --release perf_ -- --ignored --nocapture).
    // The plan's gate is >= 100 blocks/s; one coinbase per block, batched per
    // apply as the live pipeline does.
    #[test]
    #[ignore]
    fn perf_ingest_throughput() {
        let s = temp_store();
        let blocks = 20_000u64;
        let batch = 500u64;
        let start = std::time::Instant::now();
        let mut daa = 0u64;
        let mut n = 0u64;
        while n < blocks {
            let groups: Vec<AcceptedGroup> = (0..batch)
                .map(|_| {
                    daa += 1;
                    group(
                        &format!("blk{daa}"),
                        daa,
                        vec![coinbase(&format!("cb{daa}"), PK1, 5_000)],
                    )
                })
                .collect();
            s.apply(&groups).unwrap();
            n += batch;
        }
        let secs = start.elapsed().as_secs_f64();
        let rate = blocks as f64 / secs;
        println!("ingested {blocks} blocks in {secs:.2}s = {rate:.0} blocks/s");
        assert_eq!(s.total_txs().unwrap(), blocks);
        assert!(
            rate >= 100.0,
            "ingest {rate:.0} blocks/s below the 100 gate"
        );
    }

    #[test]
    fn apply_records_history_totals_and_checkpoint() {
        let s = temp_store();
        let stats = s
            .apply(&[group("blk1", 100, vec![coinbase("cb1", PK1, 5_000)])])
            .unwrap();
        assert_eq!(stats.applied_txs, 1);
        assert_eq!(s.total_txs().unwrap(), 1);
        assert_eq!(
            s.checkpoint().unwrap(),
            Some(Checkpoint {
                chain_block_hash: "blk1".into(),
                daa_score: 100
            })
        );
        let (recv, cnt) = s.address_totals(&addr(PK1)).unwrap();
        assert_eq!((recv, cnt), (5_000, 1));
        let hist = s.address_history(&addr(PK1), 10, 0).unwrap();
        assert_eq!(hist.len(), 1);
        assert_eq!(hist[0].amount_sompi, 5_000);
        assert!(!hist[0].is_spend);
    }

    #[test]
    fn apply_is_idempotent() {
        let s = temp_store();
        let g = || group("blk1", 100, vec![coinbase("cb1", PK1, 5_000)]);
        s.apply(&[g()]).unwrap();
        s.apply(&[g()]).unwrap(); // replay
        assert_eq!(s.total_txs().unwrap(), 1);
    }

    #[test]
    fn spend_attributes_debit_and_records_outpoint() {
        let s = temp_store();
        s.apply(&[group("blk1", 100, vec![coinbase("cb1", PK1, 5_000)])])
            .unwrap();
        // PK1 spends cb1:0, sending 3000 to PK2 (2000 fee, no change).
        s.apply(&[group("blk2", 101, vec![spend("txA", "cb1", 0, PK2, 3_000)])])
            .unwrap();

        // Spend lookup returns the spending tx (preimage path).
        let spender = s.spend_of("cb1", 0).unwrap().expect("spend recorded");
        assert_eq!(spender.tx_id, "txA");
        assert_eq!(spender.inputs[0].signature_script, "41aa");

        // PK1 sees a debit of its full 5000 UTXO; PK2 a 3000 credit.
        let h1 = s.address_history(&addr(PK1), 10, 0).unwrap();
        assert_eq!(h1.len(), 2);
        assert!(h1[0].is_spend); // newest first: the spend
        assert_eq!(h1[0].amount_sompi, 5_000);
        let h2 = s.address_history(&addr(PK2), 10, 0).unwrap();
        assert_eq!(h2[0].amount_sompi, 3_000);
        assert!(!h2[0].is_spend);
    }

    #[test]
    fn history_pagination_newest_first() {
        let s = temp_store();
        for i in 0..5u64 {
            s.apply(&[group(
                &format!("blk{i}"),
                100 + i,
                vec![coinbase(&format!("cb{i}"), PK1, 1_000 + i)],
            )])
            .unwrap();
        }
        let page0 = s.address_history(&addr(PK1), 2, 0).unwrap();
        assert_eq!(page0.len(), 2);
        assert_eq!(page0[0].tx_id, "cb4"); // newest
        assert_eq!(page0[1].tx_id, "cb3");
        let page1 = s.address_history(&addr(PK1), 2, 2).unwrap();
        assert_eq!(page1[0].tx_id, "cb2");
        let (_recv, cnt) = s.address_totals(&addr(PK1)).unwrap();
        assert_eq!(cnt, 5);
    }

    #[test]
    fn unwind_reverses_a_chain_block() {
        let s = temp_store();
        s.apply(&[group("blk1", 100, vec![coinbase("cb1", PK1, 5_000)])])
            .unwrap();
        s.apply(&[group("blk2", 101, vec![coinbase("cb2", PK1, 6_000)])])
            .unwrap();
        assert_eq!(s.total_txs().unwrap(), 2);

        let removed = s.unwind_chain_block("blk2").unwrap();
        assert_eq!(removed, 1);
        assert_eq!(s.total_txs().unwrap(), 1);
        assert!(!s.is_applied("blk2").unwrap());
        assert!(s.tx_by_id("cb2").unwrap().is_none());
        let (recv, cnt) = s.address_totals(&addr(PK1)).unwrap();
        assert_eq!((recv, cnt), (5_000, 1)); // cb2's 6000 rolled back
    }

    #[test]
    fn expiry_drops_old_rows_but_keeps_counters() {
        let s = temp_store();
        for i in 0..5u64 {
            s.apply(&[group(
                &format!("blk{i}"),
                100 + i,
                vec![coinbase(&format!("cb{i}"), PK1, 1_000)],
            )])
            .unwrap();
        }
        // Drop everything with DAA < 103 (blk0,blk1,blk2).
        let removed = s.expire_below(103).unwrap();
        assert_eq!(removed, 3);
        // Counters are monotonic — untouched by expiry.
        assert_eq!(s.total_txs().unwrap(), 5);
        let (_recv, cnt) = s.address_totals(&addr(PK1)).unwrap();
        assert_eq!(cnt, 5);
        // Only recent rows remain; the window's low edge is now DAA 103.
        assert_eq!(s.window_low_daa().unwrap(), Some(103));
        assert!(s.tx_by_id("cb0").unwrap().is_none());
        assert!(s.tx_by_id("cb3").unwrap().is_some());
        let hist = s.address_history(&addr(PK1), 10, 0).unwrap();
        assert_eq!(hist.len(), 2); // blk3, blk4
    }

    #[test]
    fn indexes_inference_request_response_challenge_and_caps() {
        let s = temp_store();
        let model_id = "ad50ad0bd461d8ab44efc0214989eb33291685ef4ade22a0f4f217d03266d837";
        let miner_pk = "cc".repeat(32);

        // AiRequest payload + CSV-escrow output naming the miner.
        let mut req_payload = inference::from_hex(model_id).unwrap();
        req_payload.extend_from_slice(&128u32.to_le_bytes());
        req_payload.extend_from_slice(&60_000_000u64.to_le_bytes());
        req_payload.extend_from_slice(&30_000_000u64.to_le_bytes());
        req_payload.extend_from_slice(b"why is the sky blue?");
        let req_hash = inference::request_hash(&req_payload);
        let ai_request = RawTx {
            tx_id: "req1".into(),
            is_coinbase: false,
            version: 0,
            lock_time: 0,
            subnetwork_id: inference::SUBNET_AI_REQUEST.into(),
            gas: 0,
            payload: inference::to_hex(&req_payload),
            inputs: vec![],
            outputs: vec![
                RawOut {
                    amount: 0,
                    script_version: 0,
                    script_public_key: p2pk(PK1),
                },
                RawOut {
                    amount: 60_000_000,
                    script_version: 0,
                    script_public_key: format!("02a08cb120{miner_pk}ac"),
                },
            ],
        };

        // AiResponse referencing the request by request_hash, carrying a CID.
        let mut resp_payload = vec![0u8; 78];
        resp_payload[0..32].copy_from_slice(&inference::from_hex(&req_hash).unwrap());
        resp_payload[32..40].copy_from_slice(&9_000u64.to_le_bytes());
        resp_payload[40] = 0x12;
        resp_payload[41] = 0x20;
        for (i, b) in resp_payload.iter_mut().enumerate().take(74).skip(42) {
            *b = i as u8;
        }
        resp_payload[74..78].copy_from_slice(&256u32.to_le_bytes());
        let resp_hash = inference::request_hash(&resp_payload);
        let ai_response = RawTx {
            tx_id: "resp1".into(),
            is_coinbase: false,
            version: 0,
            lock_time: 0,
            subnetwork_id: inference::SUBNET_AI_RESPONSE.into(),
            gas: 0,
            payload: inference::to_hex(&resp_payload),
            inputs: vec![],
            outputs: vec![RawOut {
                amount: 0,
                script_version: 0,
                script_public_key: p2pk(PK1),
            }],
        };

        // AiChallenge referencing the response, proof_data = request_hash.
        let mut chal_payload = vec![0u8; 74 + 32];
        chal_payload[0..32].copy_from_slice(&inference::from_hex(&resp_hash).unwrap());
        chal_payload[74..106].copy_from_slice(&inference::from_hex(&req_hash).unwrap());
        let ai_challenge = RawTx {
            tx_id: "chal1".into(),
            is_coinbase: false,
            version: 0,
            lock_time: 0,
            subnetwork_id: inference::SUBNET_AI_CHALLENGE.into(),
            gas: 0,
            payload: inference::to_hex(&chal_payload),
            inputs: vec![],
            outputs: vec![RawOut {
                amount: 0,
                script_version: 0,
                script_public_key: p2pk(PK1),
            }],
        };

        // Coinbase declaring the model capability + escrow pubkey.
        let cb_payload = format!("v1.3/ai:cap:{model_id}/escrow:{miner_pk}/");
        let coinbase = RawTx {
            tx_id: "cbAI".into(),
            is_coinbase: true,
            version: 0,
            lock_time: 0,
            subnetwork_id: "01".repeat(20),
            gas: 0,
            payload: inference::to_hex(cb_payload.as_bytes()),
            inputs: vec![],
            outputs: vec![RawOut {
                amount: 5_000,
                script_version: 0,
                script_public_key: p2pk(PK1),
            }],
        };

        s.apply(&[group("blk1", 100, vec![coinbase])]).unwrap();
        s.apply(&[group("blk2", 101, vec![ai_request])]).unwrap();
        s.apply(&[group("blk3", 102, vec![ai_response])]).unwrap();
        s.apply(&[group("blk4", 103, vec![ai_challenge])]).unwrap();

        // Capabilities from the coinbase marker.
        let caps = s.capabilities().unwrap();
        assert_eq!(caps.len(), 1);
        assert_eq!(caps[0].model_id, model_id);
        assert_eq!(caps[0].miner_pubkeys, vec![miner_pk.clone()]);
        assert_eq!(caps[0].last_seen_daa, 100);

        // Inference feed: request joined with its response CID.
        let feed = s.inference_feed(10, 0).unwrap();
        assert_eq!(feed.len(), 1);
        let (req, resp) = &feed[0];
        assert_eq!(req.tx_id, "req1");
        assert_eq!(req.model_id, model_id);
        assert_eq!(req.prompt, "why is the sky blue?");
        assert_eq!(req.max_tokens, 128);
        assert_eq!(req.request_hash, req_hash);
        assert_eq!(req.miner_pubkey.as_deref(), Some(miner_pk.as_str()));
        let resp = resp.as_ref().expect("response joined");
        assert!(resp.cid.starts_with("Qm"));
        assert_eq!(resp.result_block_hash, "blk3");

        // Challenge references the request_hash; fraud not provable on-chain.
        let challenges = s.challenges(10).unwrap();
        assert_eq!(challenges.len(), 1);
        assert_eq!(challenges[0].tx_id, "chal1");
        assert_eq!(challenges[0].request_hash, req_hash);
    }

    #[test]
    fn inference_rows_expire_with_the_window() {
        let s = temp_store();
        let model_id = "ad50ad0bd461d8ab44efc0214989eb33291685ef4ade22a0f4f217d03266d837";
        let mut req_payload = inference::from_hex(model_id).unwrap();
        req_payload.extend_from_slice(&64u32.to_le_bytes());
        req_payload.extend_from_slice(&50_000_000u64.to_le_bytes());
        req_payload.extend_from_slice(&30_000_000u64.to_le_bytes());
        req_payload.extend_from_slice(b"hi");
        let ai_request = RawTx {
            tx_id: "reqOld".into(),
            is_coinbase: false,
            version: 0,
            lock_time: 0,
            subnetwork_id: inference::SUBNET_AI_REQUEST.into(),
            gas: 0,
            payload: inference::to_hex(&req_payload),
            inputs: vec![],
            outputs: vec![RawOut {
                amount: 0,
                script_version: 0,
                script_public_key: p2pk(PK1),
            }],
        };
        s.apply(&[group("blkO", 100, vec![ai_request])]).unwrap();
        assert_eq!(s.inference_feed(10, 0).unwrap().len(), 1);
        s.expire_below(200).unwrap();
        assert_eq!(s.inference_feed(10, 0).unwrap().len(), 0);
    }

    #[test]
    fn reopen_recovers_checkpoint() {
        static N: AtomicU32 = AtomicU32::new(9000);
        let dir = std::env::temp_dir().join(format!(
            "keryx-shim-reopen-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        {
            let s = Store::open(&dir, "keryxsim").unwrap();
            s.apply(&[group("blk1", 100, vec![coinbase("cb1", PK1, 5_000)])])
                .unwrap();
        }
        // Reopen: checkpoint and counters survive the "restart".
        let s = Store::open(&dir, "keryxsim").unwrap();
        assert_eq!(s.total_txs().unwrap(), 1);
        assert_eq!(s.checkpoint().unwrap().unwrap().daa_score, 100);
        assert!(s.is_applied("blk1").unwrap());
    }
}
