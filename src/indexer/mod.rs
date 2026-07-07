//! Windowed indexer — the durable chain follower (M2).
//!
//! A single task follows keryxd's virtual selected-parent chain and folds
//! accepted transactions into a redb-backed [`Store`], maintaining a
//! checkpoint, per-address history, an outpoint→spender index, and monotonic
//! counters. Fed by [`Notification`]s from the node client:
//!
//! - `Connected` → open the store (first time), (re)subscribe, gap-backfill.
//! - `BlockAdded` → stage full block bodies (the join source for acceptance).
//! - `VirtualChainChanged` → unwind removed chain blocks, then apply the
//!   accepted transactions of each added chain block.
//!
//! Retention is a periodic sweep dropping rows below the DAA window; reorgs are
//! unwound via the store's `accepted_by` list. Everything is crash-safe: each
//! apply is one redb transaction and restart resumes from the committed
//! checkpoint (idempotent by chain-block hash).

pub mod address;
pub mod store;

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock, RwLock};

use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::node::{proto, NodeClient, NodeError, Notification};
use store::{AcceptedGroup, RawIn, RawOut, RawTx, Store};

const STAGE_BLOCK_CAP: usize = 20_000;
const STAGE_TX_CAP: usize = 50_000;
/// ~10 bps → sweep expiry roughly every 90 s of chain time.
const EXPIRE_INTERVAL_DAA: u64 = 900;
const DAA_PER_DAY: u64 = 864_000;
/// Kaspa/Keryx coinbase subnetwork id.
const COINBASE_SUBNETWORK: &str = "0100000000000000000000000000000000000000";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexerState {
    Connecting,
    Backfilling,
    Live,
}

impl IndexerState {
    fn as_str(self) -> &'static str {
        match self {
            IndexerState::Connecting => "connecting",
            IndexerState::Backfilling => "backfilling",
            IndexerState::Live => "live",
        }
    }
}

#[derive(Clone, Debug)]
struct IndexerStatus {
    state: IndexerState,
    window_days: u64,
    checkpoint_daa: Option<u64>,
    window_low_daa: Option<u64>,
    total_txs: u64,
    chain_blocks: u64,
    resolve_misses: u64,
    staged_blocks: usize,
    generation: u64,
}

/// Cheap-to-clone handle: read-only status for `/health` plus access to the
/// store for the indexed read endpoints (M3).
#[derive(Clone)]
pub struct IndexerHandle {
    status: Arc<RwLock<IndexerStatus>>,
    store: Arc<OnceLock<Store>>,
}

impl IndexerHandle {
    /// The store, once the indexer has learned the network and opened it.
    pub fn store(&self) -> Option<&Store> {
        self.store.get()
    }

    pub fn json(&self) -> Value {
        let s = self.status.read().unwrap();
        json!({
            "state": s.state.as_str(),
            "window_days": s.window_days,
            "checkpoint_daa": s.checkpoint_daa,
            "window_low_daa": s.window_low_daa,
            "total_txs": s.total_txs,
            "chain_blocks": s.chain_blocks,
            "resolve_misses": s.resolve_misses,
            "staged_blocks": s.staged_blocks,
            "generation": s.generation,
        })
    }
}

pub fn spawn(
    node: NodeClient,
    notifs: mpsc::Receiver<Notification>,
    window_days: u64,
    data_dir: PathBuf,
) -> IndexerHandle {
    let status = Arc::new(RwLock::new(IndexerStatus {
        state: IndexerState::Connecting,
        window_days,
        checkpoint_daa: None,
        window_low_daa: None,
        total_txs: 0,
        chain_blocks: 0,
        resolve_misses: 0,
        staged_blocks: 0,
        generation: 0,
    }));
    let store = Arc::new(OnceLock::new());
    let handle = IndexerHandle {
        status: status.clone(),
        store: store.clone(),
    };
    tokio::spawn(run(node, notifs, status, store, data_dir, window_days));
    handle
}

fn edit(status: &RwLock<IndexerStatus>, f: impl FnOnce(&mut IndexerStatus)) {
    f(&mut status.write().unwrap());
}

/// Mutable per-run bookkeeping threaded through the apply paths.
struct Ctx {
    window_daa: u64,
    last_expire_daa: u64,
    total_misses: u64,
}

async fn run(
    node: NodeClient,
    mut notifs: mpsc::Receiver<Notification>,
    status: Arc<RwLock<IndexerStatus>>,
    store_cell: Arc<OnceLock<Store>>,
    data_dir: PathBuf,
    window_days: u64,
) {
    let mut staging = Staging::new();
    let mut ctx = Ctx {
        window_daa: window_days.saturating_mul(DAA_PER_DAY),
        last_expire_daa: 0,
        total_misses: 0,
    };

    while let Some(notification) = notifs.recv().await {
        match notification {
            Notification::Connected { generation } => {
                edit(&status, |s| {
                    s.state = IndexerState::Connecting;
                    s.generation = generation;
                });
                if store_cell.get().is_none() {
                    match open_store(&node, &data_dir).await {
                        Ok(store) => {
                            let _ = store_cell.set(store);
                        }
                        Err(e) => {
                            tracing::error!(
                                "indexer: cannot open store: {e}; retrying next connect"
                            );
                            continue;
                        }
                    }
                }
                let store = store_cell.get().expect("store just opened");
                if let Err(e) = subscribe(&node).await {
                    tracing::warn!("indexer: subscribe failed on generation {generation}: {e}");
                    continue;
                }
                edit(&status, |s| s.state = IndexerState::Backfilling);
                if let Err(e) = backfill(&node, store, &mut staging, &status, &mut ctx).await {
                    tracing::warn!("indexer: backfill failed: {e}; relying on live stream");
                }
                edit(&status, |s| s.state = IndexerState::Live);
                tracing::info!(
                    "indexer live at generation {generation}: {} txs, checkpoint DAA {:?}",
                    store.total_txs().unwrap_or(0),
                    store.checkpoint().ok().flatten().map(|c| c.daa_score)
                );
            }
            Notification::BlockAdded(block) => {
                staging.stage_block(&block);
                let staged = staging.blocks();
                edit(&status, |s| s.staged_blocks = staged);
            }
            Notification::VirtualChainChanged(vc) => {
                let Some(store) = store_cell.get() else {
                    continue;
                };
                // Reorg: unwind removed chain blocks (given high-to-low) first.
                for hash in &vc.removed_chain_block_hashes {
                    match store.unwind_chain_block(hash) {
                        Ok(n) if n > 0 => {
                            tracing::warn!("indexer: unwound {n} txs from reorged block {hash}")
                        }
                        Ok(_) => {}
                        Err(e) => tracing::error!("indexer: unwind failed for {hash}: {e}"),
                    }
                }
                let (groups, misses) =
                    build_groups(&node, &staging, &vc.accepted_transaction_ids).await;
                apply_groups(store, groups, misses, &status, &mut ctx);
            }
        }
    }
    tracing::info!("indexer: node notification stream closed; stopping");
}

async fn open_store(node: &NodeClient, data_dir: &std::path::Path) -> Result<Store, String> {
    let network = node
        .get_block_dag_info(proto::GetBlockDagInfoRequestMessage {})
        .await
        .map(|d| d.network_name)
        .unwrap_or_default();
    let prefix = address::prefix_for_network(&network);
    tracing::info!(
        "indexer: opening store at {} (prefix {prefix})",
        data_dir.display()
    );
    Store::open(data_dir, prefix).map_err(|e| e.to_string())
}

async fn subscribe(node: &NodeClient) -> Result<(), NodeError> {
    let start = proto::RpcNotifyCommand::NotifyStart as i32;
    node.notify_block_added(proto::NotifyBlockAddedRequestMessage { command: start })
        .await?;
    node.notify_virtual_chain_changed(proto::NotifyVirtualChainChangedRequestMessage {
        include_accepted_transaction_ids: true,
        command: start,
    })
    .await?;
    Ok(())
}

async fn backfill(
    node: &NodeClient,
    store: &Store,
    staging: &mut Staging,
    status: &RwLock<IndexerStatus>,
    ctx: &mut Ctx,
) -> Result<(), NodeError> {
    let start = match store.checkpoint().ok().flatten() {
        Some(cp) => cp.chain_block_hash,
        None => {
            node.get_block_dag_info(proto::GetBlockDagInfoRequestMessage {})
                .await?
                .sink
        }
    };
    if start.is_empty() {
        return Ok(());
    }

    let vc = node
        .get_virtual_chain_from_block(proto::GetVirtualChainFromBlockRequestMessage {
            start_hash: start,
            include_accepted_transaction_ids: true,
            min_confirmation_count: None,
        })
        .await?;

    if let Some(low) = vc.added_chain_block_hashes.first() {
        match node
            .get_blocks(proto::GetBlocksRequestMessage {
                low_hash: low.clone(),
                include_blocks: true,
                include_transactions: true,
            })
            .await
        {
            Ok(blocks) => {
                for block in &blocks.blocks {
                    staging.stage_block(block);
                }
            }
            Err(e) => tracing::warn!("indexer: backfill getBlocks failed: {e}; DAA via getBlock"),
        }
    }

    let (groups, misses) = build_groups(node, staging, &vc.accepted_transaction_ids).await;
    apply_groups(store, groups, misses, status, ctx);
    Ok(())
}

/// Turn the node's acceptance lists into ready-to-apply groups, pulling each
/// accepted tx body from staging. A staged miss (tx body not seen) is counted
/// and skipped — rare, and confined to the startup boundary.
async fn build_groups(
    node: &NodeClient,
    staging: &Staging,
    accepted: &[proto::RpcAcceptedTransactionIds],
) -> (Vec<AcceptedGroup>, u64) {
    let mut groups = Vec::with_capacity(accepted.len());
    let mut misses = 0u64;
    for a in accepted {
        let daa = match staging.block_daa(&a.accepting_block_hash) {
            Some(daa) => daa,
            None => resolve_daa(node, &a.accepting_block_hash).await,
        };
        let mut txs = Vec::with_capacity(a.accepted_transaction_ids.len());
        for id in &a.accepted_transaction_ids {
            match staging.get_tx(id) {
                Some(raw) => txs.push(raw),
                None => misses += 1,
            }
        }
        groups.push(AcceptedGroup {
            chain_block_hash: a.accepting_block_hash.clone(),
            daa_score: daa,
            txs,
        });
    }
    (groups, misses)
}

fn apply_groups(
    store: &Store,
    groups: Vec<AcceptedGroup>,
    misses: u64,
    status: &RwLock<IndexerStatus>,
    ctx: &mut Ctx,
) {
    if groups.is_empty() {
        return;
    }
    let stats = match store.apply(&groups) {
        Ok(stats) => stats,
        Err(e) => {
            tracing::error!("indexer: apply failed: {e}");
            return;
        }
    };
    ctx.total_misses += misses;

    // Retention sweep, rate-limited so we don't scan every block.
    if ctx.window_daa > 0
        && stats.checkpoint_daa > ctx.window_daa
        && stats.checkpoint_daa >= ctx.last_expire_daa + EXPIRE_INTERVAL_DAA
    {
        let cutoff = stats.checkpoint_daa - ctx.window_daa;
        match store.expire_below(cutoff) {
            Ok(n) if n > 0 => tracing::debug!("indexer: expired {n} txs below DAA {cutoff}"),
            Ok(_) => {}
            Err(e) => tracing::warn!("indexer: expiry failed: {e}"),
        }
        ctx.last_expire_daa = stats.checkpoint_daa;
    }

    let checkpoint_daa = store.checkpoint().ok().flatten().map(|c| c.daa_score);
    let window_low_daa = store.window_low_daa().ok().flatten();
    let chain_blocks = store.chain_blocks().unwrap_or(0);
    let total_misses = ctx.total_misses;
    edit(status, |s| {
        s.checkpoint_daa = checkpoint_daa;
        s.window_low_daa = window_low_daa;
        s.total_txs = stats.total_txs;
        s.chain_blocks = chain_blocks;
        s.resolve_misses = total_misses;
        s.staged_blocks = 0; // refreshed on the next BlockAdded
    });
}

async fn resolve_daa(node: &NodeClient, hash: &str) -> u64 {
    match node
        .get_block(proto::GetBlockRequestMessage {
            hash: hash.to_string(),
            include_transactions: false,
        })
        .await
    {
        Ok(resp) => resp
            .block
            .and_then(|b| b.header)
            .map(|h| h.daa_score)
            .unwrap_or(0),
        Err(e) => {
            tracing::warn!("indexer: could not resolve DAA for chain block {hash}: {e}");
            0
        }
    }
}

fn raw_from_proto(tx: &proto::RpcTransaction) -> Option<RawTx> {
    let tx_id = tx.verbose_data.as_ref()?.transaction_id.clone();
    if tx_id.is_empty() {
        return None;
    }
    let is_coinbase = tx.subnetwork_id.eq_ignore_ascii_case(COINBASE_SUBNETWORK);
    let inputs = tx
        .inputs
        .iter()
        .map(|i| {
            let op = i.previous_outpoint.clone().unwrap_or_default();
            RawIn {
                previous_tx_id: op.transaction_id,
                previous_index: op.index,
                signature_script: i.signature_script.clone(),
                sequence: i.sequence,
                sig_op_count: i.sig_op_count,
            }
        })
        .collect();
    let outputs = tx
        .outputs
        .iter()
        .map(|o| {
            let spk = o.script_public_key.clone().unwrap_or_default();
            RawOut {
                amount: o.amount,
                script_version: spk.version,
                script_public_key: spk.script_public_key,
            }
        })
        .collect();
    Some(RawTx {
        tx_id,
        is_coinbase,
        version: tx.version,
        lock_time: tx.lock_time,
        subnetwork_id: tx.subnetwork_id.clone(),
        gas: tx.gas,
        payload: tx.payload.clone(),
        inputs,
        outputs,
    })
}

// --- staging ------------------------------------------------------------------

/// Insertion-ordered map with a hard capacity; evicts oldest on overflow.
struct Bounded<V> {
    map: HashMap<String, V>,
    order: VecDeque<String>,
    cap: usize,
}

impl<V> Bounded<V> {
    fn new(cap: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            cap,
        }
    }

    fn insert(&mut self, key: String, value: V) {
        if self.map.insert(key.clone(), value).is_none() {
            self.order.push_back(key);
            while self.order.len() > self.cap {
                if let Some(old) = self.order.pop_front() {
                    self.map.remove(&old);
                }
            }
        }
    }

    fn get(&self, key: &str) -> Option<&V> {
        self.map.get(key)
    }

    fn len(&self) -> usize {
        self.map.len()
    }
}

struct Staging {
    block_daa: Bounded<u64>,
    tx_bodies: Bounded<RawTx>,
}

impl Staging {
    fn new() -> Self {
        Self {
            block_daa: Bounded::new(STAGE_BLOCK_CAP),
            tx_bodies: Bounded::new(STAGE_TX_CAP),
        }
    }

    fn stage_block(&mut self, block: &proto::RpcBlock) {
        if let (Some(vd), Some(header)) = (&block.verbose_data, &block.header) {
            if !vd.hash.is_empty() {
                self.block_daa.insert(vd.hash.clone(), header.daa_score);
            }
        }
        for tx in &block.transactions {
            if let Some(raw) = raw_from_proto(tx) {
                self.tx_bodies.insert(raw.tx_id.clone(), raw);
            }
        }
    }

    fn block_daa(&self, hash: &str) -> Option<u64> {
        self.block_daa.get(hash).copied()
    }

    fn get_tx(&self, tx_id: &str) -> Option<RawTx> {
        self.tx_bodies.get(tx_id).cloned()
    }

    fn blocks(&self) -> usize {
        self.block_daa.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_evicts_oldest() {
        let mut b = Bounded::new(2);
        b.insert("a".into(), 1);
        b.insert("b".into(), 2);
        b.insert("c".into(), 3);
        assert!(b.get("a").is_none());
        assert!(b.get("b").is_some());
        assert_eq!(b.len(), 2);
    }

    #[test]
    fn stages_full_tx_bodies() {
        let mut staging = Staging::new();
        staging.stage_block(&proto::RpcBlock {
            header: Some(proto::RpcBlockHeader {
                daa_score: 4242,
                ..Default::default()
            }),
            transactions: vec![proto::RpcTransaction {
                subnetwork_id: "00".repeat(20),
                outputs: vec![proto::RpcTransactionOutput {
                    amount: 777,
                    script_public_key: Some(proto::RpcScriptPublicKey {
                        version: 0,
                        script_public_key: "2011ac".into(),
                    }),
                    verbose_data: None,
                }],
                verbose_data: Some(proto::RpcTransactionVerboseData {
                    transaction_id: "tx1".into(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            verbose_data: Some(proto::RpcBlockVerboseData {
                hash: "blockA".into(),
                ..Default::default()
            }),
            pom_proof: None,
        });
        assert_eq!(staging.block_daa("blockA"), Some(4242));
        let raw = staging.get_tx("tx1").expect("staged");
        assert_eq!(raw.outputs[0].amount, 777);
        assert!(!raw.is_coinbase);
    }
}
