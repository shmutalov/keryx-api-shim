//! Windowed indexer — M1 pipeline skeleton.
//!
//! A single task follows keryxd's virtual selected-parent chain and folds
//! accepted transactions into an [`IndexStore`], maintaining a checkpoint and a
//! monotonic tx counter. It is fed by [`Notification`]s from the node client:
//!
//! - `Connected` → (re)subscribe and gap-backfill from the checkpoint to tip.
//! - `BlockAdded` → stage block bodies (join source for acceptance).
//! - `VirtualChainChanged` → apply the accepted transactions of each added
//!   chain block, advancing the checkpoint.
//!
//! M1 is linear-chain only: reorgs (removed chain blocks) are logged but not
//! unwound, and the store is in-memory. M2 adds the durable, DAA-segmented
//! store and reorg unwind; M3 adds the address ledger and read endpoints.

mod store;

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};

use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::node::{proto, NodeClient, NodeError, Notification};
use store::{IndexStore, MemStore};

/// Recently-added blocks kept in RAM so acceptance can be joined to a DAA
/// score (and accepted tx ids matched against what we actually saw) without a
/// node round-trip. Both maps are bounded; at 10 bps these caps cover minutes
/// between a block being added and the chain block that accepts it.
const STAGE_BLOCK_CAP: usize = 50_000;
const STAGE_TX_CAP: usize = 500_000;

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
    total_txs: u64,
    applied_blocks: u64,
    resolve_misses: u64,
    staged_blocks: usize,
    generation: u64,
}

/// Cheap-to-clone read handle for `/health` (and, from M3, indexed reads).
#[derive(Clone)]
pub struct IndexerHandle {
    status: Arc<RwLock<IndexerStatus>>,
}

impl IndexerHandle {
    pub fn json(&self) -> Value {
        let s = self.status.read().unwrap();
        json!({
            "state": s.state.as_str(),
            "window_days": s.window_days,
            "checkpoint_daa": s.checkpoint_daa,
            "total_txs": s.total_txs,
            "applied_blocks": s.applied_blocks,
            "resolve_misses": s.resolve_misses,
            "staged_blocks": s.staged_blocks,
            "generation": s.generation,
        })
    }
}

/// Spawn the indexer task. Returns immediately with a status handle; the task
/// starts working once the node reports `Connected`.
pub fn spawn(
    node: NodeClient,
    notifs: mpsc::Receiver<Notification>,
    window_days: u64,
) -> IndexerHandle {
    let status = Arc::new(RwLock::new(IndexerStatus {
        state: IndexerState::Connecting,
        window_days,
        checkpoint_daa: None,
        total_txs: 0,
        applied_blocks: 0,
        resolve_misses: 0,
        staged_blocks: 0,
        generation: 0,
    }));
    let store: Arc<dyn IndexStore> = Arc::new(MemStore::new());
    let handle = IndexerHandle {
        status: status.clone(),
    };
    tokio::spawn(run(node, notifs, store, status));
    handle
}

fn edit(status: &RwLock<IndexerStatus>, f: impl FnOnce(&mut IndexerStatus)) {
    f(&mut status.write().unwrap());
}

async fn run(
    node: NodeClient,
    mut notifs: mpsc::Receiver<Notification>,
    store: Arc<dyn IndexStore>,
    status: Arc<RwLock<IndexerStatus>>,
) {
    let mut staging = Staging::new();
    while let Some(notification) = notifs.recv().await {
        match notification {
            Notification::Connected { generation } => {
                edit(&status, |s| {
                    s.state = IndexerState::Connecting;
                    s.generation = generation;
                });
                if let Err(e) = subscribe(&node).await {
                    tracing::warn!("indexer: subscribe failed on generation {generation}: {e}");
                    continue; // the next Connected will retry
                }
                edit(&status, |s| s.state = IndexerState::Backfilling);
                if let Err(e) = backfill(&node, store.as_ref(), &mut staging, &status).await {
                    tracing::warn!("indexer: backfill failed: {e}; relying on live stream");
                }
                edit(&status, |s| s.state = IndexerState::Live);
                tracing::info!(
                    "indexer live at generation {generation}: {} txs across {} chain blocks",
                    store.total_txs(),
                    store.applied_blocks()
                );
            }
            Notification::BlockAdded(block) => {
                staging.stage_block(&block);
                let staged = staging.blocks();
                edit(&status, |s| s.staged_blocks = staged);
            }
            Notification::VirtualChainChanged(vc) => {
                if !vc.removed_chain_block_hashes.is_empty() {
                    // M1 is linear-only. A reorg here means our checkpoint may
                    // include now-orphaned blocks until M2 adds unwind.
                    tracing::warn!(
                        "indexer: chain reorg removed {} block(s); unwind lands in M2",
                        vc.removed_chain_block_hashes.len()
                    );
                }
                apply_accepted(
                    &node,
                    store.as_ref(),
                    &mut staging,
                    &status,
                    &vc.accepted_transaction_ids,
                )
                .await;
            }
        }
    }
    tracing::info!("indexer: node notification stream closed; stopping");
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

/// Catch up from the checkpoint (or the current sink on a fresh index) to the
/// virtual tip. One `getVirtualChainFromBlock` returns the whole added range;
/// `getBlocks` from its low hash stages the bodies so DAA resolves locally.
async fn backfill(
    node: &NodeClient,
    store: &dyn IndexStore,
    staging: &mut Staging,
    status: &RwLock<IndexerStatus>,
) -> Result<(), NodeError> {
    let start = match store.checkpoint() {
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

    apply_accepted(node, store, staging, status, &vc.accepted_transaction_ids).await;
    Ok(())
}

/// Fold each accepting chain block's accepted transactions into the store.
/// Order-preserving (groups arrive low-to-high) so the checkpoint advances
/// monotonically; the store dedupes by hash so overlap is harmless.
async fn apply_accepted(
    node: &NodeClient,
    store: &dyn IndexStore,
    staging: &mut Staging,
    status: &RwLock<IndexerStatus>,
    groups: &[proto::RpcAcceptedTransactionIds],
) {
    for group in groups {
        let hash = &group.accepting_block_hash;
        let count = group.accepted_transaction_ids.len() as u64;
        let resolved = group
            .accepted_transaction_ids
            .iter()
            .filter(|id| staging.saw_tx(id))
            .count() as u64;
        let daa = match staging.block_daa(hash) {
            Some(daa) => daa,
            None => resolve_daa(node, hash).await,
        };
        if store.apply_chain_block(hash, daa, count) {
            let total = store.total_txs();
            let blocks = store.applied_blocks();
            let staged = staging.blocks();
            edit(status, |s| {
                s.checkpoint_daa = Some(daa);
                s.total_txs = total;
                s.applied_blocks = blocks;
                s.resolve_misses += count - resolved;
                s.staged_blocks = staged;
            });
        }
    }
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

    fn contains(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    fn len(&self) -> usize {
        self.map.len()
    }
}

struct Staging {
    block_daa: Bounded<u64>,
    seen_tx: Bounded<()>,
}

impl Staging {
    fn new() -> Self {
        Self {
            block_daa: Bounded::new(STAGE_BLOCK_CAP),
            seen_tx: Bounded::new(STAGE_TX_CAP),
        }
    }

    fn stage_block(&mut self, block: &proto::RpcBlock) {
        if let (Some(vd), Some(header)) = (&block.verbose_data, &block.header) {
            if !vd.hash.is_empty() {
                self.block_daa.insert(vd.hash.clone(), header.daa_score);
            }
        }
        for tx in &block.transactions {
            if let Some(vd) = &tx.verbose_data {
                if !vd.transaction_id.is_empty() {
                    self.seen_tx.insert(vd.transaction_id.clone(), ());
                }
            }
        }
    }

    fn block_daa(&self, hash: &str) -> Option<u64> {
        self.block_daa.get(hash).copied()
    }

    fn saw_tx(&self, tx_id: &str) -> bool {
        self.seen_tx.contains(tx_id)
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
        assert!(!b.contains("a"));
        assert!(b.contains("b"));
        assert!(b.contains("c"));
        assert_eq!(b.len(), 2);
    }

    #[test]
    fn staging_records_daa_and_tx_ids() {
        let mut staging = Staging::new();
        staging.stage_block(&proto::RpcBlock {
            header: Some(proto::RpcBlockHeader {
                daa_score: 4242,
                ..Default::default()
            }),
            transactions: vec![proto::RpcTransaction {
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
        assert!(staging.saw_tx("tx1"));
        assert!(!staging.saw_tx("tx2"));
    }
}
