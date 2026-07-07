//! Storage seam for the indexer.
//!
//! M1 ships only the pieces the pipeline skeleton needs — the checkpoint, the
//! monotonic tx counter, and idempotent per-chain-block application. M2
//! replaces [`MemStore`] with a redb-backed, DAA-segmented store and extends
//! this trait with the address ledger, outpoint-spend, and unwind operations.

use std::collections::HashSet;
use std::sync::Mutex;

/// The last chain block folded into the index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    pub chain_block_hash: String,
    pub daa_score: u64,
}

pub trait IndexStore: Send + Sync {
    /// Last applied chain block, or `None` on a fresh index.
    fn checkpoint(&self) -> Option<Checkpoint>;

    /// Fold one accepting chain block into the index.
    ///
    /// Idempotent and keyed by `hash`: re-applying an already-seen chain block
    /// is a no-op returning `false` (so a crash-recovery replay or an
    /// overlapping backfill cannot double-count). A first application records
    /// the block, adds `accepted_tx_count` to the total, advances the
    /// checkpoint, and returns `true`.
    fn apply_chain_block(&self, hash: &str, daa_score: u64, accepted_tx_count: u64) -> bool;

    /// Monotonic count of accepted transactions folded in so far.
    fn total_txs(&self) -> u64;

    /// Number of distinct chain blocks applied.
    fn applied_blocks(&self) -> u64;
}

#[derive(Default)]
struct Inner {
    checkpoint: Option<Checkpoint>,
    total_txs: u64,
    applied: HashSet<String>,
}

/// In-memory store for M1. Not durable — a restart re-backfills from the node.
/// The durable, segmented store lands in M2 behind this same trait.
#[derive(Default)]
pub struct MemStore {
    inner: Mutex<Inner>,
}

impl MemStore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl IndexStore for MemStore {
    fn checkpoint(&self) -> Option<Checkpoint> {
        self.inner.lock().unwrap().checkpoint.clone()
    }

    fn apply_chain_block(&self, hash: &str, daa_score: u64, accepted_tx_count: u64) -> bool {
        let mut inner = self.inner.lock().unwrap();
        if !inner.applied.insert(hash.to_string()) {
            return false;
        }
        inner.total_txs += accepted_tx_count;
        inner.checkpoint = Some(Checkpoint {
            chain_block_hash: hash.to_string(),
            daa_score,
        });
        true
    }

    fn total_txs(&self) -> u64 {
        self.inner.lock().unwrap().total_txs
    }

    fn applied_blocks(&self) -> u64 {
        self.inner.lock().unwrap().applied.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_store_has_no_checkpoint() {
        let s = MemStore::new();
        assert_eq!(s.checkpoint(), None);
        assert_eq!(s.total_txs(), 0);
        assert_eq!(s.applied_blocks(), 0);
    }

    #[test]
    fn apply_advances_checkpoint_and_counter() {
        let s = MemStore::new();
        assert!(s.apply_chain_block("aa", 100, 3));
        assert!(s.apply_chain_block("bb", 101, 2));
        assert_eq!(s.total_txs(), 5);
        assert_eq!(s.applied_blocks(), 2);
        assert_eq!(
            s.checkpoint(),
            Some(Checkpoint {
                chain_block_hash: "bb".into(),
                daa_score: 101,
            })
        );
    }

    #[test]
    fn reapplying_a_block_is_a_noop() {
        let s = MemStore::new();
        assert!(s.apply_chain_block("aa", 100, 3));
        // Same hash again (crash-replay / backfill overlap): must not double-count.
        assert!(!s.apply_chain_block("aa", 100, 3));
        assert_eq!(s.total_txs(), 3);
        assert_eq!(s.applied_blocks(), 1);
    }
}
