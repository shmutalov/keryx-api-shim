//! In-RAM mempool overlay (M4).
//!
//! A poller periodically snapshots keryxd's mempool and indexes it two ways:
//! outpoint → spending tx (so an HTLC claim's preimage is visible the moment
//! it is *relayed*, not only when mined) and address → pending rows (so the
//! wallet's history shows unconfirmed activity immediately). Everything here is
//! transient — dropped and rebuilt each poll, never persisted.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use serde_json::{json, Value};

use super::address::script_to_address;
use super::raw_from_proto;
use super::store::{RawTx, Store};
use crate::node::{proto, NodeClient};

fn outpoint_key(txid: &str, index: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(txid.len() + 5);
    k.extend_from_slice(txid.as_bytes());
    k.push(0xFF);
    k.extend_from_slice(&index.to_be_bytes());
    k
}

/// One unconfirmed history row for an address.
#[derive(Clone)]
pub struct PendingRow {
    pub tx_id: String,
    pub amount_sompi: u64,
    pub is_spend: bool,
}

#[derive(Default)]
struct Snapshot {
    /// outpoint → the pending transaction that spends it (rendered JSON).
    spends: HashMap<Vec<u8>, Arc<Value>>,
    /// address → pending history rows (all unconfirmed).
    history: HashMap<String, Vec<PendingRow>>,
    count: usize,
}

/// Shared, cheaply-cloned mempool overlay. Empty until the poller runs (or
/// forever, if polling is disabled).
#[derive(Default)]
pub struct Mempool {
    snapshot: RwLock<Snapshot>,
}

impl Mempool {
    /// The pending transaction spending `(txid, index)`, if any is in the mempool.
    pub fn spend_of(&self, txid: &str, index: u32) -> Option<Value> {
        self.snapshot
            .read()
            .unwrap()
            .spends
            .get(&outpoint_key(txid, index))
            .map(|a| (**a).clone())
    }

    /// Pending history rows for an address.
    pub fn history(&self, address: &str) -> Vec<PendingRow> {
        self.snapshot
            .read()
            .unwrap()
            .history
            .get(address)
            .cloned()
            .unwrap_or_default()
    }

    pub fn pending_count(&self) -> usize {
        self.snapshot.read().unwrap().count
    }

    fn install(&self, snapshot: Snapshot) {
        *self.snapshot.write().unwrap() = snapshot;
    }
}

/// Render a pending transaction as the wallet's wire shape, tagged as
/// unconfirmed (`daa_score: 0`, `block_hash: null`).
fn render_pending(raw: &RawTx) -> Value {
    let inputs: Vec<Value> = raw
        .inputs
        .iter()
        .map(|i| {
            json!({
                "transaction_id": i.previous_tx_id,
                "index": i.previous_index,
                "signature_script": i.signature_script,
                "sequence": i.sequence.to_string(),
                "sig_op_count": i.sig_op_count,
            })
        })
        .collect();
    let outputs: Vec<Value> = raw
        .outputs
        .iter()
        .map(|o| {
            json!({
                "amount": o.amount,
                "script_version": o.script_version,
                "script_public_key": o.script_public_key,
            })
        })
        .collect();
    json!({
        "tx_id": raw.tx_id,
        "version": raw.version,
        "inputs": inputs,
        "outputs": outputs,
        "lock_time": raw.lock_time,
        "subnetwork_id": raw.subnetwork_id,
        "gas": raw.gas,
        "payload": raw.payload,
        "block_hash": Value::Null,
        "daa_score": 0,
        "is_coinbase": raw.is_coinbase,
    })
}

/// Build the overlay from a batch of mempool transactions. Debits are attributed
/// by resolving each input's funding transaction in the confirmed store
/// (best-effort — a funding tx still only in the mempool is not attributed, but
/// spend detection, which needs no address, is unaffected).
fn build_snapshot(txs: &[RawTx], store: &Store, prefix: &str) -> Snapshot {
    let mut spends: HashMap<Vec<u8>, Arc<Value>> = HashMap::new();
    let mut history: HashMap<String, Vec<PendingRow>> = HashMap::new();
    let mut count = 0;

    for raw in txs {
        count += 1;
        let rendered = Arc::new(render_pending(raw));

        if !raw.is_coinbase {
            for inp in &raw.inputs {
                spends.insert(
                    outpoint_key(&inp.previous_tx_id, inp.previous_index),
                    rendered.clone(),
                );
            }
        }

        let mut acc: HashMap<String, (u64, u64)> = HashMap::new();
        for o in &raw.outputs {
            if let Some(a) = script_to_address(&o.script_public_key, prefix) {
                acc.entry(a).or_default().0 += o.amount;
            }
        }
        if !raw.is_coinbase {
            for inp in &raw.inputs {
                if let Ok(Some(funding)) = store.tx_by_id(&inp.previous_tx_id) {
                    if let Some(o) = funding.outputs.get(inp.previous_index as usize) {
                        if let Some(a) = script_to_address(&o.script_public_key, prefix) {
                            acc.entry(a).or_default().1 += o.amount;
                        }
                    }
                }
            }
        }
        for (address, (credit, debit)) in acc {
            let net = credit as i64 - debit as i64;
            history.entry(address).or_default().push(PendingRow {
                tx_id: raw.tx_id.clone(),
                amount_sompi: net.unsigned_abs(),
                is_spend: net < 0,
            });
        }
    }

    Snapshot {
        spends,
        history,
        count,
    }
}

/// Poll the node's mempool on an interval, rebuilding the overlay each time.
/// Waits for the store to be open (needed for debit attribution).
pub fn spawn_poller(
    node: NodeClient,
    store_cell: Arc<OnceLock<Store>>,
    mempool: Arc<Mempool>,
    prefix: String,
    poll_ms: u64,
) {
    if poll_ms == 0 {
        return;
    }
    tokio::spawn(async move {
        let interval = Duration::from_millis(poll_ms);
        loop {
            tokio::time::sleep(interval).await;
            let Some(store) = store_cell.get() else {
                continue;
            };
            let entries = match node
                .get_mempool_entries(proto::GetMempoolEntriesRequestMessage {
                    include_orphan_pool: false,
                    filter_transaction_pool: false,
                })
                .await
            {
                Ok(resp) => resp.entries,
                Err(e) => {
                    tracing::debug!("mempool poll failed: {e}");
                    continue;
                }
            };
            let txs: Vec<RawTx> = entries
                .into_iter()
                .filter_map(|e| e.transaction.as_ref().and_then(raw_from_proto))
                .collect();
            mempool.install(build_snapshot(&txs, store, &prefix));
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexer::store::{AcceptedGroup, RawIn, RawOut};
    use std::sync::atomic::{AtomicU32, Ordering};

    const PK1: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const PK2: &str = "2222222222222222222222222222222222222222222222222222222222222222";

    fn temp_store() -> Store {
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "keryx-shim-mp-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::SeqCst)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        Store::open(&dir, "keryxsim").unwrap()
    }

    fn addr(pk: &str) -> String {
        script_to_address(&format!("20{pk}ac"), "keryxsim").unwrap()
    }

    #[test]
    fn overlay_indexes_spend_and_pending_history() {
        // Confirmed funding: a coinbase paying PK1 5000.
        let store = temp_store();
        store
            .apply(&[AcceptedGroup {
                chain_block_hash: "blk1".into(),
                daa_score: 100,
                txs: vec![RawTx {
                    tx_id: "cb1".into(),
                    is_coinbase: true,
                    version: 0,
                    lock_time: 0,
                    subnetwork_id: "01".repeat(20),
                    gas: 0,
                    payload: String::new(),
                    inputs: vec![],
                    outputs: vec![RawOut {
                        amount: 5_000,
                        script_version: 0,
                        script_public_key: format!("20{PK1}ac"),
                    }],
                }],
            }])
            .unwrap();

        // A pending tx spending cb1:0, sending 3000 to PK2 (the "claim").
        let pending = RawTx {
            tx_id: "pend1".into(),
            is_coinbase: false,
            version: 0,
            lock_time: 0,
            subnetwork_id: "00".repeat(20),
            gas: 0,
            payload: String::new(),
            inputs: vec![RawIn {
                previous_tx_id: "cb1".into(),
                previous_index: 0,
                signature_script: "41cafe01".into(), // the "preimage push"
                sequence: u64::MAX,
                sig_op_count: 1,
            }],
            outputs: vec![RawOut {
                amount: 3_000,
                script_version: 0,
                script_public_key: format!("20{PK2}ac"),
            }],
        };

        let mempool = Mempool::default();
        mempool.install(build_snapshot(&[pending], &store, "keryxsim"));

        assert_eq!(mempool.pending_count(), 1);

        // Relay-time spend detection: the preimage is reachable before mining.
        let spend = mempool.spend_of("cb1", 0).expect("pending spend indexed");
        assert_eq!(spend["tx_id"], "pend1");
        assert_eq!(spend["inputs"][0]["signature_script"], "41cafe01");
        assert_eq!(spend["daa_score"], 0); // flagged pending

        // Pending history: PK2 credit, PK1 debit (attributed via the store).
        let h2 = mempool.history(&addr(PK2));
        assert_eq!(h2.len(), 1);
        assert_eq!(h2[0].amount_sompi, 3_000);
        assert!(!h2[0].is_spend);
        let h1 = mempool.history(&addr(PK1));
        assert_eq!(h1[0].amount_sompi, 5_000);
        assert!(h1[0].is_spend);
    }
}
