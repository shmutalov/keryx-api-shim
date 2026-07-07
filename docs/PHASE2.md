# Phase 2 — Windowed Indexer

Implementation plan, agreed 2026-07-08. Phase 1 (v0.1.0) serves everything a bare
`--utxoindex` keryxd can answer; phase 2 adds the pieces that need indexing —
sized for the real product, not for an explorer.

## 0. Source code locations (local dev machine)

| repo | path | notes |
|---|---|---|
| Keryx Node (`keryxd`) | `D:\Projects\other\keryx-node` | rusty-kaspa fork; protos vendored from `rpc/grpc/core/proto` |
| Keryx Wallet Extension | `D:\Projects\mine\keryx-wallet-extension` | wire contract: `src/lib/api.js`, `docs/PROTOCOL.md` |
| Keryx Swap | `D:\Projects\mine\keryx-swap` | the HTLC atomic-swap app this phase is sized for |
| Keryx Miner (upstream, CUDA) | `D:\Projects\other\keryx-miner` | |
| Keryx Miner RDNA3 (Vulkan) | `D:\Projects\other\keryx-miner-rdna3` | |

## 1. Context and driving constraints

- The shim fronts the **Keryx Wallet Extension** for an **atomic-swap
  application**. The swap app is **backend-less**: all swap state (swaps,
  addresses, transactions, redeem scripts, preimages) lives in browser storage
  and is lost if the user clears it.
- Swap protocol is **HTLC-based** (Kaspa-style P2SH: `OP_BLAKE2B <hash>
  OP_EQUAL`; CLTV = `0xb0`, CSV = `0xb1`; see the wallet's `docs/PROTOCOL.md`).
- **Maximum coin lock time is capped at 1 day** (excluding late claim/refund
  broadcasts, which need no observation by the counterparty — only claims
  reveal the preimage, and a preimage is only useful within the other leg's
  ≤1-day timelock).
- Keryx runs at **10 bps ≈ 864k blocks/day**. A Fulcrum-style all-or-nothing
  full index is the wrong shape for this chain: unbounded growth dominated by
  empty-block coinbases (observed on simnet: ~240k UTXOs on the miner address
  after &lt;1 day). Phase 2 is therefore a **rolling-window index**, not an
  archival one.

### What the swap app needs that phase 1 cannot answer

| Swap step | Need | Phase 1 |
|---|---|---|
| Verify counterparty funded HTLC | UTXOs on P2SH address, confirmations | ✅ live |
| **Detect claim, extract preimage** | the **spending tx** of an outpoint (preimage is a push in its `signature_script`) | ❌ |
| Recover swap after storage loss | funding txs incl. **`payload`** (app may embed encrypted redeem-script hints), recent address history | ❌ |
| Refund after timeout | current DAA, broadcast | ✅ live |
| Wallet history UX | recent txs, honest pagination | ❌ (stub) |

## 2. Non-goals

- Full archival history, explorer features (rich lists, block pages), search.
- The inference-oracle endpoints (`/capabilities`, `/infer`, `/challenges`) —
  deferred to phase 2c pending research (see §12).
- Market data (stays a config-only upstream proxy).
- **Wallet-side work is out of scope**: client-side tx-history persistence and
  the "older transactions — see explorer" placeholder belong to the wallet
  extension. The shim's job is a truthful, self-describing window.

## 3. Retention model — the window

- Ledger data is kept for a **rolling DAA-score window**, default **7 days**
  (`7 × 864_000 DAA`), configurable. Rationale: protocol-critical
  observability is ~2 days (lock + counter-lock); the rest is
  crash/offline-recovery headroom — the window doubles as the **recovery
  buffer** for the backend-less swap app, so it is sized by "how long can a
  device be offline mid-swap and still self-recover", not by lock time alone.
- Storage is **segmented by DAA range, one embedded-DB file per day-segment**
  (`segment-<start_daa>.redb`). Expiry = delete the oldest file (instant,
  reclaims space, optionally uploadable to object storage first = archiving
  for free). No row-by-row deletes ever.
- **redb** first (pure Rust, no cmake/clang in the Docker builder), behind a
  narrow storage trait so RocksDB can replace it if volume demands.
- Estimated volume: 1–2M rows/day from coinbases alone ≈ 0.1–0.2 GB/day →
  **~1 GB steady state**, flat forever.
- Live state (balances, UTXOs, broadcast) stays node-backed and never expires;
  only history and spend-lookup age out.

## 4. Data model

Per segment (keyed for range scans; values bincode/borsh-encoded):

| table | key | value |
|---|---|---|
| `tx` | `tx_num: u64` (monotonic, block order — Fulcrum-style compact ids) | full tx: inputs (with `signature_script`), outputs, `payload`, accepting block hash, accepting DAA, is_coinbase |
| `tx_by_id` | `tx_id: [u8;32]` | `tx_num` |
| `addr_history` | `(address, tx_num)` | `delta_sompi: i64`, `is_spend: bool`, `block_hash` |
| `outpoint_spend` | `(funding_tx_id, index)` | spending `tx_num` |
| `utxo_owned` | `(tx_id, index)` | `(address, amount_sompi)` — for attributing later spends |
| `accepted_by` | `chain_block_hash` | `Vec<tx_num>` — reorg unwind list |

Global (never pruned, in a small persistent `meta.redb`):

| table | contents |
|---|---|
| `meta` | schema version, checkpoint (last processed chain block hash + DAA), window bounds |
| `counters` | monotonic `total_txs`, plus anything `/info` needs |
| `addr_totals` | `address → (total_received_sompi, total_tx_count)` — 16 bytes/address, survives segment drops so wallet pagination math stays truthful |

**Input attribution caveat:** a spend of an outpoint created *before* the
window can't be attributed from `utxo_owned`. Prefer the node's
`RpcTransactionInputVerboseData.utxoEntry` when populated; otherwise record
the spend in `outpoint_spend` (swap safety is unaffected) and skip the
`addr_history` debit row. This degradation self-heals after one full window of
continuous operation. Document it in the API docs.

## 5. Chain follower (feed pipeline)

Two-stream design (same shape as simply-kaspa-indexer):

1. **`BlockAdded` subscription** → full block bodies land in a short-lived
   in-RAM **staging cache** (`tx_id → tx body`), since accepted txs live in
   merged blocks, not the accepting chain block.
2. **`VirtualChainChanged` subscription** (`includeAcceptedTransactionIds`) →
   the authority on acceptance. For each added chain block: move its accepted
   txs from staging (fallback: fetch the containing block) into the ledger,
   append `accepted_by`, bump counters, advance the checkpoint. For each
   **removed** chain block: unwind via `accepted_by` (delete ledger rows,
   decrement counters) — cheap, structural reorg handling.
3. **Backfill on startup / after gaps:** from the checkpoint (or
   `virtual tip − window`, or the pruning point — whichever is latest) via
   `getVirtualChainFromBlock` + `getBlocks(includeTransactions)` in batches
   until caught up, then switch to live notifications. Reconnects re-subscribe
   and re-enter backfill for the gap.
4. **Mempool overlay (in-RAM only):** poll `getMempoolEntries` (adaptive
   ~2 s; later optimizable to `getMempoolEntriesByAddresses` over a watch
   set). Maintains `address → pending entries` and `outpoint → spending tx` so
   **an HTLC claim yields its preimage at relay time**, before it is mined.

### NodeClient changes required

`src/node.rs` currently drops messages with unknown ids (notifications).
Extend it with:
- a notification channel (`mpsc` to the indexer) for `BlockAdded` /
  `VirtualChainChanged` payloads;
- `Notify*Request` subscription calls issued after every (re)connect;
- a connection-generation counter so the indexer knows when to gap-backfill.

The indexer runs as one sequential task (single writer); RPC handlers read
via the storage trait. Everything stays in the one binary behind `--indexer`.

## 6. API changes

Modified:

| endpoint | change |
|---|---|
| `GET /api/v1/addresses/{addr}?limit&offset` | real data: window rows (newest-first) + mempool overlay entries; `total_tx_count` / `total_received_sompi` from `addr_totals`; **adds `history_since_daa`** so clients can distinguish "no txs" from "no txs in window" (additive field — current wallet ignores it) |
| `GET /api/v1/info` | `total_txs` from counters; window metadata |
| `GET /health` | `indexer` section: `disabled \| backfilling \| live`, checkpoint DAA, window bounds, segment count, db size |

New (all answerable from window + mempool; 404 beyond the window):

| endpoint | purpose |
|---|---|
| `GET /api/v1/outpoints/{txid}/{index}/spend` | spending tx (full JSON incl. `signature_script`) + `{status: "mempool" \| "accepted"}` — **HTLC preimage extraction**; also serves refund-race detection |
| `GET /api/v1/transactions/{id}` | tx by id from the window (full JSON **incl. `payload`**) — swap-recovery scans read redeem-script hints from funding-tx payloads; path already reserved in the official API surface |

Response shapes stay snake_case/sompi, matching the wallet's wire contract.
Raw `payload` must be included from day one (painful to retrofit).

## 7. Configuration additions

| flag | env | default |
|---|---|---|
| `--indexer` | `KERYX_SHIM_INDEXER` | off |
| `--indexer-dir` | `KERYX_SHIM_INDEXER_DIR` | `./indexer-data` |
| `--indexer-window-days` | `KERYX_SHIM_INDEXER_WINDOW_DAYS` | `7` |
| `--mempool-poll-ms` | `KERYX_SHIM_MEMPOOL_POLL_MS` | `2000` |

**Operational requirement:** run keryxd with `--retention-period-days` ≥ the
shim window (block bodies must outlive the backfill horizon; the node default
is only ~2 days). Document in README; warn from the startup probe when the
node's retention is unknown/short.

## 8. Testing & verification

- **Unit:** segment boundary math, delta/`is_spend` attribution, unwind
  correctness, counter monotonicity across segment drops.
- **Mock e2e (extend `src/e2e_test.rs`):** mock node streams
  `BlockAdded`/`VirtualChainChanged`; scripted scenarios: linear growth,
  reorg (removed chain blocks), restart-with-checkpoint, window expiry.
  Assert history/spend/tx endpoints byte-for-byte.
- **Live simnet (Docker stack `keryx-swap-devnet`):** drive real transfers
  with the wallet's own `tx.js` via a Node script against `/broadcast`; verify
  history rows, spend lookup on a spent outpoint, payload passthrough;
  compare `addr_totals` against `utxos/count` + balance.
- **Perf gate:** replay ≥1 day of simnet history; ingest must sustain ≥10×
  real-time (i.e. ≥100 blocks/s) on developer hardware.
- **Crash recovery:** kill -9 during ingest; restart must resume from
  checkpoint with no duplicate or missing rows (idempotent apply keyed by
  chain block hash).

## 9. Milestones

1. **M1 — pipeline skeleton ✅ (2026-07-08):** NodeClient notifications +
   subscriptions, staging/acceptance flow, checkpointing, counters; linear
   chain only; behind `--indexer`. In-memory store (`MemStore`) behind the
   `IndexStore` trait; `/health` exposes indexer state. Verified live on the
   simnet stack: went live in ~6 s and tracked the tip with checkpoint,
   `total_txs`, and `applied_blocks` advancing in lockstep. Durable store and
   reorg unwind are M2.
2. **M2 — durable store & reorgs ✅ (2026-07-08):** redb-backed store
   (`src/indexer/store.rs`), one write transaction per apply, idempotent by
   chain-block hash; address attribution via a ported bech32 codec
   (`src/indexer/address.rs`); `accepted_by` reorg unwind; retention sweep
   below the DAA cutoff (monotonic counters preserved). Note: retention is a
   bounded row-delete sweep, not physical segment-file drops — simpler and
   correct; segment-file archiving stays a future option. Verified live:
   crash-safe resume (kill -9 → checkpoint/counters continued from the
   committed state, no re-index from scratch).
3. **M3 — read endpoints ✅ (2026-07-08):** real history with
   `history_since_daa`, `GET /transactions/{id}` (payload passthrough),
   `GET /outpoints/{txid}/{index}/spend` (HTLC preimage path), `/info.total_txs`
   from the indexer counter. Verified live on simnet against real coinbase
   data (address attribution, coinbase detection, payload passthrough, 404s).
4. **M4 — mempool overlay:** pending history entries + relay-time spend/preimage
   visibility.
5. **M5 — hardening & release:** perf replay, crash tests, README/API docs,
   `v0.2.0`.

Each milestone lands green on the existing CI (fmt, clippy `-D warnings`,
tests) and keeps `--indexer` off-by-default until M5.

## 10. Phase 2b — `/info` explorer counters

`total_txs` ships with M1 counters. `burned_krx` / `total_escrow_krx` (via
`CsvPubKey` escrow-script detection) ride on the same ledger pass — cheap
add-ons once M2 is stable.

## 11. Phase 2c — inference oracle (research first)

Blocked on two questions in keryx-node before schema design:
1. How miner **results** land on-chain (which tx form carries the IPFS CID /
   `result`), and what `request_hash` is computed over (challenges correlate
   via `request_hash_hex[..16] == payload_prefix`).
2. Whether `/capabilities` derives from the block `pomProof` field
   (Proof-of-Model witness; see `POM_CONSENSUS_SPEC.md` in keryx-node).

## 12. Open questions

- Payload recovery-hint format (what the swap app embeds, encryption) — swap
  app protocol concern; shim only guarantees payload passthrough.
- Whether history should merge mempool entries inline (current plan: yes,
  flagged by `daa_score: 0` entries) or via a separate endpoint — decide at M4
  against real wallet rendering.
- redb vs RocksDB revisit threshold: if sustained ingest &lt; 10× real-time in
  the M5 perf gate, switch the storage backend before release.
