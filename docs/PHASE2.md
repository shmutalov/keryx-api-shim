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
4. **M4 — mempool overlay ✅ (2026-07-08):** in-RAM overlay
   (`src/indexer/mempool.rs`) refreshed by a poller (`--mempool-poll-ms`),
   indexing the mempool as outpoint→pending-tx (relay-time preimage) and
   address→pending-rows. `/outpoints/.../spend` returns `status: "mempool"`
   before mining; history prepends pending rows (`daa_score: 0`, `pending:
   true`) on the first page. Verified live: poller runs clean against the
   node; overlay logic covered by a unit test (spend indexing + preimage +
   debit attribution via the store).
5. **M5 — hardening & release ✅ (2026-07-08):** store ingest benched at
   ~48k blocks/s (478× the 100/s gate; the node RPC, not the store, is the
   bound); crash-recovery verified live (kill -9 resume). README documents the
   indexer, endpoints, retention, and the node `--retention-period-days`
   requirement. Released as `v0.2.0`.

   **Design note — forward-fill:** a fresh index starts at the current tip and
   fills forward; it does not deep-replay history from before it first ran
   (mapping tip−window to a start hash needs a block at a target DAA, which the
   RPC doesn't expose; the pruning point is the only natural deep-start and can
   be very large). After a restart it gap-backfills from its committed
   checkpoint. Since the window is the swap app's recovery buffer, the shim is
   meant to run continuously. Deep historical backfill from the pruning point
   is a future enhancement.

Each milestone lands green on the existing CI (fmt, clippy `-D warnings`,
tests) and keeps `--indexer` off-by-default until M5.

## 10. Phase 2b — `/info` explorer counters

`total_txs` ships with M1 counters. `burned_krx` / `total_escrow_krx` (via
`CsvPubKey` escrow-script detection) ride on the same ledger pass — cheap
add-ons once M2 is stable.

## 11. Phase 2c — inference oracle ✅ (2026-07-08, v0.3.0)

Research resolved both questions (see the on-chain mechanics below); implemented
in `src/indexer/inference.rs` (decoders) + inference tables in the store,
extracted in the **same apply pass** as everything else (gated by the same
per-block idempotency, so no double-indexing).

On-chain mechanics (from keryx-node `inference/src/ai_payload.rs`):
- Three subnetworks: **03** AiRequest (model_id, max_tokens, reward, fee,
  prompt), **04** AiResponse (request_hash, IPFS CIDv0, length), **05**
  AiChallenge (response_hash, proof_data = request_hash).
- **`request_hash = BLAKE2b-512(payload)[0..32]`** (512 truncated over the raw
  payload — not 256) until H8; **the request's tx id** from H8 on (see the H8
  bullet below). `payload_prefix = request_hash_hex[..16]`.
- Requests↔responses join on `request_hash`; the **result text is off-chain**
  (only the CIDv0 is on-chain — clients fetch it via the shim's `/ipfs/{cid}`).
- `/capabilities` comes from coinbase `extra_data` markers
  (`/ai:cap:<model_id>,…/` + `/escrow:<pubkey>/`), aggregated per model.
- **`fraud_proven` is always `false`**: keryx-node removed on-chain slashing in
  v1.2.3, so no fraud is provable from consensus state. Documented in the
  endpoint and the response.
- PoM (`RpcBlock.pomProof`) is live on mainnet (`pom_activation = 37_780_000`)
  but `never()` on the simnet the shim is tested against, so caps come from the
  coinbase markers, not PoM — forward-compatible either way.
- No on-chain model-name registry; the shim mirrors the wallet's hardcoded
  model_id→name table (`src/lib/models.js`). Currently the **H6** lineup —
  keryx-node's `POM_TIERS_H6` (gated by `pom_v3_activation`): tier 0 =
  Qwen3.5-9B-abliterated (id `bd34568c…`, replaces both Qwen3-8B and
  Mistral-7B), tier 1 = GLM-4-9B, tier 2 = gemma-4-12B-abliterated (new, id
  `39998404…`), tiers 3-4 unchanged. Retired ids (Qwen3-8B, EXAONE,
  Mistral-7B) stay in the table so pre-fork rows still render by name.
- H6 also introduced **V2 AiResponses** (`ai_payload.rs`): 78 v1 bytes +
  `[escrow_pubkey: 32][schnorr_sig: 64]` = 174 bytes. `decode_response`
  accepts both lengths (v1 fields sit at the same offsets); the responder's
  escrow pubkey is surfaced as `responder_pubkey` (an H14 answer is sealed
  under it). The signature is not verified — the node already judged it.
- **H8 reward routing** (keryx-node `reward_routing_activation`, mainnet DAA
  79 210 000, testnet from genesis, never on simnet/devnet): `AiRequest.outputs[1]`
  must be the keyless vault `OP_RETURN "aivault"`
  (`keryx_inference::INFERENCE_VAULT_SCRIPT`) instead of a CSV escrow naming a
  miner; a later coinbase mints the reward to the first accepted responder, or
  it burns. The store recognises the vault (`is_inference_vault`) and records
  `miner_pubkey: None` for routed requests; pre-H8 rows keep their CSV
  attribution. The same gate also switched the **request identity**
  (keryx-node `service_bond.rs`, `txid_identity`; the miner mirrors it): from
  H8 a request is named by its **tx id** in `AiResponse.request_hash` and in
  the service ledger, because identical payloads used to collide into one
  assignment. Both rules are judged at the accepting chain block's DAA, so the
  store derives the identity from the vault itself
  (`inference::request_identity`) — no per-network activation table. Shim
  0.3.0 missed this and joined routed requests by payload digest, so their
  responses never showed; the feed now also tries the tx id, which repairs
  rows that release indexed. H9-H13 are node-only consensus gates (PoM v4
  re-walk seed, service-ledger and production-index snapshots, exact
  verification).
- **H14 private inference** (keryx-node v1.6.4 `private_inference_activation`,
  mainnet DAA 121 985 000 ≈ 2026-10-09 14:00 UTC, testnet 6 000, never on
  simnet/devnet; `inference/src/private.rs`, `docs/private-inference.md`):
  - Every AiRequest prompt must be a sealed envelope
    (`00 'K' 'X' 'P' 01 | ephemeral key | nonce | n | n × (escrow key, wrapped
    root key) | ciphertext`) readable only by the tier cohort it names;
    plaintext requests are invalid, and the payload cap rises to 64 KiB. The
    shim never renders the ciphertext: `prompt: ""`, `sealed: true`,
    `sealed_recipients` from the envelope layout (keryx-labs.com's names for
    the node's "private" request and its recipients). Like keryx-labs.com's
    feed, `/infer` leaves sealed requests out (decided 2026-10-28); the store
    skips them before applying `limit`/`offset`, and `/inference/{tx_id}`
    still serves them.
  - Each named responder must answer (silent ones are struck), each sealing
    the answer under its own key and carrying it **inline**: a V2 head +
    `[0x80][len: 4 LE][body ≤ 32 KiB]`. The CID field is then just the body's
    sha2-256 multihash (not pinned). One horizon (72 000 DAA) after the gate a
    body-less response is invalid. The store keeps **every** response per
    request (`ai_resp_list`, `request_hash‖FF‖tx_num`) for
    `GET /inference/{tx_id}`, whose `sealed_responses[]` carries each inline
    `body` hex with its `responder_pubkey`, oldest first; the feed summarises
    the first accepted (`result_daa_score`, plus shim extras `result_inline`,
    `result_tx_id`, `responder_pubkey`). A body that does not open is the
    miner's plaintext "could not open" note. Decryption needs the requester's
    root key, the request tx id and `responder_pubkey`; it is the requester's
    job, never the shim's. Path and field names follow keryx-labs.com, whose
    `/infer` page already seals and opens H14 requests in the browser, so a
    client works against either host. Omitted from the official shape:
    `ipfs_url`, `result_status`, `result_fetch_attempts` (its server-side IPFS
    fetch; the shim leaves IPFS to the client).
  - Tier 3 swaps Qwen3.6-27B → Qwen3.8-27B (`73740b44…`, key `qwen3.8-27b`,
    the miner's name); Qwen3.6-27B stays in the registry for old rows.
  - Fixed per-model reward floors, the IBD possession-proof rule and the new
    PoM seed are node-only.
  - `GET /ai/providers` proxies `getServiceProviders` in keryx-labs.com's
    shape: `cohort` (the eligibility window a request is checked against) and
    `window` (the last `window_daa` = 18 000 DAA, as keryx-cli seals to), each
    `{tier, model, model_id, identity, escrow_pubkey}`, cached 3 s. A pre-1.6.4
    node takes the unknown request for a protocol error and drops the whole
    gRPC stream (in-flight calls and the indexer's subscriptions), so the
    handler checks `getServerInfo.serverVersion` (cached 30 s) first and
    answers 503 without sending it.
  - Store rows gained trailing fields; rows written by 0.3.0 decode through
    their legacy layout (`dec_compat`), so an upgrade keeps the window. Rows
    indexed by 0.3.0 are not addressable by tx id (it keyed requests by
    digest only); they age out with the window.
- **H15 verifiable inference** (keryx-node v1.6.5/v1.6.6, one gate for every
  rule: mainnet DAA 140 115 000, testnet 3 500, never on simnet/devnet;
  `verifiable-inference/`, `consensus/src/processes/verifiable_inference.rs`):
  - A sealed answer body is followed by a second extension,
    `[0x81][len: 4 LE][trace_root: 32][n_prompt: 4][n_pos: 4][stage: 1]`
    plus an optional `[tokens_root: 32]` (len 41 or 73), covered by the
    responder signature. One horizon after the gate a body without it is
    invalid. The body cap rises from 32 KiB to 48 KiB. `decode_response`
    accepts a body followed by nothing or by exactly one well-formed trace
    extension; the commitment itself is not surfaced. Without this, H15
    responses decoded with no body and `/inference/{tx_id}` returned no
    `sealed_responses`. `/inference/{tx_id}` re-decodes stored payloads on
    every read, so responses indexed before the fix display once it is
    deployed; only the feed's stored `result_inline` flag would stay false.
  - Every traced response is audited: its responder sends **opening**
    transactions (new subnetwork `06`, zero inputs/outputs, payload
    `[version: 1][response_txid: 32] … [escrow_pubkey: 32][signature: 64]`,
    up to 112 KiB), chained layer to layer, plus attention/recurrent/residual
    pieces. The shim stores them as plain transactions (served by
    `/transactions/{id}`) and does not interpret them; audit verdicts live in
    the node's service ledger, not in transaction data. The new
    `getAwaitedOpenings` RPC (miner-facing) is vendored but not proxied.
  - The reward is drawn among the miners credited on a request, untraced
    answers are not credited, and PoM v5, deadlines, context floors and the
    holder-reward table are node-only. Request identity (tx id), the request
    envelope layout and the tier lineup (`POM_TIERS_H14`) are unchanged.
    Key schedule 2 (`private.rs`) changes how clients derive keys, not the
    envelope bytes; the shim never handles keys.
  - Vendored protos refreshed to v1.6.6 (`RpcBlockHeader.taskRoot`,
    `getAwaitedOpenings`). `/inference/{tx_id}` `sealed_responses[]` gained
    `response_length`, which keryx-labs.com now emits.

Endpoints served from the indexer (else `[]`): `/capabilities`, `/infer`
(joined feed of public requests, `payload_prefix`, `result` CID,
`result_text: null`), `/challenges`; `/inference/{tx_id}` (else 404). `/ai/providers` is
served live from the node. Inference rows respect the retention window.
Verified: store unit tests drive real AiRequest/Response/Challenge/coinbase
payloads through `apply()` and assert all reads + expiry, incl. routed (tx-id)
joins, multi-responder inline bodies and 0.3.0-format rows; the indexer e2e
test pushes a sealed request and two responses through the mock node and
asserts `/infer` and `/inference/{tx_id}` over HTTP; the API e2e test asserts
`/ai/providers` against a 1.6.4 mock and the 503 (request never sent) against
a 1.6.3 one; endpoints return well-formed `[]` live on the idle simnet (which
runs no inference).

## 12. Open questions

- Payload recovery-hint format (what the swap app embeds, encryption) — swap
  app protocol concern; shim only guarantees payload passthrough.
- Whether history should merge mempool entries inline (current plan: yes,
  flagged by `daa_score: 0` entries) or via a separate endpoint — decide at M4
  against real wallet rendering.
- redb vs RocksDB revisit threshold: if sustained ingest &lt; 10× real-time in
  the M5 perf gate, switch the storage backend before release.
