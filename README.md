# keryx-api-shim

A fast, small REST proxy between the **Keryx Wallet Extension** and a **keryxd** node,
with an optional built-in **windowed indexer** (phase 2) for the pieces a bare node
can't answer.

The wallet speaks REST + snake_case JSON (`/api/v1/...`, sompi integers); keryxd only
speaks Kaspa-style gRPC/wRPC (camelCase protobuf, no REST, no auth, no CORS, no TLS).
This shim translates between the two. In its default mode it holds no database and
serves everything from the node's live state plus tiny TTL caches. With `--indexer` it
also follows the chain into a small, bounded, crash-safe store to serve transaction
history and spend lookups — deliberately a **rolling window**, not a full archival
indexer (see [docs/PHASE2.md](docs/PHASE2.md)).

```
Keryx Wallet Extension ── HTTPS/REST ──> keryx-api-shim ── gRPC (protowire) ──> keryxd
        (browser, MV3)                     (this repo)          --utxoindex
                                     (optional redb window index)
```

## Running

Prebuilt binaries for Linux and Windows are attached to
[GitHub Releases](../../releases); alternatively build from source
(Rust toolchain + `protoc` on PATH) or use Docker (below).

```sh
# 1. Node — must expose gRPC (on by default, loopback :22110) and run the UTXO index:
keryxd --utxoindex

# 2. Shim (proxy only):
cargo run --release

# 2b. Or with the windowed indexer enabled:
cargo run --release -- --indexer
# → listening on http://127.0.0.1:8787
```

### Docker

The image binds `0.0.0.0:8787` inside the container (override with
`KERYX_SHIM_LISTEN`); only `protoc`-less Docker is needed on the host.

```sh
docker build -t keryx-api-shim .

# node on the host:
docker run --rm -p 8787:8787 \
  -e KERYX_SHIM_NODE_GRPC=http://host.docker.internal:22110 keryx-api-shim

# node in a compose stack: attach to its network and use the service name
docker run --rm -p 8787:8787 --network <stack-network> \
  -e KERYX_SHIM_NODE_GRPC=http://keryxd:22110 keryx-api-shim
```

### Configuration (flags or env vars)

| flag | env | default | purpose |
|---|---|---|---|
| `--listen` | `KERYX_SHIM_LISTEN` | `127.0.0.1:8787` | HTTP bind address |
| `--node-grpc` | `KERYX_SHIM_NODE_GRPC` | `http://127.0.0.1:22110` | keryxd gRPC endpoint (mainnet default port; testnet 22210, simnet 22510, devnet 22610) |
| `--node-timeout-secs` | `KERYX_SHIM_NODE_TIMEOUT_SECS` | `10` | per-request node timeout; keep under the wallet's 15 s fetch abort |
| `--ipfs-gateway` | `KERYX_SHIM_IPFS_GATEWAY` | *(off)* | base URL of an IPFS gateway backing `GET /ipfs/{cid}` (1 MiB cap) |
| `--market-upstream` | `KERYX_SHIM_MARKET_UPSTREAM` | *(off)* | URL whose JSON is served (cached 30 s) as `GET /api/v1/market` |
| `--max-utxo-limit` | `KERYX_SHIM_MAX_UTXO_LIMIT` | `10000` | hard cap for the UTXO `limit` query param |
| `--indexer` | `KERYX_SHIM_INDEXER` | `false` | enable the windowed indexer (phase 2) |
| `--indexer-window-days` | `KERYX_SHIM_INDEXER_WINDOW_DAYS` | `7` | retention window (days of DAA) |
| `--indexer-dir` | `KERYX_SHIM_INDEXER_DIR` | `./indexer-data` | redb data directory |
| `--mempool-poll-ms` | `KERYX_SHIM_MEMPOOL_POLL_MS` | `2000` | mempool poll interval for the pending-tx overlay (0 disables) |

Logs: `RUST_LOG` (e.g. `RUST_LOG=keryx_api_shim=debug`).

## API

Implemented against the wallet's real client (`src/lib/api.js`) and protocol notes
(`docs/PROTOCOL.md`). All errors are `{ "error": "<message>" }` with a non-2xx status.

| endpoint | source | notes |
|---|---|---|
| `GET /api/v1/info` | `getBlockDagInfo` + `getCoinSupply` (+ cached hashrate & block reward) | cached 2 s; `total_txs` from the indexer counter when enabled, else 0; `burned_krx`/`total_escrow_krx`/`total_real_inferences` still 0 |
| `GET /api/v1/addresses/{addr}/balance` | `getBalanceByAddress` | needs `--utxoindex` |
| `GET /api/v1/addresses/{addr}/utxos?limit=N` | `getUtxosByAddresses` | sorted largest-first, then truncated to `limit` |
| `GET /api/v1/addresses/{addr}/utxos/count` | `getUtxosByAddresses` | count of the full set |
| `GET /api/v1/addresses/{addr}?limit&offset` | **indexer** or empty stub | real per-address history (newest-first, `is_spend` direction) when `--indexer`; adds `history_since_daa` and `pending` mempool rows |
| `GET /api/v1/transactions/{id}` | **indexer** or 404 | full wire tx incl. `payload` (swap recovery) |
| `GET /api/v1/outpoints/{txid}/{index}/spend` | **indexer** or 404 | the spending tx (HTLC preimage is in its `signature_script`); `status: "mempool"` before mining, else `"accepted"` |
| `POST /api/v1/broadcast` | `submitTransaction` | wallet wire JSON incl. string-encoded u64 `sequence`; `allowOrphan` always false |
| `GET /api/v1/market` | proxied upstream or **404** | wallet catch-guards this and hides USD values |
| `GET /api/v1/capabilities` | **indexer** or `[]` | models declared in coinbase `/ai:cap:` markers, with miner pubkeys/count/`last_seen_daa` |
| `GET /api/v1/infer?limit&offset` | **indexer** or `[]` | AiRequest feed joined with responses; `payload_prefix`, `result` (IPFS CID), `result_text: null` (fetch via `/ipfs/{cid}`) |
| `GET /api/v1/challenges?limit` | **indexer** or `[]` | fraud challenges; `fraud_proven` always `false` (node removed on-chain slashing in v1.2.3) |
| `GET /ipfs/{cid}` | proxied gateway or **404** | CIDv0 only, 1 MiB cap |
| `GET /health` | `getServerInfo` | shim + node health; `indexer` section (state, checkpoint/window DAA, counts) when enabled |

Status mapping: node RPC rejection → **400**, node unreachable → **503**, node timeout →
**504**, upstream (ipfs/market) failure → **502**, store error → **500**, malformed
input → **400**.

## The windowed indexer (`--indexer`)

A single task follows keryxd's virtual selected-parent chain
(`BlockAdded` + `VirtualChainChanged` subscriptions) and folds accepted
transactions into a redb store: per-address history with signed direction, an
outpoint→spender index (HTLC preimage extraction), transaction bodies (with
`payload`), and monotonic counters. A mempool poller overlays unconfirmed
activity so a claim's preimage is visible at **relay time**. It is designed for
the [HTLC atomic-swap app](docs/PHASE2.md): bounded, ~1 GB steady state, and
crash-safe (each apply is one transaction; restart resumes from the committed
checkpoint).

Operational notes:

- **Rolling window, not archival.** Data older than `--indexer-window-days` is
  swept away; `history_since_daa` tells clients where the window starts so they
  can point the user at an explorer for older history.
- **Forward-fill on first run.** A fresh index starts at the current tip and
  fills going forward — it does not deep-replay history from before it first
  ran. After a restart it gap-backfills from its checkpoint. Because the
  window is the swap app's recovery buffer, run the shim **continuously**.
- **Node retention must cover the window.** Run keryxd with
  `--retention-period-days` ≥ `--indexer-window-days` (the node default is only
  ~2 days) so a restart can always backfill its gap; otherwise pruned block
  bodies leave a hole.
- Transaction history and the inference/market endpoints are the only ones that
  need indexing; balances, UTXOs, and broadcast are always served live from the
  node whether or not the indexer is on.
- **AI inference oracle** (`/capabilities`, `/infer`, `/challenges`): the
  indexer decodes the AI subnetwork txs (03/04/05) and coinbase capability
  markers. Inference result *text* is off-chain — only the IPFS CID is on-chain,
  fetched via `/ipfs/{cid}`. `fraud_proven` is always `false` (keryx-node
  removed on-chain slashing in v1.2.3).

## Pointing the wallet extension at the shim

Current wallet builds retarget at **runtime**: open **Settings → Network** and set the
shim's URL (stored as `krx_api_base`; leave empty to reset to the official host). No
rebuild, and you do **not** add the shim to `host_permissions` — a custom host is
deliberately reached over ordinary CORS, so the shim's permissive
`Access-Control-Allow-Origin: *` (on by default) is what makes it work. Two constraints:

- **HTTPS unless loopback.** The extension is a secure context, so the browser blocks
  plain-`http://` requests to any non-loopback host as mixed content;
  `http://127.0.0.1` / `http://localhost` are exempt. The wallet's Settings field
  rejects a non-loopback `http://` host for this reason — use `https://` for a remote
  shim.
- **Don't strip CORS at your proxy.** If you terminate TLS in front of the shim
  (Caddy/nginx), ensure it preserves `Access-Control-Allow-Origin: *` and passes the
  `OPTIONS` preflight through (the wallet's `POST /broadcast` sends
  `Content-Type: application/json`, which triggers one). A proxy that drops either makes
  every wallet request fail as an opaque "node unreachable".

To change the **baked-in default** host instead (so a fresh install points here without
touching Settings), edit `src/lib/api.js` → `DEFAULT_API_BASE` and add the origin to
`manifest.json` → `host_permissions`, then rebuild.

keryxd itself has no TLS/auth/rate limits, so keep the node loopback-bound and add those
controls at or in front of the shim.

## Development

```sh
cargo test        # unit + in-process e2e (real router → real gRPC client → mock node)
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test --release perf_ingest_throughput -- --ignored --nocapture   # store ingest bench
```

CI (fmt, clippy, tests) runs on every push/PR; pushing a `v*` tag builds
Linux + Windows binaries and publishes a GitHub release
(`.github/workflows/`).

Protos under `proto/` are vendored verbatim from `keryx-node/rpc/grpc/core/proto` so the
shim builds standalone. The node client (`src/node.rs`) multiplexes every RPC over
keryxd's single `MessageStream` bidi stream with request-id correlation and automatic
reconnect + backoff; the indexer lives under `src/indexer/`.
