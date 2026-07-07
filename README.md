# keryx-api-shim

A fast, small REST proxy between the **Keryx Wallet Extension** and a **keryxd** node.

The wallet speaks REST + snake_case JSON (`/api/v1/...`, sompi integers); keryxd only
speaks Kaspa-style gRPC/wRPC (camelCase protobuf, no REST, no auth, no CORS, no TLS).
This shim translates between the two — and deliberately **is not an indexer**: it holds
no database and serves everything from the node's live state plus tiny in-memory TTL
caches. Endpoints that genuinely need historical indexing return honest, well-formed
stubs until the indexer phase lands (see [Phase 1 scope](#phase-1-scope)).

```
Keryx Wallet Extension ── HTTPS/REST ──> keryx-api-shim ── gRPC (protowire) ──> keryxd
        (browser, MV3)                     (this repo)          --utxoindex
```

## Running

Prebuilt binaries for Linux and Windows are attached to
[GitHub Releases](../../releases); alternatively build from source
(Rust toolchain + `protoc` on PATH) or use Docker (below).

```sh
# 1. Node — must expose gRPC (on by default, loopback :22110) and run the UTXO index:
keryxd --utxoindex

# 2. Shim:
cargo run --release
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

Logs: `RUST_LOG` (e.g. `RUST_LOG=keryx_api_shim=debug`).

## API

Implemented against the wallet's real client (`src/lib/api.js`) and protocol notes
(`docs/PROTOCOL.md`). All errors are `{ "error": "<message>" }` with a non-2xx status.

| endpoint | source | notes |
|---|---|---|
| `GET /api/v1/info` | `getBlockDagInfo` + `getCoinSupply` (+ cached best-effort hashrate & coinbase-median block reward) | cached 2 s; indexer-only fields (`total_txs`, `burned_krx`, `total_escrow_krx`, `total_real_inferences`) are `0` in phase 1 |
| `GET /api/v1/addresses/{addr}/balance` | `getBalanceByAddress` | needs `--utxoindex` |
| `GET /api/v1/addresses/{addr}/utxos?limit=N` | `getUtxosByAddresses` | sorted largest-first, then truncated to `limit`, so a truncated page is the most useful set for the wallet's greedy coin selection |
| `GET /api/v1/addresses/{addr}/utxos/count` | `getUtxosByAddresses` | count of the full set |
| `GET /api/v1/addresses/{addr}?limit&offset` | **stub** | tx history needs the indexer; returns an empty well-formed page |
| `POST /api/v1/broadcast` | `submitTransaction` | accepts the wallet's wire JSON incl. string-encoded u64 `sequence`; `allowOrphan` always false |
| `GET /api/v1/market` | proxied upstream or **404** | wallet catch-guards this and hides USD values |
| `GET /api/v1/capabilities` `/infer` `/challenges` | **stub** `[]` | AI-inference oracle data needs the indexer |
| `GET /ipfs/{cid}` | proxied gateway or **404** | CIDv0 only, 1 MiB cap |
| `GET /health` | `getServerInfo` | shim + node health, `has_utxo_index`, `is_synced` |

Status mapping: node RPC rejection → **400**, node unreachable → **503**, node timeout →
**504**, upstream (ipfs/market) failure → **502**, malformed input → **400**.

## Pointing the wallet extension at the shim

The extension pins its API origin in **two** places — both must change, then rebuild:

1. `src/lib/api.js` → `API_BASE`
2. `manifest.json` → `host_permissions` (MV3 blocks all other origins)

The extension requires HTTPS in production contexts; put the shim behind your TLS
terminator (Caddy/nginx) — keryxd itself has no TLS/auth/rate limits, so keep the node
loopback-bound and add those controls at or in front of the shim.

## Phase 1 scope

Serves everything a bare `--utxoindex` node can answer: balances, UTXOs, broadcast,
chain info, health. The wallet's hard-required paths (`balance`, `utxos`, `broadcast`)
are fully live; polling-driven UI (5–15 s intervals, no websockets) is absorbed by
small TTL caches, no database anywhere.

Deferred to the indexer phase (stubs today, seams ready):

- per-address transaction history (`GET /api/v1/addresses/{addr}`) with `total_tx_count`
  and `is_spend` direction — needs an address→tx ledger a node doesn't keep
- AI-inference oracle feeds (`/capabilities`, `/infer`, `/challenges`) — needs decoding
  and indexing AiRequest subnetwork transactions
- explorer statistics in `/info` (`total_txs`, `burned_krx`, ...)

## Development

```sh
cargo test        # unit tests + an in-process e2e: real router → real gRPC client → mock protowire node
cargo fmt
cargo clippy --all-targets -- -D warnings
```

CI (fmt, clippy, tests) runs on every push/PR; pushing a `v*` tag builds
Linux + Windows binaries and publishes a GitHub release
(`.github/workflows/`).

Protos under `proto/` are vendored verbatim from `keryx-node/rpc/grpc/core/proto` so the
shim builds standalone. The node client (`src/node.rs`) multiplexes every RPC over
keryxd's single `MessageStream` bidi stream with request-id correlation and automatic
reconnect + backoff.
