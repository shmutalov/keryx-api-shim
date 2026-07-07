//! In-process end-to-end test: real axum router → real NodeClient (gRPC over
//! a real socket, id-correlated bidi stream) → mock protowire node.
//! Asserts the exact JSON the wallet extension expects on the wire.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use clap::Parser;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::node::proto::rpc_server::{Rpc, RpcServer};
use crate::node::proto::{self, kaspad_request, kaspad_response, KaspadRequest, KaspadResponse};
use crate::node::NodeClient;

const SINK_HASH: &str = "ee00000000000000000000000000000000000000000000000000000000000011";
const TXID: &str = "aa11223344556677889900aabbccddeeff00112233445566778899aabbccddee";
const BROADCAST_TXID: &str = "cc00000000000000000000000000000000000000000000000000000000000022";

#[derive(Clone, Default)]
struct MockNode {
    /// Last transaction received via SubmitTransaction, for wire assertions.
    submitted: Arc<Mutex<Option<proto::RpcTransaction>>>,
}

impl MockNode {
    fn respond(&self, request: KaspadRequest) -> KaspadResponse {
        use kaspad_request::Payload as Req;
        use kaspad_response::Payload as Resp;
        let payload = match request.payload.expect("request payload") {
            Req::GetBlockDagInfoRequest(_) => {
                Resp::GetBlockDagInfoResponse(proto::GetBlockDagInfoResponseMessage {
                    network_name: "keryx-simnet".into(),
                    block_count: 1000,
                    header_count: 1000,
                    tip_hashes: vec![SINK_HASH.into()],
                    difficulty: 1.0,
                    past_median_time: 0,
                    virtual_parent_hashes: vec![],
                    pruning_point_hash: String::new(),
                    virtual_daa_score: 123_456,
                    sink: SINK_HASH.into(),
                    error: None,
                })
            }
            Req::GetCoinSupplyRequest(_) => {
                Resp::GetCoinSupplyResponse(proto::GetCoinSupplyResponseMessage {
                    max_sompi: 10_000_000_000_000_000,        // 100M KRX
                    circulating_sompi: 2_500_000_000_000_000, // 25M KRX
                    error: None,
                })
            }
            Req::EstimateNetworkHashesPerSecondRequest(_) => {
                Resp::EstimateNetworkHashesPerSecondResponse(
                    proto::EstimateNetworkHashesPerSecondResponseMessage {
                        network_hashes_per_second: 987_654,
                        error: None,
                    },
                )
            }
            Req::GetBlockRequest(req) => {
                assert_eq!(req.hash, SINK_HASH);
                Resp::GetBlockResponse(proto::GetBlockResponseMessage {
                    block: Some(proto::RpcBlock {
                        header: None,
                        transactions: vec![proto::RpcTransaction {
                            version: 0,
                            inputs: vec![],
                            outputs: vec![proto::RpcTransactionOutput {
                                amount: 5_000_000_000, // 50 KRX coinbase output
                                script_public_key: None,
                                verbose_data: None,
                            }],
                            lock_time: 0,
                            subnetwork_id: String::new(),
                            gas: 0,
                            payload: String::new(),
                            verbose_data: None,
                            mass: 0,
                        }],
                        verbose_data: None,
                        pom_proof: None,
                    }),
                    error: None,
                })
            }
            Req::GetBalanceByAddressRequest(req) => {
                assert!(req.address.starts_with("keryx:"));
                Resp::GetBalanceByAddressResponse(proto::GetBalanceByAddressResponseMessage {
                    balance: 12_345_678,
                    error: None,
                })
            }
            Req::GetUtxosByAddressesRequest(req) => {
                let address = req.addresses[0].clone();
                let entry = |amount: u64, index: u32| proto::RpcUtxosByAddressesEntry {
                    address: address.clone(),
                    outpoint: Some(proto::RpcOutpoint {
                        transaction_id: TXID.into(),
                        index,
                    }),
                    utxo_entry: Some(proto::RpcUtxoEntry {
                        amount,
                        script_public_key: Some(proto::RpcScriptPublicKey {
                            version: 0,
                            script_public_key: "20aa11ac".into(),
                        }),
                        block_daa_score: 100_000,
                        is_coinbase: false,
                        verbose_data: None,
                    }),
                };
                Resp::GetUtxosByAddressesResponse(proto::GetUtxosByAddressesResponseMessage {
                    // Smaller amount first: the shim must return largest-first.
                    entries: vec![entry(1_000, 0), entry(2_000, 1)],
                    error: None,
                })
            }
            Req::SubmitTransactionRequest(req) => {
                assert!(!req.allow_orphan, "shim must not allow orphans");
                *self.submitted.lock().unwrap() = req.transaction;
                Resp::SubmitTransactionResponse(proto::SubmitTransactionResponseMessage {
                    transaction_id: BROADCAST_TXID.into(),
                    error: None,
                })
            }
            Req::GetServerInfoRequest(_) => {
                Resp::GetServerInfoResponse(proto::GetServerInfoResponseMessage {
                    rpc_api_version: 1,
                    rpc_api_revision: 0,
                    server_version: "1.3.1-mock".into(),
                    network_id: "keryx-simnet".into(),
                    has_utxo_index: true,
                    is_synced: true,
                    virtual_daa_score: 123_456,
                    error: None,
                })
            }
            other => panic!("mock node got an unexpected request: {other:?}"),
        };
        KaspadResponse {
            id: request.id,
            payload: Some(payload),
        }
    }
}

#[tonic::async_trait]
impl Rpc for MockNode {
    type MessageStreamStream = Pin<Box<dyn Stream<Item = Result<KaspadResponse, Status>> + Send>>;

    async fn message_stream(
        &self,
        request: Request<Streaming<KaspadRequest>>,
    ) -> Result<Response<Self::MessageStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let node = self.clone();
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            while let Some(Ok(req)) = inbound.next().await {
                if tx.send(Ok(node.respond(req))).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

async fn spawn_stack() -> (String, Arc<Mutex<Option<proto::RpcTransaction>>>) {
    // Mock node on an ephemeral port.
    let node_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_addr: SocketAddr = node_listener.local_addr().unwrap();
    let mock = MockNode::default();
    let submitted = mock.submitted.clone();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RpcServer::new(mock))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                node_listener,
            ))
            .await
            .unwrap();
    });

    // Real shim wired to the mock node.
    let cfg = crate::config::Config::try_parse_from([
        "keryx-api-shim",
        "--node-grpc",
        &format!("http://{node_addr}"),
    ])
    .unwrap();
    let node = NodeClient::spawn(cfg.node_grpc.clone(), std::time::Duration::from_secs(5));
    let state: crate::api::AppState = Arc::new(crate::api::AppInner {
        node,
        caches: crate::cache::Caches::default(),
        http: reqwest::Client::new(),
        indexer: None,
        cfg,
    });
    let app = crate::api::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    (base, submitted)
}

fn test_address() -> String {
    format!("keryx:{}", "q".repeat(61))
}

#[tokio::test]
async fn wallet_contract_end_to_end() {
    let (base, submitted) = spawn_stack().await;
    let http = reqwest::Client::new();
    let address = test_address();

    // /health
    let health: serde_json::Value = http
        .get(format!("{base}/health"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["status"], "ok");
    assert_eq!(health["node"]["has_utxo_index"], true);

    // /api/v1/info — exact field names the wallet reads.
    let info: serde_json::Value = http
        .get(format!("{base}/api/v1/info"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(info["network"], "keryx-simnet");
    assert_eq!(info["last_daa_score"], 123_456);
    assert_eq!(info["total_blocks"], 1000);
    assert_eq!(info["hashrate_hps"], 987_654.0);
    assert_eq!(info["block_reward_krx"], 50.0);
    assert_eq!(info["max_supply_krx"], 100_000_000.0);
    assert_eq!(info["total_supply_krx"], 25_000_000.0);
    assert_eq!(info["mined_pct"], 25.0);
    assert_eq!(info["total_txs"], 0);

    // /balance
    let balance: serde_json::Value = http
        .get(format!("{base}/api/v1/addresses/{address}/balance"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        balance,
        serde_json::json!({ "address": address, "balance_sompi": 12_345_678 })
    );

    // /utxos — bare array, largest-first, limit respected.
    let utxos: serde_json::Value = http
        .get(format!("{base}/api/v1/addresses/{address}/utxos?limit=2"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        utxos,
        serde_json::json!([
            { "transaction_id": TXID, "index": 1, "amount_sompi": 2000, "script_version": 0,
              "script_public_key": "20aa11ac", "block_daa_score": 100_000, "is_coinbase": false },
            { "transaction_id": TXID, "index": 0, "amount_sompi": 1000, "script_version": 0,
              "script_public_key": "20aa11ac", "block_daa_score": 100_000, "is_coinbase": false }
        ])
    );
    let one: Vec<serde_json::Value> = http
        .get(format!("{base}/api/v1/addresses/{address}/utxos?limit=1"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0]["amount_sompi"], 2000);

    // /utxos/count
    let count: serde_json::Value = http
        .get(format!("{base}/api/v1/addresses/{address}/utxos/count"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(count, serde_json::json!({ "count": 2 }));

    // /addresses/{addr} — phase-1 stub must stay well-formed.
    let history: serde_json::Value = http
        .get(format!(
            "{base}/api/v1/addresses/{address}?limit=15&offset=0"
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        history,
        serde_json::json!({
            "address": address,
            "total_received_sompi": 0,
            "total_tx_count": 0,
            "transactions": []
        })
    );

    // POST /broadcast with the wallet's real serialization (string sequence).
    let tx = serde_json::json!({
        "version": 0,
        "inputs": [{
            "transaction_id": TXID,
            "index": 0,
            "signature_script": "41deadbeef01",
            "sequence": "18446744073709551615",
            "sig_op_count": 1
        }],
        "outputs": [{ "amount": 12345, "script_version": 0, "script_public_key": "20aa11ac" }],
        "lock_time": 0,
        "subnetwork_id": "0000000000000000000000000000000000000000",
        "gas": 0,
        "payload": ""
    });
    let broadcast: serde_json::Value = http
        .post(format!("{base}/api/v1/broadcast"))
        .json(&tx)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        broadcast,
        serde_json::json!({ "transaction_id": BROADCAST_TXID })
    );
    let sent = submitted
        .lock()
        .unwrap()
        .clone()
        .expect("mock saw the transaction");
    assert_eq!(sent.inputs[0].sequence, u64::MAX);
    assert_eq!(
        sent.inputs[0]
            .previous_outpoint
            .as_ref()
            .unwrap()
            .transaction_id,
        TXID
    );
    assert_eq!(sent.outputs[0].amount, 12345);
    assert_eq!(
        sent.outputs[0]
            .script_public_key
            .as_ref()
            .unwrap()
            .script_public_key,
        "20aa11ac"
    );

    // Inference stubs: well-formed empty lists.
    for path in [
        "/api/v1/capabilities",
        "/api/v1/infer?limit=10",
        "/api/v1/challenges?limit=50",
    ] {
        let v: serde_json::Value = http
            .get(format!("{base}{path}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(v, serde_json::json!([]));
    }

    // Unconfigured market + errors keep the wallet's { error } shape.
    let resp = http
        .get(format!("{base}/api/v1/market"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
    assert!(resp.json::<serde_json::Value>().await.unwrap()["error"].is_string());

    let resp = http
        .get(format!("{base}/api/v1/addresses/kaspa:notkeryx/balance"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
    assert!(resp.json::<serde_json::Value>().await.unwrap()["error"].is_string());
}
