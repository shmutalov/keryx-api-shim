//! In-process end-to-end test for the M2 indexer: real NodeClient → real
//! indexer task → durable redb store, driven by a mock node that pushes
//! `BlockAdded` bodies and `VirtualChainChanged` acceptance. Asserts the
//! pipeline through `/health` and the store's indexed reads (history, tx-by-id,
//! outpoint spend).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::indexer::address::script_to_address;
use crate::node::proto::rpc_server::{Rpc, RpcServer};
use crate::node::proto::{self, kaspad_request, kaspad_response, KaspadRequest, KaspadResponse};
use crate::node::NodeClient;

const SINK_HASH: &str = "ee00000000000000000000000000000000000000000000000000000000000011";
const BLOCK_A: &str = "a100000000000000000000000000000000000000000000000000000000000000";
const BLOCK_B: &str = "b200000000000000000000000000000000000000000000000000000000000000";
const CB_TX: &str = "cb00000000000000000000000000000000000000000000000000000000000001";
const SPEND_TX: &str = "5900000000000000000000000000000000000000000000000000000000000002";
const PK1: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const PK2: &str = "2222222222222222222222222222222222222222222222222222222222222222";
const COINBASE_SUBNET: &str = "0100000000000000000000000000000000000000";
const NATIVE_SUBNET: &str = "0000000000000000000000000000000000000000";

fn output(amount: u64, pk: &str) -> proto::RpcTransactionOutput {
    proto::RpcTransactionOutput {
        amount,
        script_public_key: Some(proto::RpcScriptPublicKey {
            version: 0,
            script_public_key: format!("20{pk}ac"),
        }),
        verbose_data: None,
    }
}

fn tx(
    id: &str,
    subnet: &str,
    inputs: Vec<proto::RpcTransactionInput>,
    outputs: Vec<proto::RpcTransactionOutput>,
) -> proto::RpcTransaction {
    proto::RpcTransaction {
        version: 0,
        inputs,
        outputs,
        lock_time: 0,
        subnetwork_id: subnet.into(),
        gas: 0,
        payload: String::new(),
        verbose_data: Some(proto::RpcTransactionVerboseData {
            transaction_id: id.into(),
            ..Default::default()
        }),
        mass: 0,
    }
}

fn block(hash: &str, daa: u64, txs: Vec<proto::RpcTransaction>) -> proto::RpcBlock {
    proto::RpcBlock {
        header: Some(proto::RpcBlockHeader {
            daa_score: daa,
            ..Default::default()
        }),
        transactions: txs,
        verbose_data: Some(proto::RpcBlockVerboseData {
            hash: hash.into(),
            ..Default::default()
        }),
        pom_proof: None,
    }
}

fn block_added(b: proto::RpcBlock) -> kaspad_response::Payload {
    kaspad_response::Payload::BlockAddedNotification(proto::BlockAddedNotificationMessage {
        block: Some(b),
    })
}

fn vcc(hash: &str, tx_ids: &[&str]) -> kaspad_response::Payload {
    kaspad_response::Payload::VirtualChainChangedNotification(
        proto::VirtualChainChangedNotificationMessage {
            removed_chain_block_hashes: vec![],
            added_chain_block_hashes: vec![hash.into()],
            accepted_transaction_ids: vec![proto::RpcAcceptedTransactionIds {
                accepting_block_hash: hash.into(),
                accepted_transaction_ids: tx_ids.iter().map(|s| s.to_string()).collect(),
            }],
        },
    )
}

#[derive(Clone, Default)]
struct MockNode;

fn reply(id: u64, payload: kaspad_response::Payload) -> KaspadResponse {
    KaspadResponse {
        id,
        payload: Some(payload),
    }
}

fn push(payload: kaspad_response::Payload) -> KaspadResponse {
    KaspadResponse {
        id: 0,
        payload: Some(payload),
    }
}

impl MockNode {
    fn handle(&self, request: KaspadRequest) -> Vec<KaspadResponse> {
        use kaspad_request::Payload as Req;
        use kaspad_response::Payload as Resp;
        let id = request.id;
        match request.payload.expect("request payload") {
            Req::GetServerInfoRequest(_) => vec![reply(
                id,
                Resp::GetServerInfoResponse(proto::GetServerInfoResponseMessage {
                    server_version: "1.3.1-mock".into(),
                    network_id: "simnet".into(),
                    has_utxo_index: true,
                    is_synced: true,
                    virtual_daa_score: 101,
                    ..Default::default()
                }),
            )],
            Req::GetBlockDagInfoRequest(_) => vec![reply(
                id,
                Resp::GetBlockDagInfoResponse(proto::GetBlockDagInfoResponseMessage {
                    network_name: "keryx-simnet".into(),
                    sink: SINK_HASH.into(),
                    virtual_daa_score: 101,
                    ..Default::default()
                }),
            )],
            Req::NotifyBlockAddedRequest(_) => vec![reply(
                id,
                Resp::NotifyBlockAddedResponse(proto::NotifyBlockAddedResponseMessage {
                    error: None,
                }),
            )],
            // On subscribing to VCC, drive the whole scenario: a coinbase to
            // PK1, then a spend of it to PK2. Block bodies precede acceptance.
            Req::NotifyVirtualChainChangedRequest(_) => vec![
                reply(
                    id,
                    Resp::NotifyVirtualChainChangedResponse(
                        proto::NotifyVirtualChainChangedResponseMessage { error: None },
                    ),
                ),
                push(block_added(block(
                    BLOCK_A,
                    100,
                    vec![tx(CB_TX, COINBASE_SUBNET, vec![], vec![output(5_000, PK1)])],
                ))),
                push(vcc(BLOCK_A, &[CB_TX])),
                push(block_added(block(
                    BLOCK_B,
                    101,
                    vec![tx(
                        SPEND_TX,
                        NATIVE_SUBNET,
                        vec![proto::RpcTransactionInput {
                            previous_outpoint: Some(proto::RpcOutpoint {
                                transaction_id: CB_TX.into(),
                                index: 0,
                            }),
                            signature_script: "41beef01".into(),
                            sequence: u64::MAX,
                            sig_op_count: 1,
                            verbose_data: None,
                        }],
                        vec![output(3_000, PK2)],
                    )],
                ))),
                push(vcc(BLOCK_B, &[SPEND_TX])),
            ],
            Req::GetVirtualChainFromBlockRequest(_) => vec![reply(
                id,
                Resp::GetVirtualChainFromBlockResponse(
                    proto::GetVirtualChainFromBlockResponseMessage {
                        removed_chain_block_hashes: vec![],
                        added_chain_block_hashes: vec![],
                        accepted_transaction_ids: vec![],
                        error: None,
                    },
                ),
            )],
            Req::GetBlocksRequest(_) => vec![reply(
                id,
                Resp::GetBlocksResponse(proto::GetBlocksResponseMessage {
                    block_hashes: vec![],
                    blocks: vec![],
                    error: None,
                }),
            )],
            Req::GetBlockRequest(r) => vec![reply(
                id,
                Resp::GetBlockResponse(proto::GetBlockResponseMessage {
                    block: Some(block(&r.hash, 100, vec![])),
                    error: None,
                }),
            )],
            other => panic!("mock node got an unexpected request: {other:?}"),
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
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            while let Some(Ok(req)) = inbound.next().await {
                for msg in node.handle(req) {
                    if tx.send(Ok(msg)).await.is_err() {
                        return;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

fn temp_dir() -> PathBuf {
    static N: AtomicU32 = AtomicU32::new(0);
    let dir = std::env::temp_dir().join(format!(
        "keryx-shim-e2e-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn addr(pk: &str) -> String {
    script_to_address(&format!("20{pk}ac"), "keryxsim").unwrap()
}

#[tokio::test]
async fn indexer_pipeline_end_to_end() {
    let node_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_addr: SocketAddr = node_listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RpcServer::new(MockNode))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                node_listener,
            ))
            .await
            .unwrap();
    });

    let url = format!("http://{node_addr}");
    let (node, notifs) = NodeClient::spawn_indexed(url.clone(), Duration::from_secs(5));
    let indexer = crate::indexer::spawn(node.clone(), notifs, 7, temp_dir());
    let probe = indexer.clone();

    let cfg =
        crate::config::Config::try_parse_from(["keryx-api-shim", "--node-grpc", &url, "--indexer"])
            .unwrap();
    let state: crate::api::AppState = Arc::new(crate::api::AppInner {
        node,
        caches: crate::cache::Caches::default(),
        http: reqwest::Client::new(),
        indexer: Some(indexer),
        cfg,
    });
    let app = crate::api::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    // Wait for both chain blocks to be folded in.
    let http = reqwest::Client::new();
    let mut health = serde_json::Value::Null;
    for _ in 0..100 {
        health = http
            .get(format!("{base}/health"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if health["indexer"]["state"] == "live" && health["indexer"]["total_txs"] == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let ix = &health["indexer"];
    assert_eq!(ix["state"], "live", "indexer never went live: {health}");
    assert_eq!(ix["total_txs"], 2, "wrong tx count: {health}");
    assert_eq!(ix["checkpoint_daa"], 101, "wrong checkpoint: {health}");
    assert_eq!(ix["window_low_daa"], 100, "wrong window low: {health}");

    // Indexed reads through the store.
    let store = probe.store().expect("store open");
    assert!(store.tx_by_id(CB_TX).unwrap().is_some(), "coinbase indexed");
    let spend = store.tx_by_id(SPEND_TX).unwrap().expect("spend indexed");
    assert_eq!(spend.inputs[0].signature_script, "41beef01");

    // Outpoint spend lookup (the HTLC preimage path).
    let spender = store.spend_of(CB_TX, 0).unwrap().expect("outpoint spent");
    assert_eq!(spender.tx_id, SPEND_TX);

    // Address history with direction.
    let h1 = store.address_history(&addr(PK1), 10, 0).unwrap();
    assert_eq!(h1.len(), 2, "PK1 sees coinbase credit + spend debit");
    assert!(h1[0].is_spend, "newest PK1 row is the spend");
    assert_eq!(h1[0].amount_sompi, 5_000);
    let h2 = store.address_history(&addr(PK2), 10, 0).unwrap();
    assert_eq!(h2.len(), 1);
    assert_eq!(h2[0].amount_sompi, 3_000);
    assert!(!h2[0].is_spend);
    let (recv, cnt) = store.address_totals(&addr(PK2)).unwrap();
    assert_eq!((recv, cnt), (3_000, 1));

    // --- M3 HTTP read endpoints ---

    // GET /api/v1/transactions/{id}
    let txj: serde_json::Value = http
        .get(format!("{base}/api/v1/transactions/{SPEND_TX}"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(txj["tx_id"], SPEND_TX);
    assert_eq!(txj["inputs"][0]["transaction_id"], CB_TX);
    assert_eq!(txj["inputs"][0]["signature_script"], "41beef01");
    assert_eq!(txj["inputs"][0]["sequence"], u64::MAX.to_string());
    assert_eq!(txj["outputs"][0]["amount"], 3_000);
    assert_eq!(txj["daa_score"], 101);

    // Unknown tx → 404 with the wallet's { error } shape.
    let missing = http
        .get(format!("{base}/api/v1/transactions/{}", "0".repeat(64)))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), 404);
    assert!(missing.json::<serde_json::Value>().await.unwrap()["error"].is_string());

    // GET /api/v1/outpoints/{txid}/{index}/spend — the HTLC preimage path.
    let spendj: serde_json::Value = http
        .get(format!("{base}/api/v1/outpoints/{CB_TX}/0/spend"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(spendj["status"], "accepted");
    assert_eq!(spendj["transaction"]["tx_id"], SPEND_TX);
    // The preimage lives in the spending input's signature_script.
    assert_eq!(
        spendj["transaction"]["inputs"][0]["signature_script"],
        "41beef01"
    );

    // An unspent outpoint → 404.
    let unspent = http
        .get(format!("{base}/api/v1/outpoints/{SPEND_TX}/0/spend"))
        .send()
        .await
        .unwrap();
    assert_eq!(unspent.status(), 404);

    // GET /api/v1/addresses/{addr} — real history with window metadata.
    let hist: serde_json::Value = http
        .get(format!(
            "{base}/api/v1/addresses/{}?limit=15&offset=0",
            addr(PK1)
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(hist["total_tx_count"], 2);
    assert_eq!(hist["history_since_daa"], 100);
    assert_eq!(hist["transactions"].as_array().unwrap().len(), 2);
    assert_eq!(hist["transactions"][0]["is_spend"], true);
}
