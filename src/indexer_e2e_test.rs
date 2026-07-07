//! In-process end-to-end test for the M1 indexer pipeline: real NodeClient
//! (gRPC bidi stream) → real indexer task → mock protowire node that pushes
//! `BlockAdded`/`VirtualChainChanged` notifications, asserted through `/health`.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::node::proto::rpc_server::{Rpc, RpcServer};
use crate::node::proto::{self, kaspad_request, kaspad_response, KaspadRequest, KaspadResponse};
use crate::node::NodeClient;

const SINK_HASH: &str = "ee00000000000000000000000000000000000000000000000000000000000011";
const CHAIN_A: &str = "a100000000000000000000000000000000000000000000000000000000000000";
const TX1: &str = "0000000000000000000000000000000000000000000000000000000000000001";
const TX2: &str = "0000000000000000000000000000000000000000000000000000000000000002";
const CHAIN_A_DAA: u64 = 500;

#[derive(Clone, Default)]
struct MockIndexerNode;

fn reply(id: u64, payload: kaspad_response::Payload) -> KaspadResponse {
    KaspadResponse {
        id,
        payload: Some(payload),
    }
}

/// An unsolicited push (notification): id 0, as keryxd sends them.
fn push(payload: kaspad_response::Payload) -> KaspadResponse {
    KaspadResponse {
        id: 0,
        payload: Some(payload),
    }
}

impl MockIndexerNode {
    /// Returns the response(s) for one request. Subscribing to
    /// virtual-chain-changed also emits one acceptance notification, so the
    /// indexer has exactly one chain block (CHAIN_A, 2 accepted txs) to fold.
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
                    virtual_daa_score: CHAIN_A_DAA,
                    ..Default::default()
                }),
            )],
            Req::NotifyBlockAddedRequest(_) => vec![reply(
                id,
                Resp::NotifyBlockAddedResponse(proto::NotifyBlockAddedResponseMessage {
                    error: None,
                }),
            )],
            Req::NotifyVirtualChainChangedRequest(_) => vec![
                reply(
                    id,
                    Resp::NotifyVirtualChainChangedResponse(
                        proto::NotifyVirtualChainChangedResponseMessage { error: None },
                    ),
                ),
                push(Resp::VirtualChainChangedNotification(
                    proto::VirtualChainChangedNotificationMessage {
                        removed_chain_block_hashes: vec![],
                        added_chain_block_hashes: vec![CHAIN_A.into()],
                        accepted_transaction_ids: vec![proto::RpcAcceptedTransactionIds {
                            accepting_block_hash: CHAIN_A.into(),
                            accepted_transaction_ids: vec![TX1.into(), TX2.into()],
                        }],
                    },
                )),
            ],
            Req::GetBlockDagInfoRequest(_) => vec![reply(
                id,
                Resp::GetBlockDagInfoResponse(proto::GetBlockDagInfoResponseMessage {
                    network_name: "keryx-simnet".into(),
                    sink: SINK_HASH.into(),
                    virtual_daa_score: CHAIN_A_DAA,
                    ..Default::default()
                }),
            )],
            // Fresh index → nothing to backfill from the sink.
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
            // DAA resolution fallback for the accepting block (not staged here).
            Req::GetBlockRequest(r) => {
                assert_eq!(r.hash, CHAIN_A, "indexer resolved DAA for the wrong block");
                vec![reply(
                    id,
                    Resp::GetBlockResponse(proto::GetBlockResponseMessage {
                        block: Some(proto::RpcBlock {
                            header: Some(proto::RpcBlockHeader {
                                daa_score: CHAIN_A_DAA,
                                ..Default::default()
                            }),
                            transactions: vec![],
                            verbose_data: None,
                            pom_proof: None,
                        }),
                        error: None,
                    }),
                )]
            }
            other => panic!("mock indexer node got an unexpected request: {other:?}"),
        }
    }
}

#[tonic::async_trait]
impl Rpc for MockIndexerNode {
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

#[tokio::test]
async fn indexer_pipeline_end_to_end() {
    let node_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let node_addr: SocketAddr = node_listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(RpcServer::new(MockIndexerNode))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                node_listener,
            ))
            .await
            .unwrap();
    });

    let url = format!("http://{node_addr}");
    let (node, notifs) = NodeClient::spawn_indexed(url.clone(), Duration::from_secs(5));
    let indexer = crate::indexer::spawn(node.clone(), notifs, 7);

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
    assert_eq!(ix["applied_blocks"], 1, "wrong block count: {health}");
    assert_eq!(
        ix["checkpoint_daa"], CHAIN_A_DAA,
        "wrong checkpoint: {health}"
    );
    assert_eq!(ix["window_days"], 7);
}
