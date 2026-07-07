//! Minimal gRPC client for keryxd's protowire `RPC.MessageStream`.
//!
//! keryxd multiplexes every RPC over a single bidirectional stream: each
//! `KaspadRequest` carries a client-chosen `id`, echoed back on the matching
//! `KaspadResponse`. A background actor owns the stream, pairs responses with
//! waiting callers by id, and reconnects with backoff when the stream breaks.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::codec::CompressionEncoding;
use tonic::transport::{Channel, Endpoint};

pub mod proto {
    #![allow(dead_code, clippy::all)]
    tonic::include_proto!("protowire");
}

use proto::rpc_client::RpcClient;
use proto::{kaspad_request, kaspad_response, KaspadRequest, KaspadResponse};

const CMD_QUEUE: usize = 256;
const NOTIF_QUEUE: usize = 4096;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RECONNECT_MAX: Duration = Duration::from_secs(15);
/// Matches the node's own RPC bound. The default 4 MiB is far too small for
/// `getUtxosByAddresses` on a heavily mined address (observed live on simnet).
const MAX_MESSAGE_SIZE: usize = 128 * 1024 * 1024;

/// Push messages the node streams outside the request/response pairing.
/// Delivered to the (optional) indexer; ordinary RPC callers never see these.
#[derive(Debug)]
pub enum Notification {
    /// A fresh node stream is up (first connect or a reconnect). The indexer
    /// must (re)issue its subscriptions and gap-backfill on each of these.
    Connected {
        generation: u64,
    },
    BlockAdded(Box<proto::RpcBlock>),
    VirtualChainChanged(Box<proto::VirtualChainChangedNotificationMessage>),
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum NodeError {
    #[error("keryx node unreachable: {0}")]
    Unreachable(String),
    #[error("keryx node request timed out")]
    Timeout,
    /// The node processed the request and returned an RPC-level error
    /// (bad address, rejected transaction, missing --utxoindex, ...).
    #[error("{0}")]
    Rpc(String),
    #[error("node protocol error: {0}")]
    Protocol(String),
}

type Reply = oneshot::Sender<Result<kaspad_response::Payload, NodeError>>;

struct Cmd {
    payload: kaspad_request::Payload,
    reply: Reply,
}

#[derive(Clone)]
pub struct NodeClient {
    cmd_tx: mpsc::Sender<Cmd>,
    timeout: Duration,
}

macro_rules! rpc_method {
    ($name:ident, $req_variant:ident, $resp_variant:ident, $req:ty, $resp:ty) => {
        pub async fn $name(&self, req: $req) -> Result<$resp, NodeError> {
            match self
                .call(kaspad_request::Payload::$req_variant(req))
                .await?
            {
                kaspad_response::Payload::$resp_variant(resp) => {
                    if let Some(e) = resp.error.as_ref().filter(|e| !e.message.is_empty()) {
                        Err(NodeError::Rpc(e.message.clone()))
                    } else {
                        Ok(resp)
                    }
                }
                _ => Err(NodeError::Protocol(
                    concat!("unexpected node response for ", stringify!($name)).to_string(),
                )),
            }
        }
    };
}

impl NodeClient {
    /// Spawn the connection actor and return a cheap-to-clone handle.
    /// `url` must have been validated with `Endpoint::from_shared` beforehand.
    pub fn spawn(url: String, timeout: Duration) -> Self {
        Self::spawn_inner(url, timeout, None)
    }

    /// Like [`Self::spawn`], but also returns a stream of [`Notification`]s for
    /// the indexer. Only one consumer is supported (the single indexer task).
    pub fn spawn_indexed(url: String, timeout: Duration) -> (Self, mpsc::Receiver<Notification>) {
        let (notif_tx, notif_rx) = mpsc::channel(NOTIF_QUEUE);
        (Self::spawn_inner(url, timeout, Some(notif_tx)), notif_rx)
    }

    fn spawn_inner(
        url: String,
        timeout: Duration,
        notif_tx: Option<mpsc::Sender<Notification>>,
    ) -> Self {
        let (cmd_tx, cmd_rx) = mpsc::channel(CMD_QUEUE);
        tokio::spawn(actor(url, cmd_rx, notif_tx));
        Self { cmd_tx, timeout }
    }

    async fn call(
        &self,
        payload: kaspad_request::Payload,
    ) -> Result<kaspad_response::Payload, NodeError> {
        let (reply, reply_rx) = oneshot::channel();
        self.cmd_tx
            .send(Cmd { payload, reply })
            .await
            .map_err(|_| NodeError::Unreachable("node client task is gone".into()))?;
        match tokio::time::timeout(self.timeout, reply_rx).await {
            Err(_) => Err(NodeError::Timeout),
            Ok(Err(_)) => Err(NodeError::Unreachable("connection to node lost".into())),
            Ok(Ok(result)) => result,
        }
    }

    rpc_method!(
        get_balance_by_address,
        GetBalanceByAddressRequest,
        GetBalanceByAddressResponse,
        proto::GetBalanceByAddressRequestMessage,
        proto::GetBalanceByAddressResponseMessage
    );
    rpc_method!(
        get_utxos_by_addresses,
        GetUtxosByAddressesRequest,
        GetUtxosByAddressesResponse,
        proto::GetUtxosByAddressesRequestMessage,
        proto::GetUtxosByAddressesResponseMessage
    );
    rpc_method!(
        submit_transaction,
        SubmitTransactionRequest,
        SubmitTransactionResponse,
        proto::SubmitTransactionRequestMessage,
        proto::SubmitTransactionResponseMessage
    );
    rpc_method!(
        get_block_dag_info,
        GetBlockDagInfoRequest,
        GetBlockDagInfoResponse,
        proto::GetBlockDagInfoRequestMessage,
        proto::GetBlockDagInfoResponseMessage
    );
    rpc_method!(
        get_coin_supply,
        GetCoinSupplyRequest,
        GetCoinSupplyResponse,
        proto::GetCoinSupplyRequestMessage,
        proto::GetCoinSupplyResponseMessage
    );
    rpc_method!(
        get_server_info,
        GetServerInfoRequest,
        GetServerInfoResponse,
        proto::GetServerInfoRequestMessage,
        proto::GetServerInfoResponseMessage
    );
    rpc_method!(
        estimate_network_hashes_per_second,
        EstimateNetworkHashesPerSecondRequest,
        EstimateNetworkHashesPerSecondResponse,
        proto::EstimateNetworkHashesPerSecondRequestMessage,
        proto::EstimateNetworkHashesPerSecondResponseMessage
    );
    rpc_method!(
        get_block,
        GetBlockRequest,
        GetBlockResponse,
        proto::GetBlockRequestMessage,
        proto::GetBlockResponseMessage
    );
    rpc_method!(
        get_virtual_chain_from_block,
        GetVirtualChainFromBlockRequest,
        GetVirtualChainFromBlockResponse,
        proto::GetVirtualChainFromBlockRequestMessage,
        proto::GetVirtualChainFromBlockResponseMessage
    );
    rpc_method!(
        get_blocks,
        GetBlocksRequest,
        GetBlocksResponse,
        proto::GetBlocksRequestMessage,
        proto::GetBlocksResponseMessage
    );

    // --- subscriptions (indexer only) ---
    // The node replies with an id-matched `Notify*Response`; the actual events
    // then arrive asynchronously as [`Notification`]s. Must be re-issued on
    // every `Connected`.

    rpc_method!(
        notify_block_added,
        NotifyBlockAddedRequest,
        NotifyBlockAddedResponse,
        proto::NotifyBlockAddedRequestMessage,
        proto::NotifyBlockAddedResponseMessage
    );
    rpc_method!(
        notify_virtual_chain_changed,
        NotifyVirtualChainChangedRequest,
        NotifyVirtualChainChangedResponse,
        proto::NotifyVirtualChainChangedRequestMessage,
        proto::NotifyVirtualChainChangedResponseMessage
    );
}

async fn actor(
    url: String,
    mut cmd_rx: mpsc::Receiver<Cmd>,
    notif_tx: Option<mpsc::Sender<Notification>>,
) {
    let generation = AtomicU64::new(0);
    let mut backoff = Duration::from_secs(1);
    loop {
        let endpoint = Endpoint::from_shared(url.clone())
            .expect("node gRPC url validated at startup")
            .connect_timeout(CONNECT_TIMEOUT)
            .tcp_nodelay(true);
        match endpoint.connect().await {
            Err(e) => {
                tracing::debug!("connect to keryxd failed: {e}");
                // Fail queued callers fast instead of letting them ride out their timeout.
                while let Ok(cmd) = cmd_rx.try_recv() {
                    let _ = cmd.reply.send(Err(NodeError::Unreachable(format!(
                        "cannot reach keryx node: {e}"
                    ))));
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(RECONNECT_MAX);
            }
            Ok(channel) => {
                backoff = Duration::from_secs(1);
                let gen = generation.fetch_add(1, Ordering::SeqCst) + 1;
                tracing::info!("connected to keryxd at {url} (generation {gen})");
                if serve(channel, &mut cmd_rx, notif_tx.as_ref(), gen)
                    .await
                    .is_none()
                {
                    return; // shim is shutting down
                }
                tracing::warn!("lost connection to keryxd; reconnecting");
            }
        }
    }
}

/// Forward a push notification to the indexer without ever blocking the serve
/// loop. Blocking here would deadlock: the indexer's own backfill RPC calls
/// need this loop to route their responses. A full channel means the indexer
/// is behind; we drop and let the next `Connected`-driven backfill reconcile.
fn forward(notif_tx: Option<&mpsc::Sender<Notification>>, notification: Notification) {
    if let Some(tx) = notif_tx {
        if let Err(e) = tx.try_send(notification) {
            tracing::warn!("indexer notification dropped ({e}); will reconcile on next backfill");
        }
    }
}

/// Drive one live connection. Returns `None` when the command channel closed
/// (shutdown), `Some(())` when the stream broke and a reconnect is needed.
async fn serve(
    channel: Channel,
    cmd_rx: &mut mpsc::Receiver<Cmd>,
    notif_tx: Option<&mpsc::Sender<Notification>>,
    generation: u64,
) -> Option<()> {
    let mut client = RpcClient::new(channel)
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(MAX_MESSAGE_SIZE);
    let (out_tx, out_rx) = mpsc::channel::<KaspadRequest>(CMD_QUEUE);
    let mut responses = match client.message_stream(ReceiverStream::new(out_rx)).await {
        Ok(resp) => resp.into_inner(),
        Err(e) => {
            tracing::warn!("failed to open node message stream: {e}");
            return Some(());
        }
    };

    // Tell the indexer a fresh stream is up so it (re)subscribes and backfills.
    forward(notif_tx, Notification::Connected { generation });

    let mut pending: HashMap<u64, Reply> = HashMap::new();
    let mut next_id: u64 = 1;
    loop {
        tokio::select! {
            cmd = cmd_rx.recv() => {
                let cmd = cmd?;
                let id = next_id;
                next_id += 1;
                let request = KaspadRequest { id, payload: Some(cmd.payload) };
                if out_tx.send(request).await.is_err() {
                    let _ = cmd.reply.send(Err(NodeError::Unreachable("node stream closed".into())));
                    fail_all(&mut pending);
                    return Some(());
                }
                pending.insert(id, cmd.reply);
            }
            resp = responses.next() => {
                match resp {
                    Some(Ok(KaspadResponse { id, payload })) => {
                        route(payload, id, &mut pending, notif_tx);
                    }
                    Some(Err(status)) => {
                        tracing::warn!("node stream error: {status}");
                        fail_all(&mut pending);
                        return Some(());
                    }
                    None => {
                        fail_all(&mut pending);
                        return Some(());
                    }
                }
            }
        }
    }
}

/// Route one inbound message: unsolicited notification variants go to the
/// indexer; everything else is an id-matched RPC reply.
fn route(
    payload: Option<kaspad_response::Payload>,
    id: u64,
    pending: &mut HashMap<u64, Reply>,
    notif_tx: Option<&mpsc::Sender<Notification>>,
) {
    use kaspad_response::Payload;
    match payload {
        Some(Payload::BlockAddedNotification(n)) => {
            if let Some(block) = n.block {
                forward(notif_tx, Notification::BlockAdded(Box::new(block)));
            }
        }
        Some(Payload::VirtualChainChangedNotification(n)) => {
            forward(notif_tx, Notification::VirtualChainChanged(Box::new(n)));
        }
        payload => {
            if let Some(reply) = pending.remove(&id) {
                let _ = reply.send(payload.ok_or_else(|| {
                    NodeError::Protocol("node sent an empty response payload".into())
                }));
            } else {
                // A response to a caller that already timed out, or a
                // notification variant we don't consume.
                tracing::debug!("dropping node message with unknown id {id}");
            }
        }
    }
}

fn fail_all(pending: &mut HashMap<u64, Reply>) {
    for (_, reply) in pending.drain() {
        let _ = reply.send(Err(NodeError::Unreachable(
            "connection to node lost".into(),
        )));
    }
}
