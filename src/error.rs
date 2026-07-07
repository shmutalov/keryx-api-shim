use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

use crate::node::NodeError;

/// Every error leaves the shim as the wallet's expected shape:
/// non-2xx status + JSON `{ "error": "<message>" }`.
#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    NotFound(String),
    Upstream(String),
    Node(NodeError),
}

impl From<NodeError> for ApiError {
    fn from(e: NodeError) -> Self {
        ApiError::Node(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ApiError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            ApiError::NotFound(m) => (StatusCode::NOT_FOUND, m),
            ApiError::Upstream(m) => (StatusCode::BAD_GATEWAY, m),
            // The node accepted the connection but rejected the request
            // (bad address, orphan/rejected tx, missing --utxoindex, ...).
            ApiError::Node(NodeError::Rpc(m)) => (StatusCode::BAD_REQUEST, m),
            ApiError::Node(NodeError::Timeout) => {
                (StatusCode::GATEWAY_TIMEOUT, "keryx node request timed out".into())
            }
            ApiError::Node(NodeError::Unreachable(m)) => (StatusCode::SERVICE_UNAVAILABLE, m),
            ApiError::Node(NodeError::Protocol(m)) => (StatusCode::BAD_GATEWAY, m),
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}
