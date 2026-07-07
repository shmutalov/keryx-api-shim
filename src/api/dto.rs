//! Wallet-facing wire shapes (snake_case, sompi integers) and their
//! conversions to the node's protowire types. The contract is the one the
//! Keryx Wallet Extension actually speaks — see its `src/lib/api.js` and
//! `docs/PROTOCOL.md`.

use serde::{Deserialize, Deserializer, Serialize};

use crate::node::proto;

pub const SOMPI_PER_KRX: f64 = 100_000_000.0;

// --- lenient u64 -------------------------------------------------------------
// u64::MAX exceeds the JS safe-integer range, so the wallet serializes some
// u64 fields (notably input `sequence`) as decimal strings. Accept both.
fn flex_u64<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    struct V;
    impl serde::de::Visitor<'_> for V {
        type Value = u64;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("an unsigned 64-bit integer, as a number or a decimal string")
        }
        fn visit_u64<E>(self, v: u64) -> Result<u64, E> {
            Ok(v)
        }
        fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<u64, E> {
            u64::try_from(v).map_err(|_| E::custom("value must not be negative"))
        }
        fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<u64, E> {
            s.trim()
                .parse()
                .map_err(|_| E::custom(format!("invalid u64 string {s:?}")))
        }
    }
    d.deserialize_any(V)
}

// --- responses ---------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct InfoResponse {
    pub network: String,
    pub last_daa_score: u64,
    pub block_reward_krx: f64,
    pub total_supply_krx: f64,
    pub max_supply_krx: f64,
    pub hashrate_hps: f64,
    pub total_blocks: u64,
    pub total_txs: u64,
    pub burned_krx: f64,
    pub total_escrow_krx: f64,
    pub total_real_inferences: u64,
    pub mined_pct: f64,
}

#[derive(Debug, Serialize)]
pub struct BalanceResponse {
    pub address: String,
    pub balance_sompi: u64,
}

#[derive(Debug, Serialize)]
pub struct UtxoDto {
    pub transaction_id: String,
    pub index: u32,
    pub amount_sompi: u64,
    pub script_version: u32,
    pub script_public_key: String,
    pub block_daa_score: u64,
    pub is_coinbase: bool,
}

#[derive(Debug, Serialize)]
pub struct UtxoCountResponse {
    pub count: usize,
}

#[derive(Debug, Serialize)]
pub struct HistoryTx {
    pub tx_id: String,
    pub amount_sompi: u64,
    pub is_spend: bool,
    pub daa_score: u64,
    pub block_hash: String,
    pub address: String,
}

#[derive(Debug, Serialize)]
pub struct AddressHistoryResponse {
    pub address: String,
    pub total_received_sompi: u64,
    pub total_tx_count: u64,
    pub transactions: Vec<HistoryTx>,
    /// Oldest DAA score the indexer's retention window still covers, so a
    /// client can tell "no transactions" from "none within the window" and
    /// point the user at an explorer for older history. `null` when the
    /// indexer is disabled (this endpoint is then an empty stub). Additive —
    /// the current wallet ignores unknown fields.
    pub history_since_daa: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct BroadcastResponse {
    pub transaction_id: String,
}

// --- broadcast request --------------------------------------------------------

#[derive(Debug, Deserialize)]
pub struct TxJson {
    #[serde(default)]
    pub version: u32,
    pub inputs: Vec<TxInputJson>,
    #[serde(default)]
    pub outputs: Vec<TxOutputJson>,
    #[serde(default, deserialize_with = "flex_u64")]
    pub lock_time: u64,
    #[serde(default = "native_subnetwork")]
    pub subnetwork_id: String,
    #[serde(default, deserialize_with = "flex_u64")]
    pub gas: u64,
    #[serde(default)]
    pub payload: String,
    #[serde(default, deserialize_with = "flex_u64")]
    pub mass: u64,
}

fn native_subnetwork() -> String {
    "0000000000000000000000000000000000000000".into()
}

fn one() -> u32 {
    1
}

#[derive(Debug, Deserialize)]
pub struct TxInputJson {
    pub transaction_id: String,
    pub index: u32,
    pub signature_script: String,
    #[serde(deserialize_with = "flex_u64")]
    pub sequence: u64,
    #[serde(default = "one")]
    pub sig_op_count: u32,
}

#[derive(Debug, Deserialize)]
pub struct TxOutputJson {
    #[serde(deserialize_with = "flex_u64")]
    pub amount: u64,
    #[serde(default)]
    pub script_version: u32,
    pub script_public_key: String,
}

fn is_hex(s: &str) -> bool {
    s.len().is_multiple_of(2) && s.bytes().all(|b| b.is_ascii_hexdigit())
}

impl TxJson {
    /// Cheap shape checks so obviously malformed submissions get a clear 400
    /// here instead of an opaque node error. Consensus validation stays with
    /// the node.
    pub fn validate(&self) -> Result<(), String> {
        if self.inputs.is_empty() {
            return Err("transaction has no inputs".into());
        }
        if self.subnetwork_id.len() != 40 || !is_hex(&self.subnetwork_id) {
            return Err("subnetwork_id must be 40 hex chars".into());
        }
        if !is_hex(&self.payload) {
            return Err("payload must be hex".into());
        }
        for (i, input) in self.inputs.iter().enumerate() {
            if input.transaction_id.len() != 64 || !is_hex(&input.transaction_id) {
                return Err(format!("input {i}: transaction_id must be 64 hex chars"));
            }
            if !is_hex(&input.signature_script) {
                return Err(format!("input {i}: signature_script must be hex"));
            }
        }
        for (i, output) in self.outputs.iter().enumerate() {
            if output.script_public_key.is_empty() || !is_hex(&output.script_public_key) {
                return Err(format!(
                    "output {i}: script_public_key must be non-empty hex"
                ));
            }
        }
        Ok(())
    }

    pub fn into_proto(self) -> proto::RpcTransaction {
        proto::RpcTransaction {
            version: self.version,
            inputs: self
                .inputs
                .into_iter()
                .map(|input| proto::RpcTransactionInput {
                    previous_outpoint: Some(proto::RpcOutpoint {
                        transaction_id: input.transaction_id,
                        index: input.index,
                    }),
                    signature_script: input.signature_script,
                    sequence: input.sequence,
                    sig_op_count: input.sig_op_count,
                    verbose_data: None,
                })
                .collect(),
            outputs: self
                .outputs
                .into_iter()
                .map(|output| proto::RpcTransactionOutput {
                    amount: output.amount,
                    script_public_key: Some(proto::RpcScriptPublicKey {
                        version: output.script_version,
                        script_public_key: output.script_public_key,
                    }),
                    verbose_data: None,
                })
                .collect(),
            lock_time: self.lock_time,
            subnetwork_id: self.subnetwork_id,
            gas: self.gas,
            payload: self.payload,
            verbose_data: None,
            mass: self.mass,
        }
    }
}

// --- node → wallet conversions -------------------------------------------------

pub fn utxo_dto(entry: proto::RpcUtxosByAddressesEntry) -> Option<UtxoDto> {
    let outpoint = entry.outpoint?;
    let utxo = entry.utxo_entry?;
    let spk = utxo.script_public_key.unwrap_or_default();
    Some(UtxoDto {
        transaction_id: outpoint.transaction_id,
        index: outpoint.index,
        amount_sompi: utxo.amount,
        script_version: spk.version,
        script_public_key: spk.script_public_key,
        block_daa_score: utxo.block_daa_score,
        is_coinbase: utxo.is_coinbase,
    })
}

// --- address sanity check -------------------------------------------------------

const ADDRESS_PREFIXES: [&str; 4] = ["keryx", "keryxtest", "keryxsim", "keryxdev"];
const BECH32_CHARSET: &str = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";

/// Prefix + charset gate to keep garbage off the node; the node still performs
/// the full checksum validation.
pub fn validate_address(address: &str) -> Result<(), String> {
    let (prefix, payload) = address
        .split_once(':')
        .ok_or_else(|| "invalid address: expected <prefix>:<payload>".to_string())?;
    if !ADDRESS_PREFIXES.contains(&prefix) {
        return Err(format!("invalid address prefix {prefix:?}"));
    }
    if payload.is_empty() || !payload.chars().all(|c| BECH32_CHARSET.contains(c)) {
        return Err("invalid address payload".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // The exact wire example from the wallet's docs/PROTOCOL.md, with
    // `sequence` as the string the extension really sends.
    const PROTOCOL_EXAMPLE: &str = r#"{
        "version": 0,
        "inputs": [{
            "transaction_id": "aa11223344556677889900aabbccddeeff00112233445566778899aabbccddee",
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
    }"#;

    #[test]
    fn parses_wallet_tx_with_string_sequence() {
        let tx: TxJson = serde_json::from_str(PROTOCOL_EXAMPLE).unwrap();
        assert_eq!(tx.inputs[0].sequence, u64::MAX);
        assert_eq!(tx.outputs[0].amount, 12345);
        tx.validate().unwrap();

        let rpc = tx.into_proto();
        assert_eq!(rpc.version, 0);
        assert_eq!(rpc.inputs.len(), 1);
        let input = &rpc.inputs[0];
        assert_eq!(input.sequence, u64::MAX);
        assert_eq!(input.sig_op_count, 1);
        assert_eq!(
            input.previous_outpoint.as_ref().unwrap().transaction_id,
            "aa11223344556677889900aabbccddeeff00112233445566778899aabbccddee"
        );
        let output = &rpc.outputs[0];
        assert_eq!(output.amount, 12345);
        let spk = output.script_public_key.as_ref().unwrap();
        assert_eq!(spk.version, 0);
        assert_eq!(spk.script_public_key, "20aa11ac");
        assert_eq!(
            rpc.subnetwork_id,
            "0000000000000000000000000000000000000000"
        );
        assert_eq!(rpc.mass, 0);
    }

    #[test]
    fn parses_numeric_sequence_too() {
        let json = PROTOCOL_EXAMPLE.replace("\"18446744073709551615\"", "42");
        let tx: TxJson = serde_json::from_str(&json).unwrap();
        assert_eq!(tx.inputs[0].sequence, 42);
    }

    #[test]
    fn rejects_negative_amount() {
        let json = PROTOCOL_EXAMPLE.replace("12345", "-5");
        assert!(serde_json::from_str::<TxJson>(&json).is_err());
    }

    #[test]
    fn validate_catches_bad_hex() {
        let mut tx: TxJson = serde_json::from_str(PROTOCOL_EXAMPLE).unwrap();
        tx.inputs[0].transaction_id = "zz".repeat(32);
        assert!(tx.validate().is_err());
        let mut tx: TxJson = serde_json::from_str(PROTOCOL_EXAMPLE).unwrap();
        tx.subnetwork_id = "00".into();
        assert!(tx.validate().is_err());
    }

    #[test]
    fn address_validation() {
        assert!(validate_address(&format!("keryx:{}", "q".repeat(61))).is_ok());
        assert!(validate_address(&format!("keryxtest:{}", "p2z9".repeat(15))).is_ok());
        assert!(validate_address("keryx").is_err()); // no separator
        assert!(validate_address("kaspa:qqqq").is_err()); // wrong prefix
        assert!(validate_address("keryx:QQQQ").is_err()); // charset is lowercase-only
        assert!(validate_address("keryx:").is_err()); // empty payload
        assert!(validate_address("keryx:q1bio").is_err()); // b, i, o not in charset
    }
}
