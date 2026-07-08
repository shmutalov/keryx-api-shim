//! Decoders for keryx-node's on-chain AI subnetwork transactions (phase 2c).
//!
//! Binary layouts are ported from `keryx-node/inference/src/ai_payload.rs`.
//! Three subnetworks carry the inference protocol:
//!   03 = AiRequest, 04 = AiResponse, 05 = AiChallenge.
//! Requests and responses are joined by `request_hash =
//! BLAKE2b-512(payload)[0..32]` (note: 512 truncated, over the raw payload —
//! not BLAKE2b-256, not the tx id). The inference result text is off-chain
//! (only the IPFS CID is on-chain); clients fetch it via the shim's /ipfs proxy.

pub const SUBNET_AI_REQUEST: &str = "0300000000000000000000000000000000000000";
pub const SUBNET_AI_RESPONSE: &str = "0400000000000000000000000000000000000000";
pub const SUBNET_AI_CHALLENGE: &str = "0500000000000000000000000000000000000000";

const MIN_REQUEST_LEN: usize = 52;
const RESPONSE_LEN: usize = 78;
const MIN_CHALLENGE_LEN: usize = 74;

// --- model registry (model_id hex → wallet key) -------------------------------
// keryx-node keeps no on-chain name registry; these mirror the wallet's
// hardcoded list (src/lib/models.js), which is what the UI displays.

const MODELS: &[(&str, &str)] = &[
    (
        "4f21ddeb7d62bd2265bc54230d536ca3f1749927780f528c3c41fa2911df4d72",
        "qwen3-1.7b",
    ),
    (
        "ad50ad0bd461d8ab44efc0214989eb33291685ef4ade22a0f4f217d03266d837",
        "gemma-3-4b",
    ),
    (
        "9421066a6400c98ba137114f7f4b7d4a2ddf13ab163a5de38c0184793af6313a",
        "dolphin-llama3-8b",
    ),
    (
        "65c6eb6fe18b9efd8060ab9d2d03bb9b01050a3b1378cbac000c5cc0acdc0d2a",
        "qwen3-32b-abliterated",
    ),
    (
        "6df46a78cbe4dc579f04dbd801f1a520b9eae28ce7b50c8da7874bfa3fb5108d",
        "llama-3.3-70b-q2",
    ),
];

/// Wallet model key for a model id, or `None` for an unrecognised model.
pub fn model_key(model_id_hex: &str) -> Option<&'static str> {
    MODELS
        .iter()
        .find(|(id, _)| id.eq_ignore_ascii_case(model_id_hex))
        .map(|(_, key)| *key)
}

// --- hex helpers --------------------------------------------------------------

pub fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0xf) as u32, 16).unwrap());
    }
    s
}

pub fn from_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

// --- request_hash (join key) --------------------------------------------------

/// `BLAKE2b-512(payload)[0..32]`, hex — the value AiResponse/AiChallenge carry
/// to reference a request, and the source of the wallet's `payload_prefix`
/// (`= request_hash_hex[..16]`).
pub fn request_hash(payload: &[u8]) -> String {
    let digest = blake2b_simd::blake2b(payload);
    to_hex(&digest.as_bytes()[..32])
}

// --- AiRequest (03) -----------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AiRequest {
    pub model_id: String,
    pub max_tokens: u32,
    pub inference_reward: u64,
    pub priority_fee: u64,
    pub prompt: String,
}

pub fn decode_request(payload: &[u8]) -> Option<AiRequest> {
    if payload.len() < MIN_REQUEST_LEN {
        return None;
    }
    Some(AiRequest {
        model_id: to_hex(&payload[0..32]),
        max_tokens: u32::from_le_bytes(payload[32..36].try_into().unwrap()),
        inference_reward: u64::from_le_bytes(payload[36..44].try_into().unwrap()),
        priority_fee: u64::from_le_bytes(payload[44..52].try_into().unwrap()),
        prompt: String::from_utf8_lossy(&payload[52..]).into_owned(),
    })
}

// --- AiResponse (04) ----------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AiResponse {
    pub request_hash: String,
    pub challenge_window_end: u64,
    /// IPFS CIDv0 ("Qm…") of the off-chain result.
    pub cid: String,
    pub response_length: u32,
}

pub fn decode_response(payload: &[u8]) -> Option<AiResponse> {
    if payload.len() < RESPONSE_LEN {
        return None;
    }
    // bytes [40..74] are the raw multihash (0x12 0x20 || 32-byte sha2-256);
    // base58btc of that is the CIDv0.
    let cid = bs58::encode(&payload[40..74]).into_string();
    Some(AiResponse {
        request_hash: to_hex(&payload[0..32]),
        challenge_window_end: u64::from_le_bytes(payload[32..40].try_into().unwrap()),
        cid,
        response_length: u32::from_le_bytes(payload[74..78].try_into().unwrap()),
    })
}

// --- AiChallenge (05) ---------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AiChallenge {
    pub response_hash: String,
    /// Phase-3C challenges carry the 32-byte request_hash as `proof_data`.
    pub request_hash: Option<String>,
}

pub fn decode_challenge(payload: &[u8]) -> Option<AiChallenge> {
    if payload.len() < MIN_CHALLENGE_LEN {
        return None;
    }
    let request_hash = if payload.len() >= MIN_CHALLENGE_LEN + 32 {
        Some(to_hex(&payload[74..106]))
    } else {
        None
    };
    Some(AiChallenge {
        response_hash: to_hex(&payload[0..32]),
        request_hash,
    })
}

// --- coinbase capability markers ----------------------------------------------

/// Model ids a miner declared in its coinbase `extra_data` via the
/// `/ai:cap:<hex>,<hex>,…/` marker.
pub fn parse_ai_caps(coinbase_payload: &[u8]) -> Vec<String> {
    let Some(start) = find(coinbase_payload, b"/ai:cap:") else {
        return vec![];
    };
    let rest = &coinbase_payload[start + 8..];
    // Read ASCII up to the next '/' (or end).
    let end = rest.iter().position(|&b| b == b'/').unwrap_or(rest.len());
    let list = match std::str::from_utf8(&rest[..end]) {
        Ok(s) => s,
        Err(_) => return vec![],
    };
    list.split(',')
        .map(|s| s.trim().to_ascii_lowercase())
        .filter(|s| s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()))
        .collect()
}

/// The miner's escrow/payout x-only pubkey from the coinbase `/escrow:<hex64>/`
/// marker, if present.
pub fn parse_escrow_pubkey(coinbase_payload: &[u8]) -> Option<String> {
    let start = find(coinbase_payload, b"/escrow:")?;
    let rest = &coinbase_payload[start + 8..];
    let end = rest.len().min(64);
    let hex = std::str::from_utf8(&rest[..end]).ok()?.to_ascii_lowercase();
    (hex.len() == 64 && hex.bytes().all(|b| b.is_ascii_hexdigit())).then_some(hex)
}

/// x-only pubkey from a version-0 P2PK script (`OP_DATA_32 <32> OP_CHECKSIG`).
pub fn p2pk_pubkey(script_hex: &str) -> Option<String> {
    let bytes = from_hex(script_hex)?;
    match bytes.as_slice() {
        [0x20, key @ .., 0xac] if key.len() == 32 => Some(to_hex(key)),
        _ => None,
    }
}

/// x-only miner pubkey from an OPoI CSV-escrow script
/// (`<seq_len><seq> OP_CSV(0xb1) OP_DATA_32(0x20) <32> OP_CHECKSIG(0xac)`),
/// i.e. `AiRequest.outputs[1]`.
pub fn csv_escrow_pubkey(script_hex: &str) -> Option<String> {
    let b = from_hex(script_hex)?;
    if b.len() < 37 {
        return None;
    }
    let seq_len = b[0] as usize;
    if !(1..=8).contains(&seq_len) || b.len() != seq_len + 36 {
        return None;
    }
    if b[seq_len + 1] != 0xb1 || b[seq_len + 2] != 0x20 || b[b.len() - 1] != 0xac {
        return None;
    }
    Some(to_hex(&b[seq_len + 3..seq_len + 35]))
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn le_u32(v: u32) -> [u8; 4] {
        v.to_le_bytes()
    }
    fn le_u64(v: u64) -> [u8; 8] {
        v.to_le_bytes()
    }

    #[test]
    fn decodes_request_matching_wallet_layout() {
        let model = [0xabu8; 32];
        let mut p = Vec::new();
        p.extend_from_slice(&model);
        p.extend_from_slice(&le_u32(128));
        p.extend_from_slice(&le_u64(60_000_000));
        p.extend_from_slice(&le_u64(30_000_000));
        p.extend_from_slice(b"hello world");
        let r = decode_request(&p).unwrap();
        assert_eq!(r.model_id, "ab".repeat(32));
        assert_eq!(r.max_tokens, 128);
        assert_eq!(r.inference_reward, 60_000_000);
        assert_eq!(r.priority_fee, 30_000_000);
        assert_eq!(r.prompt, "hello world");
        // too short → None
        assert!(decode_request(&p[..40]).is_none());
    }

    #[test]
    fn request_hash_is_blake2b512_truncated() {
        // Independently: blake2b-512 of empty input, first 32 bytes.
        let h = request_hash(b"");
        let expect = to_hex(&blake2b_simd::blake2b(b"").as_bytes()[..32]);
        assert_eq!(h, expect);
        assert_eq!(h.len(), 64);
        // payload_prefix is the first 16 hex chars.
        assert_eq!(&h[..16], &expect[..16]);
    }

    #[test]
    fn decodes_response_cid() {
        let mut p = vec![0u8; RESPONSE_LEN];
        p[0..32].copy_from_slice(&[0x11u8; 32]); // request_hash
        p[32..40].copy_from_slice(&le_u64(9_000));
        // multihash: sha2-256 (0x12), length 32 (0x20), then digest.
        p[40] = 0x12;
        p[41] = 0x20;
        for (i, byte) in p.iter_mut().enumerate().take(74).skip(42) {
            *byte = (i as u8).wrapping_mul(3);
        }
        p[74..78].copy_from_slice(&le_u32(256));
        let r = decode_response(&p).unwrap();
        assert_eq!(r.request_hash, "11".repeat(32));
        assert_eq!(r.challenge_window_end, 9_000);
        assert_eq!(r.response_length, 256);
        assert!(r.cid.starts_with("Qm"), "CIDv0 starts with Qm: {}", r.cid);
        assert_eq!(r.cid.len(), 46);
    }

    #[test]
    fn decodes_challenge_with_request_hash() {
        let mut p = vec![0u8; MIN_CHALLENGE_LEN + 32];
        p[0..32].copy_from_slice(&[0x22u8; 32]); // response_hash
        p[74..106].copy_from_slice(&[0x33u8; 32]); // proof_data = request_hash
        let c = decode_challenge(&p).unwrap();
        assert_eq!(c.response_hash, "22".repeat(32));
        assert_eq!(c.request_hash.as_deref(), Some("33".repeat(32).as_str()));
        // no proof_data → request_hash None
        let c2 = decode_challenge(&p[..MIN_CHALLENGE_LEN]).unwrap();
        assert_eq!(c2.request_hash, None);
    }

    #[test]
    fn parses_coinbase_caps_and_escrow() {
        let m1 = "ad50ad0bd461d8ab44efc0214989eb33291685ef4ade22a0f4f217d03266d837";
        let m2 = "9421066a6400c98ba137114f7f4b7d4a2ddf13ab163a5de38c0184793af6313a";
        let pk = "cc".repeat(32);
        let payload = format!("1.3.1/ai:cap:{m1},{m2}/escrow:{pk}/blah");
        let caps = parse_ai_caps(payload.as_bytes());
        assert_eq!(caps, vec![m1.to_string(), m2.to_string()]);
        assert_eq!(
            parse_escrow_pubkey(payload.as_bytes()).as_deref(),
            Some(pk.as_str())
        );
        assert_eq!(parse_ai_caps(b"no markers here"), Vec::<String>::new());
    }

    #[test]
    fn extracts_pubkeys_from_scripts() {
        let pk = "dd".repeat(32);
        assert_eq!(
            p2pk_pubkey(&format!("20{pk}ac")).as_deref(),
            Some(pk.as_str())
        );
        // CSV escrow: seq_len=2, seq=A08C (36000 LE), CSV, DATA32, pk, CHECKSIG
        let csv = format!("02a08cb120{pk}ac");
        assert_eq!(csv_escrow_pubkey(&csv).as_deref(), Some(pk.as_str()));
        assert_eq!(p2pk_pubkey(&csv), None);
    }

    #[test]
    fn model_registry_resolves_wallet_keys() {
        assert_eq!(
            model_key("ad50ad0bd461d8ab44efc0214989eb33291685ef4ade22a0f4f217d03266d837"),
            Some("gemma-3-4b")
        );
        assert_eq!(model_key("00".repeat(32).as_str()), None);
    }
}
