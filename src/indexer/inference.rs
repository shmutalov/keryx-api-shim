//! Decoders for keryx-node's on-chain AI subnetwork transactions (phase 2c).
//!
//! Binary layouts are ported from `keryx-node/inference/src/ai_payload.rs`.
//! Three subnetworks carry the inference protocol:
//!   03 = AiRequest, 04 = AiResponse, 05 = AiChallenge.
//! An AiResponse names its request by `request_hash`, the request's identity
//! (see `request_identity`): `BLAKE2b-512(payload)[0..32]` before the node's H8
//! gate (512 truncated, over the raw payload — not BLAKE2b-256), the request's
//! transaction id from H8 on.
//!
//! Since H8 (`reward_routing_activation`) an AiRequest no longer names its
//! miner: `outputs[1]` is the keyless reward vault (see
//! `INFERENCE_VAULT_SCRIPT_HEX`) and the reward is minted by a later coinbase to
//! the first accepted responder. Pre-H8 rows carry a CSV escrow naming the miner.
//!
//! Since H14 (`private_inference_activation`) every AiRequest prompt is a sealed
//! envelope only the tier's miners can open, and each answer travels inline in
//! its AiResponse, sealed to the requester (`inference/src/private.rs`). Before
//! H14 the prompt is plaintext and the answer is off-chain on IPFS (only its CID
//! is on-chain); clients fetch it via the shim's /ipfs proxy.

pub const SUBNET_AI_REQUEST: &str = "0300000000000000000000000000000000000000";
pub const SUBNET_AI_RESPONSE: &str = "0400000000000000000000000000000000000000";
pub const SUBNET_AI_CHALLENGE: &str = "0500000000000000000000000000000000000000";

const MIN_REQUEST_LEN: usize = 52;
const RESPONSE_LEN: usize = 78;
/// V2 (H6, signed): the 78 v1 bytes + `[escrow_pubkey: 32] [schnorr_sig: 64]`.
const RESPONSE_V2_LEN: usize = RESPONSE_LEN + 32 + 64;
const MIN_CHALLENGE_LEN: usize = 74;

// --- private inference (H14), mirrors keryx-node inference/src/private.rs -----

/// Leading bytes of a sealed prompt or answer: a NUL, so no UTF-8 prompt starts
/// with it.
const PRIVATE_MAGIC: [u8; 4] = [0x00, b'K', b'X', b'P'];
const PRIVATE_VERSION: u8 = 1;
const MAX_PRIVATE_RECIPIENTS: usize = 128;
/// `[magic: 4] [version: 1] [ephemeral_pubkey: 33] [nonce: 12] [n_recipients: 1]`.
const PRIVATE_REQUEST_HEADER_LEN: usize = 4 + 1 + 33 + 12 + 1;
/// `[escrow_pubkey: 32] [wrapped_root_key: 32 + 16-byte tag]`.
const PRIVATE_RECIPIENT_ENTRY_LEN: usize = 32 + 48;
const PRIVATE_TAG_LEN: usize = 16;
/// AiResponse extension kind carrying the inline (sealed) answer:
/// `[0x80] [len: 4 LE] [body: len]` after the 174-byte V2 head.
const RESPONSE_EXT_PRIVATE_BODY: u8 = 0x80;
/// H15 extension kind after the body: the inference trace commitment
/// `[trace_root: 32] [n_prompt: 4] [n_pos: 4] [stage: 1]`, optionally followed
/// by `[tokens_root: 32]` (keryx-node `TraceCommitment`).
const RESPONSE_EXT_TRACE: u8 = 0x81;
const RESPONSE_TRACE_LEN: usize = 32 + 4 + 4 + 1;
const RESPONSE_TRACE_TOKENS_LEN: usize = RESPONSE_TRACE_LEN + 32;
const RESPONSE_EXT_HEADER_LEN: usize = 1 + 4;
/// 32 KiB until the node's `trace_chain_activation` (H15), 48 KiB after; the
/// node enforces the era, the shim accepts either.
const MAX_RESPONSE_PRIVATE_BODY_LEN: usize = 48 * 1024;

/// Whether a prompt (or an inline answer body) is a private-inference envelope —
/// the node's `PrivateRequestEnvelope::is_private`, the check its miners use.
pub fn is_private(bytes: &[u8]) -> bool {
    bytes.starts_with(&PRIVATE_MAGIC)
}

/// How many responders a sealed prompt names, from the envelope's own layout:
/// supported version, 1..=128 recipients in strictly ascending key order, room
/// for the ciphertext tag. `None` for a plaintext or malformed prompt. Point
/// validity is not checked (no secp256k1 here) — the node already did that for
/// every accepted request.
pub fn private_recipient_count(prompt: &[u8]) -> Option<u32> {
    if !is_private(prompt)
        || prompt.len() < PRIVATE_REQUEST_HEADER_LEN
        || prompt[4] != PRIVATE_VERSION
    {
        return None;
    }
    let n = prompt[PRIVATE_REQUEST_HEADER_LEN - 1] as usize;
    if n == 0 || n > MAX_PRIVATE_RECIPIENTS {
        return None;
    }
    let entries_end = PRIVATE_REQUEST_HEADER_LEN + n * PRIVATE_RECIPIENT_ENTRY_LEN;
    if prompt.len() < entries_end + PRIVATE_TAG_LEN {
        return None;
    }
    let keys = prompt[PRIVATE_REQUEST_HEADER_LEN..entries_end]
        .as_chunks::<PRIVATE_RECIPIENT_ENTRY_LEN>()
        .0
        .iter()
        .map(|entry| &entry[..32]);
    let ascending = keys.clone().zip(keys.skip(1)).all(|(a, b)| a < b);
    ascending.then_some(n as u32)
}

/// Canonical keyless reward vault of a routed AiRequest (`OP_RETURN "aivault"`),
/// hex. At/after the node's H8 gate (`reward_routing_activation`, mainnet DAA
/// 79 210 000) consensus requires `AiRequest.outputs[1]` to carry EXACTLY this
/// version-0 script: the reward names nobody up front and is minted by a later
/// coinbase to the first accepted responder, or burns. Before H8 the same output
/// was a CSV-escrow script naming the target miner (see `csv_escrow_pubkey`).
/// Mirrors `keryx_inference::INFERENCE_VAULT_SCRIPT`.
pub const INFERENCE_VAULT_SCRIPT_HEX: &str = "6a0761697661756c74";

/// Whether a script is the H8 inference reward vault. Exact match only: a
/// look-alike `OP_RETURN` is not a vault (the node rejects it as non-standard).
pub fn is_inference_vault(script_hex: &str) -> bool {
    script_hex.eq_ignore_ascii_case(INFERENCE_VAULT_SCRIPT_HEX)
}

// --- model registry (model_id hex → wallet key) -------------------------------
// The H14 lineup. keryx-node keeps no on-chain *name* registry, only the model_id
// consensus constants (params.rs); these ids mirror those and the miner's
// models.rs, and the keys mirror the miner's model names and the wallet's
// src/lib/models.js. Keep them in lockstep. An id not listed here resolves to
// its raw hex (model_name()).
//
// H6 (`POM_TIERS_H6`, gated by `pom_v3_activation`) keeps 5 tiers: tier 0 =
// Qwen3.5-9B-abliterated (replaces BOTH Qwen3-8B and Mistral-7B), tier 1 =
// GLM-4-9B (slides from position 2), tier 2 = gemma-4-12B-abliterated (NEW,
// 16 GB cards), tiers 3-4 unchanged. H14 (`POM_TIERS_H14`, gated by
// `private_inference_activation`) swaps tier 3 only: Qwen3.8-27B replaces
// Qwen3.6-27B. The registry is name resolution only, not a consensus table, so
// retired ids stay listed — the indexer window can still straddle a fork gate
// and carry pre-fork AiRequests that must render by name.

const MODELS: &[(&str, &str)] = &[
    (
        "bd34568cd89f5f19c6c3a6e1a61b929bc868709409eaad8e672d85f3c1eb5710",
        "qwen3.5-9b-abliterated",
    ),
    (
        // retired at H6 (H5 tier 0) — kept for historical AiRequest decode
        "d42fa6ee00e07d49b046090a56af0e7bd61025937c502e2c574a72874c350d24",
        "qwen3-8b-abliterated",
    ),
    (
        // retired at H5 (pre-fork tier 0) — kept for historical AiRequest decode
        "300a99b3a85b0ab45d1d930bb7b1d4b0f35983d521e79ff21193a6908dc4b810",
        "exaone-4.0-1.2b",
    ),
    (
        // retired at H6 (H4/H5 tier 1) — kept for historical AiRequest decode
        "8c2fea600f0eefe7048741a5119cb7be303037f59fc026e48382658f23581e0a",
        "mistral-7b-v0.3",
    ),
    (
        "fa2f13be0850e26c5ce86c7ac79da85e300c1da8b3290f9a18d47105f1f2140a",
        "glm-4-9b-0414",
    ),
    (
        "399984045600f7d58d1b2cf01e6a4bf466fa15c7ac31bd0dd1a71e003b617cc6",
        "gemma-4-12b-abliterated",
    ),
    (
        "73740b443bdc00afda5fa34eb9999d3fea77dcc3f6de238fab701394cdc96fb3",
        "qwen3.8-27b",
    ),
    (
        // retired at H14 (H4-H6 tier 3) — kept for historical AiRequest decode
        "b8bdc01fa407eab943e4fefc807483b39f8142785256049e1f559698a5284746",
        "qwen3.6-27b",
    ),
    (
        "3dc09358ad75c6ef0c9c86ee4f47c4d6acda961fecbd0e4f9cf55e8f0fdffddb",
        "kimi-linear-48b",
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

/// `BLAKE2b-512(payload)[0..32]`, hex — a pre-H8 request's identity (and the
/// key the shim files any AI payload under, e.g. a response's own hash).
pub fn request_hash(payload: &[u8]) -> String {
    let digest = blake2b_simd::blake2b(payload);
    to_hex(&digest.as_bytes()[..32])
}

/// The identity an AiRequest is referenced by — what AiResponse/AiChallenge
/// carry as `request_hash`, and the source of the wallet's `payload_prefix`
/// (`= identity[..16]`). The node's service ledger switches it at H8
/// (`reward_routing_activation`, judged at the accepting chain block): the
/// payload digest before, the transaction id from H8 on, because identical
/// payloads used to collide into one assignment. The same gate at the same DAA
/// makes the keyless vault mandatory in `outputs[1]`, so `routed` (the request
/// funds the vault) decides it without knowing the network's activation score.
pub fn request_identity(tx_id: &str, payload: &[u8], routed: bool) -> String {
    if routed {
        tx_id.to_string()
    } else {
        request_hash(payload)
    }
}

// --- AiRequest (03) -----------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AiRequest {
    pub model_id: String,
    pub max_tokens: u32,
    pub inference_reward: u64,
    pub priority_fee: u64,
    /// Plaintext prompt; empty for a private request (the ciphertext means
    /// nothing to anyone but the tier's miners, so it is never rendered).
    pub prompt: String,
    /// The prompt is a sealed private-inference envelope (H14).
    pub private: bool,
    /// Responders a private prompt is sealed to (`None` if not parseable).
    pub recipient_count: Option<u32>,
}

pub fn decode_request(payload: &[u8]) -> Option<AiRequest> {
    if payload.len() < MIN_REQUEST_LEN {
        return None;
    }
    let prompt = &payload[52..];
    let private = is_private(prompt);
    Some(AiRequest {
        model_id: to_hex(&payload[0..32]),
        max_tokens: u32::from_le_bytes(payload[32..36].try_into().unwrap()),
        inference_reward: u64::from_le_bytes(payload[36..44].try_into().unwrap()),
        priority_fee: u64::from_le_bytes(payload[44..52].try_into().unwrap()),
        prompt: if private {
            String::new()
        } else {
            String::from_utf8_lossy(prompt).into_owned()
        },
        private,
        recipient_count: if private {
            private_recipient_count(prompt)
        } else {
            None
        },
    })
}

// --- AiResponse (04) ----------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AiResponse {
    pub request_hash: String,
    pub challenge_window_end: u64,
    /// IPFS CIDv0 ("Qm…") of the off-chain result. With an inline body (H14)
    /// it is the body's sha2-256 multihash and need not be pinned anywhere.
    pub cid: String,
    pub response_length: u32,
    /// V2 (H6+) responder escrow x-only pubkey, hex — what the answer is sealed
    /// under (`k_response` is derived per responder).
    pub responder_pubkey: Option<String>,
    /// Inline answer body (H14 extension): a private-inference envelope sealed
    /// to the requester, or plaintext when the responder could not open the
    /// request (the miner's documented "could not open" reply).
    pub private_body: Option<Vec<u8>>,
}

pub fn decode_response(payload: &[u8]) -> Option<AiResponse> {
    // V2 responses (H6) append [escrow_pubkey: 32][schnorr_sig: 64] after the
    // 78 v1 bytes; from H14 a V2 head may be followed by the extension
    // [0x80][len: 4 LE][body] carrying the answer inline, and from H15 the body
    // by [0x81][len: 4 LE][trace commitment]. The v1 fields sit at the same
    // offsets in every era, so `>=` decodes them all; extensions that do not
    // account for every trailing byte yield no body rather than failing the
    // whole response (the node has already judged validity).
    if payload.len() < RESPONSE_LEN {
        return None;
    }
    let responder_pubkey = (payload.len() >= RESPONSE_V2_LEN).then(|| to_hex(&payload[78..110]));
    let private_body = payload
        .get(RESPONSE_V2_LEN..)
        .filter(|ext| ext.len() > RESPONSE_EXT_HEADER_LEN && ext[0] == RESPONSE_EXT_PRIVATE_BODY)
        .and_then(|ext| {
            let len = u32::from_le_bytes(ext[1..5].try_into().unwrap()) as usize;
            let body =
                ext.get(RESPONSE_EXT_HEADER_LEN..RESPONSE_EXT_HEADER_LEN.checked_add(len)?)?;
            let rest = &ext[RESPONSE_EXT_HEADER_LEN + len..];
            (len <= MAX_RESPONSE_PRIVATE_BODY_LEN && (rest.is_empty() || is_trace_extension(rest)))
                .then(|| body.to_vec())
        });
    // bytes [40..74] are the raw multihash (0x12 0x20 || 32-byte sha2-256);
    // base58btc of that is the CIDv0.
    let cid = bs58::encode(&payload[40..74]).into_string();
    Some(AiResponse {
        request_hash: to_hex(&payload[0..32]),
        challenge_window_end: u64::from_le_bytes(payload[32..40].try_into().unwrap()),
        cid,
        response_length: u32::from_le_bytes(payload[74..78].try_into().unwrap()),
        responder_pubkey,
        private_body,
    })
}

/// Whether `ext` is exactly one H15 trace-commitment extension. The commitment
/// itself (consumed by the node's audit) is not surfaced.
fn is_trace_extension(ext: &[u8]) -> bool {
    let n = ext.len().saturating_sub(RESPONSE_EXT_HEADER_LEN);
    ext.len() > RESPONSE_EXT_HEADER_LEN
        && ext[0] == RESPONSE_EXT_TRACE
        && u32::from_le_bytes(ext[1..5].try_into().unwrap()) as usize == n
        && (n == RESPONSE_TRACE_LEN || n == RESPONSE_TRACE_TOKENS_LEN)
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
/// i.e. a pre-H8 `AiRequest.outputs[1]`. A routed request's vault yields `None`.
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
        assert!(!r.private);
        assert_eq!(r.recipient_count, None);
        // too short → None
        assert!(decode_request(&p[..40]).is_none());
    }

    /// A sealed prompt as keryx-node `PrivateRequestEnvelope::serialize` lays
    /// it out, with `keys` as the recipient escrow pubkeys (in the given order).
    fn sealed_prompt(keys: &[[u8; 32]], ciphertext_len: usize) -> Vec<u8> {
        let mut p = PRIVATE_MAGIC.to_vec();
        p.push(PRIVATE_VERSION);
        p.push(0x02); // ephemeral pubkey (compressed, even Y)
        p.extend_from_slice(&[0x5a; 32]);
        p.extend_from_slice(&[0x6b; 12]); // nonce
        p.push(keys.len() as u8);
        for k in keys {
            p.extend_from_slice(k);
            p.extend_from_slice(&[0x7c; 48]); // wrapped root key
        }
        p.extend(std::iter::repeat_n(0xee, ciphertext_len));
        p
    }

    #[test]
    fn private_request_hides_the_ciphertext_and_counts_recipients() {
        let keys = [[0x11u8; 32], [0x22u8; 32], [0x33u8; 32]];
        let prompt = sealed_prompt(&keys, PRIVATE_TAG_LEN + 40);
        assert_eq!(private_recipient_count(&prompt), Some(3));

        let mut p = vec![0xabu8; 32];
        p.extend_from_slice(&le_u32(512));
        p.extend_from_slice(&le_u64(50_000_000));
        p.extend_from_slice(&le_u64(30_000_000));
        p.extend_from_slice(&prompt);
        let r = decode_request(&p).unwrap();
        assert!(r.private);
        assert_eq!(r.recipient_count, Some(3));
        assert_eq!(r.prompt, "", "ciphertext is never rendered as text");
        assert_eq!(r.max_tokens, 512);

        // Layout checks: the bare tag fits, one byte short of it does not;
        // unordered or duplicate keys, an unknown version and zero recipients
        // are not a parseable envelope — but still private (never rendered).
        assert_eq!(
            private_recipient_count(&sealed_prompt(&keys, PRIVATE_TAG_LEN)),
            Some(3)
        );
        assert_eq!(
            private_recipient_count(&sealed_prompt(&keys, PRIVATE_TAG_LEN - 1)),
            None
        );
        assert_eq!(
            private_recipient_count(&sealed_prompt(&[keys[1], keys[0]], 16)),
            None
        );
        assert_eq!(
            private_recipient_count(&sealed_prompt(&[keys[0], keys[0]], 16)),
            None
        );
        assert_eq!(private_recipient_count(&sealed_prompt(&[], 16)), None);
        let mut v2 = prompt.clone();
        v2[4] = 2;
        assert_eq!(private_recipient_count(&v2), None);
        assert!(is_private(&v2));
        // A plaintext prompt is never private.
        assert!(!is_private(b"KXP plain"));
        assert_eq!(private_recipient_count(b"hello"), None);
    }

    #[test]
    fn request_identity_is_the_txid_once_routed() {
        let payload = b"same payload";
        let txid = "ab".repeat(32);
        // Pre-H8 (CSV escrow): the payload digest, regardless of tx id.
        assert_eq!(
            request_identity(&txid, payload, false),
            request_hash(payload)
        );
        // H8+ (vault): the tx id — what the miner puts in AiResponse.request_hash.
        assert_eq!(request_identity(&txid, payload, true), txid);
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
        assert_eq!(r.responder_pubkey, None);
        assert_eq!(r.private_body, None);

        // H6 V2 payload (78 + 32 pubkey + 64 sig = 174 bytes): the v1 fields
        // decode identically from the same offsets, plus the responder key.
        let mut v2 = p.clone();
        v2.extend_from_slice(&[0x44u8; 32]);
        v2.extend_from_slice(&[0x55u8; 64]);
        assert_eq!(v2.len(), RESPONSE_V2_LEN);
        let r2 = decode_response(&v2).unwrap();
        assert_eq!(
            r2.responder_pubkey.as_deref(),
            Some("44".repeat(32).as_str())
        );
        assert_eq!(r2.private_body, None);
        assert_eq!(
            AiResponse {
                responder_pubkey: None,
                ..r2.clone()
            },
            r
        );

        // H14: V2 head + [0x80][len LE][body] carries the sealed answer inline.
        let body = [&PRIVATE_MAGIC[..], &[PRIVATE_VERSION], &[0x9du8; 12 + 30]].concat();
        let mut v3 = v2.clone();
        v3.push(RESPONSE_EXT_PRIVATE_BODY);
        v3.extend_from_slice(&le_u32(body.len() as u32));
        v3.extend_from_slice(&body);
        let r3 = decode_response(&v3).unwrap();
        assert_eq!(r3.private_body.as_deref(), Some(body.as_slice()));
        assert_eq!(
            AiResponse {
                private_body: None,
                ..r3.clone()
            },
            r2
        );

        // An extension that does not account for every trailing byte, or of
        // an unknown kind, yields no body (the v1 fields still decode).
        let mut short = v3.clone();
        short.pop();
        assert_eq!(decode_response(&short).unwrap().private_body, None);
        let mut kind = v3.clone();
        kind[RESPONSE_V2_LEN] = 0x01; // pipeline-link count range, not a body
        assert_eq!(decode_response(&kind).unwrap().private_body, None);

        // H15: the body is followed by [0x81][len LE][trace commitment], with
        // or without the 32-byte tokens root; the body still decodes.
        for trace_len in [RESPONSE_TRACE_LEN, RESPONSE_TRACE_TOKENS_LEN] {
            let mut v4 = v3.clone();
            v4.push(RESPONSE_EXT_TRACE);
            v4.extend_from_slice(&le_u32(trace_len as u32));
            v4.extend(std::iter::repeat_n(0x5a, trace_len));
            let r4 = decode_response(&v4).unwrap();
            assert_eq!(r4, r3, "trace of {trace_len} bytes");

            // A malformed trailer is not a trace: wrong kind, a length that
            // disagrees with the bytes, a truncated or an overlong trailer.
            let at = v3.len();
            let mut bad_kind = v4.clone();
            bad_kind[at] = 0x82;
            let mut bad_len = v4.clone();
            bad_len[at + 1..at + 5].copy_from_slice(&le_u32(trace_len as u32 - 1));
            let mut truncated = v4.clone();
            truncated.pop();
            let mut longer = v4.clone();
            longer.push(0);
            for bad in [bad_kind, bad_len, truncated, longer] {
                assert_eq!(decode_response(&bad).unwrap().private_body, None);
            }
        }
        // Neither trace length is a valid commitment size on its own.
        let mut stray = v3.clone();
        stray.push(RESPONSE_EXT_TRACE);
        stray.extend_from_slice(&le_u32(40));
        stray.extend(std::iter::repeat_n(0, 40));
        assert_eq!(decode_response(&stray).unwrap().private_body, None);

        // H15 raises the inline body cap from 32 KiB to 48 KiB.
        let big = |len: usize| {
            let mut p = v2.clone();
            p.push(RESPONSE_EXT_PRIVATE_BODY);
            p.extend_from_slice(&le_u32(len as u32));
            p.extend(std::iter::repeat_n(1, len));
            p
        };
        assert!(decode_response(&big(48 * 1024))
            .unwrap()
            .private_body
            .is_some());
        assert_eq!(
            decode_response(&big(48 * 1024 + 1)).unwrap().private_body,
            None
        );
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
        let m1 = "d42fa6ee00e07d49b046090a56af0e7bd61025937c502e2c574a72874c350d24";
        let m2 = "fa2f13be0850e26c5ce86c7ac79da85e300c1da8b3290f9a18d47105f1f2140a";
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
    fn recognises_the_h8_reward_vault() {
        // OP_RETURN(0x6a) PUSH7 "aivault" = keryx_inference::INFERENCE_VAULT_SCRIPT.
        let vault = to_hex(&[0x6a, 0x07, b'a', b'i', b'v', b'a', b'u', b'l', b't']);
        assert_eq!(vault, INFERENCE_VAULT_SCRIPT_HEX);
        assert!(is_inference_vault(&vault));
        assert!(is_inference_vault(&vault.to_ascii_uppercase()));
        // A look-alike OP_RETURN is not the vault (the node calls it non-standard).
        assert!(!is_inference_vault(&to_hex(b"jaivaulu")));
        assert!(!is_inference_vault(""));
        // The vault names no miner, so neither pubkey extractor matches it.
        assert_eq!(csv_escrow_pubkey(&vault), None);
        assert_eq!(p2pk_pubkey(&vault), None);
    }

    #[test]
    fn model_registry_resolves_wallet_keys() {
        assert_eq!(
            model_key("fa2f13be0850e26c5ce86c7ac79da85e300c1da8b3290f9a18d47105f1f2140a"),
            Some("glm-4-9b-0414")
        );
        // H6 tiers 0 and 2 (POM_TIERS_H6[0]/[2]) resolve.
        assert_eq!(
            model_key("bd34568cd89f5f19c6c3a6e1a61b929bc868709409eaad8e672d85f3c1eb5710"),
            Some("qwen3.5-9b-abliterated")
        );
        assert_eq!(
            model_key("399984045600f7d58d1b2cf01e6a4bf466fa15c7ac31bd0dd1a71e003b617cc6"),
            Some("gemma-4-12b-abliterated")
        );
        // H14 tier 3 (POM_TIERS_H14[3], QWEN3_8_27B_MODEL_ID) and the tier-3
        // model it retires.
        assert_eq!(
            model_key("73740b443bdc00afda5fa34eb9999d3fea77dcc3f6de238fab701394cdc96fb3"),
            Some("qwen3.8-27b")
        );
        assert_eq!(
            model_key("b8bdc01fa407eab943e4fefc807483b39f8142785256049e1f559698a5284746"),
            Some("qwen3.6-27b")
        );
        // Retired pre-fork tier-0s (H5's Qwen3-8B, H4's EXAONE) still resolve
        // for historical decode, case-insensitively.
        assert_eq!(
            model_key("D42FA6EE00E07D49B046090A56AF0E7BD61025937C502E2C574A72874C350D24"),
            Some("qwen3-8b-abliterated")
        );
        assert_eq!(
            model_key("300a99b3a85b0ab45d1d930bb7b1d4b0f35983d521e79ff21193a6908dc4b810"),
            Some("exaone-4.0-1.2b")
        );
        assert_eq!(model_key("00".repeat(32).as_str()), None);
    }
}
