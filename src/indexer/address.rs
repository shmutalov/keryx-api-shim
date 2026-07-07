//! Address attribution for the ledger — a Rust port of the wallet extension's
//! cashaddr-style codec (`keryx-wallet-extension/src/lib/keryx.js`), so the
//! addresses we index match the ones the wallet derives byte-for-byte.
//!
//! Only the standard script forms are recognised; anything else (e.g. the
//! CSV-pubkey inference escrow) is left unattributed, which is fine — the swap
//! app's HTLC (P2SH) and normal P2PK sends are covered, and spend detection
//! (the swap-critical path) never needs an address.

const CHARSET: &[u8; 32] = b"qpzry9x8gf2tvdw0s3jn54khce6mua7l";

/// keryxd network name (from getBlockDagInfo) → address prefix.
pub fn prefix_for_network(network_name: &str) -> &'static str {
    let n = network_name.to_ascii_lowercase();
    if n.contains("mainnet") {
        "keryx"
    } else if n.contains("testnet") {
        "keryxtest"
    } else if n.contains("devnet") {
        "keryxdev"
    } else {
        // simnet and anything unrecognised default to simnet's prefix; the
        // shim is only ever pointed at one network at a time.
        "keryxsim"
    }
}

fn to_words(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() * 8 / 5 + 1);
    let mut acc: u32 = 0;
    let mut bits = 0u32;
    for &b in bytes {
        acc = (acc << 8) | b as u32;
        bits += 8;
        while bits >= 5 {
            bits -= 5;
            out.push(((acc >> bits) & 31) as u8);
        }
    }
    if bits > 0 {
        out.push(((acc << (5 - bits)) & 31) as u8);
    }
    out
}

/// 40-bit BCH polymod (cashaddr generators), computed in u64.
fn polymod(values: &[u8]) -> u64 {
    const GEN: [u64; 5] = [
        0x98f2bc8e61,
        0x79b76d99e2,
        0xf33e5fb3c4,
        0xae2eabe2a8,
        0x1e4f43e470,
    ];
    let mut c: u64 = 1;
    for &v in values {
        let top = c >> 35;
        c = ((c & 0x07ffffffff) << 5) ^ v as u64;
        for (i, g) in GEN.iter().enumerate() {
            if (top >> i) & 1 == 1 {
                c ^= g;
            }
        }
    }
    c ^ 1
}

fn checksum_words(data_words: &[u8], prefix: &str) -> [u8; 8] {
    // prefix chars & 31, then a 0 separator, the data words, and 8 zero slots.
    let mut input: Vec<u8> = prefix.bytes().map(|c| c & 31).collect();
    input.push(0);
    input.extend_from_slice(data_words);
    input.extend_from_slice(&[0u8; 8]);
    let m = polymod(&input);
    let mut bytes = [0u8; 5];
    let mut mod_val = m;
    for i in (0..5).rev() {
        bytes[i] = (mod_val & 255) as u8;
        mod_val >>= 8;
    }
    let words = to_words(&bytes);
    let mut check = [0u8; 8];
    check.copy_from_slice(&words[..8]);
    check
}

/// Encode `version || payload` as `prefix:<base32+checksum>`.
pub fn encode_address(version: u8, payload: &[u8], prefix: &str) -> String {
    let mut raw = Vec::with_capacity(1 + payload.len());
    raw.push(version);
    raw.extend_from_slice(payload);
    let data = to_words(&raw);
    let check = checksum_words(&data, prefix);
    let mut s = String::with_capacity(prefix.len() + 1 + data.len() + 8);
    s.push_str(prefix);
    s.push(':');
    for &w in &data {
        s.push(CHARSET[w as usize] as char);
    }
    for &w in &check {
        s.push(CHARSET[w as usize] as char);
    }
    s
}

/// Map a `script_public_key` (hex) to its address, for the standard forms:
/// version-0 schnorr P2PK, version-1 ECDSA P2PK, and version-8 P2SH (HTLC).
/// Returns `None` for non-standard scripts.
pub fn script_to_address(script_hex: &str, prefix: &str) -> Option<String> {
    let script = hex_to_bytes(script_hex)?;
    match script.as_slice() {
        // OP_DATA_32 <32-byte x-only pubkey> OP_CHECKSIG
        [0x20, rest @ .., 0xac] if rest.len() == 32 => Some(encode_address(0, rest, prefix)),
        // OP_DATA_33 <33-byte compressed pubkey> OP_CHECKSIGECDSA
        [0x21, rest @ .., 0xab] if rest.len() == 33 => Some(encode_address(1, rest, prefix)),
        // OP_BLAKE2B OP_DATA_32 <32-byte script hash> OP_EQUAL
        [0xaa, 0x20, rest @ .., 0x87] if rest.len() == 32 => Some(encode_address(8, rest, prefix)),
        _ => None,
    }
}

fn hex_to_bytes(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Vector generated with the wallet's own encoder (encodeAddress) for
    // payload = 0x11 * 32.
    const PUBKEY_HEX: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const SIMNET_ADDR: &str =
        "keryxsim:qqg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zs5zp29vm";
    const MAINNET_ADDR: &str =
        "keryx:qqg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3z6j06xt9k";

    #[test]
    fn encode_matches_wallet_vector() {
        let payload = hex_to_bytes(PUBKEY_HEX).unwrap();
        assert_eq!(encode_address(0, &payload, "keryxsim"), SIMNET_ADDR);
        assert_eq!(encode_address(0, &payload, "keryx"), MAINNET_ADDR);
    }

    #[test]
    fn p2pk_script_decodes_to_address() {
        let script = format!("20{PUBKEY_HEX}ac");
        assert_eq!(
            script_to_address(&script, "keryxsim").as_deref(),
            Some(SIMNET_ADDR)
        );
    }

    #[test]
    fn non_standard_script_is_unattributed() {
        // CSV-pubkey escrow: <push> OP_CHECKSEQUENCEVERIFY OP_DATA_32 <pk> OP_CHECKSIG
        let script = format!("03a08c00b120{PUBKEY_HEX}ac");
        assert_eq!(script_to_address(&script, "keryxsim"), None);
    }

    #[test]
    fn prefix_mapping() {
        assert_eq!(prefix_for_network("keryx-mainnet"), "keryx");
        assert_eq!(prefix_for_network("keryx-testnet-11"), "keryxtest");
        assert_eq!(prefix_for_network("keryx-simnet"), "keryxsim");
        assert_eq!(prefix_for_network("keryx-devnet"), "keryxdev");
    }
}
