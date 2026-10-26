//! Authentication (M6): user catalog + SCRAM-SHA-256 (RFC 5802/7677) +
//! cleartext password verification. TLS itself is transport (main.rs); this
//! module is the crypto and the user file.
//!
//! Verifier format (PostgreSQL-compatible):
//!   SCRAM-SHA-256$<iterations>:<salt_b64>$<stored_key_b64>:<server_key_b64>
//!
//! Unknown users run the full exchange against a built-in dummy verifier so
//! failures are indistinguishable (no user enumeration via error shape).

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

type HmacSha256 = Hmac<Sha256>;

// ------------------------------------------------------------------- b64 ---

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn b64(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            B64[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

pub fn unb64(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some((c - b'A') as u32),
            b'a'..=b'z' => Some((c - b'a' + 26) as u32),
            b'0'..=b'9' => Some((c - b'0' + 52) as u32),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let s = s.trim_end_matches('=').as_bytes();
    let mut out = Vec::with_capacity(s.len() * 3 / 4 + 3);
    for chunk in s.chunks(4) {
        if chunk.len() == 1 {
            return None;
        }
        let mut n: u32 = 0;
        for (i, c) in chunk.iter().enumerate() {
            n |= val(*c)? << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    Some(out)
}

// ---------------------------------------------------------------- random ---

/// n random bytes from the OS (/dev/urandom; getrandom(2) via a read).
pub fn random_bytes(n: usize) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let mut f = std::fs::File::open("/dev/urandom")?;
    let mut v = vec![0u8; n];
    f.read_exact(&mut v)?;
    Ok(v)
}

// ---------------------------------------------------------------- pbkdf2 ---

/// PBKDF2-HMAC-SHA256 (RFC 2898), hand-rolled over the `hmac`/`sha2` deps.
pub fn pbkdf2_sha256(password: &[u8], salt: &[u8], iterations: u32, dklen: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(dklen);
    let mut block: u32 = 1;
    while out.len() < dklen {
        let mut mac = HmacSha256::new_from_slice(password).expect("hmac accepts any key len");
        mac.update(salt);
        mac.update(&block.to_be_bytes());
        let mut u = mac.finalize().into_bytes();
        let mut t = u;
        for _ in 1..iterations {
            let mut mac = HmacSha256::new_from_slice(password).expect("hmac accepts any key len");
            mac.update(&u);
            u = mac.finalize().into_bytes();
            for (ti, ui) in t.iter_mut().zip(u.iter()) {
                *ti ^= *ui;
            }
        }
        out.extend_from_slice(&t);
        block += 1;
    }
    out.truncate(dklen);
    out
}

// --------------------------------------------------------------- verifiers --

#[derive(Clone)]
pub struct ScramVerifier {
    pub iterations: u32,
    pub salt: Vec<u8>,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
}

/// Builds a verifier from a password (the admin-side operation; also what
/// load_users() does for entries that only carry a plaintext password).
pub fn build_verifier(password: &str, iterations: u32, salt: &[u8]) -> ScramVerifier {
    let salted = pbkdf2_sha256(password.as_bytes(), salt, iterations, 32);
    let client_key = {
        let mut mac = HmacSha256::new_from_slice(&salted).expect("hmac");
        mac.update(b"Client Key");
        mac.finalize().into_bytes().to_vec()
    };
    let stored_key: [u8; 32] = Sha256::digest(&client_key).into();
    let server_key = {
        let mut mac = HmacSha256::new_from_slice(&salted).expect("hmac");
        mac.update(b"Server Key");
        mac.finalize().into_bytes().to_vec()
    };
    let mut sk = [0u8; 32];
    sk.copy_from_slice(&server_key);
    ScramVerifier {
        iterations,
        salt: salt.to_vec(),
        stored_key,
        server_key: sk,
    }
}

pub fn verifier_string(v: &ScramVerifier) -> String {
    format!(
        "SCRAM-SHA-256${}:{}${}:{}",
        v.iterations,
        b64(&v.salt),
        b64(&v.stored_key),
        b64(&v.server_key)
    )
}

/// Parses "SCRAM-SHA-256$4096:c2FsdA==$c3RvcmVk:c2VydmVy".
pub fn parse_verifier(s: &str) -> Option<ScramVerifier> {
    let rest = s.strip_prefix("SCRAM-SHA-256$")?;
    let (iter_salt, keys) = rest.split_once('$')?;
    let (it, salt) = iter_salt.split_once(':')?;
    let (stk, srk) = keys.split_once(':')?;
    let iterations: u32 = it.parse().ok()?;
    if iterations == 0 || iterations > 1_000_000 {
        return None;
    }
    let salt = unb64(salt)?;
    let mut stored_key = [0u8; 32];
    stored_key.copy_from_slice(&unb64(stk)?);
    let mut server_key = [0u8; 32];
    server_key.copy_from_slice(&unb64(srk)?);
    Some(ScramVerifier {
        iterations,
        salt,
        stored_key,
        server_key,
    })
}

// ------------------------------------------------------------ user catalog --

pub struct UserEntry {
    pub password: Option<String>,
    pub scram: Option<ScramVerifier>,
}

#[derive(Default)]
pub struct Users {
    pub by_name: HashMap<String, UserEntry>,
}

impl Users {
    /// Loads {"users": {name: {"password": "...", "scram": "SCRAM-..."}}}.
    /// Entries with only a password get a freshly-salted SCRAM verifier.
    pub fn load(path: &str) -> Result<Users, String> {
        let raw = std::fs::read_to_string(path).map_err(|e| format!("auth file: {}", e))?;
        let v: serde_json::Value =
            serde_json::from_str(&raw).map_err(|e| format!("auth file parse: {}", e))?;
        let mut users = Users::default();
        let empty = serde_json::json!({});
        let map = v.get("users").unwrap_or(&empty).as_object().ok_or("users must be an object")?;
        for (name, e) in map {
            let password = e.get("password").and_then(|p| p.as_str()).map(String::from);
            let scram = match e.get("scram").and_then(|s| s.as_str()) {
                Some(s) => Some(parse_verifier(s).ok_or_else(|| format!("bad scram verifier for {}", name))?),
                None => None,
            };
            let scram = match scram {
                Some(s) => Some(s),
                None => match &password {
                    Some(p) => {
                        let salt = random_bytes(16).map_err(|e| e.to_string())?;
                        Some(build_verifier(p, 4096, &salt))
                    }
                    None => None,
                },
            };
            if password.is_none() && scram.is_none() {
                return Err(format!("user {} has neither password nor scram", name));
            }
            users.by_name.insert(
                name.clone(),
                UserEntry { password, scram },
            );
        }
        Ok(users)
    }

    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }
}

/// Dummy verifier used for unknown users so the exchange runs identically.
pub fn dummy_verifier() -> ScramVerifier {
    // fixed salt; the password is unknowable — verification always fails
    build_verifier(
        "\u{0}puddle-dummy-not-a-real-password-\u{1}do-not-use",
        4096,
        &[0u8; 16],
    )
}

// ------------------------------------------------------------- SASL server --

/// State of one SCRAM-SHA-256 exchange.
pub struct ScramServer {
    verifier: ScramVerifier,
    client_first_bare: String,
    /// gs2 header as sent ("n,," or "y,,"); the client-final c= field must
    /// be its base64 (biws / eSws).
    gs2: String,
    pub server_first: String,
    server_nonce: String,
}

impl ScramServer {
    /// Handles client-first ("n,,n=user,r=cnonce" or "y,,n=...,r=...");
    /// returns server-first. "y" = client supports channel binding but the
    /// server did not advertise SCRAM-SHA-256-PLUS (common over TLS with
    /// libpq's channel_binding=prefer default).
    pub fn start(verifier: ScramVerifier, client_first: &str) -> Result<ScramServer, String> {
        let gs2 = if client_first.starts_with("n,,") {
            "n,,".to_string()
        } else if client_first.starts_with("y,,") {
            "y,,".to_string()
        } else {
            return Err("unsupported channel binding".into());
        };
        let bare = &client_first[3..];
        let mut user = "";
        let mut cnonce = "";
        for part in bare.split(',') {
            if let Some(v) = part.strip_prefix("n=") {
                user = v;
            } else if let Some(v) = part.strip_prefix("r=") {
                cnonce = v;
            }
        }
        if cnonce.is_empty() {
            return Err("missing client nonce".into());
        }
        let _ = user; // user identity comes from the startup packet
        let snonce_raw = random_bytes(18).map_err(|e| e.to_string())?;
        let server_nonce = b64(&snonce_raw);
        let server_first = format!(
            "r={}{},s={},i={}",
            cnonce,
            server_nonce,
            b64(&verifier.salt),
            verifier.iterations
        );
        Ok(ScramServer {
            verifier,
            client_first_bare: bare.to_string(),
            gs2,
            server_first,
            server_nonce,
        })
    }

    /// Handles client-final ("c=biws,r=nonce,p=proof"); returns the
    /// server-signature bytes to send as "v=...".
    pub fn finish(&self, client_final: &str) -> Result<Vec<u8>, String> {
        let mut channel = "";
        let mut nonce = "";
        let mut proof_b64 = "";
        let mut without_proof = String::new();
        for (i, part) in client_final.split(',').enumerate() {
            if let Some(v) = part.strip_prefix("c=") {
                channel = v;
            } else if let Some(v) = part.strip_prefix("r=") {
                nonce = v;
            } else if let Some(v) = part.strip_prefix("p=") {
                proof_b64 = v;
                continue; // proof is excluded from the auth message
            }
            if i > 0 {
                without_proof.push(',');
            }
            without_proof.push_str(part);
        }
        let want_channel = b64(self.gs2.as_bytes());
        if channel != want_channel {
            return Err("channel binding mismatch in client-final".into());
        }
        if nonce.is_empty() || proof_b64.is_empty() {
            return Err("malformed client-final".into());
        }
        // nonce must be client||server exactly
        let expected_nonce_len = nonce.len();
        if !nonce.ends_with(&self.server_nonce)
            || expected_nonce_len <= self.server_nonce.len()
        {
            return Err("nonce mismatch".into());
        }
        let proof = unb64(proof_b64).ok_or("bad proof encoding")?;
        if proof.len() != 32 {
            return Err("bad proof length".into());
        }
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, without_proof
        );
        // ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_sig = {
            let mut mac = HmacSha256::new_from_slice(&self.verifier.stored_key).expect("hmac");
            mac.update(auth_message.as_bytes());
            mac.finalize().into_bytes().to_vec()
        };
        // ClientKey = proof XOR ClientSignature
        let mut client_key = [0u8; 32];
        for i in 0..32 {
            client_key[i] = proof[i] ^ client_sig[i];
        }
        // H(ClientKey) must equal StoredKey
        let h: [u8; 32] = Sha256::digest(client_key).into();
        if h != self.verifier.stored_key {
            return Err("password authentication failed".into());
        }
        // ServerSignature = HMAC(ServerKey, AuthMessage)
        let mut mac = HmacSha256::new_from_slice(&self.verifier.server_key).expect("hmac");
        mac.update(auth_message.as_bytes());
        Ok(mac.finalize().into_bytes().to_vec())
    }
}

// ------------------------------------------------------------------ tests --

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b64_roundtrip() {
        for data in [&b""[..], b"f", b"fo", b"foo", b"foob", b"fooba", b"foobar"] {
            assert_eq!(unb64(&b64(data)).unwrap(), data);
        }
        assert_eq!(b64(&[0, 0, 0]), "AAAA");
        assert_eq!(b64(&[0xff, 0xff, 0xff]), "////");
        assert!(unb64("!!!!").is_none());
    }

    #[test]
    fn pbkdf2_rfc6070_style() {
        // Known SHA-256 vectors (RFC 7914 §11 / widely cross-checked):
        // PBKDF2-HMAC-SHA256("passwd", "salt", 1, 64)
        let d = pbkdf2_sha256(b"passwd", b"salt", 1, 64);
        assert_eq!(
            d[..32].to_vec(),
            vec![
                0x55, 0xac, 0x04, 0x6e, 0x56, 0xe3, 0x08, 0x9f, 0xec, 0x16, 0x91, 0xc2, 0x25,
                0x44, 0xb6, 0x05, 0xf9, 0x41, 0x85, 0x21, 0x6d, 0xde, 0x04, 0x65, 0xe6, 0x8b,
                0x9d, 0x57, 0xc2, 0x0d, 0xac, 0xbc
            ]
        );
        // PBKDF2-HMAC-SHA256("password", "salt", 2, 32)
        // (both vectors cross-checked against python hashlib.pbkdf2_hmac)
        let d2 = pbkdf2_sha256(b"password", b"salt", 2, 32);
        assert_eq!(
            d2,
            vec![
                0xae, 0x4d, 0x0c, 0x95, 0xaf, 0x6b, 0x46, 0xd3, 0x2d, 0x0a, 0xdf, 0xf9, 0x28,
                0xf0, 0x6d, 0xd0, 0x2a, 0x30, 0x3f, 0x8e, 0xf3, 0xc2, 0x51, 0xdf, 0xd6, 0xe2,
                0xd8, 0x5a, 0x95, 0x47, 0x4c, 0x43
            ]
        );
    }

    #[test]
    fn verifier_roundtrip_and_exchange() {
        let salt = [7u8; 16];
        let v = build_verifier("correct horse battery staple", 4096, &salt);
        let s = verifier_string(&v);
        let v2 = parse_verifier(&s).unwrap();
        assert_eq!(v2.iterations, 4096);
        assert_eq!(v2.salt, salt.to_vec());
        assert_eq!(v2.stored_key, v.stored_key);
        assert_eq!(v2.server_key, v.server_key);

        // full exchange with a hand-rolled SCRAM client
        let cnonce = b64(&[1, 2, 3, 4]);
        let client_first = format!("n,,n=alice,r={}", cnonce);
        let srv = ScramServer::start(v2, &client_first).unwrap();
        // parse server-first
        let mut snonce = "";
        let mut ssalt = "";
        let mut iters = 0u32;
        for part in srv.server_first.split(',') {
            if let Some(x) = part.strip_prefix("r=") {
                snonce = x;
            } else if let Some(x) = part.strip_prefix("s=") {
                ssalt = x;
            } else if let Some(x) = part.strip_prefix("i=") {
                iters = x.parse().unwrap();
            }
        }
        assert!(snonce.starts_with(&cnonce));
        // client computes with the true password
        let salted = pbkdf2_sha256(b"correct horse battery staple", &unb64(ssalt).unwrap(), iters, 32);
        let client_key = {
            let mut mac = HmacSha256::new_from_slice(&salted).unwrap();
            mac.update(b"Client Key");
            mac.finalize().into_bytes().to_vec()
        };
        let stored_key: [u8; 32] = Sha256::digest(&client_key).into();
        let full_nonce = snonce.to_string();
        let client_final_wo = format!("c=biws,r={}", full_nonce);
        let auth_message = format!("{},{},{}", &client_first[3..], srv.server_first, client_final_wo);
        let client_sig = {
            let mut mac = HmacSha256::new_from_slice(&stored_key).unwrap();
            mac.update(auth_message.as_bytes());
            mac.finalize().into_bytes().to_vec()
        };
        let mut proof = [0u8; 32];
        for i in 0..32 {
            proof[i] = client_key[i] ^ client_sig[i];
        }
        let client_final = format!("{},p={}", client_final_wo, b64(&proof));
        let server_sig = srv.finish(&client_final).unwrap();
        // recompute expected server signature
        let server_key = {
            let mut mac = HmacSha256::new_from_slice(&salted).unwrap();
            mac.update(b"Server Key");
            mac.finalize().into_bytes().to_vec()
        };
        let expect = {
            let mut mac = HmacSha256::new_from_slice(&server_key).unwrap();
            mac.update(auth_message.as_bytes());
            mac.finalize().into_bytes().to_vec()
        };
        assert_eq!(server_sig, expect);

        // wrong password must fail
        let bad = srv.finish(&format!("{},p={}", client_final_wo, b64(&[9u8; 32])));
        assert!(bad.is_err());
    }
}
