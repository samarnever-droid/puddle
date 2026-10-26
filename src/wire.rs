//! PostgreSQL wire protocol v3 — packet framing and message builders.
//! Byte order: all integers big-endian. Stream helpers are generic over
//! Read+Write so plain TCP and TLS-wrapped connections share one code path.

use std::io::{Read, Write};

// ---------------------------------------------------------------- read side --

/// A protocol packet: 1-byte type + i32 length (including itself) + payload.
pub struct Packet {
    pub kind: u8,
    pub body: Vec<u8>,
}

/// What a (typeless) startup-family packet turned out to be.
pub enum Startup {
    /// protocol 3.0 + key/value parameters (user, database, ...)
    Params(Vec<(String, String)>),
    SslRequest,
    GssencRequest,
    CancelRequest,
}

/// Reads exactly `n` bytes, allocating incrementally: a hostile client that
/// claims a huge length but never sends the body must not be able to make
/// the server preallocate it. Cap the in-flight buffer at 64 KiB chunks and
/// return `UnexpectedEof` on clean close (callers treat that kind as a
/// normal disconnect).
fn read_exact_n<S: Read>(s: &mut S, n: usize) -> std::io::Result<Vec<u8>> {
    let mut v = Vec::with_capacity(n.min(64 * 1024));
    let mut left = n;
    let mut chunk = [0u8; 64 * 1024];
    while left > 0 {
        let want = left.min(chunk.len());
        match s.read(&mut chunk[..want]) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "connection closed mid-packet",
                ));
            }
            Ok(k) => {
                v.extend_from_slice(&chunk[..k]);
                left -= k;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(v)
}

/// Reads the first (typeless) startup-family packet.
pub fn read_startup<S: Read>(s: &mut S) -> std::io::Result<Startup> {
    let hdr = read_exact_n(s, 4)?;
    let len = i32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
    if !(8..=10_000).contains(&len) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad startup length",
        ));
    }
    let body = read_exact_n(s, len - 4)?;
    let code = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
    match code {
        196_608 => {
            // protocol 3.0: null-terminated key\0value\0 pairs
            let mut params = Vec::new();
            let mut pos = 4usize;
            while pos < body.len() {
                let read_cstr = |pos: &mut usize| -> String {
                    let start = *pos;
                    while *pos < body.len() && body[*pos] != 0 {
                        *pos += 1;
                    }
                    let s = String::from_utf8_lossy(&body[start..*pos]).into_owned();
                    *pos += 1; // skip NUL
                    s
                };
                let k = read_cstr(&mut pos);
                if k.is_empty() {
                    break;
                }
                let v = read_cstr(&mut pos);
                params.push((k, v));
            }
            Ok(Startup::Params(params))
        }
        80_877_103 => Ok(Startup::SslRequest),
        80_877_104 => Ok(Startup::GssencRequest),
        80_877_102 => Ok(Startup::CancelRequest),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unsupported protocol",
        )),
    }
}

/// Reads one typed packet. Ok(None) on clean EOF.
pub fn read_packet<S: Read>(s: &mut S) -> std::io::Result<Option<Packet>> {
    let mut hdr = [0u8; 5];
    match s.read_exact(&mut hdr) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    if !(4..=64 * 1024 * 1024).contains(&len) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad packet length",
        ));
    }
    let body = read_exact_n(s, len - 4)?;
    Ok(Some(Packet {
        kind: hdr[0],
        body,
    }))
}

// --------------------------------------------------------------- write side --

/// Appends a typed message to `out`.
pub fn msg(out: &mut Vec<u8>, kind: u8, body: &[u8]) {
    out.push(kind);
    out.extend_from_slice(&((body.len() as u32 + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
}

pub fn auth_ok(out: &mut Vec<u8>) {
    msg(out, b'R', &0i32.to_be_bytes());
}

/// AuthenticationCleartextPassword (code 3).
pub fn auth_cleartext_password(out: &mut Vec<u8>) {
    msg(out, b'R', &3i32.to_be_bytes());
}

/// AuthenticationSASL (code 10): mechanism list. Each name is a C string;
/// the list ends with an EMPTY C string (no count field — libpq reads until
/// the empty terminator).
pub fn auth_sasl(out: &mut Vec<u8>, mechanisms: &[&str]) {
    let mut b = Vec::new();
    b.extend_from_slice(&10i32.to_be_bytes());
    for m in mechanisms {
        b.extend_from_slice(m.as_bytes());
        b.push(0);
    }
    b.push(0); // terminating empty string
    msg(out, b'R', &b);
}

/// AuthenticationSASLContinue (code 11).
pub fn auth_sasl_continue(out: &mut Vec<u8>, data: &[u8]) {
    let mut b = Vec::new();
    b.extend_from_slice(&11i32.to_be_bytes());
    b.extend_from_slice(data);
    msg(out, b'R', &b);
}

/// AuthenticationSASLFinal (code 12).
pub fn auth_sasl_final(out: &mut Vec<u8>, data: &[u8]) {
    let mut b = Vec::new();
    b.extend_from_slice(&12i32.to_be_bytes());
    b.extend_from_slice(data);
    msg(out, b'R', &b);
}

/// Reads a PasswordMessage / SASL response ('p'): the whole payload.
pub fn password_message(body: &[u8]) -> String {
    String::from_utf8_lossy(body).into_owned()
}

/// SASLInitialResponse body: mechanism cstr + i32 len + client-first bytes.
pub fn parse_sasl_initial(body: &[u8]) -> Option<(String, Vec<u8>)> {
    let mut rd = Rd::new(body);
    let mech = rd.cstr()?;
    let len = rd.i32()?;
    let len = if len < 0 { 0 } else { len as usize };
    let data = rd.take(len)?.to_vec();
    Some((mech, data))
}

pub fn param_status(out: &mut Vec<u8>, name: &str, val: &str) {
    let mut b = Vec::new();
    b.extend_from_slice(name.as_bytes());
    b.push(0);
    b.extend_from_slice(val.as_bytes());
    b.push(0);
    msg(out, b'S', &b);
}

pub fn backend_key_data(out: &mut Vec<u8>, pid: u32, secret: u32) {
    let mut b = Vec::new();
    b.extend_from_slice(&pid.to_be_bytes());
    b.extend_from_slice(&secret.to_be_bytes());
    msg(out, b'K', &b);
}

pub fn ready_for_query(out: &mut Vec<u8>, txn: char) {
    msg(out, b'Z', &[txn as u8]);
}

pub fn empty_query_response(out: &mut Vec<u8>) {
    msg(out, b'I', &[]);
}

/// ErrorResponse: severity, sqlstate, message (+ terminator).
pub fn error(out: &mut Vec<u8>, code: &str, msg_text: &str) {
    let mut b = Vec::new();
    b.extend_from_slice(b"SERROR\0");
    b.push(b'C');
    b.extend_from_slice(code.as_bytes());
    b.push(0);
    b.push(b'M');
    b.extend_from_slice(msg_text.as_bytes());
    b.push(0);
    b.push(0); // end of fields
    msg(out, b'E', &b);
}

pub fn command_complete(out: &mut Vec<u8>, tag: &str) {
    let mut b = Vec::with_capacity(tag.len() + 1);
    b.extend_from_slice(tag.as_bytes());
    b.push(0);
    msg(out, b'C', &b);
}

pub fn flush<S: Write>(s: &mut S, out: &[u8]) -> std::io::Result<()> {
    s.write_all(out)
}

// ------------------------------------------------ extended query protocol --

/// Cursor over a message body.
pub struct Rd<'a> {
    pub b: &'a [u8],
    pub p: usize,
}

impl<'a> Rd<'a> {
    pub fn new(b: &'a [u8]) -> Rd<'a> {
        Rd { b, p: 0 }
    }
    pub fn cstr(&mut self) -> Option<String> {
        let start = self.p;
        while self.p < self.b.len() && self.b[self.p] != 0 {
            self.p += 1;
        }
        if self.p >= self.b.len() {
            return None; // missing terminator
        }
        let s = String::from_utf8_lossy(&self.b[start..self.p]).into_owned();
        self.p += 1;
        Some(s)
    }
    pub fn i16(&mut self) -> Option<i16> {
        if self.p + 2 > self.b.len() {
            return None;
        }
        let v = i16::from_be_bytes([self.b[self.p], self.b[self.p + 1]]);
        self.p += 2;
        Some(v)
    }
    pub fn i32(&mut self) -> Option<i32> {
        if self.p + 4 > self.b.len() {
            return None;
        }
        let v = i32::from_be_bytes([
            self.b[self.p],
            self.b[self.p + 1],
            self.b[self.p + 2],
            self.b[self.p + 3],
        ]);
        self.p += 4;
        Some(v)
    }
    pub fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.p + n > self.b.len() {
            return None;
        }
        let s = &self.b[self.p..self.p + n];
        self.p += n;
        Some(s)
    }
}

pub fn parse_complete(out: &mut Vec<u8>) {
    msg(out, b'1', &[]);
}
pub fn bind_complete(out: &mut Vec<u8>) {
    msg(out, b'2', &[]);
}
pub fn close_complete(out: &mut Vec<u8>) {
    msg(out, b'3', &[]);
}
pub fn no_data(out: &mut Vec<u8>) {
    msg(out, b'n', &[]);
}
pub fn portal_suspended(out: &mut Vec<u8>) {
    msg(out, b's', &[]);
}

pub fn parameter_description(out: &mut Vec<u8>, oids: &[u32]) {
    let mut b = Vec::new();
    b.extend_from_slice(&(oids.len() as i16).to_be_bytes());
    for o in oids {
        b.extend_from_slice(&o.to_be_bytes());
    }
    msg(out, b't', &b);
}

/// RowDescription with per-column format codes. cols = (name, oid, typlen, fmt).
pub fn row_description_fmt(out: &mut Vec<u8>, cols: &[(String, u32, i16, i16)]) {
    let mut b = Vec::new();
    b.extend_from_slice(&(cols.len() as i16).to_be_bytes());
    for (name, oid, typlen, fmt) in cols {
        b.extend_from_slice(name.as_bytes());
        b.push(0);
        b.extend_from_slice(&0i32.to_be_bytes());
        b.extend_from_slice(&0i16.to_be_bytes());
        b.extend_from_slice(&oid.to_be_bytes());
        b.extend_from_slice(&typlen.to_be_bytes());
        b.extend_from_slice(&(-1i32).to_be_bytes());
        b.extend_from_slice(&fmt.to_be_bytes());
    }
    msg(out, b'T', &b);
}

/// DataRow over pre-encoded values (text or binary), None = NULL.
pub fn data_row_raw(out: &mut Vec<u8>, vals: &[Option<Vec<u8>>]) {
    let mut b = Vec::new();
    b.extend_from_slice(&(vals.len() as i16).to_be_bytes());
    for v in vals {
        match v {
            None => b.extend_from_slice(&(-1i32).to_be_bytes()),
            Some(d) => {
                b.extend_from_slice(&(d.len() as i32).to_be_bytes());
                b.extend_from_slice(d);
            }
        }
    }
    msg(out, b'D', &b);
}
