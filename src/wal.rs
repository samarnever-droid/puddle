//! WAL: framed, CRC32C-checked records in a GLOBAL segment stream
//! (Aurora: "the log is the database"). One transaction (or autocommit
//! statement) = exactly one immutable segment object, so multi-table
//! commits are atomic for free: the object either exists or doesn't.
//!
//! Segment key: wal/<seq:016>-<uuid>.wal (zero-padded: lexicographic
//! order == numeric order). Frame:
//!   [u32 payload_len][u32 crc32c(payload)][u64 lsn][payload]
//! payload = [u8 kind][u64 table_id][kind-specific]
//!   1 Insert: u64 rowid, u32 ncells, cells
//!   2 Update: u64 rowid, u32 ncells, cells
//!   3 Delete: u64 rowid
//!   4 Create: u32 name_len, name, u32 ncols, per col {u32 name_len, name, u8 ty, u8 not_null}
//!   5 Drop:   u32 name_len, name
//! A torn tail (truncation / CRC fail) ends replay of that segment; only the
//! highest-seq segment may be torn. Structural garbage is a loud error.

use crate::engine::{Ty, Value};

pub const REC_INSERT: u8 = 1;
pub const REC_UPDATE: u8 = 2;
pub const REC_DELETE: u8 = 3;
pub const REC_CREATE: u8 = 4;
pub const REC_DROP: u8 = 5;
pub const REC_ALTER_ADD: u8 = 6;

// ty codes shared with the part format
pub const TY_BOOL: u8 = 0;
pub const TY_INT: u8 = 1;
pub const TY_FLOAT: u8 = 2;
pub const TY_TEXT: u8 = 3;
pub const TY_DATE: u8 = 4;
pub const TY_TIME: u8 = 5;
pub const TY_TIMESTAMP: u8 = 6;
pub const TY_INTERVAL: u8 = 7;

pub fn ty_code(t: Ty) -> u8 {
    match t {
        Ty::Bool => TY_BOOL,
        Ty::Int => TY_INT,
        Ty::Float => TY_FLOAT,
        Ty::Text => TY_TEXT,
        Ty::Date => TY_DATE,
        Ty::Time => TY_TIME,
        Ty::Timestamp => TY_TIMESTAMP,
        Ty::Interval => TY_INTERVAL,
    }
}

pub fn ty_of(code: u8) -> Option<Ty> {
    match code {
        TY_BOOL => Some(Ty::Bool),
        TY_INT => Some(Ty::Int),
        TY_FLOAT => Some(Ty::Float),
        TY_TEXT => Some(Ty::Text),
        TY_DATE => Some(Ty::Date),
        TY_TIME => Some(Ty::Time),
        TY_TIMESTAMP => Some(Ty::Timestamp),
        TY_INTERVAL => Some(Ty::Interval),
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum WalRec {
    Insert { rowid: u64, cells: Vec<Value> },
    Update { rowid: u64, cells: Vec<Value> },
    Delete { rowid: u64 },
    Create { name: String, cols: Vec<(String, Ty, bool)> },
    Drop { name: String },
    /// ALTER TABLE ADD COLUMN (nullable, no default): the column exists
    /// from the record's seq onward; existing rows read as NULL. Old parts
    /// simply lack the trailing column and are padded at load time.
    AlterAdd { name: String, col: (String, Ty, bool) },
}

// ------------------------------------------------------------- value codec --

pub fn enc_value(out: &mut Vec<u8>, v: &Value) {
    match v {
        Value::Null => out.push(0),
        Value::Bool(b) => {
            out.push(1);
            out.push(*b as u8);
        }
        Value::Int(i) => {
            out.push(2);
            out.extend_from_slice(&i.to_be_bytes());
        }
        Value::Float(f) => {
            out.push(3);
            out.extend_from_slice(&f.to_bits().to_be_bytes());
        }
        Value::Text(s) => {
            out.push(4);
            out.extend_from_slice(&(s.len() as u32).to_be_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        Value::Date(d) => {
            out.push(5);
            out.extend_from_slice(&d.to_be_bytes());
        }
        Value::Time(t) => {
            out.push(6);
            out.extend_from_slice(&t.to_be_bytes());
        }
        Value::Timestamp(us) => {
            out.push(7);
            out.extend_from_slice(&us.to_be_bytes());
        }
        Value::Interval { months, days, micros } => {
            out.push(8);
            out.extend_from_slice(&months.to_be_bytes());
            out.extend_from_slice(&days.to_be_bytes());
            out.extend_from_slice(&micros.to_be_bytes());
        }
    }
}

pub fn dec_value(b: &[u8], p: &mut usize) -> Option<Value> {
    let tag = *b.get(*p)?;
    *p += 1;
    match tag {
        0 => Some(Value::Null),
        1 => {
            let x = *b.get(*p)?;
            *p += 1;
            Some(Value::Bool(x != 0))
        }
        2 => {
            if *p + 8 > b.len() {
                return None;
            }
            let mut a = [0u8; 8];
            a.copy_from_slice(&b[*p..*p + 8]);
            *p += 8;
            Some(Value::Int(i64::from_be_bytes(a)))
        }
        3 => {
            if *p + 8 > b.len() {
                return None;
            }
            let mut a = [0u8; 8];
            a.copy_from_slice(&b[*p..*p + 8]);
            *p += 8;
            Some(Value::Float(f64::from_bits(u64::from_be_bytes(a))))
        }
        4 => {
            if *p + 4 > b.len() {
                return None;
            }
            let n = u32::from_be_bytes([b[*p], b[*p + 1], b[*p + 2], b[*p + 3]]) as usize;
            *p += 4;
            if *p + n > b.len() {
                return None;
            }
            let s = String::from_utf8_lossy(&b[*p..*p + n]).into_owned();
            *p += n;
            Some(Value::Text(s))
        }
        5 => {
            if *p + 4 > b.len() {
                return None;
            }
            let v = i32::from_be_bytes([b[*p], b[*p + 1], b[*p + 2], b[*p + 3]]);
            *p += 4;
            Some(Value::Date(v))
        }
        6 => {
            if *p + 8 > b.len() {
                return None;
            }
            let mut a = [0u8; 8];
            a.copy_from_slice(&b[*p..*p + 8]);
            *p += 8;
            Some(Value::Time(i64::from_be_bytes(a)))
        }
        7 => {
            if *p + 8 > b.len() {
                return None;
            }
            let mut a = [0u8; 8];
            a.copy_from_slice(&b[*p..*p + 8]);
            *p += 8;
            Some(Value::Timestamp(i64::from_be_bytes(a)))
        }
        8 => {
            if *p + 16 > b.len() {
                return None;
            }
            let months = i32::from_be_bytes([b[*p], b[*p + 1], b[*p + 2], b[*p + 3]]);
            let days = i32::from_be_bytes([b[*p + 4], b[*p + 5], b[*p + 6], b[*p + 7]]);
            let mut a = [0u8; 8];
            a.copy_from_slice(&b[*p + 8..*p + 16]);
            *p += 16;
            Some(Value::Interval { months, days, micros: i64::from_be_bytes(a) })
        }
        _ => None,
    }
}

fn enc_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

// ----------------------------------------------------------------- framing --

/// Frames `items` (table id + record) into `out`. One call = one segment.
pub fn encode_segment(out: &mut Vec<u8>, items: &[(u64, WalRec)], lsn: u64) {
    for (id, r) in items {
        let mut payload = Vec::with_capacity(32);
        match r {
            WalRec::Insert { rowid, cells } | WalRec::Update { rowid, cells } => {
                payload.push(if matches!(r, WalRec::Insert { .. }) {
                    REC_INSERT
                } else {
                    REC_UPDATE
                });
                payload.extend_from_slice(&id.to_be_bytes());
                payload.extend_from_slice(&rowid.to_be_bytes());
                payload.extend_from_slice(&(cells.len() as u32).to_be_bytes());
                for c in cells {
                    enc_value(&mut payload, c);
                }
            }
            WalRec::Delete { rowid } => {
                payload.push(REC_DELETE);
                payload.extend_from_slice(&id.to_be_bytes());
                payload.extend_from_slice(&rowid.to_be_bytes());
            }
            WalRec::Create { name, cols } => {
                payload.push(REC_CREATE);
                payload.extend_from_slice(&id.to_be_bytes());
                enc_str(&mut payload, name);
                payload.extend_from_slice(&(cols.len() as u32).to_be_bytes());
                for (cn, ct, cnn) in cols {
                    enc_str(&mut payload, cn);
                    payload.push(ty_code(*ct));
                    payload.push(*cnn as u8);
                }
            }
            WalRec::Drop { name } => {
                payload.push(REC_DROP);
                payload.extend_from_slice(&id.to_be_bytes());
                enc_str(&mut payload, name);
            }
            WalRec::AlterAdd { name, col } => {
                payload.push(REC_ALTER_ADD);
                payload.extend_from_slice(&id.to_be_bytes());
                enc_str(&mut payload, name);
                enc_str(&mut payload, &col.0);
                payload.push(ty_code(col.1));
                payload.push(col.2 as u8);
            }
        }
        let crc = crate::crc32c::crc32c(&payload);
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(&crc.to_be_bytes());
        out.extend_from_slice(&lsn.to_be_bytes());
        out.extend_from_slice(&payload);
    }
}

/// The LSN of a segment (the value every frame in it was encoded with).
/// None for a zero-byte / truncated-before-header object.
pub fn segment_lsn(data: &[u8]) -> Option<u64> {
    // [u32 len][u32 crc][u64 lsn]: need the 16-byte frame header present
    if data.len() < 16 {
        return None;
    }
    let mut a = [0u8; 8];
    a.copy_from_slice(&data[8..16]);
    Some(u64::from_be_bytes(a))
}

pub enum Replay {
    /// Decoded (table_id, record) items + whether a torn tail ended the scan.
    Ok { items: Vec<(u64, WalRec)>, torn: bool },
    /// Structurally impossible data (not merely a torn tail): refuse loudly.
    Corrupt(String),
}

fn u32_at(b: &[u8], p: usize) -> Option<u32> {
    if p + 4 > b.len() {
        return None;
    }
    Some(u32::from_be_bytes([b[p], b[p + 1], b[p + 2], b[p + 3]]))
}

fn u64_at(b: &[u8], p: usize) -> Option<u64> {
    if p + 8 > b.len() {
        return None;
    }
    let mut a = [0u8; 8];
    a.copy_from_slice(&b[p..p + 8]);
    Some(u64::from_be_bytes(a))
}

fn dec_str(b: &[u8], p: &mut usize) -> Option<String> {
    let n = u32_at(b, *p)? as usize;
    *p += 4;
    if *p + n > b.len() {
        return None;
    }
    let s = String::from_utf8_lossy(&b[*p..*p + n]).into_owned();
    *p += n;
    Some(s)
}

pub fn decode_segment(data: &[u8]) -> Replay {
    // the writer never emits empty segments (flush skips empty buffers), so
    // a 0-byte object is a fully-truncated write: torn, not valid-empty
    if data.is_empty() {
        return Replay::Ok { items: Vec::new(), torn: true };
    }
    let mut items = Vec::new();
    let mut p = 0usize;
    while p < data.len() {
        if p + 16 > data.len() {
            return Replay::Ok { items, torn: true }; // truncated header
        }
        let len = u32_at(data, p).unwrap() as usize;
        let crc = u32_at(data, p + 4).unwrap();
        if len > 64 * 1024 * 1024 {
            return Replay::Corrupt(format!("implausible frame length {}", len));
        }
        if p + 16 + len > data.len() {
            return Replay::Ok { items, torn: true }; // truncated payload
        }
        let payload = &data[p + 16..p + 16 + len];
        if crate::crc32c::crc32c(payload) != crc {
            return Replay::Ok { items, torn: true }; // torn write
        }
        p += 16 + len;
        if payload.len() < 9 {
            return Replay::Corrupt("short payload".into());
        }
        let kind = payload[0];
        let id = match u64_at(payload, 1) {
            Some(v) => v,
            None => return Replay::Corrupt("bad table id".into()),
        };
        let mut q = 9usize;
        let rec = match kind {
            REC_INSERT | REC_UPDATE => {
                let rowid = match u64_at(payload, q) {
                    Some(v) => v,
                    None => return Replay::Corrupt("bad rowid".into()),
                };
                q += 8;
                let n = match u32_at(payload, q) {
                    Some(v) => v as usize,
                    None => return Replay::Corrupt("bad cell count".into()),
                };
                q += 4;
                if n > 1024 {
                    return Replay::Corrupt(format!("implausible cell count {}", n));
                }
                let mut cells = Vec::with_capacity(n);
                for _ in 0..n {
                    match dec_value(payload, &mut q) {
                        Some(v) => cells.push(v),
                        None => return Replay::Corrupt("bad cell".into()),
                    }
                }
                if kind == REC_INSERT {
                    WalRec::Insert { rowid, cells }
                } else {
                    WalRec::Update { rowid, cells }
                }
            }
            REC_DELETE => {
                let rowid = match u64_at(payload, q) {
                    Some(v) => v,
                    None => return Replay::Corrupt("bad rowid".into()),
                };
                WalRec::Delete { rowid }
            }
            REC_CREATE => {
                let name = match dec_str(payload, &mut q) {
                    Some(v) => v,
                    None => return Replay::Corrupt("bad table name".into()),
                };
                let ncols = match u32_at(payload, q) {
                    Some(v) => v as usize,
                    None => return Replay::Corrupt("bad column count".into()),
                };
                q += 4;
                if ncols == 0 || ncols > 1024 {
                    return Replay::Corrupt(format!("implausible column count {}", ncols));
                }
                let mut cols = Vec::with_capacity(ncols);
                for _ in 0..ncols {
                    let cn = match dec_str(payload, &mut q) {
                        Some(v) => v,
                        None => return Replay::Corrupt("bad column name".into()),
                    };
                    let tc = match payload.get(q) {
                        Some(v) => *v,
                        None => return Replay::Corrupt("bad ty".into()),
                    };
                    q += 1;
                    let nn = payload.get(q).copied().unwrap_or(0) != 0;
                    q += 1;
                    let ty = match ty_of(tc) {
                        Some(t) => t,
                        None => return Replay::Corrupt(format!("bad ty code {}", tc)),
                    };
                    cols.push((cn, ty, nn));
                }
                WalRec::Create { name, cols }
            }
            REC_DROP => {
                let name = match dec_str(payload, &mut q) {
                    Some(v) => v,
                    None => return Replay::Corrupt("bad table name".into()),
                };
                WalRec::Drop { name }
            }
            REC_ALTER_ADD => {
                let name = match dec_str(payload, &mut q) {
                    Some(v) => v,
                    None => return Replay::Corrupt("bad table name".into()),
                };
                let cn = match dec_str(payload, &mut q) {
                    Some(v) => v,
                    None => return Replay::Corrupt("bad column name".into()),
                };
                let ty = match payload.get(q) {
                    Some(&tc) => match ty_of(tc) {
                        Some(t) => t,
                        None => return Replay::Corrupt(format!("bad ty code {}", tc)),
                    },
                    None => return Replay::Corrupt("missing ty code".into()),
                };
                q += 1;
                let nn = matches!(payload.get(q), Some(1));
                WalRec::AlterAdd { name, col: (cn, ty, nn) }
            }
            k => return Replay::Corrupt(format!("unknown record kind {}", k)),
        };
        items.push((id, rec));
    }
    Replay::Ok { items, torn: false }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_lsn_roundtrip() {
        let mut buf = Vec::new();
        encode_segment(&mut buf, &[(7, WalRec::Delete { rowid: 3 })], 42);
        assert_eq!(segment_lsn(&buf), Some(42));
        assert_eq!(segment_lsn(&[]), None);
        assert_eq!(segment_lsn(&[0u8; 10]), None);
        // matches what decode replays
        assert!(matches!(decode_segment(&buf), Replay::Ok { items, torn: false } if items.len() == 1));
    }

    #[test]
    fn roundtrip() {
        let items = vec![
            (
                7u64,
                WalRec::Create {
                    name: "lakes".into(),
                    cols: vec![
                        ("name".into(), Ty::Text, true),
                        ("depth".into(), Ty::Int, false),
                    ],
                },
            ),
            (
                7,
                WalRec::Insert {
                    rowid: 1,
                    cells: vec![Value::Text("Baikal".into()), Value::Int(1642)],
                },
            ),
            (
                7,
                WalRec::Update {
                    rowid: 1,
                    cells: vec![Value::Text("Baikal!".into()), Value::Null],
                },
            ),
            (7, WalRec::Delete { rowid: 2 }),
            (9, WalRec::Drop { name: "old".into() }),
        ];
        let mut buf = Vec::new();
        encode_segment(&mut buf, &items, 42);
        match decode_segment(&buf) {
            Replay::Ok { items: got, torn } => {
                assert!(!torn);
                assert_eq!(got, items);
            }
            Replay::Corrupt(m) => panic!("corrupt: {}", m),
        }
        // truncated tail
        match decode_segment(&buf[..buf.len() - 3]) {
            Replay::Ok { torn, .. } => assert!(torn),
            Replay::Corrupt(m) => panic!("corrupt: {}", m),
        }
        // bit flip mid-payload -> torn tail
        let mut bad = buf.clone();
        let mid = bad.len() / 2;
        bad[mid] ^= 0xff;
        match decode_segment(&bad) {
            Replay::Ok { torn, .. } => assert!(torn),
            Replay::Corrupt(_) => panic!("bit flip should read as torn tail"),
        }
        // structural garbage -> loud
        let mut evil = buf.clone();
        // inflate a frame length absurdly in the first frame
        evil[0] = 0xff;
        evil[1] = 0xff;
        evil[2] = 0xff;
        evil[3] = 0xff;
        match decode_segment(&evil) {
            Replay::Corrupt(_) => {}
            Replay::Ok { .. } => panic!("implausible length must be Corrupt"),
        }
    }
}
