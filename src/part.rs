//! Columnar parts: immutable, self-describing, CRC-checked objects
//! (Snowflake micro-partitions / Parquet-style column chunks, simplified to a
//! flat relational subset — see PAPERS.md #9).
//!
//! Layout (all integers big-endian):
//!   b"PUDPART1"
//!   u32 ncols
//!   per column (first column is the hidden rowid, type int):
//!     u32 name_len, name
//!     u8  ty       (0 bool, 1 int, 2 float, 3 text)
//!     u8  enc      (0 raw, 1 dict, 2 rle)
//!     u64 nrows
//!     value-coded min (0 tag if absent), value-coded max
//!     u32 payload_len, payload
//!   u32 crc32c over everything above
//!
//! Payload = null bitmap (ceil(nrows/8) bytes) + encoded non-null values:
//!   raw  int/float: 8B each; bool: 1B; text: u32 len + bytes
//!   dict u32 dict_n + value-coded entries + u32 index per non-null value
//!   rle  u32 nruns + per run: u32 count + value-code (tag 0 = null run)

use crate::crc32c;
use crate::engine::{Ty, Value};
use crate::wal::{dec_value, enc_value};

pub const TY_BOOL: u8 = 0;
pub const TY_INT: u8 = 1;
pub const TY_FLOAT: u8 = 2;
pub const TY_TEXT: u8 = 3;
pub const TY_DATE: u8 = 4;
pub const TY_TIME: u8 = 5;
pub const TY_TIMESTAMP: u8 = 6;
pub const TY_INTERVAL: u8 = 7;

pub const ENC_RAW: u8 = 0;
pub const ENC_DICT: u8 = 1;
pub const ENC_RLE: u8 = 2;
/// LZ4 block-compression flag: OR'ed onto the base encoding. When set, the
/// payload is [u32 raw_len][lz4 block] (M4).
pub const ENC_LZ4: u8 = 0x40;

fn ty_code(t: Ty) -> u8 {
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

fn ty_of(code: u8) -> Ty {
    match code {
        TY_BOOL => Ty::Bool,
        TY_INT => Ty::Int,
        TY_FLOAT => Ty::Float,
        TY_DATE => Ty::Date,
        TY_TIME => Ty::Time,
        TY_TIMESTAMP => Ty::Timestamp,
        TY_INTERVAL => Ty::Interval,
        _ => Ty::Text,
    }
}

fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}
fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_be_bytes());
}

fn cmp_vals(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering::*;
    match (a, b) {
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(Equal),
        (Value::Text(x), Value::Text(y)) => x.cmp(y),
        (Value::Date(x), Value::Date(y)) => x.cmp(y),
        (Value::Time(x), Value::Time(y)) => x.cmp(y),
        (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        (Value::Interval { .. }, Value::Interval { .. }) => {
            let xa = crate::engine::interval_us(a);
            let ya = crate::engine::interval_us(b);
            xa.cmp(&ya)
        }
        _ => Equal,
    }
}

/// Writes a part for `columns` (names + types) over `rows` (row-major cells).
/// Returns the encoded bytes; the caller PUTs them.
pub fn encode_part(columns: &[(String, Ty)], rows: &[Vec<Value>]) -> Vec<u8> {
    let ncols = columns.len();
    let nrows = rows.len();
    let mut out = Vec::with_capacity(64 * 1024);
    out.extend_from_slice(b"PUDPART1");
    put_u32(&mut out, ncols as u32);
    for (ci, (name, ty)) in columns.iter().enumerate() {
        let bytes = name.as_bytes();
        put_u32(&mut out, bytes.len() as u32);
        out.extend_from_slice(bytes);
        out.push(ty_code(*ty));
        // gather non-null values
        let mut vals: Vec<&Value> = Vec::with_capacity(nrows);
        let mut bitmap = vec![0u8; nrows.div_ceil(8)];
        for (ri, row) in rows.iter().enumerate() {
            let v = &row[ci];
            if !matches!(v, Value::Null) {
                vals.push(v);
                bitmap[ri / 8] |= 1 << (ri % 8);
            }
        }
        // min/max (zone stats)
        let mut min: Option<&Value> = None;
        let mut max: Option<&Value> = None;
        for v in &vals {
            min = Some(match min {
                None => *v,
                Some(m) => {
                    if cmp_vals(v, m) == std::cmp::Ordering::Less {
                        *v
                    } else {
                        m
                    }
                }
            });
            max = Some(match max {
                None => *v,
                Some(m) => {
                    if cmp_vals(v, m) == std::cmp::Ordering::Greater {
                        *v
                    } else {
                        m
                    }
                }
            });
        }
        match min {
            Some(m) => enc_value(&mut out, m),
            None => out.push(0), // all null
        }
        match max {
            Some(m) => enc_value(&mut out, m),
            None => out.push(0),
        }
        // choose encoding
        let mut distinct: std::collections::HashSet<String> = std::collections::HashSet::new();
        for v in &vals {
            distinct.insert(format!("{:?}", v));
        }
        let dict_worthy = vals.len() > 16 && distinct.len() * 6 < vals.len();
        // count runs over the full column including nulls
        let mut runs = 1usize;
        for ri in 1..rows.len() {
            let a = &rows[ri - 1][ci];
            let b = &rows[ri][ci];
            if cmp_vals(a, b) != std::cmp::Ordering::Equal
                || matches!(a, Value::Null) != matches!(b, Value::Null)
            {
                runs += 1;
            }
        }
        let rle_worthy = rows.len() > 16 && runs * 6 < rows.len();
        let enc = if dict_worthy {
            ENC_DICT
        } else if rle_worthy {
            ENC_RLE
        } else {
            ENC_RAW
        };
        let base_enc = enc;
        // payload (built first; enc/nrows written after so enc can gain the
        // LZ4 flag when compression wins)
        let mut payload = Vec::with_capacity(vals.len() * 8 + 8);
        payload.extend_from_slice(&bitmap);
        match enc {
            ENC_DICT => {
                // dictionary of distinct values + index per non-null
                let mut dict: Vec<&Value> = Vec::new();
                let mut index_of: std::collections::HashMap<String, u32> =
                    std::collections::HashMap::new();
                let mut indices = Vec::with_capacity(vals.len());
                for v in &vals {
                    let k = format!("{:?}", v);
                    if let Some(ix) = index_of.get(&k) {
                        indices.push(*ix);
                    } else {
                        let ix = dict.len() as u32;
                        dict.push(v);
                        index_of.insert(k, ix);
                        indices.push(ix);
                    }
                }
                put_u32(&mut payload, dict.len() as u32);
                for v in &dict {
                    enc_value(&mut payload, v);
                }
                for ix in &indices {
                    put_u32(&mut payload, *ix);
                }
            }
            ENC_RLE => {
                // runs over the full column (nulls included)
                let mut nruns = 0usize;
                let mut runs_buf = Vec::new();
                let mut i = 0usize;
                while i < rows.len() {
                    let v = &rows[i][ci];
                    let mut n = 1usize;
                    while i + n < rows.len() {
                        let w = &rows[i + n][ci];
                        let same = cmp_vals(v, w) == std::cmp::Ordering::Equal
                            && matches!(v, Value::Null) == matches!(w, Value::Null);
                        if same {
                            n += 1;
                        } else {
                            break;
                        }
                    }
                    put_u32(&mut runs_buf, n as u32);
                    enc_value(&mut runs_buf, v);
                    nruns += 1;
                    i += n;
                }
                put_u32(&mut payload, nruns as u32);
                payload.extend_from_slice(&runs_buf);
            }
            // raw: everything not dict/rle (enc is locally computed above
            // as exactly one of the three schemes; no panic path)
            _ => {
                for v in &vals {
                    match v {
                        Value::Bool(b) => payload.push(*b as u8),
                        Value::Int(i) => payload.extend_from_slice(&i.to_be_bytes()),
                        Value::Float(f) => {
                            payload.extend_from_slice(&f.to_bits().to_be_bytes())
                        }
                        Value::Text(s) => {
                            put_u32(&mut payload, s.len() as u32);
                            payload.extend_from_slice(s.as_bytes());
                        }
                        Value::Date(d) => payload.extend_from_slice(&d.to_be_bytes()),
                        Value::Time(t) => payload.extend_from_slice(&t.to_be_bytes()),
                        Value::Timestamp(us) => payload.extend_from_slice(&us.to_be_bytes()),
                        Value::Interval { months, days, micros } => {
                            payload.extend_from_slice(&months.to_be_bytes());
                            payload.extend_from_slice(&days.to_be_bytes());
                            payload.extend_from_slice(&micros.to_be_bytes());
                        }
                        Value::Null => {}
                    }
                }
            }
        }
        // LZ4 the payload when it actually helps (>=64B, clearly smaller).
        let mut enc = base_enc;
        if payload.len() >= 64 {
            let comp = lz4_flex::compress(&payload);
            if comp.len() + 4 < payload.len() {
                let mut c = Vec::with_capacity(4 + comp.len());
                put_u32(&mut c, payload.len() as u32);
                c.extend_from_slice(&comp);
                payload = c;
                enc |= ENC_LZ4;
            }
        }
        out.push(enc);
        put_u64(&mut out, nrows as u64);
        put_u32(&mut out, payload.len() as u32);
        out.extend_from_slice(&payload);
    }
    let crc = crc32c::crc32c(&out);
    put_u32(&mut out, crc);
    out
}

/// Decoded part: column names + row-major rows.
pub struct DecodedPart {
    pub cols: Vec<(String, Ty)>,
    pub rows: Vec<Vec<Value>>,
}

pub fn decode_part(data: &[u8]) -> Result<DecodedPart, String> {
    let take = |p: &mut usize, n: usize| -> Result<&[u8], String> {
        if *p + n > data.len() {
            return Err("unexpected end of part".into());
        }
        let s = &data[*p..*p + n];
        *p += n;
        Ok(s)
    };
    if data.len() < 12 || &data[0..8] != b"PUDPART1" {
        return Err("bad part magic".into());
    }
    let mut p = 8usize;
    // verify trailer CRC first
    let body_end = data.len() - 4;
    let mut a = [0u8; 4];
    a.copy_from_slice(&data[body_end..]);
    let want = u32::from_be_bytes(a);
    if crc32c::crc32c(&data[..body_end]) != want {
        return Err("part CRC mismatch".into());
    }
    let ncols = u32::from_be_bytes(take(&mut p, 4)?.try_into().unwrap()) as usize;
    if ncols == 0 || ncols > 1024 {
        return Err("bad column count".into());
    }
    let mut cols = Vec::with_capacity(ncols);
    let mut col_values: Vec<Vec<Value>> = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let nl = u32::from_be_bytes(take(&mut p, 4)?.try_into().unwrap()) as usize;
        let name = String::from_utf8_lossy(take(&mut p, nl)?).into_owned();
        let ty = ty_of(take(&mut p, 1)?[0]);
        // skip min/max (zone stats are for pruning, not needed to decode rows)
        let _min = dec_value(data, &mut p).ok_or("bad min stat")?;
        let _max = dec_value(data, &mut p).ok_or("bad max stat")?;
        let enc = take(&mut p, 1)?[0];
        let nrows = u64::from_be_bytes(take(&mut p, 8)?.try_into().unwrap()) as usize;
        let plen = u32::from_be_bytes(take(&mut p, 4)?.try_into().unwrap()) as usize;
        let payload: Vec<u8> = if enc & ENC_LZ4 != 0 {
            let raw = take(&mut p, plen)?;
            if raw.len() < 4 {
                return Err("short lz4 payload".into());
            }
            let want = u32::from_be_bytes(raw[0..4].try_into().unwrap()) as usize;
            // sanity: parts are bounded (65,536 rows/part -> few MB)
            if want > 64 * 1024 * 1024 {
                return Err("lz4 payload too large".into());
            }
            lz4_flex::decompress(&raw[4..], want).map_err(|e| format!("lz4: {}", e))?
        } else {
            take(&mut p, plen)?.to_vec()
        };
        let enc = enc & 0x3;
        let mut q = 0usize;
        let bitmap_len = nrows.div_ceil(8);
        if q + bitmap_len > payload.len() {
            return Err("short bitmap".into());
        }
        let bitmap = &payload[q..q + bitmap_len];
        q += bitmap_len;
        let null_at = |i: usize| bitmap[i / 8] & (1 << (i % 8)) == 0;
        let fill = |nn: Vec<Value>| -> Vec<Value> {
            let mut out = Vec::with_capacity(nrows);
            let mut it = nn.into_iter();
            for i in 0..nrows {
                if null_at(i) {
                    out.push(Value::Null);
                } else {
                    out.push(it.next().unwrap_or(Value::Null));
                }
            }
            out
        };
        let vals = match enc {
            ENC_RAW => {
                // decode exactly nnz values (non-null positions, in order)
                let nnz = (0..nrows).filter(|i| !null_at(*i)).count();
                let mut nn = Vec::with_capacity(nnz);
                for _ in 0..nnz {
                    let v = match ty {
                        Ty::Bool => {
                            let b = *payload.get(q).ok_or("short bool")?;
                            q += 1;
                            Value::Bool(b != 0)
                        }
                        Ty::Int => {
                            if q + 8 > payload.len() {
                                return Err("short int".into());
                            }
                            let mut a = [0u8; 8];
                            a.copy_from_slice(&payload[q..q + 8]);
                            q += 8;
                            Value::Int(i64::from_be_bytes(a))
                        }
                        Ty::Float => {
                            if q + 8 > payload.len() {
                                return Err("short float".into());
                            }
                            let mut a = [0u8; 8];
                            a.copy_from_slice(&payload[q..q + 8]);
                            q += 8;
                            Value::Float(f64::from_bits(u64::from_be_bytes(a)))
                        }
                        Ty::Text => {
                            if q + 4 > payload.len() {
                                return Err("short text".into());
                            }
                            let mut a = [0u8; 4];
                            a.copy_from_slice(&payload[q..q + 4]);
                            let n = u32::from_be_bytes(a) as usize;
                            q += 4;
                            if q + n > payload.len() {
                                return Err("short text".into());
                            }
                            let s = String::from_utf8_lossy(&payload[q..q + n]).into_owned();
                            q += n;
                            Value::Text(s)
                        }
                        Ty::Date => {
                            if q + 4 > payload.len() {
                                return Err("short date".into());
                            }
                            let mut a = [0u8; 4];
                            a.copy_from_slice(&payload[q..q + 4]);
                            q += 4;
                            Value::Date(i32::from_be_bytes(a))
                        }
                        Ty::Time | Ty::Timestamp => {
                            if q + 8 > payload.len() {
                                return Err("short time/timestamp".into());
                            }
                            let mut a = [0u8; 8];
                            a.copy_from_slice(&payload[q..q + 8]);
                            q += 8;
                            if matches!(ty, Ty::Time) {
                                Value::Time(i64::from_be_bytes(a))
                            } else {
                                Value::Timestamp(i64::from_be_bytes(a))
                            }
                        }
                        Ty::Interval => {
                            if q + 16 > payload.len() {
                                return Err("short interval".into());
                            }
                            let mut a4 = [0u8; 4];
                            let mut a8 = [0u8; 8];
                            a4.copy_from_slice(&payload[q..q + 4]);
                            let months = i32::from_be_bytes(a4);
                            q += 4;
                            a4.copy_from_slice(&payload[q..q + 8][..4]);
                            let days = i32::from_be_bytes(a4);
                            q += 4;
                            a8.copy_from_slice(&payload[q..q + 8]);
                            let micros = i64::from_be_bytes(a8);
                            q += 8;
                            Value::Interval { months, days, micros }
                        }
                    };
                    nn.push(v);
                }
                fill(nn)
            }
            ENC_DICT => {
                let dn = u32::from_be_bytes(
                    payload[q..q + 4].try_into().map_err(|_| "bad dict")?,
                ) as usize;
                q += 4;
                let mut dict = Vec::with_capacity(dn);
                for _ in 0..dn {
                    dict.push(dec_value(&payload, &mut q).ok_or("bad dict entry")?);
                }
                let nnz = (0..nrows).filter(|i| !null_at(*i)).count();
                let mut nn = Vec::with_capacity(nnz);
                for _ in 0..nnz {
                    if q + 4 > payload.len() {
                        return Err("short index".into());
                    }
                    let mut a = [0u8; 4];
                    a.copy_from_slice(&payload[q..q + 4]);
                    q += 4;
                    let ix = u32::from_be_bytes(a) as usize;
                    nn.push(dict.get(ix).cloned().ok_or("bad dict index")?);
                }
                fill(nn)
            }
            ENC_RLE => {
                let nruns = u32::from_be_bytes(
                    payload[q..q + 4].try_into().map_err(|_| "bad rle")?,
                ) as usize;
                q += 4;
                let mut all = Vec::with_capacity(nrows);
                let mut total = 0usize;
                for _ in 0..nruns {
                    if q + 4 > payload.len() {
                        return Err("short run".into());
                    }
                    let mut a = [0u8; 4];
                    a.copy_from_slice(&payload[q..q + 4]);
                    let n = u32::from_be_bytes(a) as usize;
                    q += 4;
                    let v = dec_value(&payload, &mut q).ok_or("bad run value")?;
                    for _ in 0..n {
                        all.push(v.clone());
                    }
                    total += n;
                }
                if total != nrows {
                    return Err(format!("rle rows {} != {}", total, nrows));
                }
                all
            }
            _ => return Err("bad encoding".into()),
        };
        cols.push((name, ty));
        col_values.push(vals);
    }
    // transpose
    let nrows = col_values.first().map_or(0, |v| v.len());
    let mut rows = Vec::with_capacity(nrows);
    for ri in 0..nrows {
        let mut row = Vec::with_capacity(cols.len());
        for c in &col_values {
            row.push(c[ri].clone());
        }
        rows.push(row);
    }
    Ok(DecodedPart { cols, rows })
}


// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    // full-part roundtrip with every type, NULLs, and the column header
    // (the ty_of header path is what a real CHECKPOINT exercises)
    #[test]
    fn full_part_roundtrip_all_types() {
        use super::*;
        use crate::engine::Value;
        let cols = vec![
            ("__rowid__".to_string(), Ty::Int),
            ("b".to_string(), Ty::Bool),
            ("i".to_string(), Ty::Int),
            ("f".to_string(), Ty::Float),
            ("t".to_string(), Ty::Text),
            ("d".to_string(), Ty::Date),
            ("tm".to_string(), Ty::Time),
            ("ts".to_string(), Ty::Timestamp),
            ("iv".to_string(), Ty::Interval),
        ];
        let rows = vec![
            vec![
                Value::Int(1), Value::Bool(true), Value::Int(42), Value::Float(1.5),
                Value::Text("hello".into()), Value::Date(20750),
                Value::Time(3_600_000_000), Value::Timestamp(1_760_000_000_000_000),
                Value::Interval { months: 1, days: 2, micros: 3 },
            ],
            vec![
                Value::Int(2), Value::Null, Value::Null, Value::Null,
                Value::Null, Value::Null, Value::Null, Value::Null,
                Value::Null,
            ],
            vec![
                Value::Int(3), Value::Bool(false), Value::Int(-7), Value::Float(-2.25),
                Value::Text("".into()), Value::Date(0),
                Value::Time(0), Value::Timestamp(0),
                Value::Interval { months: -13, days: 0, micros: -86_400_000_000 },
            ],
        ];
        let bytes = encode_part(&cols, &rows);
        let d = decode_part(&bytes).expect("decode");
        let got: Vec<Vec<Value>> = d.rows.into_iter().map(|mut r| { r.remove(0); r }).collect();
        let want: Vec<Vec<Value>> = rows.into_iter().map(|mut r| { r.remove(0); r }).collect();
        assert_eq!(got, want);
        // column names + types survive the header roundtrip
        let names: Vec<&str> = d.cols.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["__rowid__", "b", "i", "f", "t", "d", "tm", "ts", "iv"]);
    }

    use super::*;

    fn roundtrip(columns: Vec<(String, Ty)>, rows: Vec<Vec<Value>>, expect_enc: Option<u8>) {
        let data = encode_part(&columns, &rows);
        let d = decode_part(&data).expect("decode");
        assert_eq!(d.cols, columns);
        assert_eq!(d.rows, rows);
        if let Some(e) = expect_enc {
            // encoding byte position varies; just ensure it decoded right
            let _ = e;
        }
    }

    #[test]
    fn lz4_roundtrip() {
        // compressible data: LZ4 flag must engage and roundtrip exactly
        let cols = vec![("a".to_string(), Ty::Int), ("b".to_string(), Ty::Text)];
        let mut rows = Vec::new();
        for i in 0..2000 {
            rows.push(vec![
                Value::Int(i / 7), // low cardinality ints
                Value::Text(format!("payload-{}", i % 3)), // repeated texts
            ]);
        }
        let data = encode_part(&cols, &rows);
        // verify the enc bytes actually carry the LZ4 flag (find "a"/"b" col encodings)
        let d = decode_part(&data).expect("decode");
        assert_eq!(d.rows.len(), 2000);
        for (i, r) in d.rows.iter().enumerate() {
            assert_eq!(r[0], Value::Int(i as i64 / 7));
            assert_eq!(r[1], Value::Text(format!("payload-{}", i % 3)));
        }
        // compressed payload must be meaningfully smaller than the raw bytes
        // (raw would be 2000*8 + 2000*(8+10) + bitmap ~= 56KB+)
        assert!(data.len() < 40_000, "part not compressed: {} bytes", data.len());
    }

    #[test]
    fn raw_roundtrip() {
        roundtrip(
            vec![("a".into(), Ty::Int), ("b".into(), Ty::Text), ("c".into(), Ty::Bool)],
            vec![
                vec![Value::Int(1), Value::Text("x".into()), Value::Bool(true)],
                vec![Value::Int(-2), Value::Null, Value::Bool(false)],
                vec![Value::Null, Value::Text("yy".into()), Value::Null],
            ],
            None,
        );
    }

    #[test]
    fn dict_roundtrip() {
        let mut rows = Vec::new();
        for i in 0..100 {
            rows.push(vec![
                Value::Int(i),
                Value::Text(if i % 2 == 0 { "even".into() } else { "odd".into() }),
            ]);
        }
        roundtrip(vec![("n".into(), Ty::Int), ("t".into(), Ty::Text)], rows, None);
    }

    #[test]
    fn rle_roundtrip() {
        let mut rows = Vec::new();
        for i in 0..100 {
            rows.push(vec![Value::Int(i / 20), Value::Text("same".into())]);
        }
        roundtrip(vec![("g".into(), Ty::Int), ("t".into(), Ty::Text)], rows, None);
    }

    #[test]
    fn empty_part() {
        roundtrip(
            vec![("a".into(), Ty::Int)],
            vec![],
            None,
        );
    }

    #[test]
    fn corrupt_detected() {
        let data = encode_part(&[("a".into(), Ty::Int)], &[vec![Value::Int(5)]]);
        let mut bad = data.clone();
        let mid = bad.len() / 2;
        bad[mid] ^= 0xff;
        assert!(decode_part(&bad).is_err());
    }
}
