//! Persistence: bootstrap/recovery, the meta.json registry (CAS), WAL
//! segment logging, and CHECKPOINT (part materialization + manifest CAS).
//!
//! Bucket layout (all under the store prefix):
//!   meta.json                     {format, next_id, tables:{name:id}}
//!   wal/<seq:016>-<uuid>.wal      global segment stream (the truth layer)
//!   t/<id>/manifest/<n>.json      versioned table manifests (CAS commit)
//!   t/<id>/part/<uuid>.part       immutable columnar parts
//!
//! Recovery = load manifests (+parts) for registered ids, replay ALL segments
//! in seq order (idempotently), then self-heal meta.json. See ARCHITECTURE.md.

use crate::engine::{Table, Ty, Value};
use crate::part::{decode_part, encode_part};
use crate::sql::ColumnDef;
use crate::store::{ObjectStore, StoreError};
use crate::wal::{decode_segment, encode_segment, Replay, WalRec};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AckLevel {
    /// Buffer records; a background flusher PUTs a segment every 500 ms.
    /// A crash may lose the tail.
    Relaxed,
    /// PUT the whole segment before acknowledging the statement (default).
    Committed,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Meta {
    pub format: u32,
    #[serde(default)]
    pub next_id: u64,
    pub tables: HashMap<String, u64>,
    /// named snapshots (v2.0): LSN pointer + the pinned manifest version of
    /// every table that existed at snapshot time
    #[serde(default)]
    pub snapshots: HashMap<String, SnapshotDef>,
    /// secondary index definitions (M5); derived structures are rebuilt at
    /// recovery from table data, keyed by table incarnation id
    #[serde(default)]
    pub indexes: HashMap<String, IndexDef>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SnapPin {
    /// table incarnation id at snapshot time (survives DROP + recreate)
    pub id: u64,
    /// manifest version pinned (0 = table pre-first-checkpoint, WAL-only)
    pub manifest_n: u64,
    /// the pinned manifest's coverage watermark: it already contains every
    /// txn with lsn below this (0 = unknown/pre-2.0 manifest)
    pub manifest_lsn: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SnapshotDef {
    /// the last txn INCLUDED in the snapshot: its state is every txn with
    /// lsn <= this. AS OF LSN <this> == AS OF SNAPSHOT.
    pub lsn: u64,
    /// wall clock, micros since epoch
    pub created_us: i64,
    pub pins: HashMap<String, SnapPin>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct IndexDef {
    pub table: String,
    pub table_id: u64,
    pub method: String,
    pub col: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ManCol {
    pub name: String,
    pub ty: u8,
    #[serde(default)]
    pub not_null: bool,
}

/// Checkpoint granularity: rows per part (Snowflake-style micro-partitions).
pub const PART_ROWS: usize = 65_536;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ManPart {
    pub key: String,
    pub rows: u64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Manifest {
    pub format: u32,
    pub n: u64,
    pub table: String,
    pub columns: Vec<ManCol>,
    pub parts: Vec<ManPart>,
    pub next_rowid: u64,
    /// coverage watermark: this manifest contains every txn with a lower
    /// lsn. 0 on manifests written before v2.0 (treated as unknown).
    #[serde(default)]
    pub lsn: u64,
}

/// Recovery working set: row-major with a rowid index (O(1) replay), with
/// tombstoned rows simply absent from `map`. Converted to a columnar Table
/// once after the whole stream is replayed.
struct ReplayTab {
    cols: Vec<ColumnDef>,
    rows: Vec<(u64, Vec<Value>)>,
    map: HashMap<u64, usize>,
    next_rowid_seed: u64,
}

impl ReplayTab {
    fn fresh(cols: Vec<(String, Ty, bool)>) -> ReplayTab {
        ReplayTab {
            cols: cols
                .into_iter()
                .map(|(n, t, nn)| ColumnDef { name: n, ty: t, not_null: nn })
                .collect(),
            rows: Vec::new(),
            map: HashMap::new(),
            next_rowid_seed: 1,
        }
    }
    fn into_table(self) -> Table {
        let seed = self.next_rowid_seed;
        let mut t = Table::empty(self.cols, seed);
        for (id, cells) in self.rows {
            if self.map.contains_key(&id) {
                t.push_row(id, cells);
            }
        }
        t
    }
}

struct TState {
    manifest_n: u64,
    manifest_etag: String,
    /// part keys referenced by the manifest we loaded/wrote last
    live_parts: Vec<String>,
    /// the current manifest's coverage watermark (see Manifest::lsn)
    manifest_lsn: u64,
}

struct PState {
    meta: Meta,
    meta_etag: String,
    next_seq: u64,
    lsn: u64,
    /// highest lsn whose segment is durable on the store (0 = none)
    flushed_lsn: u64,
    wal_buf: Vec<u8>,
    tables: HashMap<u64, TState>,
}

pub struct Persist {
    store: Box<dyn ObjectStore + Send + Sync>,
    ack: AckLevel,
    st: Mutex<PState>,
}

/// The highest durable LSN, published for read-only contexts (the scalar
/// current_lsn() has no Db handle). 0 = no WAL (pure-RAM mode).
static CURRENT_LSN: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub fn published_lsn() -> u64 {
    CURRENT_LSN.load(std::sync::atomic::Ordering::Relaxed)
}

fn publish_lsn(v: u64) {
    CURRENT_LSN.fetch_max(v, std::sync::atomic::Ordering::Relaxed);
}

fn uuid() -> String {
    // good enough for object-name uniqueness (not a security boundary)
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut seed = nanos as u64 ^ (std::process::id() as u64) << 32;
    let mut out = String::new();
    for _ in 0..16 {
        // xorshift64*
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        out.push_str(&format!("{:02x}", (seed >> 56) as u8));
    }
    out
}

impl Persist {
    // ------------------------------------------------------------- opening --

    /// Boot: recover tables from the store, replay the WAL, heal meta.
    pub fn open(
        store: Box<dyn ObjectStore + Send + Sync>,
        ack: AckLevel,
    ) -> Result<(Persist, HashMap<String, Table>), String> {
        let log = |msg: String| eprintln!("* recover: {}", msg);
        // ---- meta.json
        let (meta, meta_etag) = match store.get("meta.json") {
            Ok(bytes) => {
                let m: Meta = serde_json::from_slice(&bytes)
                    .map_err(|e| format!("meta.json parse: {}", e))?;
                let etag = store.head("meta.json").map(|(e, _)| e).unwrap_or_default();
                (m, etag)
            }
            Err(StoreError::NotFound(_)) => {
                // A fresh start is only legal when nothing else exists: WAL
                // segments or table objects without a catalog mean the
                // catalog itself was lost — refuse rather than silently
                // come up empty (silent data loss is never acceptable).
                let stray_wal = store.list("wal/").map_err(|e| e.to_string())?;
                let stray_tbl = store.list("t/").map_err(|e| e.to_string())?;
                if !stray_wal.is_empty() || !stray_tbl.is_empty() {
                    return Err(
                        "meta.json is missing but wal/ or t/ objects exist; refusing to start                          (catalog lost: possible corruption or accidental deletion)"
                            .into(),
                    );
                }
                let m = Meta { format: 1, next_id: 1, tables: HashMap::new(), indexes: HashMap::new(), snapshots: HashMap::new() };
                let bytes = serde_json::to_vec(&m).map_err(|e| e.to_string())?;
                store.put("meta.json", &bytes).map_err(|e| e.to_string())?;
                let etag = store.head("meta.json").map(|(e, _)| e).unwrap_or_default();
                (m, etag)
            }
            Err(e) => return Err(format!("meta.json: {}", e)),
        };
        if meta.format != 1 {
            return Err(format!("unsupported meta format {}", meta.format));
        }
        let next_id_floor = meta
            .tables
            .values()
            .copied()
            .fold(meta.next_id, |acc, v| acc.max(v + 1));

        // ---- manifests + parts (only ids registered in meta)
        let mut by_id: HashMap<u64, ReplayTab> = HashMap::new();
        let mut name_of: HashMap<u64, String> = HashMap::new();
        let mut max_rowid_seen: HashMap<u64, u64> = HashMap::new();
        let mut tstates: HashMap<u64, TState> = HashMap::new();
        let mut ids: Vec<(String, u64)> = meta
            .tables
            .iter()
            .map(|(n, i)| (n.clone(), *i))
            .collect();
        ids.sort_by_key(|(_, i)| *i);
        for (name, id) in &ids {
            let mdir = format!("t/{}/manifest/", id);
            let manifests = store.list(&mdir).map_err(|e| e.to_string())?;
            let mut best: Option<(u64, Manifest)> = None;
            for key in manifests {
                let n = key
                    .trim_start_matches(&mdir)
                    .trim_end_matches(".json")
                    .parse::<u64>()
                    .ok()
                    .filter(|&v| (1..=u64::MAX / 2).contains(&v));
                let n = match n {
                    Some(v) => v,
                    None => continue, // stray object; ignore
                };
                if best.as_ref().is_some_and(|(bn, _)| n >= *bn) {
                    continue; // already have a newer one
                }
                let bytes = store
                    .get(&key)
                    .map_err(|e| format!("manifest {}: {}", key, e))?;
                let man: Manifest = serde_json::from_slice(&bytes)
                    .map_err(|e| format!("manifest {} parse: {}", key, e))?;
                if man.format != 1 {
                    return Err(format!("manifest {} format {}", key, man.format));
                }
                if man.table != *name {
                    log(format!(
                        "ignoring manifest {} (table {:?}, registered as {:?})",
                        key, man.table, name
                    ));
                    continue;
                }
                best = Some((n, man));
            }
            let (n, man) = match best {
                Some(v) => v,
                None => {
                    // registered but no manifest: pre-first-checkpoint table;
                    // its truth must be in the WAL (Create records)
                    continue;
                }
            };
            let mut cols: Vec<ColumnDef> = Vec::with_capacity(man.columns.len());
            for c in &man.columns {
                let ty = crate::wal::ty_of(c.ty)
                    .ok_or_else(|| format!("manifest: bad ty code {}", c.ty))?;
                cols.push(ColumnDef {
                    name: c.name.clone(),
                    ty,
                    not_null: c.not_null,
                });
            }
            let mut tab = ReplayTab { cols, rows: Vec::new(), map: HashMap::new(), next_rowid_seed: man.next_rowid };
            for p in &man.parts {
                let bytes = store
                    .get(&p.key)
                    .map_err(|e| format!("part {}: {}", p.key, e))?;
                let d = decode_part(&bytes).map_err(|e| format!("part {}: {}", p.key, e))?;
                if d.cols.first().is_none_or(|(n, _)| n != "__rowid__") {
                    return Err(format!("part {}: first column must be __rowid__", p.key));
                }
                let want_width = man.columns.len();
                for mut row in d.rows {
                    let rid = match row.remove(0) {
                        Value::Int(i) if i >= 0 => i as u64,
                        _ => return Err(format!("part {}: bad rowid value", p.key)),
                    };
                    // parts written before an ALTER TABLE ADD COLUMN lack the
                    // trailing column(s): pad with NULL (schema evolution)
                    while row.len() < want_width {
                        row.push(Value::Null);
                    }
                    tab.map.insert(rid, tab.rows.len());
                    tab.rows.push((rid, row));
                }
            }
            log(format!(
                "table {:?} (id {}) manifest n={} parts={} rows={}",
                name,
                id,
                n,
                man.parts.len(),
                tab.rows.len()
            ));
            by_id.insert(*id, tab);
            name_of.insert(*id, name.clone());
            let man_lsn = man.lsn;
            tstates.insert(
                *id,
                TState {
                    manifest_n: n,
                    manifest_etag: store
                        .head(&format!("t/{}/manifest/{}.json", id, n))
                        .map(|(e, _)| e)
                        .unwrap_or_default(),
                    live_parts: man.parts.iter().map(|p| p.key.clone()).collect(),
                    manifest_lsn: man_lsn,
                },
            );
        }

        // ---- replay the global WAL stream in seq order
        let wal_keys = store.list("wal/").map_err(|e| e.to_string())?;
        let mut segs: Vec<(u64, String)> = Vec::new();
        for k in wal_keys {
            match wal_seq_of(&k) {
                Some(seq) => segs.push((seq, k)),
                None => log(format!("ignoring stray wal object {}", k)),
            }
        }
        segs.sort();
        // Two segments with the same seq can only come from two puddle
        // processes writing one prefix concurrently (single-process seq
        // allocation is lock-serialized). Deduping silently would DROP one
        // process's transactions on replay — refuse loudly instead.
        for w in segs.windows(2) {
            if w[0].0 == w[1].0 {
                return Err(format!(
                    "WAL seq collision at {} ({} and {}): two writers used \
                     this prefix concurrently; refusing to pick silently",
                    w[0].0,
                    w[0].1,
                    w[1].1
                ));
            }
        }
        segs.dedup_by_key(|(s, _)| *s);
        // Only ids registered in meta.json are replayed: a segment for an
        // unknown id belongs to an unacknowledged CREATE that crashed before
        // its meta CAS — rolling it back is allowed (it was never acked).
        let known_ids: std::collections::HashSet<u64> = meta.tables.values().copied().collect();
        let n_segs = segs.len();
        for (i, (seq, key)) in segs.into_iter().enumerate() {
            let data = store.get(&key).map_err(|e| format!("wal {}: {}", key, e))?;
            let (mut items, torn) = match decode_segment(&data) {
                Replay::Ok { items, torn } => (items, torn),
                Replay::Corrupt(m) => {
                    return Err(format!("wal {}: CORRUPT ({}); refusing to start", key, m))
                }
            };
            let _ = &mut items;
            if torn && i + 1 != n_segs {
                return Err(format!(
                    "wal {}: torn tail in a non-latest segment ({} of {}); refusing to start",
                    key,
                    i + 1,
                    n_segs
                ));
            }
            if torn {
                // One segment = one transaction (Committed ack flushes each
                // txn to its own segment; a Relaxed batch is never acked).
                // A torn / short read means the segment write never finished,
                // so the transaction was never acked: discard the ENTIRE
                // segment. Replaying the complete-frame prefix would expose
                // half a transaction (torn inserts).
                items.clear();
                log(format!(
                    "{}: torn tail, discarding whole segment (unacked txn)",
                    key
                ));
            }
            for (id, rec) in items {
                if known_ids.contains(&id) {
                    apply_rec(&mut by_id, &mut name_of, &mut max_rowid_seen, id, rec, seq);
                }
            }
        }

        // ---- anti-vanish: a registered table that materialized neither from
        // a manifest nor from WAL Create records may only be a crash-window
        // create that was never acked (no parts on disk yet). If parts DO
        // exist for it, the manifest was lost/deleted — refuse instead of
        // silently dropping the table and its data.
        for (name, id) in &meta.tables {
            if by_id.contains_key(id) {
                continue;
            }
            let orphans = store
                .list(&format!("t/{}/part/", id))
                .map_err(|e| e.to_string())?;
            if !orphans.is_empty() {
                return Err(format!(
                    "table {:?} (id {}) is registered but has no manifest and no WAL Create \
                     record, yet {} part object(s) exist; refusing to start (manifest lost)",
                    name, id, orphans.len()
                ));
            }
        }

        // ---- heal meta.json against the replayed truth
        let mut healed = meta.clone();
        healed.next_id = next_id_floor;
        healed.tables.retain(|n, i| by_id.contains_key(i) && name_of.get(i) == Some(n));
        // drop index defs whose table vanished or whose table was recreated
        // (different incarnation id) or whose column no longer exists
        healed.indexes.retain(|_, d| {
            meta.tables.get(&d.table) == Some(&d.table_id)
                && by_id
                    .get(&d.table_id)
                    .is_some_and(|rt| rt.cols.iter().any(|c| c.name == d.col))
        });
        if healed.tables != meta.tables || healed.next_id != meta.next_id {
            let bytes = serde_json::to_vec(&healed).map_err(|e| e.to_string())?;
            store
                .put_if_match("meta.json", &bytes, &meta_etag)
                .map_err(|e| format!("meta heal: {}", e))?;
            log("healed meta.json".to_string());
        }

        // ---- convert to columnar tables + next_rowid watermarks
        let mut tables: HashMap<String, Table> = HashMap::new();
        for (id, rt) in by_id {
            let name = match name_of.get(&id) {
                Some(n) => n.clone(),
                None => continue,
            };
            let mut t = rt.into_table();
            let live_max = t.rowids.iter().copied().max().unwrap_or(0);
            let wm = max_rowid_seen.get(&id).copied().unwrap_or(0);
            t.next_rowid = t.next_rowid.max(live_max + 1).max(wm);
            tables.insert(name, t);
        }

        // ---- rebuild secondary indexes from meta defs (derived structures)
        let mut built: Vec<String> = Vec::new();
        for (name, def) in healed.indexes.clone() {
            let tab = match tables.get_mut(&def.table) {
                Some(t) => t,
                None => continue,
            };
            let colpos = match tab.cols.iter().position(|c| c.name == def.col) {
                Some(p) => p,
                None => continue,
            };
            let method = match def.method.as_str() {
                "hash" => crate::sql::IdxMethod::Hash,
                _ => crate::sql::IdxMethod::BTree,
            };
            let idx = crate::engine::TableIndex::build(tab, colpos, method);
            tab.indexes.insert(name.clone(), idx);
            built.push(name);
        }
        if !built.is_empty() {
            log(format!("rebuilt {} secondary indexes", built.len()));
        }

        let next_seq = store
            .list("wal/")
            .map_err(|e| e.to_string())?
            .iter()
            .filter_map(|k| wal_seq_of(k))
            .max()
            .unwrap_or(0)
            + 1;
        // durable LSN watermark: LSNs must never be reused across restarts
        // (snapshots address state by LSN). Everything on the store is
        // flushed by definition.
        let flushed_lsn = {
            let mut mx = 0u64;
            for k in store.list("wal/").map_err(|e| e.to_string())? {
                if wal_seq_of(&k).is_some() {
                    if let Ok(data) = store.get(&k) {
                        if let Some(l) = crate::wal::segment_lsn(&data) {
                            mx = mx.max(l);
                        }
                    }
                }
            }
            mx
        };
        // best-effort cleanup: .tmp leftovers are killed in-flight writes
        // (their rename never completed, so the txn was never acked)
        for k in store.list("wal/").unwrap_or_default() {
            if k.ends_with(".tmp") {
                let _ = store.delete(&k);
            }
        }
        publish_lsn(flushed_lsn);
        Ok((
            Persist {
                store,
                ack,
                st: Mutex::new(PState {
                    meta: healed,
                    meta_etag,
                    next_seq,
                    lsn: flushed_lsn + 1,
                    flushed_lsn,
                    wal_buf: Vec::new(),
                    tables: tstates,
                }),
            },
            tables,
        ))
    }

    // ------------------------------------------------------------ writing --

    /// Logs one transaction (or autocommit statement): all records go into a
    /// single segment, so the batch is atomic. Committed mode PUTs the
    /// segment before returning; relaxed mode buffers.
    pub fn log_txn(&self, entries: &[(String, Vec<WalRec>)]) -> Result<(), String> {
        let mut st = self.st.lock().unwrap_or_else(|e| e.into_inner());
        let mut items: Vec<(u64, WalRec)> = Vec::new();
        for (name, recs) in entries {
            let id = *st
                .meta
                .tables
                .get(name)
                .ok_or_else(|| format!("table {:?} not registered", name))?;
            for r in recs {
                items.push((id, r.clone()));
            }
        }
        if items.is_empty() {
            return Ok(());
        }
        let lsn = st.lsn;
        st.lsn += 1;
        let buf_len = st.wal_buf.len();
        encode_segment(&mut st.wal_buf, &items, lsn);
        if self.ack == AckLevel::Committed {
            if let Err(e) = self.flush_locked(&mut st) {
                // the statement will fail and its in-RAM effects never happen;
                // drop its records so a later flush can't resurrect them
                st.wal_buf.truncate(buf_len);
                return Err(e);
            }
            st.flushed_lsn = st.flushed_lsn.max(lsn);
            publish_lsn(st.flushed_lsn);
        }
        Ok(())
    }

    fn flush_locked(&self, st: &mut PState) -> Result<(), String> {
        if st.wal_buf.is_empty() {
            return Ok(());
        }
        let key = format!("wal/{:016}-{}.wal", st.next_seq, uuid());
        self.store
            .put(&key, &st.wal_buf)
            .map_err(|e| format!("wal flush: {}", e))?;
        st.next_seq += 1;
        st.wal_buf.clear();
        // the whole buffer (every assigned-but-unflushed lsn) is now durable
        st.flushed_lsn = st.flushed_lsn.max(st.lsn.saturating_sub(1));
        publish_lsn(st.flushed_lsn);
        Ok(())
    }

    /// Flushes any buffered records (relaxed flusher tick / shutdown).
    pub fn flush_all(&self) -> Result<(), String> {
        let mut st = self.st.lock().unwrap_or_else(|e| e.into_inner());
        self.flush_locked(&mut st)
    }

    /// Registers a new table in meta.json (CAS). Must be called before the
    /// table's first WAL record is logged. Reuses the id if the name is
    /// already registered (crash-window recreate).
    pub fn register(&self, name: &str, cols: &[(String, Ty, bool)]) -> Result<u64, String> {
        let mut st = self.st.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(id) = st.meta.tables.get(name) {
            return Ok(*id);
        }
        let _ = cols; // schema travels in the WAL Create record
        let id = st.meta.next_id;
        st.meta.next_id += 1;
        st.meta.tables.insert(name.to_string(), id);
        let bytes = serde_json::to_vec(&st.meta).map_err(|e| e.to_string())?;
        match self.store.put_if_match("meta.json", &bytes, &st.meta_etag) {
            Ok(true) => {}
            Ok(false) => {
                return Err("meta.json CAS failed (concurrent writer?)".into());
            }
            Err(e) => return Err(format!("meta.json put: {}", e)),
        }
        st.meta_etag = self.store.head("meta.json").map(|(e, _)| e).unwrap_or_default();
        Ok(id)
    }

    /// Current registered id of a table (0 if unknown).
    pub fn table_id(&self, name: &str) -> Result<u64, String> {
        let st = self.st.lock().unwrap_or_else(|e| e.into_inner());
        Ok(st.meta.tables.get(name).copied().unwrap_or(0))
    }

    /// Registers a secondary index definition in meta.json (CAS).
    pub fn register_index(&self, name: &str, def: &IndexDef) -> Result<(), String> {
        let mut st = self.st.lock().unwrap_or_else(|e| e.into_inner());
        st.meta.indexes.insert(name.to_string(), def.clone());
        let bytes = serde_json::to_vec(&st.meta).map_err(|e| e.to_string())?;
        match self.store.put_if_match("meta.json", &bytes, &st.meta_etag) {
            Ok(true) => {}
            Ok(false) => return Err("meta.json CAS failed (concurrent writer?)".into()),
            Err(e) => return Err(format!("meta.json put: {}", e)),
        }
        st.meta_etag = self.store.head("meta.json").map(|(e, _)| e).unwrap_or_default();
        Ok(())
    }

    /// Removes one index definition (CAS).
    pub fn unregister_index(&self, name: &str) -> Result<(), String> {
        let mut st = self.st.lock().unwrap_or_else(|e| e.into_inner());
        st.meta.indexes.remove(name);
        let bytes = serde_json::to_vec(&st.meta).map_err(|e| e.to_string())?;
        match self.store.put_if_match("meta.json", &bytes, &st.meta_etag) {
            Ok(true) => {}
            Ok(false) => return Err("meta.json CAS failed (concurrent writer?)".into()),
            Err(e) => return Err(format!("meta.json put: {}", e)),
        }
        st.meta_etag = self.store.head("meta.json").map(|(e, _)| e).unwrap_or_default();
        Ok(())
    }

    /// Removes every index definition belonging to a table (CAS). Used by
    /// DROP TABLE: indexes die with their table.
    pub fn unregister_indexes_for_table(&self, table: &str) -> Result<(), String> {
        let mut st = self.st.lock().unwrap_or_else(|e| e.into_inner());
        let before = st.meta.indexes.len();
        st.meta.indexes.retain(|_, d| d.table != table);
        if st.meta.indexes.len() == before {
            return Ok(());
        }
        let bytes = serde_json::to_vec(&st.meta).map_err(|e| e.to_string())?;
        match self.store.put_if_match("meta.json", &bytes, &st.meta_etag) {
            Ok(true) => {}
            Ok(false) => return Err("meta.json CAS failed (concurrent writer?)".into()),
            Err(e) => return Err(format!("meta.json put: {}", e)),
        }
        st.meta_etag = self.store.head("meta.json").map(|(e, _)| e).unwrap_or_default();
        Ok(())
    }

    /// ALTER TABLE ADD COLUMN durability: one WAL record (the truth) plus an
    /// immediate manifest n+1 that keeps the SAME parts and adds the column
    /// (old parts are NULL-padded at load). Cheap: no part rewrite, ever.
    pub fn alter_add(
        &self,
        name: &str,
        cols: &[(String, Ty, bool)],
        next_rowid: u64,
        nrows: u64,
    ) -> Result<(), String> {
        let mut st = self.st.lock().unwrap_or_else(|e| e.into_inner());
        let id = *st
            .meta
            .tables
            .get(name)
            .ok_or_else(|| format!("table {:?} not registered", name))?;
        let col = cols
            .last()
            .ok_or("alter_add needs the new column as the last entry")?
            .clone();
        // ---- 1. WAL record (Committed: on storage before the ack) ----
        let items = vec![(id, WalRec::AlterAdd { name: name.to_string(), col })];
        let lsn = st.lsn;
        st.lsn += 1;
        let buf_len = st.wal_buf.len();
        encode_segment(&mut st.wal_buf, &items, lsn);
        if self.ack == AckLevel::Committed {
            if let Err(e) = self.flush_locked(&mut st) {
                st.wal_buf.truncate(buf_len);
                return Err(e);
            }
        }
        // ---- 2. manifest n+1: same parts, new column list ----
        let lsn_now = st.lsn;
        let ts = st.tables.entry(id).or_insert_with(|| TState {
            manifest_n: 0,
            manifest_etag: String::new(),
            live_parts: Vec::new(),
            manifest_lsn: 0,
        });
        let n = ts.manifest_n + 1;
        let man = Manifest {
            format: 1,
            n,
            table: name.to_string(),
            columns: cols
                .iter()
                .map(|(cn, ct, nn)| ManCol {
                    name: cn.clone(),
                    ty: crate::wal::ty_code(*ct),
                    not_null: *nn,
                })
                .collect(),
            parts: ts
                .live_parts
                .iter()
                .map(|k| crate::persist::ManPart {
                    key: k.clone(),
                    rows: nrows,
                })
                .collect(),
            next_rowid,
            // written right after the AlterAdd record flushed: covers it
            lsn: lsn_now,
        };
        let bytes = serde_json::to_vec(&man).map_err(|e| e.to_string())?;
        let mkey = format!("t/{}/manifest/{}.json", id, n);
        let ok = self
            .store
            .put_if_absent(&mkey, &bytes)
            .map_err(|e| format!("manifest CAS: {}", e))?;
        if !ok {
            return Err(format!(
                "manifest CAS failed for {:?} n={} (concurrent writer?)",
                name, n
            ));
        }
        ts.manifest_n = n;
        ts.manifest_lsn = man.lsn;
        ts.manifest_etag = self
            .store
            .head(&mkey)
            .map(|(e, _)| e)
            .unwrap_or_default();
        // GC the superseded manifest (never a snapshot-pinned version)
        let pinned = pinned_versions(&st.meta);
        let mdir = format!("t/{}/manifest/", id);
        if let Ok(keys) = self.store.list(&mdir) {
            for k in keys {
                let mn = k
                    .trim_start_matches(&mdir)
                    .trim_end_matches(".json")
                    .parse::<u64>()
                    .unwrap_or(u64::MAX);
                if mn < n && !pinned.contains(&(id, mn)) {
                    let _ = self.store.delete(&k);
                }
            }
        }
        Ok(())
    }

    /// Removes a table from meta.json (CAS, best-effort with retry).
    pub fn unregister(&self, name: &str) -> Result<(), String> {
        let mut st = self.st.lock().unwrap_or_else(|e| e.into_inner());
        match st.meta.tables.remove(name) {
            None => return Ok(()),
            Some(id) => {
                st.tables.remove(&id);
            }
        }
        let bytes = serde_json::to_vec(&st.meta).map_err(|e| e.to_string())?;
        match self.store.put_if_match("meta.json", &bytes, &st.meta_etag) {
            Ok(true) => {}
            Ok(false) => return Err("meta.json CAS failed (concurrent writer?)".into()),
            Err(e) => return Err(format!("meta.json put: {}", e)),
        }
        st.meta_etag = self.store.head("meta.json").map(|(e, _)| e).unwrap_or_default();
        Ok(())
    }

    // ---------------------------------------------------------- checkpoint --

    /// Materializes every table into a fresh part, CAS-commits manifest n+1,
    /// then best-effort deletes superseded parts and WAL segments. The caller
    /// must hold the engine's tables write lock (single consistent view).
    pub fn checkpoint(&self, tables: &HashMap<String, Table>) -> Result<(), String> {
        let mut st = self.st.lock().unwrap_or_else(|e| e.into_inner());
        let cutoff_seq = st.next_seq;
        let mut old_parts: Vec<String> = Vec::new();
        for (name, tab) in tables {
            let id = *st
                .meta
                .tables
                .get(name)
                .ok_or_else(|| format!("checkpoint: table {:?} not registered", name))?;
            let lsn_now = st.lsn;
            let ts = st.tables.entry(id).or_insert_with(|| TState {
                manifest_n: 0,
                manifest_etag: String::new(),
                live_parts: Vec::new(),
                manifest_lsn: 0,
            });
            let n = ts.manifest_n + 1;
            let mut parts = Vec::new();
            if tab.nrows() > 0 {
                let mut cols: Vec<(String, Ty)> =
                    vec![("__rowid__".into(), Ty::Int)];
                for c in &tab.cols {
                    cols.push((c.name.clone(), c.ty));
                }
                let rows: Vec<Vec<Value>> = (0..tab.nrows())
                    .map(|i| {
                        let mut v = Vec::with_capacity(tab.columns.len() + 1);
                        v.push(Value::Int(tab.rowids[i] as i64));
                        v.extend(tab.columns.iter().map(|c| c[i].clone()));
                        v
                    })
                    .collect();
                // multi-part checkpoints: 65,536 rows per part, each part
                // self-describing with per-column min/max zone stats
                for chunk in rows.chunks(PART_ROWS) {
                    let key = format!("t/{}/part/{}.part", id, uuid());
                    let data = encode_part(&cols, chunk);
                    self.store
                        .put(&key, &data)
                        .map_err(|e| format!("part put: {}", e))?;
                    parts.push(crate::persist::ManPart { key, rows: chunk.len() as u64 });
                }
            }
            let man = Manifest {
                format: 1,
                n,
                table: name.clone(),
                columns: tab
                    .cols
                    .iter()
                    .map(|c| ManCol {
                        name: c.name.clone(),
                        ty: crate::wal::ty_code(c.ty),
                        not_null: c.not_null,
                    })
                    .collect(),
                parts: parts.clone(),
                next_rowid: tab.next_rowid,
                // covers every txn logged so far (all flushed segments are
                // below lsn_now, and the buffer is cleared below)
                lsn: lsn_now,
            };
            let bytes = serde_json::to_vec(&man).map_err(|e| e.to_string())?;
            let mkey = format!("t/{}/manifest/{}.json", id, n);
            // manifest n+1 is a NEW object: create-once semantics (a second
            // writer/concurrent process must fail loudly, never overwrite).
            // The old code CAS'd the new key against the PREVIOUS manifest's
            // etag, which broke on every second checkpoint (target missing).
            let ok = self
                .store
                .put_if_absent(&mkey, &bytes)
                .map_err(|e| format!("manifest CAS: {}", e))?;
            if !ok {
                return Err(format!(
                    "manifest CAS failed for {:?} n={} (concurrent writer?)",
                    name, n
                ));
            }
            old_parts.append(&mut ts.live_parts);
            ts.live_parts = parts.into_iter().map(|p| p.key).collect();
            ts.manifest_n = n;
            ts.manifest_lsn = man.lsn;
            ts.manifest_etag = self.store.head(&mkey).map(|(e, _)| e).unwrap_or_default();
        }
        // all manifests committed: the WAL up to cutoff is fully covered
        st.wal_buf.clear();
        st.flushed_lsn = st.flushed_lsn.max(st.lsn.saturating_sub(1));
        publish_lsn(st.flushed_lsn);
        // ---- snapshot-aware GC (v2.0) ----
        // A live snapshot needs, forever: its pinned manifests, the parts
        // they reference, and every WAL segment at or above the oldest
        // lsn its pinned manifests cover (that manifest is the base any
        // AS OF reconstruction for that snapshot walks forward from).
        let pinned = pinned_versions(&st.meta);
        let min_keep_lsn = min_pinned_manifest_lsn(&st.meta);
        // WAL: delete only segments fully covered by every live snapshot.
        // Some(0) (a WAL-only pin) keeps everything.
        let wal = self.store.list("wal/").unwrap_or_default();
        for k in wal {
            let Some(seq) = wal_seq_of(&k) else { continue };
            if seq >= cutoff_seq {
                continue;
            }
            match min_keep_lsn {
                None => {
                    let _ = self.store.delete(&k);
                }
                Some(keep) => {
                    // keep the segment unless its lsn is below every pin
                    // base; an unreadable segment is kept (safe)
                    if let Ok(data) = self.store.get(&k) {
                        if let Some(l) = crate::wal::segment_lsn(&data) {
                            if l < keep {
                                let _ = self.store.delete(&k);
                            }
                        }
                    }
                }
            }
        }
        // parts referenced by pinned manifests are protected
        let mut protected: std::collections::HashSet<String> = Default::default();
        for (pid, pn) in &pinned {
            let mkey = format!("t/{}/manifest/{}.json", pid, pn);
            match self.store.get(&mkey) {
                Ok(bytes) => {
                    if let Ok(man) = serde_json::from_slice::<Manifest>(&bytes) {
                        for p in &man.parts {
                            protected.insert(p.key.clone());
                        }
                    } else {
                        return Err(format!("pinned manifest {} is corrupt", mkey));
                    }
                }
                Err(e) => {
                    return Err(format!(
                        "pinned manifest {} is missing ({}): refusing to GC",
                        mkey, e
                    ));
                }
            }
        }
        for k in old_parts {
            if protected.contains(&k) {
                continue;
            }
            let _ = self.store.delete(&k);
        }
        // superseded manifests (older than each table's committed n) are
        // garbage too — except pinned versions
        for (id, ts) in st.tables.iter() {
            let mdir = format!("t/{}/manifest/", id);
            if let Ok(keys) = self.store.list(&mdir) {
                for k in keys {
                    let mn = k
                        .trim_start_matches(&mdir)
                        .trim_end_matches(".json")
                        .parse::<u64>()
                        .unwrap_or(u64::MAX);
                    if mn < ts.manifest_n && !pinned.contains(&(*id, mn)) {
                        let _ = self.store.delete(&k);
                    }
                }
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------ snapshots --

    /// Creates a named snapshot: pins the current manifest version of every
    /// live table + the current LSN, committed by meta.json CAS.
    pub fn create_snapshot(&self, name: &str) -> Result<u64, String> {
        let mut st = self.st.lock().unwrap_or_else(|e| e.into_inner());
        if st.meta.snapshots.contains_key(name) {
            return Err(format!("snapshot {:?} already exists", name));
        }
        let mut pins = HashMap::new();
        for (tname, id) in st.meta.tables.clone() {
            let ts = st.tables.get(&id);
            pins.insert(
                tname,
                SnapPin {
                    id,
                    manifest_n: ts.map_or(0, |t| t.manifest_n),
                    manifest_lsn: ts.map_or(0, |t| t.manifest_lsn),
                },
            );
        }
        // last included txn: every flushed segment has lsn < st.lsn, so the
        // state as of "now" is lsn st.lsn - 1 (0 = empty database)
        let lsn = st.lsn.saturating_sub(1);
        let def = SnapshotDef {
            lsn,
            created_us: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as i64)
                .unwrap_or(0),
            pins,
        };
        st.meta.snapshots.insert(name.to_string(), def);
        let bytes = serde_json::to_vec(&st.meta).map_err(|e| e.to_string())?;
        match self.store.put_if_match("meta.json", &bytes, &st.meta_etag) {
            Ok(true) => {}
            Ok(false) => return Err("meta.json CAS failed (concurrent writer?)".into()),
            Err(e) => return Err(format!("meta.json put: {}", e)),
        }
        st.meta_etag = self.store.head("meta.json").map(|(e, _)| e).unwrap_or_default();
        Ok(lsn)
    }

    /// Drops a snapshot. The data it pinned is reclaimed by the next
    /// CHECKPOINT (nothing is deleted here).
    pub fn drop_snapshot(&self, name: &str, if_exists: bool) -> Result<(), String> {
        let mut st = self.st.lock().unwrap_or_else(|e| e.into_inner());
        if st.meta.snapshots.remove(name).is_none() && !if_exists {
            return Err(format!("snapshot {:?} does not exist", name));
        }
        let bytes = serde_json::to_vec(&st.meta).map_err(|e| e.to_string())?;
        match self.store.put_if_match("meta.json", &bytes, &st.meta_etag) {
            Ok(true) => {}
            Ok(false) => return Err("meta.json CAS failed (concurrent writer?)".into()),
            Err(e) => return Err(format!("meta.json put: {}", e)),
        }
        st.meta_etag = self.store.head("meta.json").map(|(e, _)| e).unwrap_or_default();
        Ok(())
    }

    /// Snapshot listing for the catalog: (name, lsn, created_us, ntables).
    pub fn snapshot_list(&self) -> Vec<(String, u64, i64, usize)> {
        let st = self.st.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<(String, u64, i64, usize)> = st
            .meta
            .snapshots
            .iter()
            .map(|(n, d)| (n.clone(), d.lsn, d.created_us, d.pins.len()))
            .collect();
        out.sort();
        out
    }

    /// Reconstructs one table's state as of a snapshot or LSN.
    /// Ok(None) = the table did not exist at that point in time.
    pub fn read_table_as_of(
        &self,
        name: &str,
        target: &AsOfTarget,
    ) -> Result<Option<Table>, String> {
        let (lsn_cap, pin) = {
            let st = self.st.lock().unwrap_or_else(|e| e.into_inner());
            match target {
                AsOfTarget::Snapshot(sname) => {
                    let def = match st.meta.snapshots.get(sname) {
                        Some(d) => d,
                        None => return Err(format!("snapshot {:?} does not exist", sname)),
                    };
                    // def.lsn is the last INCLUDED txn
                    (def.lsn, def.pins.get(name).cloned())
                }
                AsOfTarget::Lsn(l) => {
                    if *l > st.flushed_lsn {
                        return Err(format!(
                            "LSN {} is in the future (current LSN is {})",
                            l, st.flushed_lsn
                        ));
                    }
                    (*l, None)
                }
            }
        };
        // resolve the table incarnation: the snapshot's pin (time-truthful,
        // survives DROP + recreate) or the current registration for LSN mode
        let (id, want_n) = match &pin {
            Some(p) => (p.id, Some(p.manifest_n)),
            None => {
                let st = self.st.lock().unwrap_or_else(|e| e.into_inner());
                match st.meta.tables.get(name) {
                    Some(id) => (*id, None),
                    None => return Ok(None),
                }
            }
        };
        // ---- base: pinned manifest, or the newest retained one <= lsn_cap ----
        let (man, base_lsn) = match want_n {
            Some(0) => (None, 0u64), // WAL-only table at snapshot time
            Some(n) => {
                let man = load_manifest(self.store.as_ref(), id, n)?;
                let man = match man {
                    Some(m) => m,
                    None => {
                        return Err(format!(
                            "pinned manifest {} of table {:?} is missing (retention bug)",
                            n, name
                        ))
                    }
                };
                let l = man.lsn;
                (Some(man), l)
            }
            None => {
                // LSN mode: pick the newest retained manifest with lsn <= cap+1
                let mdir = format!("t/{}/manifest/", id);
                let mut best: Option<Manifest> = None;
                for key in self.store.list(&mdir).map_err(|e| e.to_string())? {
                    let n = key
                        .trim_start_matches(&mdir)
                        .trim_end_matches(".json")
                        .parse::<u64>()
                        .ok()
                        .filter(|&v| v > 0 && v < u64::MAX / 2);
                    let Some(n) = n else { continue };
                    let m = load_manifest(self.store.as_ref(), id, n)?;
                    let Some(m) = m else { continue };
                    let m_lsn = if m.lsn == 0 { 0 } else { m.lsn };
                    if m_lsn > lsn_cap + 1 {
                        continue;
                    }
                    let better = match &best {
                        Some(b) => {
                            (m.lsn, m.n) > (b.lsn, b.n)
                        }
                        None => true,
                    };
                    if better {
                        best = Some(m);
                    }
                }
                match best {
                    Some(m) => {
                        let l = m.lsn;
                        (Some(m), l)
                    }
                    // no manifest qualifies: full replay from the oldest
                    // retained state (Create record must be in the WAL)
                    None => (None, 0),
                }
            }
        };
        // ---- build the base table (single-entry replay map) ----
        let mut by_tab: HashMap<u64, ReplayTab> = HashMap::new();
        if let Some(m) = &man {
            let mut cols: Vec<ColumnDef> = Vec::with_capacity(m.columns.len());
            for c in &m.columns {
                let ty = crate::wal::ty_of(c.ty)
                    .ok_or_else(|| format!("manifest: bad ty code {}", c.ty))?;
                cols.push(ColumnDef {
                    name: c.name.clone(),
                    ty,
                    not_null: c.not_null,
                });
            }
                let want_width = cols.len();
                let mut rt = ReplayTab {
                    cols,
                    rows: Vec::new(),
                    map: HashMap::new(),
                    next_rowid_seed: m.next_rowid,
                };
                for p in &m.parts {
                    let bytes = self
                        .store
                        .get(&p.key)
                        .map_err(|e| format!("part {}: {}", p.key, e))?;
                    let d = decode_part(&bytes).map_err(|e| format!("part {}: {}", p.key, e))?;
                    for mut row in d.rows {
                        let rid = match row.remove(0) {
                            Value::Int(i) if i >= 0 => i as u64,
                            _ => return Err(format!("part {}: bad rowid value", p.key)),
                        };
                        while row.len() < want_width {
                            row.push(Value::Null);
                        }
                        if !rt.map.contains_key(&rid) {
                            rt.map.insert(rid, rt.rows.len());
                            rt.rows.push((rid, row));
                        }
                    }
                }
            by_tab.insert(id, rt);
        }
        // ---- replay WAL segments in [base_lsn, lsn_cap] for this table ----
        let mut name_of: HashMap<u64, String> = HashMap::new();
        let mut max_rowid: HashMap<u64, u64> = HashMap::new();
        let wal_keys = self.store.list("wal/").map_err(|e| e.to_string())?;
        let mut segs: Vec<(u64, String)> = Vec::new();
        for k in wal_keys {
            if let Some(seq) = wal_seq_of(&k) {
                segs.push((seq, k));
            }
        }
        segs.sort();
        segs.dedup_by_key(|(s, _)| *s);
        for (_seq, key) in segs {
            let data = self
                .store
                .get(&key)
                .map_err(|e| format!("wal {}: {}", key, e))?;
            let seg_lsn = match crate::wal::segment_lsn(&data) {
                Some(l) => l,
                None => continue, // empty/truncated-before-header: skip
            };
            if seg_lsn < base_lsn || seg_lsn > lsn_cap {
                continue;
            }
            match decode_segment(&data) {
                Replay::Ok { items, torn } => {
                    if torn {
                        // unacked in-flight write; replaying a prefix would
                        // expose half a transaction — skip the whole segment
                        continue;
                    }
                    for (tid, rec) in items {
                        if tid == id {
                            apply_rec(&mut by_tab, &mut name_of, &mut max_rowid, id, rec, 0);
                        }
                    }
                }
                Replay::Corrupt(m) => {
                    return Err(format!("wal {}: CORRUPT ({})", key, m));
                }
            }
        }
        let tab = match by_tab.remove(&id) {
            Some(t) => t,
            None => {
                // no manifest, and no Create record within the window.
                // Distinguish "did not exist yet" from "state was GC'd":
                // if some retained segment at/below the cap exists, the
                // table genuinely did not exist; otherwise the state the
                // caller asked for has been reclaimed.
                let any_retained = {
                    let mut found = false;
                    for k in self.store.list("wal/").unwrap_or_default() {
                        if wal_seq_of(&k).is_some() {
                            if let Ok(data) = self.store.get(&k) {
                                if let Some(l) = crate::wal::segment_lsn(&data) {
                                    if l <= lsn_cap {
                                        found = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    found
                };
                if any_retained {
                    return Ok(None);
                }
                return Err(format!(
                    "LSN {} is before the earliest retained state                      (no snapshot reaches that far back)",
                    lsn_cap
                ));
            }
        };
        let mut t = tab.into_table();
        let live_max = t.rowids.iter().copied().max().unwrap_or(0);
        let wm = max_rowid.get(&id).copied().unwrap_or(0);
        t.next_rowid = t.next_rowid.max(live_max + 1).max(wm);
        Ok(Some(t))
    }
}

/// What an AS OF read addresses.
pub enum AsOfTarget {
    Snapshot(String),
    Lsn(u64),
}

fn load_manifest(
    store: &dyn ObjectStore,
    id: u64,
    n: u64,
) -> Result<Option<Manifest>, String> {
    let key = format!("t/{}/manifest/{}.json", id, n);
    match store.get(&key) {
        Ok(bytes) => {
            let man: Manifest = serde_json::from_slice(&bytes)
                .map_err(|e| format!("manifest {} parse: {}", key, e))?;
            Ok(Some(man))
        }
        Err(StoreError::NotFound(_)) => Ok(None),
        Err(e) => Err(format!("manifest {}: {}", key, e)),
    }
}

/// Every (table_id, manifest_n) pinned by a live snapshot.
fn pinned_versions(meta: &Meta) -> std::collections::HashSet<(u64, u64)> {
    let mut out = std::collections::HashSet::new();
    for def in meta.snapshots.values() {
        for p in def.pins.values() {
            if p.manifest_n > 0 {
                out.insert((p.id, p.manifest_n));
            }
        }
    }
    out
}

/// The lowest manifest coverage watermark any live snapshot depends on.
/// None = no snapshots exist (unrestricted WAL GC). Some(0) = a snapshot
/// pins a WAL-only or pre-2.0 table: every segment must be kept.
fn min_pinned_manifest_lsn(meta: &Meta) -> Option<u64> {
    meta.snapshots
        .values()
        .flat_map(|d| d.pins.values().map(|p| p.manifest_lsn))
        .min()
}

/// Strict WAL segment key parser: `wal/<16 digits>-<32 hex>.wal` only.
/// Anything else (in-flight `.tmp` writes from a killed process, stray
/// objects) is not a segment: tmp files are partial writes whose rename
/// never happened, so their transaction was never acked.
pub fn wal_seq_of(key: &str) -> Option<u64> {
    let stem = key.strip_prefix("wal/")?.strip_suffix(".wal")?;
    let (seq, uuid) = stem.split_once('-')?;
    if seq.len() != 16 || !seq.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if uuid.len() != 32 || !uuid.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    seq.parse::<u64>().ok()
}

/// Idempotent record application during recovery (O(1) per record).
fn apply_rec(
    by_id: &mut HashMap<u64, ReplayTab>,
    name_of: &mut HashMap<u64, String>,
    max_rowid: &mut HashMap<u64, u64>,
    id: u64,
    rec: WalRec,
    seq: u64,
) {
    let _ = seq;
    match rec {
        WalRec::Create { name, cols } => {
            by_id.insert(id, ReplayTab::fresh(cols));
            name_of.insert(id, name);
        }
        WalRec::Drop { name } => {
            by_id.remove(&id);
            name_of.remove(&id);
            let _ = name;
        }
        WalRec::AlterAdd { name: _, col } => {
            // schema evolution: extend the replayed table; existing rows
            // read as NULL for the new (trailing) column
            if let Some(tab) = by_id.get_mut(&id) {
                if !tab.cols.iter().any(|c| c.name == col.0) {
                    tab.cols.push(crate::sql::ColumnDef {
                        name: col.0,
                        ty: col.1,
                        not_null: col.2,
                    });
                    for (_, row) in tab.rows.iter_mut() {
                        row.push(Value::Null);
                    }
                }
            }
        }
        WalRec::Insert { rowid, cells } => {
            if let Some(tab) = by_id.get_mut(&id) {
                if !tab.map.contains_key(&rowid) {
                    tab.map.insert(rowid, tab.rows.len());
                    tab.rows.push((rowid, cells));
                    let m = max_rowid.entry(id).or_insert(0);
                    if rowid >= *m {
                        *m = rowid + 1;
                    }
                }
            }
        }
        WalRec::Update { rowid, cells } => {
            if let Some(tab) = by_id.get_mut(&id) {
                if let Some(&i) = tab.map.get(&rowid) {
                    tab.rows[i].1 = cells;
                }
            }
        }
        WalRec::Delete { rowid } => {
            if let Some(tab) = by_id.get_mut(&id) {
                tab.map.remove(&rowid);
            }
        }
    }
}
