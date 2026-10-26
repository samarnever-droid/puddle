//! Reference executor: in-RAM columnar tables, scalar evaluation, joins,
//! aggregates - plus the M4 vectorized batch path (vecx.rs) that takes the
//! hot SELECT shapes and falls back here for everything else. This executor
//! stays as the differential-testing oracle.

use crate::persist::Persist;
use crate::sql::{
    AsOf, ColumnDef, Expr, FromItem, JoinKind, SelectItem, SelectStmt, Stmt, TableRef,
};
use crate::vecx::{hash_join, veval, VSrc};
use crate::wal::WalRec;
use std::sync::atomic::{AtomicBool, Ordering};

/// Global switch for the vectorized SELECT path (off = reference oracle).
pub static VECTORIZED: AtomicBool = AtomicBool::new(true);
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::temporal;

// ------------------------------------------------------------------- values --

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Date(i32),    // days since 1970-01-01
    Time(i64),    // microseconds since midnight
    Timestamp(i64), // microseconds since the epoch (no time zone)
    Interval {
        months: i32,
        days: i32,
        micros: i64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ty {
    Bool,
    Int,
    Float,
    Text,
    Date,
    Time,
    Timestamp,
    Interval,
}

impl Ty {
    pub fn oid(self) -> u32 {
        match self {
            Ty::Bool => 16,
            Ty::Int => 20,    // int8
            Ty::Float => 701, // float8
            Ty::Text => 25,
            Ty::Date => 1082,
            Ty::Time => 1083,
            Ty::Timestamp => 1114,
            Ty::Interval => 1186,
        }
    }
    pub fn typlen(self) -> i16 {
        match self {
            Ty::Bool => 1,
            Ty::Int => 8,
            Ty::Float => 8,
            Ty::Text => -1,
            Ty::Date => 4,
            Ty::Time => 8,
            Ty::Timestamp => 8,
            Ty::Interval => 16,
        }
    }
    pub fn name(self) -> &'static str {
        match self {
            Ty::Bool => "bool",
            Ty::Int => "int8",
            Ty::Float => "float8",
            Ty::Text => "text",
            Ty::Date => "date",
            Ty::Time => "time",
            Ty::Timestamp => "timestamp",
            Ty::Interval => "interval",
        }
    }
    pub fn is_temporal(self) -> bool {
        matches!(self, Ty::Date | Ty::Time | Ty::Timestamp | Ty::Interval)
    }
}

/// PostgreSQL-style text rendering.
pub fn value_to_text(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Bool(b) => Some(if *b { "t".into() } else { "f".into() }),
        Value::Int(i) => Some(i.to_string()),
        Value::Float(f) => Some(if f.is_nan() {
            "NaN".into()
        } else if f.is_infinite() {
            if *f > 0.0 { "Infinity".into() } else { "-Infinity".into() }
        } else {
            format!("{}", f)
        }),
        Value::Text(s) => Some(s.clone()),
        Value::Date(d) => Some(crate::temporal::fmt_date(*d)),
        Value::Time(t) => Some(crate::temporal::fmt_time(*t)),
        Value::Timestamp(us) => Some(crate::temporal::fmt_timestamp(*us)),
        Value::Interval { months, days, micros } => {
            Some(crate::temporal::fmt_interval(*months, *days, *micros))
        }
    }
}

/// Hashable group key element.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum GK {
    Null,
    Bool(bool),
    Int(i64),
    Float(u64), // canonicalized bits
    Text(String),
    Date(i32),
    Time(i64),
    Timestamp(i64),
    Interval(i32, i32, i64),
}

pub fn gkey(v: &Value) -> GK {
    match v {
        Value::Null => GK::Null,
        Value::Bool(b) => GK::Bool(*b),
        Value::Int(i) => GK::Int(*i),
        Value::Float(f) => {
            let mut f = *f;
            if f == 0.0 {
                f = 0.0; // normalize -0.0
            }
            if f.is_nan() {
                GK::Float(0x7ff8_0000_0000_0000)
            } else {
                GK::Float(f.to_bits())
            }
        }
        Value::Text(s) => GK::Text(s.clone()),
        Value::Date(d) => GK::Date(*d),
        Value::Time(t) => GK::Time(*t),
        Value::Timestamp(us) => GK::Timestamp(*us),
        Value::Interval { months, days, micros } => GK::Interval(*months, *days, *micros),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggKind {
    CountStar,
    Count,
    Sum,
    Min,
    Max,
    Avg,
}

// ------------------------------------------------------------------ errors --

#[derive(Debug)]
pub struct PqError {
    pub code: &'static str,
    pub msg: String,
}

impl PqError {
    pub fn new(code: &'static str, msg: impl Into<String>) -> PqError {
        PqError {
            code,
            msg: msg.into(),
        }
    }
    pub fn syntax(msg: impl Into<String>) -> PqError {
        PqError::new("42601", msg)
    }
    pub fn undefined_table(name: &str) -> PqError {
        PqError::new("42P01", format!("relation \"{}\" does not exist", name))
    }
    pub fn undefined_column(name: &str) -> PqError {
        PqError::new("42703", format!("column \"{}\" does not exist", name))
    }
    pub fn wrong_type(msg: impl Into<String>) -> PqError {
        PqError::new("42804", msg)
    }
}

type R<T> = Result<T, PqError>;

/// The shape every table-shaped source returns: (columns, rows).
type TableShape = (Vec<(String, Ty)>, Vec<Vec<Value>>);

// --------------------------------------------------------------- scope/ctx --

pub struct TableScope {
    pub alias: String,
    pub cols: Vec<(String, Ty)>,
    pub start: usize,
}

pub struct Scope {
    pub tables: Vec<TableScope>,
}

impl Scope {
    pub fn empty() -> Scope {
        Scope { tables: Vec::new() }
    }
    pub fn push(&mut self, alias: &str, cols: Vec<(String, Ty)>) {
        let start: usize = self.tables.last().map_or(0, |t| t.start + t.cols.len());
        self.tables.push(TableScope {
            alias: alias.to_string(),
            cols,
            start,
        });
    }
    pub fn width(&self) -> usize {
        self.tables
            .last()
            .map_or(0, |t| t.start + t.cols.len())
    }
    pub fn resolve(&self, col: &str) -> R<usize> {
        let mut hits = Vec::new();
        for t in &self.tables {
            for (i, (c, _)) in t.cols.iter().enumerate() {
                if c == col {
                    hits.push(t.start + i);
                }
            }
        }
        match hits.len() {
            1 => Ok(hits[0]),
            0 => Err(PqError::undefined_column(col)),
            _ => Err(PqError::new(
                "42702",
                format!("column reference \"{}\" is ambiguous", col),
            )),
        }
    }
    pub fn resolve_q(&self, tab: &str, col: &str) -> R<usize> {
        let tabs: Vec<&TableScope> = self.tables.iter().filter(|t| t.alias == tab).collect();
        match tabs.len() {
            0 => Err(PqError::new(
                "42P01",
                format!("missing FROM-clause entry for table \"{}\"", tab),
            )),
            1 => {
                let t = tabs[0];
                for (i, (c, _)) in t.cols.iter().enumerate() {
                    if c == col {
                        return Ok(t.start + i);
                    }
                }
                Err(PqError::undefined_column(col))
            }
            _ => Err(PqError::new(
                "42712",
                format!("table name \"{}\" specified more than once", tab),
            )),
        }
    }
    pub fn col_ty(&self, idx: usize) -> Ty {
        for t in &self.tables {
            if idx >= t.start && idx < t.start + t.cols.len() {
                return t.cols[idx - t.start].1;
            }
        }
        Ty::Text
    }
}

pub struct Ctx<'a> {
    pub scope: &'a Scope,
    pub row: &'a [Value],
    pub aggs: &'a [Value],
    pub params: &'a [Value],
}

// ------------------------------------------------------------ catalog/tables --

/// Ordered, hashable non-null key for secondary indexes. Floats use the
/// sign-flip bit trick for a total order (NaN sorts last, like PG btree).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum IKey {
    Bool(bool),
    Int(i64),
    Float(u64),
    Text(String),
}

impl IKey {
    pub fn of(v: &Value) -> Option<IKey> {
        Some(match v {
            Value::Bool(b) => IKey::Bool(*b),
            Value::Int(i) => IKey::Int(*i),
            Value::Float(f) => {
                let bits = f.to_bits();
                // total order: negatives flip, positives set the high bit
                let ordered = if bits & 0x8000_0000_0000_0000 != 0 {
                    !bits
                } else {
                    bits | 0x8000_0000_0000_0000
                };
                IKey::Float(ordered)
            }
            Value::Text(s) => IKey::Text(s.clone()),
            Value::Date(d) => IKey::Int(*d as i64),
            Value::Time(t) => IKey::Int(*t),
            Value::Timestamp(us) => IKey::Int(*us),
            Value::Interval { .. } => IKey::Int(interval_us(v)),
            Value::Null => return None, // NULLs are never indexed (SQL)
        })
    }
}

/// A secondary index over one table column, mapping key -> rowids.
/// Derived structure: rebuilt from table data at recovery; maintained
/// incrementally under DML.
#[derive(Clone)]
pub struct TableIndex {
    pub method: crate::sql::IdxMethod,
    pub col: usize,
    pub map: IdxMap,
}

#[derive(Clone)]
pub enum IdxMap {
    Hash(std::collections::HashMap<IKey, Vec<u64>>),
    BTree(std::collections::BTreeMap<IKey, Vec<u64>>),
}

impl TableIndex {
    pub fn build(tab: &Table, col: usize, method: crate::sql::IdxMethod) -> TableIndex {
        let mut idx = TableIndex {
            method,
            col,
            map: match method {
                crate::sql::IdxMethod::Hash => {
                    IdxMap::Hash(std::collections::HashMap::new())
                }
                crate::sql::IdxMethod::BTree => {
                    IdxMap::BTree(std::collections::BTreeMap::new())
                }
            },
        };
        let column = &tab.columns[col];
        for (v, rid) in column.iter().zip(tab.rowids.iter()) {
            if let Some(k) = IKey::of(v) {
                idx.insert(&k, *rid);
            }
        }
        idx
    }
    fn insert(&mut self, k: &IKey, rowid: u64) {
        match &mut self.map {
            IdxMap::Hash(m) => m.entry(k.clone()).or_default().push(rowid),
            IdxMap::BTree(m) => m.entry(k.clone()).or_default().push(rowid),
        }
    }
    fn remove(&mut self, k: &IKey, rowid: u64) {
        match &mut self.map {
            IdxMap::Hash(m) => {
                if let Some(v) = m.get_mut(k) {
                    v.retain(|&r| r != rowid);
                    if v.is_empty() {
                        m.remove(k);
                    }
                }
            }
            IdxMap::BTree(m) => {
                if let Some(v) = m.get_mut(k) {
                    v.retain(|&r| r != rowid);
                    if v.is_empty() {
                        m.remove(k);
                    }
                }
            }
        }
    }
    pub fn lookup(&self, k: &IKey) -> &[u64] {
        match &self.map {
            IdxMap::Hash(m) => m.get(k).map_or(&[], |v| &v[..]),
            IdxMap::BTree(m) => m.get(k).map_or(&[], |v| &v[..]),
        }
    }
    /// Rowids with key in [lo, hi] (inclusive bounds; use the full key range
    /// for one-sided scans).
    pub fn range(&self, lo: std::ops::Bound<&IKey>, hi: std::ops::Bound<&IKey>) -> Vec<u64> {
        match &self.map {
            IdxMap::Hash(_) => Vec::new(), // hash indexes have no order
            IdxMap::BTree(m) => {
                let mut out = Vec::new();
                for (_k, v) in m.range((lo, hi)) {
                    out.extend_from_slice(v);
                }
                out
            }
        }
    }
    /// Rowids with key strictly less than `k` (or <= with or_eq).
    pub fn less_than(&self, k: &IKey, or_eq: bool) -> Vec<u64> {
        match &self.map {
            IdxMap::Hash(_) => Vec::new(),
            IdxMap::BTree(m) => {
                let mut out = Vec::new();
                let hi = if or_eq {
                    std::ops::Bound::Included(k)
                } else {
                    std::ops::Bound::Excluded(k)
                };
                for (_k, v) in m.range((std::ops::Bound::Unbounded::<&IKey>, hi)) {
                    out.extend_from_slice(v);
                }
                out
            }
        }
    }
    /// Rowids with key strictly greater than `k` (or >= with or_eq).
    pub fn greater_than(&self, k: &IKey, or_eq: bool) -> Vec<u64> {
        match &self.map {
            IdxMap::Hash(_) => Vec::new(),
            IdxMap::BTree(m) => {
                let mut out = Vec::new();
                let lo = if or_eq {
                    std::ops::Bound::Included(k)
                } else {
                    std::ops::Bound::Excluded(k)
                };
                for (_k, v) in m.range((lo, std::ops::Bound::Unbounded::<&IKey>)) {
                    out.extend_from_slice(v);
                }
                out
            }
        }
    }
}

/// Columnar in-RAM table: values stored column-major (M4). Row i is
/// `columns[*][i]` with identity `rowids[i]`.
#[derive(Clone)]
pub struct Table {
    pub cols: Vec<ColumnDef>,
    pub rowids: Vec<u64>,
    pub columns: Vec<Vec<Value>>,
    pub version: u64,
    pub next_rowid: u64,
    /// secondary indexes (derived; name -> index)
    pub indexes: HashMap<String, TableIndex>,
}

impl Table {
    pub fn empty(cols: Vec<ColumnDef>, next_rowid: u64) -> Table {
        let n = cols.len();
        Table {
            cols,
            rowids: Vec::new(),
            columns: vec![Vec::new(); n],
            version: 1,
            next_rowid,
            indexes: HashMap::new(),
        }
    }
    pub fn nrows(&self) -> usize {
        self.rowids.len()
    }
    /// Materializes row i (reference path / RETURNING).
    pub fn row(&self, i: usize) -> Vec<Value> {
        self.columns.iter().map(|c| c[i].clone()).collect()
    }
    pub fn push_row(&mut self, id: u64, cells: Vec<Value>) {
        for (col, v) in self.columns.iter_mut().zip(cells) {
            col.push(v);
        }
        self.rowids.push(id);
    }
    /// Order-preserving compaction: drop rows whose keep flag is false.
    pub fn compact(&mut self, keep: &[bool]) {
        for col in self.columns.iter_mut() {
            let mut w = 0;
            for i in 0..col.len() {
                if keep[i] {
                    if w != i {
                        col[w] = std::mem::replace(&mut col[i], Value::Null);
                    }
                    w += 1;
                }
            }
            col.truncate(w);
        }
        let mut w = 0;
        for (i, &k) in keep.iter().enumerate() {
            if k {
                self.rowids[w] = self.rowids[i];
                w += 1;
            }
        }
        self.rowids.truncate(w);
    }
}

pub struct Db {
    tables: RwLock<HashMap<String, Table>>,
    /// tables written since this Db was created (used by transactions)
    dirty: std::sync::Mutex<std::collections::HashSet<String>>,
    /// tables dropped since this Db was created
    dropped: std::sync::Mutex<std::collections::HashSet<String>>,
    /// durable layer (None for transaction snapshots and pure-RAM mode)
    persist: Option<Arc<Persist>>,
    /// READ-ONLY durable layer for time travel inside transactions: set on
    /// txn snapshot Dbs so AS OF reads work, while log_txn (which checks
    /// `persist`) stays a no-op there — a rolled-back txn must never leave
    /// a WAL segment behind
    asof_reader: Option<Arc<Persist>>,
}

/// What a statement produced, for the wire layer.
pub enum StmtResult {
    Rows {
        cols: Vec<(String, Ty)>,
        rows: Vec<Vec<Value>>,
        tag: String,
    },
    Tag(String),
}

impl Db {
    pub fn new() -> Db {
        Db {
            tables: RwLock::new(HashMap::new()),
            dirty: std::sync::Mutex::new(std::collections::HashSet::new()),
            dropped: std::sync::Mutex::new(std::collections::HashSet::new()),
            persist: None,
            asof_reader: None,
        }
    }

    /// The durable layer to READ history through (global or txn snapshot).
    fn reader(&self) -> Option<&Arc<Persist>> {
        self.persist.as_ref().or(self.asof_reader.as_ref())
    }

    /// Durable mode: recovered tables + persistence layer.
    pub fn from_parts(tables: HashMap<String, Table>, persist: Arc<Persist>) -> Db {
        Db {
            tables: RwLock::new(tables),
            dirty: std::sync::Mutex::new(std::collections::HashSet::new()),
            dropped: std::sync::Mutex::new(std::collections::HashSet::new()),
            persist: Some(persist),
            asof_reader: None,
        }
    }

    /// WAL-log one statement/txn batch (no-op without a persist layer).
    fn log_txn(&self, entries: &[(String, Vec<WalRec>)]) -> R<()> {
        if let Some(p) = &self.persist {
            p.log_txn(entries)
                .map_err(|e| PqError::new("58030", format!("durability layer: {}", e)))?;
        }
        Ok(())
    }

    fn mark_dirty(&self, name: &str) {
        self.dirty
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(name.to_string());
    }

    /// Snapshot for a new transaction: private copy + observed table versions.
    pub fn snapshot(&self) -> (Db, HashMap<String, u64>) {
        let g = self.tables.read().unwrap_or_else(|e| e.into_inner());
        let mut observed = HashMap::new();
        let mut tables = HashMap::new();
        for (name, t) in g.iter() {
            observed.insert(name.clone(), t.version);
            tables.insert(name.clone(), t.clone());
        }
        (
            Db {
                tables: RwLock::new(tables),
                dirty: std::sync::Mutex::new(std::collections::HashSet::new()),
                dropped: std::sync::Mutex::new(std::collections::HashSet::new()),
                // reads only: time travel stays available inside
                // transactions; writes still go through apply_txn at COMMIT
                persist: None,
                asof_reader: self.reader().cloned(),
            },
            observed,
        )
    }

    /// Applies a transaction's writes to this (global) Db. Fails with 40001
    /// if any table we modified changed concurrently (optimistic validation).
    /// The whole transaction is WAL-logged as ONE segment before any global
    /// state changes, so it commits atomically or not at all.
    pub fn apply_txn(&self, from: Db, observed: &HashMap<String, u64>) -> R<()> {
        let dropped: Vec<String> = from
            .dropped
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .cloned()
            .collect();
        let dirty: Vec<String> = from
            .dirty
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .cloned()
            .collect();
        let src = from.tables.into_inner().unwrap_or_else(|e| e.into_inner());
        let mut g = self.tables.write().unwrap_or_else(|e| e.into_inner());
        let conflict = || {
            PqError::new(
                "40001",
                "could not serialize access due to concurrent updates",
            )
        };
        // ---- pass 1: validate everything, mutate nothing
        for name in &dropped {
            if let Some(t) = g.get(name) {
                if observed.get(name).is_some_and(|v| *v != t.version) {
                    return Err(conflict());
                }
            }
        }
        for name in &dirty {
            let t = match src.get(name) {
                Some(t) => t,
                None => continue,
            };
            match (g.get(name), observed.get(name)) {
                (Some(gt), Some(ov)) => {
                    if gt.version != *ov {
                        return Err(conflict());
                    }
                }
                (Some(_), None) => return Err(conflict()), // created concurrently
                (None, _) => {
                    let _ = t;
                }
            }
        }
        // ---- pass 2: register new tables (meta.json CAS) before logging
        if let Some(p) = &self.persist {
            for name in &dirty {
                if g.contains_key(name) || !src.contains_key(name) {
                    continue;
                }
                let cols: Vec<(String, Ty, bool)> = src[name]
                    .cols
                    .iter()
                    .map(|c| (c.name.clone(), c.ty, c.not_null))
                    .collect();
                p.register(name, &cols)
                    .map_err(|e| PqError::new("58030", format!("register: {}", e)))?;
            }
        }
        // ---- pass 3: build the WAL batch for the whole transaction
        let mut entries: Vec<(String, Vec<WalRec>)> = Vec::new();
        for name in &dropped {
            if !src.contains_key(name) {
                entries.push((name.clone(), vec![WalRec::Drop { name: name.clone() }]));
            }
        }
        for name in &dirty {
            let new = match src.get(name) {
                Some(t) => t,
                None => continue,
            };
            let old = g.get(name);
            let recs = match old {
                Some(o) if same_schema(o, new) => diff_recs(o, new),
                _ => {
                    let mut r = vec![WalRec::Create {
                        name: name.clone(),
                        cols: new
                            .cols
                            .iter()
                            .map(|c| (c.name.clone(), c.ty, c.not_null))
                            .collect(),
                    }];
                    for i in 0..new.nrows() {
                        r.push(WalRec::Insert {
                            rowid: new.rowids[i],
                            cells: new.row(i),
                        });
                    }
                    r
                }
            };
            if !recs.is_empty() {
                entries.push((name.clone(), recs));
            }
        }
        // ---- pass 4: one atomic segment, then swap in the final state
        self.log_txn(&entries)?;
        for name in &dropped {
            g.remove(name);
        }
        for name in &dirty {
            let t = match src.get(name) {
                Some(t) => t,
                None => continue,
            };
            let mut nt = t.clone();
            nt.version = g.get(name).map_or(1, |gt| gt.version + 1);
            g.insert(name.clone(), nt);
        }
        drop(g);
        // ---- pass 5: unregister pure drops (meta.json CAS, after the log)
        if let Some(p) = &self.persist {
            for name in &dropped {
                if !src.contains_key(name) {
                    p.unregister_indexes_for_table(name)
                        .map_err(|e| PqError::new("58030", format!("unregister index: {}", e)))?;
                    p.unregister(name)
                        .map_err(|e| PqError::new("58030", format!("unregister: {}", e)))?;
                }
            }
        }
        Ok(())
    }

    /// COPY FROM STDIN: parses text/csv data and inserts all rows as ONE
    /// atomic transaction (one WAL segment). Called by the wire layer after
    /// it has collected the CopyData stream.
    pub fn copy_in(
        &self,
        tr: &TableRef,
        cols: &[String],
        opts: &crate::sql::CopyOpts,
        data: &str,
    ) -> R<usize> {
        let raw_rows = if opts.csv {
            crate::copy::parse_csv(data, opts.delimiter, opts.quote, opts.header)
        } else {
            crate::copy::parse_text(data, opts.delimiter, &opts.null, opts.header)
        }
        .map_err(|e| PqError::new("22P04", e))?;

        let mut t = self.tables.write().unwrap_or_else(|e| e.into_inner());
        let tab = match t.get_mut(&tr.name) {
            Some(t) => t,
            None => return Err(PqError::undefined_table(&tr.name)),
        };
        let ncols = tab.cols.len();
        let colpos: Vec<usize> = if cols.is_empty() {
            (0..ncols).collect()
        } else {
            let mut pos = Vec::with_capacity(cols.len());
            for c in cols {
                match tab.cols.iter().position(|cd| cd.name == *c) {
                    Some(i) => {
                        if pos.contains(&i) {
                            return Err(PqError::new(
                                "42701",
                                format!("column \"{}\" specified more than once", c),
                            ));
                        }
                        pos.push(i);
                    }
                    None => {
                        return Err(PqError::new(
                            "42703",
                            format!(
                                "column \"{}\" of relation \"{}\" does not exist",
                                c, tr.name
                            ),
                        ));
                    }
                }
            }
            pos
        };
        let mut staged: Vec<Vec<Value>> = Vec::with_capacity(raw_rows.len());
        for (i, raw) in raw_rows.iter().enumerate() {
            if raw.len() != colpos.len() {
                return Err(PqError::new(
                    "22P04",
                    format!(
                        "COPY: row {} has {} fields but {} were expected",
                        i + 1,
                        raw.len(),
                        colpos.len()
                    ),
                ));
            }
            let mut vals: Vec<Value> = vec![Value::Null; ncols];
            for (&p, f) in colpos.iter().zip(raw.iter()) {
                let v = match f {
                    None => Value::Null,
                    Some(sv) => coerce(Value::Text(sv.clone()), tab.cols[p].ty)?,
                };
                if tab.cols[p].not_null && matches!(v, Value::Null) {
                    return Err(PqError::new(
                        "23502",
                        format!(
                            "null value in column \"{}\" violates not-null constraint",
                            tab.cols[p].name
                        ),
                    ));
                }
                vals[p] = v;
            }
            staged.push(vals);
        }
        // ONE atomic segment for the whole COPY (statement-atomic, fast)
        let base = tab.next_rowid;
        let recs: Vec<WalRec> = staged
            .iter()
            .enumerate()
            .map(|(i, cells)| WalRec::Insert {
                rowid: base + i as u64,
                cells: cells.clone(),
            })
            .collect();
        self.log_txn(&[(tr.name.clone(), recs)])?;
        for cells in staged {
            let id = tab.next_rowid;
            tab.next_rowid += 1;
            for ix in tab.indexes.values_mut() {
                if let Some(k) = IKey::of(&cells[ix.col]) {
                    ix.insert(&k, id);
                }
            }
            tab.push_row(id, cells);
        }
        let inserted = raw_rows.len();
        if inserted > 0 {
            tab.version += 1;
        }
        drop(t);
        self.mark_dirty(&tr.name);
        Ok(inserted)
    }

    /// COPY TO STDOUT source: returns (columns, rows); the wire layer
    /// encodes them in text/csv format.
    pub fn copy_out(
        &self,
        src: &crate::sql::CopySource,
        _opts: &crate::sql::CopyOpts,
    ) -> R<TableShape> {
        match src {
            crate::sql::CopySource::Table(tr, cols) => {
                let (tcols, rows) = self.load_table(tr)?;
                if cols.is_empty() {
                    return Ok((tcols, rows));
                }
                let pos: Vec<usize> = cols
                    .iter()
                    .map(|c| {
                        tcols.iter().position(|(n, _)| n == c).ok_or_else(|| {
                            PqError::new(
                                "42703",
                                format!(
                                    "column \"{}\" of relation \"{}\" does not exist",
                                    c, tr.name
                                ),
                            )
                        })
                    })
                    .collect::<R<_>>()?;
                let out_cols = pos.iter().map(|&i| tcols[i].clone()).collect();
                let out_rows = rows
                    .iter()
                    .map(|r| pos.iter().map(|&i| r[i].clone()).collect())
                    .collect();
                Ok((out_cols, out_rows))
            }
            crate::sql::CopySource::Query(sel) => {
                match self.execute(&Stmt::Select((**sel).clone()), &[])? {
                    StmtResult::Rows { cols, rows, .. } => Ok((cols, rows)),
                    StmtResult::Tag(_) => Err(PqError::syntax("COPY query must return rows")),
                }
            }
        }
    }

    pub fn execute(&self, stmt: &Stmt, params: &[Value]) -> R<StmtResult> {
        match stmt {
            Stmt::CreateTable {
                name,
                if_not_exists,
                cols,
            } => {
                let mut t = self.tables.write().unwrap();
                if t.contains_key(name) {
                    if *if_not_exists {
                        return Ok(StmtResult::Tag("CREATE TABLE".into()));
                    }
                    return Err(PqError::new(
                        "42P07",
                        format!("relation \"{}\" already exists", name),
                    ));
                }
                if cols.is_empty() {
                    return Err(PqError::syntax("a table must have at least one column"));
                }
                let mut seen = std::collections::HashSet::new();
                for c in cols {
                    if !seen.insert(c.name.clone()) {
                        return Err(PqError::new(
                            "42701",
                            format!("column \"{}\" specified more than once", c.name),
                        ));
                    }
                }
                if let Some(p) = &self.persist {
                    let cts: Vec<(String, Ty, bool)> = cols
                        .iter()
                        .map(|c| (c.name.clone(), c.ty, c.not_null))
                        .collect();
                    p.register(name, &cts)
                        .map_err(|e| PqError::new("58030", format!("register: {}", e)))?;
                    self.log_txn(&[(
                        name.clone(),
                        vec![WalRec::Create {
                            name: name.clone(),
                            cols: cts,
                        }],
                    )])?;
                }
                t.insert(name.clone(), Table::empty(cols.clone(), 1));
                drop(t);
                self.mark_dirty(name);
                Ok(StmtResult::Tag("CREATE TABLE".into()))
            }
            Stmt::DropTable { name, if_exists } => {
                let mut t = self.tables.write().unwrap();
                if t.get(name).is_none() {
                    if *if_exists {
                        return Ok(StmtResult::Tag("DROP TABLE".into()));
                    }
                    return Err(PqError::undefined_table(name));
                }
                self.log_txn(&[(name.clone(), vec![WalRec::Drop { name: name.clone() }])])?;
                t.remove(name);
                drop(t);
                if let Some(p) = &self.persist {
                    // indexes die with their table (defs first, then the table)
                    p.unregister_indexes_for_table(name)
                        .map_err(|e| PqError::new("58030", format!("unregister index: {}", e)))?;
                    p.unregister(name)
                        .map_err(|e| PqError::new("58030", format!("unregister: {}", e)))?;
                }
                self.mark_dirty(name);
                self.dropped
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(name.clone());
                Ok(StmtResult::Tag("DROP TABLE".into()))
            }
            Stmt::CreateIndex {
                name,
                if_not_exists,
                table,
                method,
                col,
            } => {
                let mut t = self.tables.write().unwrap_or_else(|e| e.into_inner());
                if !t.contains_key(table) {
                    return Err(PqError::undefined_table(table));
                }
                let name = match name {
                    Some(n) => n.clone(),
                    None => format!("{}_{}_idx", table, col),
                };
                // PG: index names share the relation namespace with tables
                let table_taken = t.contains_key(&name);
                let tab = match t.get_mut(table) {
                    Some(t) => t,
                    None => return Err(PqError::undefined_table(table)),
                };
                let colpos = match tab.cols.iter().position(|c| c.name == *col) {
                    Some(p) => p,
                    None => {
                        return Err(PqError::new(
                            "42703",
                            format!("column \"{}\" does not exist", col),
                        ))
                    }
                };
                let taken = table_taken || tab.indexes.contains_key(&name);
                if taken {
                    if *if_not_exists {
                        return Ok(StmtResult::Tag("CREATE INDEX".into()));
                    }
                    return Err(PqError::new(
                        "42P07",
                        format!("relation \"{}\" already exists", name),
                    ));
                }
                let idx = TableIndex::build(tab, colpos, *method);
                if let Some(p) = &self.persist {
                    let def = crate::persist::IndexDef {
                        table: table.clone(),
                        table_id: p.table_id(table).unwrap_or(0),
                        method: match method {
                            crate::sql::IdxMethod::BTree => "btree".into(),
                            crate::sql::IdxMethod::Hash => "hash".into(),
                        },
                        col: col.clone(),
                    };
                    p.register_index(&name, &def)
                        .map_err(|e| PqError::new("58030", format!("register index: {}", e)))?;
                }
                tab.indexes.insert(name, idx);
                drop(t);
                Ok(StmtResult::Tag("CREATE INDEX".into()))
            }
            Stmt::DropIndex { name, if_exists } => {
                let mut t = self.tables.write().unwrap_or_else(|e| e.into_inner());
                let mut found = None;
                for (tn, tab) in t.iter() {
                    if tab.indexes.contains_key(name) {
                        found = Some(tn.clone());
                        break;
                    }
                }
                let tn = match found {
                    Some(tn) => tn,
                    None => {
                        if *if_exists {
                            return Ok(StmtResult::Tag("DROP INDEX".into()));
                        }
                        return Err(PqError::new(
                            "42704",
                            format!("index \"{}\" does not exist", name),
                        ));
                    }
                };
                t.get_mut(&tn).unwrap().indexes.remove(name);
                drop(t);
                if let Some(p) = &self.persist {
                    p.unregister_index(name)
                        .map_err(|e| PqError::new("58030", format!("unregister index: {}", e)))?;
                }
                Ok(StmtResult::Tag("DROP INDEX".into()))
            }
            Stmt::Insert {
                table,
                cols,
                rows,
                returning,
            } => {
                if table.as_of.is_some() {
                    return Err(PqError::new(
                        "0A000",
                        "AS OF is not allowed on a INSERT target",
                    ));
                }

                let mut t = self.tables.write().unwrap();
                let tab = match t.get_mut(&table.name) {
                    Some(t) => t,
                    None => return Err(PqError::undefined_table(&table.name)),
                };
                let ncols = tab.cols.len();
                // explicit column list -> positions in the table
                let colpos: Vec<usize> = if cols.is_empty() {
                    (0..ncols).collect()
                } else {
                    let mut pos = Vec::with_capacity(cols.len());
                    for c in cols {
                        match tab.cols.iter().position(|cd| cd.name == *c) {
                            Some(i) => {
                                if pos.contains(&i) {
                                    return Err(PqError::new(
                                        "42701",
                                        format!("column \"{}\" specified more than once", c),
                                    ));
                                }
                                pos.push(i);
                            }
                            None => {
                                return Err(PqError::new(
                                    "42703",
                                    format!(
                                        "column \"{}\" of relation \"{}\" does not exist",
                                        c, table.name
                                    ),
                                ));
                            }
                        }
                    }
                    pos
                };
                let mut scope = Scope::empty();
                scope.push(
                    table.effective_name(),
                    tab.cols.iter().map(|c| (c.name.clone(), c.ty)).collect(),
                );
                let empty_row: Vec<Value> = Vec::new();
                let ctx = Ctx {
                    scope: &scope,
                    row: &empty_row,
                    aggs: &[],
                    params,
                };
                let mut staged = Vec::new();
                let mut inserted = Vec::new();
                for row in rows {
                    if row.len() != colpos.len() {
                        return Err(PqError::new(
                            "42601",
                            format!(
                                "INSERT has {} expressions but targeted {} columns",
                                row.len(),
                                colpos.len()
                            ),
                        ));
                    }
                    if row.iter().any(|e| e.has_agg()) {
                        return Err(PqError::new(
                            "42803",
                            "aggregate functions are not allowed in INSERT VALUES",
                        ));
                    }
                    // evaluate in targeted order, then scatter to full width
                    let mut targeted = Vec::with_capacity(colpos.len());
                    for (e, &p) in row.iter().zip(colpos.iter()) {
                        let cdef = &tab.cols[p];
                        let v = coerce(eval(e, &ctx)?, cdef.ty)?;
                        if cdef.not_null && matches!(v, Value::Null) {
                            return Err(PqError::new(
                                "23502",
                                format!(
                                    "null value in column \"{}\" violates not-null constraint",
                                    cdef.name
                                ),
                            ));
                        }
                        targeted.push((p, v));
                    }
                    let mut vals: Vec<Value> = vec![Value::Null; ncols];
                    for (p, v) in targeted {
                        vals[p] = v;
                    }
                    // columns left out must tolerate NULL
                    for (i, cdef) in tab.cols.iter().enumerate() {
                        if cdef.not_null && matches!(vals[i], Value::Null) && !colpos.contains(&i) {
                            return Err(PqError::new(
                                "23502",
                                format!(
                                    "null value in column \"{}\" violates not-null constraint",
                                    cdef.name
                                ),
                            ));
                        }
                    }
                    staged.push(vals);
                }
                // statement is atomic: nothing is logged or inserted until
                // every row evaluated successfully
                let base = tab.next_rowid;
                let recs: Vec<WalRec> = staged
                    .iter()
                    .enumerate()
                    .map(|(i, cells)| WalRec::Insert {
                        rowid: base + i as u64,
                        cells: cells.clone(),
                    })
                    .collect();
                self.log_txn(&[(table.name.clone(), recs)])?;
                for cells in staged {
                    let id = tab.next_rowid;
                    tab.next_rowid += 1;
                    // secondary index maintenance (non-null keys only)
                    for ix in tab.indexes.values_mut() {
                        if let Some(k) = IKey::of(&cells[ix.col]) {
                            ix.insert(&k, id);
                        }
                    }
                    tab.push_row(id, cells.clone());
                    inserted.push(cells);
                }
                let n = inserted.len();
                if n > 0 {
                    // advance the table version: concurrent explicit
                    // transactions snapshot this version at BEGIN; without
                    // the bump their OCC validation would pass and their
                    // wholesale swap would silently swallow these rows
                    tab.version += 1;
                }
                self.mark_dirty(&table.name);
                if returning.is_empty() {
                    Ok(StmtResult::Tag(format!("INSERT 0 {}", n)))
                } else {
                    let (cols, rows) = project_returning(returning, &scope, &inserted, params)?;
                    Ok(StmtResult::Rows {
                        cols,
                        rows,
                        tag: format!("INSERT 0 {}", n),
                    })
                }
            }
            Stmt::Update {
                table,
                sets,
                where_,
                returning,
            } => {
                if table.as_of.is_some() {
                    return Err(PqError::new(
                        "0A000",
                        "AS OF is not allowed on a UPDATE target",
                    ));
                }

                let mut t = self.tables.write().unwrap();
                let tab = match t.get_mut(&table.name) {
                    Some(t) => t,
                    None => return Err(PqError::undefined_table(&table.name)),
                };
                let mut scope = Scope::empty();
                scope.push(
                    table.effective_name(),
                    tab.cols.iter().map(|c| (c.name.clone(), c.ty)).collect(),
                );
                let not_nulls: Vec<(String, bool)> = tab
                    .cols
                    .iter()
                    .map(|c| (c.name.clone(), c.not_null))
                    .collect();
                if let Some(w) = where_ {
                    if w.has_agg() {
                        return Err(PqError::new(
                            "42803",
                            "aggregate functions are not allowed in WHERE",
                        ));
                    }
                }
                let mut updated = Vec::new();
                let mut n = 0usize;
                let mut plan: Vec<(usize, Vec<Value>)> = Vec::new();
                for i in 0..tab.nrows() {
                    let row = tab.row(i);
                    let ctx = Ctx {
                        scope: &scope,
                        row: &row,
                        aggs: &[],
                        params,
                    };
                    let matches = match where_ {
                        Some(w) => matches!(eval(w, &ctx)?, Value::Bool(true)),
                        None => true,
                    };
                    if !matches {
                        continue;
                    }
                    let mut new_row = row.clone();
                    for (col, e) in sets {
                        let idx = scope.resolve(col)?;
                        let cdef_ty = scope.col_ty(idx);
                        let v = coerce(eval(e, &ctx)?, cdef_ty)?;
                        let not_null = not_nulls
                            .iter()
                            .any(|(n, nn)| n == col && *nn);
                        if not_null && matches!(v, Value::Null) {
                            return Err(PqError::new(
                                "23502",
                                format!(
                                    "null value in column \"{}\" violates not-null constraint",
                                    col
                                ),
                            ));
                        }
                        new_row[idx] = v;
                    }
                    plan.push((i, new_row.clone()));
                    updated.push(new_row);
                    n += 1;
                }
                // statement is atomic: WAL first, then apply the plan
                let recs: Vec<WalRec> = plan
                    .iter()
                    .map(|(i, cells)| WalRec::Update {
                        rowid: tab.rowids[*i],
                        cells: cells.clone(),
                    })
                    .collect();
                self.log_txn(&[(table.name.clone(), recs)])?;
                for (i, cells) in plan {
                    let rowid = tab.rowids[i];
                    // secondary index maintenance for changed columns
                    for ix in tab.indexes.values_mut() {
                        let c = ix.col;
                        let old_k = IKey::of(&tab.columns[c][i]);
                        let new_k = IKey::of(&cells[c]);
                        if old_k != new_k {
                            if let Some(k) = &old_k {
                                ix.remove(k, rowid);
                            }
                            if let Some(k) = &new_k {
                                ix.insert(k, rowid);
                            }
                        }
                    }
                    for (c, v) in cells.into_iter().enumerate() {
                        tab.columns[c][i] = v;
                    }
                }
                if n > 0 {
                    tab.version += 1;
                }
                self.mark_dirty(&table.name);
                if returning.is_empty() {
                    Ok(StmtResult::Tag(format!("UPDATE {}", n)))
                } else {
                    let (cols, rows) = project_returning(returning, &scope, &updated, params)?;
                    Ok(StmtResult::Rows {
                        cols,
                        rows,
                        tag: format!("UPDATE {}", n),
                    })
                }
            }
            Stmt::Delete {
                table,
                where_,
                returning,
            } => {
                if table.as_of.is_some() {
                    return Err(PqError::new(
                        "0A000",
                        "AS OF is not allowed on a DELETE target",
                    ));
                }

                let mut t = self.tables.write().unwrap();
                let tab = match t.get_mut(&table.name) {
                    Some(t) => t,
                    None => return Err(PqError::undefined_table(&table.name)),
                };
                let mut scope = Scope::empty();
                scope.push(
                    table.effective_name(),
                    tab.cols.iter().map(|c| (c.name.clone(), c.ty)).collect(),
                );
                if let Some(w) = where_ {
                    if w.has_agg() {
                        return Err(PqError::new(
                            "42803",
                            "aggregate functions are not allowed in WHERE",
                        ));
                    }
                }
                let mut deleted = Vec::new();
                let mut del_idx: Vec<usize> = Vec::new();
                for i in 0..tab.nrows() {
                    let row = tab.row(i);
                    let ctx = Ctx {
                        scope: &scope,
                        row: &row,
                        aggs: &[],
                        params,
                    };
                    let matches = match where_ {
                        Some(w) => matches!(eval(w, &ctx)?, Value::Bool(true)),
                        None => true,
                    };
                    if matches {
                        del_idx.push(i);
                        deleted.push(row);
                    }
                }
                let n = deleted.len();
                // statement is atomic: WAL first, then remove
                let recs: Vec<WalRec> = del_idx
                    .iter()
                    .map(|i| WalRec::Delete { rowid: tab.rowids[*i] })
                    .collect();
                self.log_txn(&[(table.name.clone(), recs)])?;
                if !del_idx.is_empty() {
                    // secondary index maintenance (rowids are stable, so this
                    // must happen before compaction renumbers positions)
                    for i in &del_idx {
                        let rowid = tab.rowids[*i];
                        for ix in tab.indexes.values_mut() {
                            if let Some(k) = IKey::of(&tab.columns[ix.col][*i]) {
                                ix.remove(&k, rowid);
                            }
                        }
                    }
                    let mut keep = vec![true; tab.nrows()];
                    for i in &del_idx {
                        keep[*i] = false;
                    }
                    tab.compact(&keep);
                }
                if n > 0 {
                    tab.version += 1;
                }
                self.mark_dirty(&table.name);
                if returning.is_empty() {
                    Ok(StmtResult::Tag(format!("DELETE {}", n)))
                } else {
                    let (cols, rows) = project_returning(returning, &scope, &deleted, params)?;
                    Ok(StmtResult::Rows {
                        cols,
                        rows,
                        tag: format!("DELETE {}", n),
                    })
                }
            }
            Stmt::AlterAddColumn { table, if_exists, if_not_exists, col } => {
                let mut t = self.tables.write().unwrap_or_else(|e| e.into_inner());
                let tab = match t.get_mut(table.as_str()) {
                    Some(t) => t,
                    None => {
                        if *if_exists {
                            return Ok(StmtResult::Tag("ALTER TABLE".into()));
                        }
                        return Err(PqError::undefined_table(table));
                    }
                };
                if tab.cols.iter().any(|c| c.name == col.name) {
                    if *if_not_exists {
                        return Ok(StmtResult::Tag("ALTER TABLE".into()));
                    }
                    return Err(PqError::new(
                        "42701",
                        format!(
                            "column \"{}\" of relation \"{}\" already exists",
                            col.name, table
                        ),
                    ));
                }
                if let Some(p) = &self.persist {
                    let mut cols: Vec<(String, Ty, bool)> = tab
                        .cols
                        .iter()
                        .map(|c| (c.name.clone(), c.ty, c.not_null))
                        .collect();
                    cols.push((col.name.clone(), col.ty, col.not_null));
                    p.alter_add(table, &cols, tab.next_rowid, tab.nrows() as u64)
                        .map_err(|e| PqError::new("58030", format!("alter: {}", e)))?;
                }
                // in-RAM: append the NULL-filled column (disk parts are
                // padded at load; no rewrite ever happens)
                tab.cols.push(col.clone());
                tab.columns.push(vec![Value::Null; tab.nrows()]);
                tab.version += 1;
                drop(t);
                self.mark_dirty(table);
                Ok(StmtResult::Tag("ALTER TABLE".into()))
            }
            // COPY is driven by the wire layer (needs the copy-data stream);
            // these arms exist for exhaustiveness and are never reached.
            Stmt::CreateSnapshot { name } => {
                let p = self.persist.as_ref().ok_or_else(|| {
                    PqError::new(
                        "0A000",
                        "snapshots require durable mode (a --data store)",
                    )
                })?;
                p.create_snapshot(name)
                    .map_err(|e| {
                        let code = if e.starts_with("snapshot ") { "42710" } else { "58030" };
                        PqError::new(code, e)
                    })?;
                Ok(StmtResult::Tag("CREATE SNAPSHOT".into()))
            }
            Stmt::DropSnapshot { name, if_exists } => {
                let p = self.persist.as_ref().ok_or_else(|| {
                    PqError::new(
                        "0A000",
                        "snapshots require durable mode (a --data store)",
                    )
                })?;
                p.drop_snapshot(name, *if_exists)
                    .map_err(|e| {
                        let code = if e.starts_with("snapshot ") { "42704" } else { "58030" };
                        PqError::new(code, e)
                    })?;
                Ok(StmtResult::Tag("DROP SNAPSHOT".into()))
            }
            Stmt::CopyIn { .. } => Err(PqError::new(
                "0A000",
                "COPY FROM STDIN must go through the simple query protocol",
            )),
            Stmt::CopyOut { .. } => Err(PqError::new(
                "0A000",
                "COPY TO STDOUT must go through the simple query protocol",
            )),
            Stmt::Checkpoint => {
                // full materialization of every table into fresh parts +
                // manifest CAS commits, under the tables write lock
                let g = self.tables.write().unwrap_or_else(|e| e.into_inner());
                if let Some(p) = &self.persist {
                    p.checkpoint(&g)
                        .map_err(|e| PqError::new("58030", format!("checkpoint: {}", e)))?;
                }
                Ok(StmtResult::Tag("CHECKPOINT".into()))
            }
            Stmt::Begin => Ok(StmtResult::Tag("BEGIN".into())),
            Stmt::Commit => Ok(StmtResult::Tag("COMMIT".into())),
            Stmt::Rollback => Ok(StmtResult::Tag("ROLLBACK".into())),
            Stmt::Select(sel) => self.select(sel, params),
            Stmt::Union {
                arms,
                all,
                order_by,
                limit,
                offset,
            } => {
                let mut cols: Option<Vec<(String, Ty)>> = None;
                let mut rows: Vec<Vec<Value>> = Vec::new();
                for arm in arms {
                    if let StmtResult::Rows {
                        cols: c,
                        rows: r,
                        ..
                    } = self.select(arm, params)?
                    {
                        if cols.is_none() {
                            cols = Some(c);
                        }
                        rows.extend(r);
                    }
                }
                let cols = cols.unwrap_or_default();
                if !*all {
                    // UNION (distinct): drop duplicate rows, keep first
                    let mut seen = std::collections::HashSet::new();
                    rows.retain(|r| seen.insert(r.iter().map(gkey).collect::<Vec<_>>()));
                }
                // ORDER BY over the combined set (positional ints or
                // first-arm column names)
                if !order_by.is_empty() {
                    let mut idx: Vec<usize> = (0..rows.len()).collect();
                    let keys: Vec<Vec<Value>> = rows
                        .iter()
                        .map(|r| {
                            order_by
                                .iter()
                                .map(|(e, _, _)| match e {
                                    Expr::Lit(Value::Int(n))
                                        if *n >= 1 && (*n as usize) <= r.len() =>
                                    {
                                        r[(*n - 1) as usize].clone()
                                    }
                                    Expr::Col(c) => {
                                        if let Some(p) = cols.iter().position(|(n, _)| n == c) {
                                            r[p].clone()
                                        } else {
                                            Value::Null
                                        }
                                    }
                                    _ => Value::Null,
                                })
                                .collect()
                        })
                        .collect();
                    let descs: Vec<(bool, bool)> =
                        order_by.iter().map(|(_, d, n)| (*d, *n)).collect();
                    idx.sort_by(|&a, &b| {
                        for (i, (desc, nf)) in descs.iter().enumerate() {
                            match order_cmp(&keys[a][i], &keys[b][i], *desc, *nf) {
                                Ok(std::cmp::Ordering::Equal) => continue,
                                Ok(o) => return o,
                                Err(_) => return std::cmp::Ordering::Equal,
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                    rows = idx.into_iter().map(|i| rows[i].clone()).collect();
                }
                // LIMIT / OFFSET
                let empty_scope = Scope::empty();
                let ectx = Ctx {
                    scope: &empty_scope,
                    row: &[],
                    aggs: &[],
                    params,
                };
                let mut start = 0usize;
                if let Some(o) = offset {
                    match eval(o, &ectx)? {
                        Value::Int(n) if n >= 0 => start = n as usize,
                        Value::Float(f) if f >= 0.0 => start = f as usize,
                        Value::Null => {}
                        v => {
                            return Err(PqError::wrong_type(format!(
                                "OFFSET must be an integer, got {}",
                                type_name(&v)
                            )))
                        }
                    }
                }
                if start > rows.len() {
                    start = rows.len();
                }
                rows.drain(..start);
                if let Some(l) = limit {
                    match eval(l, &ectx)? {
                        Value::Int(n) if n >= 0 => rows.truncate(n as usize),
                        Value::Float(f) if f >= 0.0 => rows.truncate(f as usize),
                        Value::Null => {}
                        v => {
                            return Err(PqError::wrong_type(format!(
                                "LIMIT must be an integer, got {}",
                                type_name(&v)
                            )))
                        }
                    }
                }
                Ok(StmtResult::Rows {
                    cols,
                    rows,
                    tag: "SELECT".into(),
                })
            }
        }
    }

    // ---------------------------------------------------------------- SELECT --

    /// True if any expression in this select contains a scalar subquery.
    fn has_subquery(sel: &SelectStmt) -> bool {
        fn walk(e: &Expr) -> bool {
            match e {
                Expr::Subquery(_) => true,
                Expr::Neg(a) | Expr::Not(a) | Expr::IsNull(a, _) | Expr::Cast(a, _) => walk(a),
                Expr::Like(a, b, _) | Expr::Between(a, b, _, _) => walk(a) || walk(b),
                Expr::InList(a, l, _) => walk(a) || l.iter().any(walk),
                Expr::Bin(a, _, b) => walk(a) || walk(b),
                Expr::Case {
                    operand,
                    when_then,
                    else_,
                } => {
                    operand.as_ref().is_some_and(|o| walk(o))
                        || when_then.iter().any(|(w, t)| walk(w) || walk(t))
                        || else_.as_ref().is_some_and(|e| walk(e))
                }
                Expr::Func(_, args) => args.iter().any(walk),
                Expr::Agg(_, a, _) => walk(a),
                _ => false,
            }
        }
        sel.items.iter().any(|i| match i {
            SelectItem::Star => false,
            SelectItem::Expr(e, _) => walk(e),
        }) || sel.where_.as_ref().is_some_and(walk)
            || sel.having.as_ref().is_some_and(walk)
            || sel.group_by.iter().any(walk)
            || sel.order_by.iter().any(|(e, _, _)| walk(e))
            || sel.limit.as_ref().is_some_and(walk)
            || sel.offset.as_ref().is_some_and(walk)
    }

    /// True if any SELECT-list item contains a scalar subquery.
    fn items_have_subquery(sel: &SelectStmt) -> bool {
        fn walk(e: &Expr) -> bool {
            match e {
                Expr::Subquery(_) => true,
                Expr::Neg(a) | Expr::Not(a) | Expr::IsNull(a, _) | Expr::Cast(a, _) => walk(a),
                Expr::Like(a, b, _) | Expr::Between(a, b, _, _) => walk(a) || walk(b),
                Expr::InList(a, l, _) => walk(a) || l.iter().any(walk),
                Expr::Bin(a, _, b) => walk(a) || walk(b),
                Expr::Case {
                    operand,
                    when_then,
                    else_,
                } => {
                    operand.as_ref().is_some_and(|o| walk(o))
                        || when_then.iter().any(|(w, t)| walk(w) || walk(t))
                        || else_.as_ref().is_some_and(|x| walk(x))
                }
                Expr::Func(_, args) => args.iter().any(walk),
                Expr::Agg(_, a, _) => walk(a),
                _ => false,
            }
        }
        sel.items
            .iter()
            .any(|i| matches!(i, SelectItem::Expr(e, _) if walk(e)))
    }

    /// Rewrites gating-clause subqueries (WHERE/HAVING/GROUP BY/ORDER BY/
    /// LIMIT/OFFSET) into their computed literal values. SELECT-list items
    /// are deferred: with an empty row set their values are never needed,
    /// and deferring keeps psql's correlated probes over empty catalog
    /// relations (pg_policy et al.) well-formed instead of erroring.
    fn rewrite_subqueries(&self, sel: &SelectStmt, params: &[Value]) -> R<SelectStmt> {
        let mut out = sel.clone();
        if let Some(w) = &mut out.where_ {
            *w = self.rewrite_expr_subs(w, params)?;
        }
        if let Some(h) = &mut out.having {
            *h = self.rewrite_expr_subs(h, params)?;
        }
        for g in &mut out.group_by {
            *g = self.rewrite_expr_subs(g, params)?;
        }
        for (e, _, _) in &mut out.order_by {
            *e = self.rewrite_expr_subs(e, params)?;
        }
        if let Some(l) = &mut out.limit {
            *l = self.rewrite_expr_subs(l, params)?;
        }
        if let Some(o) = &mut out.offset {
            *o = self.rewrite_expr_subs(o, params)?;
        }
        Ok(out)
    }

    fn rewrite_expr_subs(&self, e: &Expr, params: &[Value]) -> R<Expr> {
        Ok(match e {
            Expr::Subquery(sub) => {
                let res = self.execute(&Stmt::Select((**sub).clone()), params)?;
                match res {
                    StmtResult::Rows { rows, .. } => {
                        if rows.len() > 1 {
                            return Err(PqError::new(
                                "21000",
                                "more than one row returned by a subquery used as an expression",
                            ));
                        }
                        Expr::Lit(
                            rows.first()
                                .and_then(|r| r.first().cloned())
                                .unwrap_or(Value::Null),
                        )
                    }
                    StmtResult::Tag(_) => Expr::Lit(Value::Null),
                }
            }
            Expr::Lit(_) | Expr::Col(_) | Expr::QCol(..) | Expr::Param(_) | Expr::AggRef(_) => {
                e.clone()
            }
            Expr::Neg(a) => Expr::Neg(Box::new(self.rewrite_expr_subs(a, params)?)),
            Expr::Not(a) => Expr::Not(Box::new(self.rewrite_expr_subs(a, params)?)),
            Expr::IsNull(a, n) => {
                Expr::IsNull(Box::new(self.rewrite_expr_subs(a, params)?), *n)
            }
            Expr::Cast(a, t) => Expr::Cast(Box::new(self.rewrite_expr_subs(a, params)?), *t),
            Expr::Like(a, b, n) => Expr::Like(
                Box::new(self.rewrite_expr_subs(a, params)?),
                Box::new(self.rewrite_expr_subs(b, params)?),
                *n,
            ),
            Expr::InList(a, l, n) => Expr::InList(
                Box::new(self.rewrite_expr_subs(a, params)?),
                l.iter()
                    .map(|x| self.rewrite_expr_subs(x, params))
                    .collect::<R<Vec<_>>>()?,
                *n,
            ),
            Expr::Between(a, lo, hi, n) => Expr::Between(
                Box::new(self.rewrite_expr_subs(a, params)?),
                Box::new(self.rewrite_expr_subs(lo, params)?),
                Box::new(self.rewrite_expr_subs(hi, params)?),
                *n,
            ),
            Expr::Bin(a, op, b) => Expr::Bin(
                Box::new(self.rewrite_expr_subs(a, params)?),
                op,
                Box::new(self.rewrite_expr_subs(b, params)?),
            ),
            Expr::Case {
                operand,
                when_then,
                else_,
            } => Expr::Case {
                operand: match operand {
                    Some(o) => Some(Box::new(self.rewrite_expr_subs(o, params)?)),
                    None => None,
                },
                when_then: when_then
                    .iter()
                    .map(|(w, t)| {
                        Ok((
                            self.rewrite_expr_subs(w, params)?,
                            self.rewrite_expr_subs(t, params)?,
                        ))
                    })
                    .collect::<R<Vec<_>>>()?,
                else_: match else_ {
                    Some(x) => Some(Box::new(self.rewrite_expr_subs(x, params)?)),
                    None => None,
                },
            },
            Expr::Func(f, args) => Expr::Func(
                f.clone(),
                args.iter()
                    .map(|a| self.rewrite_expr_subs(a, params))
                    .collect::<R<Vec<_>>>()?,
            ),
            Expr::Agg(k, a, d) => {
                Expr::Agg(*k, Box::new(self.rewrite_expr_subs(a, params)?), *d)
            }
        })
    }

    fn select(&self, sel: &SelectStmt, params: &[Value]) -> R<StmtResult> {
        // scalar subqueries are evaluated eagerly, once (v1.1: uncorrelated
        // only — a correlated outer reference fails resolution with a clean
        // 42703 rather than silently producing wrong results)
        let sel_owned;
        let sel = if Self::has_subquery(sel) {
            sel_owned = self.rewrite_subqueries(sel, params)?;
            &sel_owned
        } else {
            sel
        };
        if let Some(w) = &sel.where_ {
            if w.has_agg() {
                return Err(PqError::new(
                    "42803",
                    "aggregate functions are not allowed in WHERE",
                ));
            }
        }
        // vectorized fast path (falls back to the reference path below for
        // any shape it doesn't take; the reference is the differential oracle);
        // SELECT-list subqueries are deferred, so they force the reference path;
        // AS OF reads reconstruct ephemeral tables — reference path only
        let has_asof = sel
            .from_items
            .iter()
            .any(|fi| fi.base.as_of.is_some() || fi.joins.iter().any(|j| j.table.as_of.is_some()));
        if VECTORIZED.load(Ordering::Relaxed) && !Self::items_have_subquery(sel) && !has_asof {
            if let Some(r) = self.select_vectorized(sel, params)? {
                return Ok(r);
            }
        }
        let (scope, rows) = self.scan_from(&sel.from_items, sel.where_.as_ref(), params)?;

        // Expand stars up-front (needed for both description and grouping).
        let items: Vec<(Expr, Option<String>)> = expand_items(&sel.items, &scope)?;

        // Column description (inference over the original expressions).
        let mut out_cols = Vec::new();
        for (e, alias) in &items {
            let name = match alias {
                Some(a) => a.clone(),
                None => expr_name(e),
            };
            out_cols.push((name, infer_ty(e, &scope)));
        }

        let group_by: Vec<Expr> = normalize_group_by(sel, &items, &out_cols, &scope);

        // Split aggregate calls out of items, HAVING and ORDER BY.
        let mut specs: Vec<AggSpec> = Vec::new();
        let mut item_t: Vec<(Expr, Option<String>)> = items
            .iter()
            .map(|(e, a)| (split_aggs(e, &mut specs), a.clone()))
            .collect();
        let having_t = sel.having.as_ref().map(|h| split_aggs(h, &mut specs));
        let order_t: Vec<(Expr, bool, bool)> = sel
            .order_by
            .iter()
            .map(|(e, d, nf)| (split_aggs(e, &mut specs), *d, *nf))
            .collect();

        let agg_mode = !specs.is_empty() || !group_by.is_empty();

        // deferred SELECT-list subquery evaluation: only when their values
        // will actually be observed (rows exist, or a group-less aggregate
        // produces its single output row even over empty input)
        if Self::items_have_subquery(sel)
            && (!rows.is_empty() || (!specs.is_empty() && group_by.is_empty()))
        {
            for (ie, _) in &mut item_t {
                *ie = self.rewrite_expr_subs(ie, params)?;
            }
        }

        let mut out_rows: Vec<Vec<Value>> = Vec::new();
        let mut order_keys: Vec<Vec<Value>> = Vec::new();

        if !agg_mode {
            // ---------------- plain projection ----------------
            for row in &rows {
                let ctx = Ctx {
                    scope: &scope,
                    row,
                    aggs: &[],
                    params,
                };
                let mut r = Vec::with_capacity(item_t.len());
                for (e, _) in &item_t {
                    r.push(eval(e, &ctx)?);
                }
                if !order_t.is_empty() {
                    let mut keys = Vec::with_capacity(order_t.len());
                    for (e, _, _) in &order_t {
                        keys.push(match e {
                            Expr::Lit(Value::Int(n))
                                if *n >= 1 && (*n as usize) <= r.len() =>
                            {
                                r[(*n - 1) as usize].clone()
                            }
                            // output alias wins over source column (PG)
                            Expr::Col(c) => {
                                if let Some(pos) = out_cols.iter().position(|(n, _)| n == c) {
                                    r[pos].clone()
                                } else {
                                    eval(e, &ctx)?
                                }
                            }
                            _ => eval(e, &ctx)?,
                        });
                    }
                    order_keys.push(keys);
                }
                out_rows.push(r);
            }
        } else {
            // ---------------- aggregation ----------------
            // GROUP BY coverage check: every plain column in the projection
            // and ORDER BY must appear in GROUP BY.
            let mut to_check: Vec<&Expr> = Vec::new();
            for (e, _) in &item_t {
                to_check.push(e);
            }
            if let Some(h) = &having_t {
                to_check.push(h);
            }
            for e in to_check {
                if let Some(c) = uncovered_col(e, &group_by) {
                    return Err(PqError::new(
                        "42803",
                        format!(
                            "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                            col_display(c)
                        ),
                    ));
                }
            }

            // group rows by key
            let mut index: HashMap<Vec<GK>, usize> = HashMap::new();
            let mut groups: Vec<Vec<Vec<Value>>> = Vec::new();
            if group_by.is_empty() {
                groups.push(rows.clone()); // single group, possibly empty
            } else {
                for row in &rows {
                    let ctx = Ctx {
                        scope: &scope,
                        row,
                        aggs: &[],
                        params,
                    };
                    let mut key = Vec::with_capacity(group_by.len());
                    for g in &group_by {
                        key.push(gkey(&eval(g, &ctx)?));
                    }
                    let gi = *index.entry(key).or_insert_with(|| {
                        groups.push(Vec::new());
                        groups.len() - 1
                    });
                    groups[gi].push(row.clone());
                }
            }

            for grows in &groups {
                let empty_row: Vec<Value> = Vec::new();
                let rep_row = grows.first().map_or(&empty_row[..], |v| &v[..]);
                // compute aggregates for this group
                let mut agg_vals: Vec<Value> = Vec::with_capacity(specs.len());
                for spec in &specs {
                    agg_vals.push(compute_agg(spec, grows, &scope, params)?);
                }
                let ctx = Ctx {
                    scope: &scope,
                    row: rep_row,
                    aggs: &agg_vals,
                    params,
                };
                // HAVING
                if let Some(h) = &having_t {
                    match eval(h, &ctx)? {
                        Value::Bool(true) => {}
                        _ => continue,
                    }
                }
                let r: R<Vec<Value>> = item_t.iter().map(|(e, _)| eval(e, &ctx)).collect();
                let r = r?;
                if !order_t.is_empty() {
                    let mut keys = Vec::with_capacity(order_t.len());
                    for (e, _, _) in &order_t {
                        keys.push(match e {
                            Expr::Lit(Value::Int(n))
                                if *n >= 1 && (*n as usize) <= r.len() =>
                            {
                                r[(*n - 1) as usize].clone()
                            }
                            Expr::Col(c) => {
                                if let Some(pos) = out_cols.iter().position(|(n, _)| n == c) {
                                    r[pos].clone()
                                } else {
                                    eval(e, &ctx)?
                                }
                            }
                            _ => eval(e, &ctx)?,
                        });
                    }
                    order_keys.push(keys);
                }
                out_rows.push(r);
            }
        }

        // ---------------- DISTINCT ----------------
        if sel.distinct {
            let mut seen = std::collections::HashSet::new();
            let mut deduped = Vec::with_capacity(out_rows.len());
            for r in out_rows {
                let key: Vec<GK> = r.iter().map(gkey).collect();
                if seen.insert(key) {
                    deduped.push(r);
                }
            }
            out_rows = deduped;
        }

        // ---------------- ORDER BY ----------------
        if !order_t.is_empty() {
            let descs: Vec<(bool, bool)> =
                sel.order_by.iter().map(|(_, d, nf)| (*d, *nf)).collect();
            let mut idx: Vec<usize> = (0..out_rows.len()).collect();
            idx.sort_by(|&a, &b| {
                for (i, (desc, nf)) in descs.iter().enumerate() {
                    match order_cmp(&order_keys[a][i], &order_keys[b][i], *desc, *nf) {
                        Ok(std::cmp::Ordering::Equal) => continue,
                        Ok(o) => return o,
                        Err(_) => return std::cmp::Ordering::Equal,
                    }
                }
                std::cmp::Ordering::Equal
            });
            out_rows = idx.into_iter().map(|i| out_rows[i].clone()).collect();
        }

        // ---------------- LIMIT / OFFSET ----------------
        let mut start = 0usize;
        if let Some(o) = &sel.offset {
            match eval(o, &Ctx { scope: &scope, row: &[], aggs: &[], params })? {
                Value::Int(n) if n >= 0 => start = n as usize,
                _ => return Err(PqError::new("2201X", "OFFSET must be a non-negative integer")),
            }
        }
        if start > 0 {
            out_rows = if start >= out_rows.len() {
                Vec::new()
            } else {
                out_rows.split_off(start)
            };
        }
        if let Some(l) = &sel.limit {
            match eval(l, &Ctx { scope: &scope, row: &[], aggs: &[], params })? {
                Value::Int(n) if n >= 0 => {
                    out_rows.truncate(n as usize);
                }
                _ => return Err(PqError::new("2201W", "LIMIT must be a non-negative integer")),
            }
        }
        let tag = format!("SELECT {}", out_rows.len());
        Ok(StmtResult::Rows {
            cols: out_cols,
            rows: out_rows,
            tag,
        })
    }

    /// Column description for a SELECT (used by Describe messages).
    pub fn describe(&self, sel: &SelectStmt) -> R<Vec<(String, Ty)>> {
        let mut scope = Scope::empty();
        self.build_scope_schema(&sel.from_items, &mut scope)?;
        let items = expand_items(&sel.items, &scope)?;
        Ok(items
            .iter()
            .map(|(e, a)| {
                (
                    match a {
                        Some(x) => x.clone(),
                        None => expr_name(e),
                    },
                    infer_ty(e, &scope),
                )
            })
            .collect())
    }

    fn build_scope_schema(&self, from_items: &[FromItem], scope: &mut Scope) -> R<()> {
        for item in from_items {
            let (cols, _) = self.load_table(&item.base)?;
            scope.push(item.base.effective_name(), apply_col_aliases(cols, &item.base));
            for j in &item.joins {
                let (jcols, _) = self.load_table(&j.table)?;
                scope.push(j.table.effective_name(), apply_col_aliases(jcols, &j.table));
            }
        }
        Ok(())
    }

    /// Set-returning functions usable in FROM. Only generate_series is
    /// supported (integers); anything else errors cleanly.
    fn load_srf(
        &self,
        name: &str,
        args: &[Expr],
        alias: Option<&str>,
    ) -> R<TableShape> {
        let bare = name.rsplit('.').next().unwrap_or(name);
        if bare != "generate_series" {
            return Err(PqError::new(
                "42P01",
                format!("set-returning function \"{}\" is not supported in FROM", bare),
            ));
        }
        if args.len() != 2 {
            return Err(PqError::new(
                "42883",
                "generate_series requires two integer arguments",
            ));
        }
        let empty = Scope::empty();
        let ctx = Ctx {
            scope: &empty,
            row: &[],
            aggs: &[],
            params: &[],
        };
        let lo = match eval(&args[0], &ctx)? {
            Value::Int(n) => n,
            v => {
                return Err(PqError::wrong_type(format!(
                    "generate_series bounds must be integers, got {}",
                    type_name(&v)
                )))
            }
        };
        let hi = match eval(&args[1], &ctx)? {
            Value::Int(n) => n,
            v => {
                return Err(PqError::wrong_type(format!(
                    "generate_series bounds must be integers, got {}",
                    type_name(&v)
                )))
            }
        };
        let rows = (lo..=hi).map(|n| vec![Value::Int(n)]).collect();
        // a bare alias renames the output column (PG behaviour)
        let colname = alias.unwrap_or("generate_series").to_string();
        Ok((vec![(colname, Ty::Int)], rows))
    }

    fn load_table(&self, tr: &TableRef) -> R<TableShape> {
        // set-returning function in FROM position
        if let Some(args) = &tr.func_args {
            if tr.as_of.is_some() {
                return Err(PqError::new(
                    "0A000",
                    "AS OF is not supported on table functions",
                ));
            }
            return self.load_srf(&tr.name, args, tr.alias.as_deref());
        }
        // time travel (v2.0): reconstruct from the durable layer, never the
        // live in-RAM state
        if let Some(asof) = &tr.as_of {
            return self.load_table_as_of(tr, asof);
        }
        // snapshot listing (before the catalog: it shadows nothing real)
        if tr.name == "puddle_snapshots" && tr.func_args.is_none() {
            if let Some(p) = self.reader() {
                let list = p.snapshot_list();
                let rows: Vec<Vec<Value>> = list
                    .into_iter()
                    .map(|(n, l, us, k)| {
                        vec![
                            Value::Text(n),
                            Value::Int(l as i64),
                            Value::Timestamp(us),
                            Value::Int(k as i64),
                        ]
                    })
                    .collect();
                let cols = vec![
                    ("name".to_string(), Ty::Text),
                    ("lsn".to_string(), Ty::Int),
                    ("created_at".to_string(), Ty::Timestamp),
                    ("pinned_tables".to_string(), Ty::Int),
                ];
                return Ok((cols, rows));
            }
        }
        let t = self.tables.read().unwrap();
        match t.get(&tr.name) {
            Some(tab) => Ok((
                tab.cols.iter().map(|c| (c.name.clone(), c.ty)).collect(),
                (0..tab.nrows()).map(|i| tab.row(i)).collect(),
            )),
            None => match crate::catalog::lookup(&tr.name, &t) {
                Some((cols, rows)) => Ok((cols, rows)),
                None => Err(PqError::undefined_table(&tr.name)),
            },
        }
    }

    /// Reconstructs one table's historical state from the durable layer.
    fn load_table_as_of(&self, tr: &TableRef, asof: &AsOf) -> R<TableShape> {
        let p = self.reader().ok_or_else(|| {
            PqError::new(
                "0A000",
                "AS OF time travel requires durable mode (a --data store)",
            )
        })?;
        let target = match asof {
            AsOf::Snapshot(name) => crate::persist::AsOfTarget::Snapshot(name.clone()),
            AsOf::Lsn(l) => crate::persist::AsOfTarget::Lsn(*l),
        };
        let tab = p.read_table_as_of(&tr.name, &target).map_err(|e| {
            let code = if e.starts_with("snapshot ") {
                "42704"
            } else if e.starts_with("LSN ") {
                "22023"
            } else {
                "58030"
            };
            PqError::new(code, e)
        })?;
        let tab = tab.ok_or_else(|| match asof {
            AsOf::Snapshot(name) => PqError::new(
                "42P01",
                format!(
                    "table \"{}\" did not exist at snapshot \"{}\"",
                    tr.name, name
                ),
            ),
            AsOf::Lsn(l) => PqError::new(
                "42P01",
                format!("table \"{}\" did not exist at LSN {}", tr.name, l),
            ),
        })?;
        Ok((
            tab.cols.iter().map(|c| (c.name.clone(), c.ty)).collect(),
            (0..tab.nrows()).map(|i| tab.row(i)).collect(),
        ))
    }

    fn scan_from(
        &self,
        from_items: &[FromItem],
        where_: Option<&Expr>,
        params: &[Value],
    ) -> R<(Scope, Vec<Vec<Value>>)> {
        let mut scope = Scope::empty();
        let mut rows: Vec<Vec<Value>> = vec![Vec::new()];
        for item in from_items {
            let (cols, trows) = self.load_table(&item.base)?;
            scope.push(
                item.base.effective_name(),
                apply_col_aliases(cols, &item.base),
            );
            // cross join with the base table
            let mut out = Vec::with_capacity(rows.len() * trows.len().max(1));
            for l in &rows {
                for r in &trows {
                    let mut c = l.clone();
                    c.extend(r.iter().cloned());
                    out.push(c);
                }
            }
            rows = out;
            for j in &item.joins {
                let (jcols, jrows) = self.load_table(&j.table)?;
                let rwidth = jcols.len();
                scope.push(
                    j.table.effective_name(),
                    apply_col_aliases(jcols, &j.table),
                );
                let mut out = Vec::new();
                match j.kind {
                    JoinKind::Inner | JoinKind::Cross => {
                        for l in &rows {
                            for r in &jrows {
                                let mut c = l.clone();
                                c.extend(r.iter().cloned());
                                let ok = match &j.on {
                                    Some(e) => {
                                        let ctx = Ctx {
                                            scope: &scope,
                                            row: &c,
                                            aggs: &[],
                                            params,
                                        };
                                        matches!(eval(e, &ctx)?, Value::Bool(true))
                                    }
                                    None => true,
                                };
                                if ok {
                                    out.push(c);
                                }
                            }
                        }
                    }
                    JoinKind::Left => {
                        for l in &rows {
                            let mut matched = false;
                            for r in &jrows {
                                let mut c = l.clone();
                                c.extend(r.iter().cloned());
                                let ok = match &j.on {
                                    Some(e) => {
                                        let ctx = Ctx {
                                            scope: &scope,
                                            row: &c,
                                            aggs: &[],
                                            params,
                                        };
                                        matches!(eval(e, &ctx)?, Value::Bool(true))
                                    }
                                    None => true,
                                };
                                if ok {
                                    out.push(c);
                                    matched = true;
                                }
                            }
                            if !matched {
                                let mut c = l.clone();
                                c.extend(std::iter::repeat_n(Value::Null, rwidth));
                                out.push(c);
                            }
                        }
                    }
                }
                rows = out;
            }
        }
        // WHERE
        if let Some(w) = where_ {
            let mut out = Vec::new();
            for r in rows {
                let ctx = Ctx {
                    scope: &scope,
                    row: &r,
                    aggs: &[],
                    params,
                };
                if matches!(eval(w, &ctx)?, Value::Bool(true)) {
                    out.push(r);
                }
            }
            rows = out;
        }
        Ok((scope, rows))
    }
}

// ---------------------------------------------------------- RETURNING helper --

fn project_returning(
    items: &[SelectItem],
    scope: &Scope,
    rows: &[Vec<Value>],
    params: &[Value],
) -> R<TableShape> {
    let items = expand_items(items, scope)?;
    let cols = items
        .iter()
        .map(|(e, a)| {
            (
                match a {
                    Some(x) => x.clone(),
                    None => expr_name(e),
                },
                infer_ty(e, scope),
            )
        })
        .collect();
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let ctx = Ctx {
            scope,
            row,
            aggs: &[],
            params,
        };
        let mut r = Vec::with_capacity(items.len());
        for (e, _) in &items {
            r.push(eval(e, &ctx)?);
        }
        out.push(r);
    }
    Ok((cols, out))
}

pub fn expand_items(items: &[SelectItem], scope: &Scope) -> R<Vec<(Expr, Option<String>)>> {
    let mut out = Vec::new();
    for it in items {
        match it {
            SelectItem::Star => {
                if scope.tables.is_empty() {
                    return Err(PqError::syntax("SELECT * with no FROM clause"));
                }
                for t in &scope.tables {
                    for (c, _) in &t.cols {
                        // qualify only when the name is ambiguous across the
                        // FROM scope (matches PG star expansion; unambiguous
                        // names stay bare so GROUP BY/ORDER BY refs line up)
                        let ambiguous = scope
                            .tables
                            .iter()
                            .any(|o| !std::ptr::eq(o, t) && o.cols.iter().any(|(oc, _)| oc == c));
                        if ambiguous {
                            out.push((Expr::QCol(t.alias.clone(), c.clone()), None));
                        } else {
                            out.push((Expr::Col(c.clone()), None));
                        }
                    }
                }
            }
            SelectItem::Expr(e, a) => out.push((e.clone(), a.clone())),
        }
    }
    if out.is_empty() {
        return Err(PqError::syntax("SELECT list cannot be empty"));
    }
    Ok(out)
}

// ----------------------------------------------------------- aggregation ------

pub struct AggSpec {
    pub kind: AggKind,
    pub arg: Expr,
    pub distinct: bool,
}

/// Replaces aggregate calls with AggRef placeholders, collecting their specs.
pub fn split_aggs(e: &Expr, specs: &mut Vec<AggSpec>) -> Expr {
    match e {
        Expr::Agg(k, a, d) => {
            specs.push(AggSpec {
                kind: *k,
                arg: (**a).clone(),
                distinct: *d,
            });
            Expr::AggRef(specs.len() - 1)
        }
        Expr::Lit(_) | Expr::Col(_) | Expr::QCol(..) | Expr::Param(_) | Expr::AggRef(_) => {
            e.clone()
        }
        Expr::Neg(x) => Expr::Neg(Box::new(split_aggs(x, specs))),
        Expr::Not(x) => Expr::Not(Box::new(split_aggs(x, specs))),
        Expr::IsNull(x, n) => Expr::IsNull(Box::new(split_aggs(x, specs)), *n),
        Expr::Cast(x, t) => Expr::Cast(Box::new(split_aggs(x, specs)), *t),
        Expr::Like(a, b, n) => Expr::Like(
            Box::new(split_aggs(a, specs)),
            Box::new(split_aggs(b, specs)),
            *n,
        ),
        Expr::InList(a, l, n) => Expr::InList(
            Box::new(split_aggs(a, specs)),
            l.iter().map(|x| split_aggs(x, specs)).collect(),
            *n,
        ),
        Expr::Between(a, lo, hi, n) => Expr::Between(
            Box::new(split_aggs(a, specs)),
            Box::new(split_aggs(lo, specs)),
            Box::new(split_aggs(hi, specs)),
            *n,
        ),
        Expr::Bin(a, op, b) => Expr::Bin(
            Box::new(split_aggs(a, specs)),
            op,
            Box::new(split_aggs(b, specs)),
        ),
        Expr::Case {
            operand,
            when_then,
            else_,
        } => Expr::Case {
            operand: operand.as_ref().map(|o| Box::new(split_aggs(o, specs))),
            when_then: when_then
                .iter()
                .map(|(w, t)| (split_aggs(w, specs), split_aggs(t, specs)))
                .collect(),
            else_: else_.as_ref().map(|o| Box::new(split_aggs(o, specs))),
        },
        Expr::Func(f, args) => Expr::Func(
            f.clone(),
            args.iter().map(|a| split_aggs(a, specs)).collect(),
        ),
        // subqueries are rewritten to literals before this runs
        Expr::Subquery(_) => e.clone(),
    }
}

fn compute_agg(spec: &AggSpec, rows: &[Vec<Value>], scope: &Scope, params: &[Value]) -> R<Value> {
    let eval_arg = |row: &Vec<Value>| -> R<Value> {
        let ctx = Ctx {
            scope,
            row,
            aggs: &[],
            params,
        };
        eval(&spec.arg, &ctx)
    };
    let mut vals = Vec::with_capacity(rows.len());
    for r in rows {
        vals.push(eval_arg(r)?);
    }
    let refs: Vec<&Value> = vals.iter().collect();
    compute_agg_vals(spec, &refs)
}

/// Aggregate over pre-evaluated argument values (the vectorized hot path).
pub fn compute_agg_vals(spec: &AggSpec, vals: &[&Value]) -> R<Value> {
    let rows: &[&Value] = vals;
    match spec.kind {
        AggKind::CountStar => Ok(Value::Int(rows.len() as i64)),
        AggKind::Count => {
            if spec.distinct {
                let mut set = std::collections::HashSet::new();
                for r in rows {
                    let v: &Value = r;
                    if !matches!(v, Value::Null) {
                        set.insert(gkey(v));
                    }
                }
                Ok(Value::Int(set.len() as i64))
            } else {
                let mut n = 0i64;
                for r in rows {
                    if !matches!(r, Value::Null) {
                        n += 1;
                    }
                }
                Ok(Value::Int(n))
            }
        }
        AggKind::Sum => {
            let mut sum_i: i64 = 0;
            let mut sum_f: f64 = 0.0;
            let mut any = false;
            let mut is_float = false;
            let mut is_interval = false;
            let mut sum_iv_m: i32 = 0;
            let mut sum_iv_d: i32 = 0;
            let mut sum_iv_u: i64 = 0;
            for r in rows {
                let v: Value = (*r).clone();
                match v {
                    Value::Null => continue,
                    Value::Int(i) => {
                        sum_i = sum_i
                            .checked_add(i)
                            .ok_or_else(|| PqError::new("22003", "bigint out of range"))?;
                        any = true;
                    }
                    Value::Float(f) => {
                        is_float = true;
                        sum_f += f;
                        any = true;
                    }
                    Value::Text(s) => {
                        is_float = true;
                        match s.trim().parse::<f64>() {
                            Ok(f) => sum_f += f,
                            Err(_) => {
                                return Err(PqError::new(
                                    "22P02",
                                    format!("invalid input syntax for type float8: \"{}\"", s),
                                ))
                            }
                        }
                        any = true;
                    }
                    Value::Interval { months, days, micros } => {
                        sum_iv_m = sum_iv_m
                            .checked_add(months)
                            .ok_or_else(|| PqError::new("22003", "interval out of range"))?;
                        sum_iv_d = sum_iv_d
                            .checked_add(days)
                            .ok_or_else(|| PqError::new("22003", "interval out of range"))?;
                        sum_iv_u = sum_iv_u
                            .checked_add(micros)
                            .ok_or_else(|| PqError::new("22003", "interval out of range"))?;
                        any = true;
                        is_interval = true;
                    }
                    v => {
                        return Err(PqError::wrong_type(format!(
                            "function sum({}) does not exist",
                            type_name(&v)
                        )))
                    }
                }
            }
            if !any {
                Ok(Value::Null)
            } else if is_interval {
                Ok(Value::Interval {
                    months: sum_iv_m,
                    days: sum_iv_d,
                    micros: sum_iv_u,
                })
            } else if is_float {
                Ok(Value::Float(sum_f + sum_i as f64))
            } else {
                Ok(Value::Int(sum_i))
            }
        }
        AggKind::Min | AggKind::Max => {
            let want_min = spec.kind == AggKind::Min;
            let mut best: Option<Value> = None;
            for r in rows {
                let v: Value = (*r).clone();
                if matches!(v, Value::Null) {
                    continue;
                }
                best = Some(match best {
                    None => v,
                    Some(b) => {
                        let o = order_cmp(&v, &b, false, false)?;
                        match (want_min, o) {
                            (true, std::cmp::Ordering::Less) => v,
                            (false, std::cmp::Ordering::Greater) => v,
                            _ => b,
                        }
                    }
                });
            }
            Ok(best.unwrap_or(Value::Null))
        }
        AggKind::Avg => {
            let mut sum = 0.0f64;
            let mut n = 0i64;
            for r in rows {
                match (*r).clone() {
                    Value::Null => {}
                    Value::Int(i) => {
                        sum += i as f64;
                        n += 1;
                    }
                    Value::Float(f) => {
                        sum += f;
                        n += 1;
                    }
                    v => {
                        return Err(PqError::wrong_type(format!(
                            "function avg({}) does not exist",
                            type_name(&v)
                        )))
                    }
                }
            }
            if n == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Float(sum / n as f64))
            }
        }
    }
}

pub fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "unknown",
        Value::Bool(_) => "boolean",
        Value::Int(_) => "bigint",
        Value::Float(_) => "double precision",
        Value::Text(_) => "text",
        Value::Date(_) => "date",
        Value::Time(_) => "time",
        Value::Timestamp(_) => "timestamp",
        Value::Interval { .. } => "interval",
    }
}

fn col_display(e: &Expr) -> String {
    match e {
        Expr::Col(c) => c.clone(),
        Expr::QCol(t, c) => format!("{}.{}", t, c),
        _ => "?column?".into(),
    }
}

// --------------------------------------------------------------- evaluation --

/// Renames leading columns per a table-ref column alias list
/// (FROM t AS x (a, b) / FROM generate_series(1,2) s(g)).
fn apply_col_aliases(mut cols: Vec<(String, Ty)>, tr: &crate::sql::TableRef) -> Vec<(String, Ty)> {
    for (i, a) in tr.col_aliases.iter().enumerate() {
        if i < cols.len() {
            cols[i].0 = a.clone();
        }
    }
    cols
}

pub fn eval(e: &Expr, ctx: &Ctx) -> R<Value> {
    match e {
        Expr::Lit(v) => Ok(v.clone()),
        // subqueries are rewritten to literals before evaluation; reaching
        // this arm means an unrewritten path (internal error, not user error)
        Expr::Subquery(_) => Err(PqError::new(
            "XX000",
            "internal error: unevaluated scalar subquery",
        )),
        Expr::Col(name) => {
            let idx = ctx.scope.resolve(name)?;
            Ok(ctx.row.get(idx).cloned().unwrap_or(Value::Null))
        }
        Expr::QCol(t, c) => {
            let idx = ctx.scope.resolve_q(t, c)?;
            Ok(ctx.row.get(idx).cloned().unwrap_or(Value::Null))
        }
        Expr::Param(n) => {
            if *n < 1 || (*n as usize) > ctx.params.len() {
                return Err(PqError::new(
                    "42P02",
                    format!("there is no parameter ${}", n),
                ));
            }
            Ok(ctx.params[(*n - 1) as usize].clone())
        }
        Expr::AggRef(i) => Ok(ctx
            .aggs
            .get(*i)
            .cloned()
            .unwrap_or(Value::Null)),
        Expr::Neg(inner) => match eval(inner, ctx)? {
            Value::Int(i) => Ok(Value::Int(-i)),
            Value::Float(f) => Ok(Value::Float(-f)),
            Value::Interval { months, days, micros } => Ok(Value::Interval {
                months: -months,
                days: -days,
                micros: -micros,
            }),
            Value::Null => Ok(Value::Null),
            v => Err(PqError::wrong_type(format!(
                "cannot negate type {}",
                type_name(&v)
            ))),
        },
        Expr::Not(inner) => match eval(inner, ctx)? {
            Value::Bool(b) => Ok(Value::Bool(!b)),
            Value::Null => Ok(Value::Null),
            v => Err(PqError::wrong_type(format!(
                "argument of NOT must be boolean, not {}",
                type_name(&v)
            ))),
        },
        Expr::Bin(l, op, r) => {
            let lv = eval(l, ctx)?;
            let rv = eval(r, ctx)?;
            eval_bin(op, &lv, &rv)
        }
        Expr::IsNull(inner, neg) => {
            let v = eval(inner, ctx)?;
            let is_null = matches!(v, Value::Null);
            Ok(Value::Bool(if *neg { !is_null } else { is_null }))
        }
        Expr::Like(l, r, neg) => {
            let lv = eval(l, ctx)?;
            let rv = eval(r, ctx)?;
            eval_like(&lv, &rv, *neg)
        }
        Expr::InList(l, list, neg) => {
            let lv = eval(l, ctx)?;
            let mut vals = Vec::with_capacity(list.len());
            for e in list {
                vals.push(eval(e, ctx)?);
            }
            eval_in(&lv, &vals, *neg)
        }
        Expr::Between(x, lo, hi, neg) => {
            let xv = eval(x, ctx)?;
            let lov = eval(lo, ctx)?;
            let hiv = eval(hi, ctx)?;
            eval_between(&xv, &lov, &hiv, *neg)
        }
        Expr::Case {
            operand,
            when_then,
            else_,
        } => {
            let op = match operand {
                Some(o) => Some(eval(o, ctx)?),
                None => None,
            };
            for (w, t) in when_then {
                let hit = match &op {
                    Some(ov) => compare(ov, &eval(w, ctx)?)? == Some(0),
                    None => matches!(eval(w, ctx)?, Value::Bool(true)),
                };
                if hit {
                    return eval(t, ctx);
                }
            }
            match else_ {
                Some(e) => eval(e, ctx),
                None => Ok(Value::Null),
            }
        }
        Expr::Cast(inner, ty) => coerce(eval(inner, ctx)?, *ty),
        Expr::Func(name, args) => {
            let mut vals = Vec::with_capacity(args.len());
            for a in args {
                vals.push(eval(a, ctx)?);
            }
            eval_func(name, &vals)
        }
        Expr::Agg(..) => Err(PqError::new(
            "42803",
            "aggregate functions are not allowed here",
        )),
    }
}

/// Scalar binary-op combine (shared by the reference and vectorized executors).
pub fn eval_bin(op: &str, lv: &Value, rv: &Value) -> R<Value> {
    match op {
        "AND" => Ok(match (lv, rv) {
            (Value::Bool(false), _) | (_, Value::Bool(false)) => Value::Bool(false),
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            (Value::Bool(true), Value::Bool(true)) => Value::Bool(true),
            _ => return Err(PqError::wrong_type("argument of AND must be boolean")),
        }),
        "OR" => Ok(match (lv, rv) {
            (Value::Bool(true), _) | (_, Value::Bool(true)) => Value::Bool(true),
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            (Value::Bool(false), Value::Bool(false)) => Value::Bool(false),
            _ => return Err(PqError::wrong_type("argument of OR must be boolean")),
        }),
        "||" => Ok(match (lv, rv) {
            (Value::Null, _) | (_, Value::Null) => Value::Null,
            _ => Value::Text(format!(
                "{}{}",
                value_to_text(lv).unwrap_or_default(),
                value_to_text(rv).unwrap_or_default()
            )),
        }),
        "[]" => Err(PqError::new(
            "0A000",
            "array subscripts are not supported",
        )),
        "=" | "<>" | "!=" | "<" | "<=" | ">" | ">=" => {
            let c = compare(lv, rv)?;
            if c.is_none() {
                Ok(Value::Null)
            } else {
                let b = match op {
                    "=" => c == Some(0),
                    "<>" | "!=" => c != Some(0),
                    "<" => c == Some(-1),
                    "<=" => c != Some(1),
                    ">" => c == Some(1),
                    _ => c != Some(-1),
                };
                Ok(Value::Bool(b))
            }
        }
        "+" | "-" | "*" | "/" | "%" => arith(lv, op, rv),
        "~" | "!~" => match (lv, rv) {
            (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
            (Value::Text(t), Value::Text(p)) => {
                let re = regex::Regex::new(p).map_err(|e| {
                    PqError::new("2201B", format!("invalid regular expression: {}", e))
                })?;
                Ok(Value::Bool(if op == "~" { re.is_match(t) } else { !re.is_match(t) }))
            }
            _ => Err(PqError::wrong_type("argument of ~ must be text")),
        },
        _ => Err(PqError::syntax(format!("unknown operator {}", op))),
    }
}

pub fn eval_like(lv: &Value, rv: &Value, neg: bool) -> R<Value> {
    match (lv, rv) {
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        (Value::Text(t), Value::Text(p)) => {
            let m = like_match(t.as_bytes(), p.as_bytes());
            Ok(Value::Bool(if neg { !m } else { m }))
        }
        _ => Err(PqError::wrong_type("LIKE requires text operands")),
    }
}

pub fn eval_in(lv: &Value, vals: &[Value], neg: bool) -> R<Value> {
    let mut any_null = matches!(lv, Value::Null);
    let mut found = false;
    for v in vals {
        if matches!(v, Value::Null) {
            any_null = true;
        } else if compare(lv, v)? == Some(0) {
            found = true;
            break;
        }
    }
    Ok(if found {
        Value::Bool(!neg)
    } else if any_null {
        Value::Null
    } else {
        Value::Bool(neg)
    })
}

pub fn eval_between(xv: &Value, lov: &Value, hiv: &Value, neg: bool) -> R<Value> {
    let c1 = compare(xv, lov)?;
    let c2 = compare(xv, hiv)?;
    let ge = map_ge(c1);
    let le = map_le(c2);
    let res = and3(ge, le);
    Ok(match res {
        None => Value::Null,
        Some(b) => Value::Bool(if neg { !b } else { b }),
    })
}

pub fn map_ge(c: Option<i32>) -> Option<bool> {
    match c {
        Some(0) | Some(1) => Some(true),
        Some(_) => Some(false),
        None => None,
    }
}
pub fn map_le(c: Option<i32>) -> Option<bool> {
    match c {
        Some(0) | Some(-1) => Some(true),
        Some(_) => Some(false),
        None => None,
    }
}
pub fn and3(a: Option<bool>, b: Option<bool>) -> Option<bool> {
    match (a, b) {
        (Some(false), _) | (_, Some(false)) => Some(false),
        (None, _) | (_, None) => None,
        _ => Some(true),
    }
}

/// SQL LIKE: % = any run, _ = any single byte. Case-sensitive.
pub fn like_match(text: &[u8], pat: &[u8]) -> bool {
    let (mut t, mut p) = (0usize, 0usize);
    let (mut star, mut mark) = (usize::MAX, 0usize);
    while t < text.len() {
        if p < pat.len() && (pat[p] == b'_' || pat[p] == text[t]) {
            t += 1;
            p += 1;
        } else if p < pat.len() && pat[p] == b'%' {
            star = p;
            mark = t;
            p += 1;
        } else if star != usize::MAX {
            p = star + 1;
            mark += 1;
            t = mark;
        } else {
            return false;
        }
    }
    while p < pat.len() && pat[p] == b'%' {
        p += 1;
    }
    p == pat.len()
}

pub fn eval_func(name: &str, vals: &[Value]) -> R<Value> {
    // ---- temporal functions (v1.2) ----
    match name {
        "now" | "current_timestamp" if vals.is_empty() => {
            Ok(Value::Timestamp(temporal::now_us()))
        }
        "current_date" if vals.is_empty() => {
            Ok(Value::Date((temporal::now_us() / temporal::US_PER_DAY) as i32))
        }
        "current_time" if vals.is_empty() => {
            Ok(Value::Time(temporal::now_us().rem_euclid(temporal::US_PER_DAY)))
        }
        "__extract" | "date_part" => {
            let Some(Value::Text(field)) = vals.first() else {
                return Err(PqError::wrong_type(
                    "extract()/date_part() needs a text field and a value",
                ));
            };
            let Some(v) = vals.get(1) else {
                return Err(PqError::wrong_type("extract() needs a value"));
            };
            match v {
                Value::Null | Value::Text(_) => Ok(Value::Null),
                v => temporal::extract_field(field, v)
                    .map(Value::Float)
                    .map_err(|e| PqError::new("22P02", e)),
            }
        }
        "date_trunc" => {
            let Some(Value::Text(field)) = vals.first() else {
                return Err(PqError::wrong_type("date_trunc() needs a text field"));
            };
            let Some(v) = vals.get(1) else {
                return Err(PqError::wrong_type("date_trunc() needs a value"));
            };
            match v {
                Value::Timestamp(us) => temporal::date_trunc(field, *us)
                    .map(Value::Timestamp)
                    .map_err(|e| PqError::new("22P02", e)),
                Value::Date(d) => temporal::date_trunc(field, *d as i64 * temporal::US_PER_DAY)
                    .map(Value::Timestamp)
                    .map_err(|e| PqError::new("22P02", e)),
                Value::Null | Value::Text(_) => Ok(Value::Null),
                v => Err(PqError::wrong_type(format!(
                    "date_trunc() does not support {}",
                    type_name(v)
                ))),
            }
        }
        "to_char" => {
            if vals.len() != 2 {
                return Err(PqError::wrong_type("to_char() takes (value, format)"));
            }
            match (&vals[0], &vals[1]) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                (v, Value::Text(fmt)) => temporal::to_char(v, fmt)
                    .map(Value::Text)
                    .map_err(|e| PqError::new("22P02", e)),
                _ => Err(PqError::wrong_type("to_char() needs (value, text format)")),
            }
        }
        "age" => match vals {
            [Value::Timestamp(a)] => {
                let now_mid = (temporal::now_us() / temporal::US_PER_DAY) * temporal::US_PER_DAY;
                let (m, d, u) = temporal::age_diff(now_mid, *a);
                Ok(Value::Interval { months: m, days: d, micros: u })
            }
            [Value::Date(a)] => {
                let now_mid = (temporal::now_us() / temporal::US_PER_DAY) * temporal::US_PER_DAY;
                let (m, d, u) = temporal::age_diff(now_mid, *a as i64 * temporal::US_PER_DAY);
                Ok(Value::Interval { months: m, days: d, micros: u })
            }
            [Value::Timestamp(a), Value::Timestamp(b)] => {
                let (m, d, u) = temporal::age_diff(*a, *b);
                Ok(Value::Interval { months: m, days: d, micros: u })
            }
            _ => Err(PqError::wrong_type(
                "age() takes one timestamp or two timestamps",
            )),
        },
        _ => eval_func_classic(name, vals),
    }
}

/// The pre-v1.2 function set.
fn eval_func_classic(name: &str, vals: &[Value]) -> R<Value> {
    let strict_err = |i: usize| {
        PqError::wrong_type(format!("function {}() argument {} must not be NULL", name, i + 1))
    };
    match name {
        "current_lsn" if vals.is_empty() => {
            let l = crate::persist::published_lsn();
            if l == 0 {
                Ok(Value::Null)
            } else {
                Ok(Value::Int(l as i64))
            }
        }
        "version" => Ok(Value::Text(format!(
            "PostgreSQL 16.9 (puddle {})",
            crate::VERSION
        ))),
        // ---- catalog support functions (psql / BI tools) ----
        "pg_get_userbyid" => Ok(Value::Text("puddle".into())),
        "pg_table_is_visible" => Ok(Value::Bool(true)),
        "pg_encoding_to_char" => {
            if vals.is_empty() {
                return Err(PqError::new("42883", "pg_encoding_to_char() needs 1 argument"));
            }
            match &vals[0] {
                Value::Null => Ok(Value::Null),
                _ => Ok(Value::Text("UTF8".into())),
            }
        }
        "format_type" => {
            if vals.is_empty() {
                return Err(PqError::new("42883", "format_type() needs 2 arguments"));
            }
            match &vals[0] {
                Value::Null => Ok(Value::Null),
                Value::Int(oid) => Ok(Value::Text(
                    crate::catalog::format_type(*oid).unwrap_or("text").to_string(),
                )),
                _ => Ok(Value::Text("text".into())),
            }
        }
        "pg_get_expr" | "pg_get_indexdef" | "pg_get_constraintdef"
            | "pg_get_statisticsobjdef_columns" => Ok(Value::Null),
        "pg_relation_is_publishable" => Ok(Value::Bool(false)),
        "array_length" => Ok(Value::Null),
        "array_to_string" => {
            if vals.len() < 2 {
                return Err(PqError::new("42883", "array_to_string() needs 2 arguments"));
            }
            match (&vals[0], &vals[1]) {
                (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
                _ => Ok(Value::Text(String::new())),
            }
        }
        "current_database" => Ok(Value::Text(crate::catalog::dbname())),
        "current_schema" => Ok(Value::Text("public".into())),
        "current_user" | "session_user" | "user" | "current_role" => {
            Ok(Value::Text("puddle".into()))
        }
        "upper" | "lower" => {
            if vals.len() != 1 {
                return Err(PqError::new(
                    "42883",
                    format!("function {}() takes exactly one argument", name),
                ));
            }
            match &vals[0] {
                Value::Null => Ok(Value::Null),
                Value::Text(s) => Ok(Value::Text(if name == "upper" {
                    s.to_uppercase()
                } else {
                    s.to_lowercase()
                })),
                v => Err(PqError::wrong_type(format!(
                    "function {}({}) does not exist",
                    name,
                    type_name(v)
                ))),
            }
        }
        "length" | "char_length" | "character_length" => match &vals[0] {
            Value::Null => Ok(Value::Null),
            Value::Text(s) => Ok(Value::Int(s.chars().count() as i64)),
            v => Err(PqError::wrong_type(format!(
                "function length({}) does not exist",
                type_name(v)
            ))),
        },
        "abs" => match &vals[0] {
            Value::Null => Ok(Value::Null),
            Value::Int(i) => Ok(Value::Int(i.abs())),
            Value::Float(f) => Ok(Value::Float(f.abs())),
            v => Err(PqError::wrong_type(format!(
                "function abs({}) does not exist",
                type_name(v)
            ))),
        },
        "round" => {
            if vals.len() == 1 {
                match &vals[0] {
                    Value::Null => Ok(Value::Null),
                    Value::Int(i) => Ok(Value::Int(*i)),
                    Value::Float(f) => Ok(Value::Float(f.round())),
                    v => Err(PqError::wrong_type(format!(
                        "function round({}) does not exist",
                        type_name(v)
                    ))),
                }
            } else if vals.len() == 2 {
                let d = match &vals[1] {
                    Value::Int(i) => *i,
                    _ => return Err(strict_err(1)),
                };
                match &vals[0] {
                    Value::Null => Ok(Value::Null),
                    Value::Float(f) => {
                        let m = 10f64.powi(d as i32);
                        Ok(Value::Float((f * m).round() / m))
                    }
                    Value::Int(i) => Ok(Value::Int(*i)),
                    _ => Err(strict_err(0)),
                }
            } else {
                Err(PqError::new("42883", "round() takes 1 or 2 arguments"))
            }
        }
        "floor" | "ceil" | "ceiling" => match &vals[0] {
            Value::Null => Ok(Value::Null),
            Value::Int(i) => Ok(Value::Int(*i)),
            Value::Float(f) => Ok(Value::Float(if name == "floor" {
                f.floor()
            } else {
                f.ceil()
            })),
            v => Err(PqError::wrong_type(format!(
                "function {}({}) does not exist",
                name,
                type_name(v)
            ))),
        },
        "sqrt" => match &vals[0] {
            Value::Null => Ok(Value::Null),
            Value::Int(i) => Ok(Value::Float((*i as f64).sqrt())),
            Value::Float(f) => Ok(Value::Float(f.sqrt())),
            v => Err(PqError::wrong_type(format!(
                "function sqrt({}) does not exist",
                type_name(v)
            ))),
        },
        "coalesce" => {
            for v in vals {
                if !matches!(v, Value::Null) {
                    return Ok(v.clone());
                }
            }
            Ok(Value::Null)
        }
        "nullif" => {
            if vals.len() != 2 {
                return Err(PqError::new("42883", "nullif() takes 2 arguments"));
            }
            if compare(&vals[0], &vals[1])? == Some(0) {
                Ok(Value::Null)
            } else {
                Ok(vals[0].clone())
            }
        }
        "greatest" | "least" => {
            let want_greatest = name == "greatest";
            let mut best: Option<Value> = None;
            for v in vals {
                if matches!(v, Value::Null) {
                    continue;
                }
                best = Some(match best {
                    None => v.clone(),
                    Some(b) => {
                        let o = order_cmp(v, &b, false, false)?;
                        match (want_greatest, o) {
                            (true, std::cmp::Ordering::Greater) => v.clone(),
                            (false, std::cmp::Ordering::Less) => v.clone(),
                            _ => b,
                        }
                    }
                });
            }
            Ok(best.unwrap_or(Value::Null))
        }
        "concat" => {
            // concat() treats NULLs as empty strings
            let mut s = String::new();
            for v in vals {
                s.push_str(&value_to_text(v).unwrap_or_default());
            }
            Ok(Value::Text(s))
        }
        _ => Err(PqError::new(
            "42883",
            format!("function {}() does not exist", name),
        )),
    }
}

pub fn compare(a: &Value, b: &Value) -> R<Option<i32>> {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Ok(None);
    }
    // implicit text coercion (unknown-typed literals / parameters)
    let (a, b) = coerce_text(a, b)?;
    match (&a, &b) {
        (Value::Bool(x), Value::Bool(y)) => Ok(Some((*x as i32) - (*y as i32))),
        (Value::Int(x), Value::Int(y)) => Ok(Some(if x < y {
            -1
        } else if x > y {
            1
        } else {
            0
        })),
        (Value::Int(x), Value::Float(y)) => Ok(cmp_f64(*x as f64, *y)),
        (Value::Float(x), Value::Int(y)) => Ok(cmp_f64(*x, *y as f64)),
        (Value::Float(x), Value::Float(y)) => Ok(cmp_f64(*x, *y)),
        (Value::Text(x), Value::Text(y)) => Ok(Some(match x.cmp(y) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Greater => 1,
            std::cmp::Ordering::Equal => 0,
        })),
        // temporal: like-with-like, plus date<->timestamp promotion
        (Value::Date(x), Value::Date(y)) => Ok(Some(ord_i32(*x, *y))),
        (Value::Time(x), Value::Time(y)) => Ok(Some(ord_i64(*x, *y))),
        (Value::Timestamp(x), Value::Timestamp(y)) => Ok(Some(ord_i64(*x, *y))),
        (Value::Interval { .. }, Value::Interval { .. }) => {
            let (ta, tb) = (interval_us(&a), interval_us(&b));
            Ok(Some(ord_i64(ta, tb)))
        }
        (Value::Date(x), Value::Timestamp(y)) => {
            Ok(Some(ord_i64(*x as i64 * temporal::US_PER_DAY, *y)))
        }
        (Value::Timestamp(x), Value::Date(y)) => {
            Ok(Some(ord_i64(*x, *y as i64 * temporal::US_PER_DAY)))
        }
        _ => Err(PqError::wrong_type(format!(
            "cannot compare {} with {}",
            type_name(&a),
            type_name(&b)
        ))),
    }
}

fn ord_i32(a: i32, b: i32) -> i32 {
    if a < b { -1 } else if a > b { 1 } else { 0 }
}

fn ord_i64(a: i64, b: i64) -> i32 {
    if a < b { -1 } else if a > b { 1 } else { 0 }
}

/// Nominal total length of an interval (30-day months, PG ordering).
pub fn interval_us(v: &Value) -> i64 {
    match v {
        Value::Interval { months, days, micros } => {
            temporal::interval_total_us(*months, *days, *micros)
        }
        _ => 0,
    }
}

fn coerce_text(a: &Value, b: &Value) -> R<(Value, Value)> {
    let parse_i = |s: &str| -> R<i64> {
        s.trim()
            .parse::<i64>()
            .map_err(|_| PqError::new("22P02", format!("invalid input syntax for type bigint: \"{}\"", s)))
    };
    let parse_f = |s: &str| -> R<f64> {
        s.trim()
            .parse::<f64>()
            .map_err(|_| PqError::new("22P02", format!("invalid input syntax for type double precision: \"{}\"", s)))
    };
    let parse_b = |s: &str| -> R<bool> {
        match s.trim().to_lowercase().as_str() {
            "t" | "true" | "yes" | "1" => Ok(true),
            "f" | "false" | "no" | "0" => Ok(false),
            _ => Err(PqError::new("22P02", format!("invalid input syntax for type boolean: \"{}\"", s))),
        }
    };
    let parse_d = |s: &str| -> R<i32> {
        temporal::parse_date(s).map_err(|e| PqError::new("22P02", e))
    };
    let parse_t = |s: &str| -> R<i64> {
        temporal::parse_time(s).map_err(|e| PqError::new("22P02", e))
    };
    let parse_ts = |s: &str| -> R<i64> {
        temporal::parse_timestamp(s).map_err(|e| PqError::new("22P02", e))
    };
    let parse_iv = |s: &str| -> R<Value> {
        temporal::parse_interval(s)
            .map(|(months, days, micros)| Value::Interval { months, days, micros })
            .map_err(|e| PqError::new("22P02", e))
    };
    use Value::*;
    Ok(match (a, b) {
        (Text(s), Date(_)) => (Date(parse_d(s)?), b.clone()),
        (Text(s), Time(_)) => (Time(parse_t(s)?), b.clone()),
        (Text(s), Timestamp(_)) => (Timestamp(parse_ts(s)?), b.clone()),
        (Text(s), Interval { .. }) => (parse_iv(s)?, b.clone()),
        (Date(_), Text(s)) => (a.clone(), Date(parse_d(s)?)),
        (Time(_), Text(s)) => (a.clone(), Time(parse_t(s)?)),
        (Timestamp(_), Text(s)) => (a.clone(), Timestamp(parse_ts(s)?)),
        (Interval { .. }, Text(s)) => (a.clone(), parse_iv(s)?),
        (Text(s), Int(_)) => (Int(parse_i(s)?), b.clone()),
        (Text(s), Float(_)) => (Float(parse_f(s)?), b.clone()),
        (Text(s), Bool(_)) => (Bool(parse_b(s)?), b.clone()),
        (Int(_), Text(s)) => (a.clone(), Int(parse_i(s)?)),
        (Float(_), Text(s)) => (a.clone(), Float(parse_f(s)?)),
        (Bool(_), Text(s)) => (a.clone(), Bool(parse_b(s)?)),
        _ => (a.clone(), b.clone()),
    })
}

fn cmp_f64(x: f64, y: f64) -> Option<i32> {
    if x.is_nan() || y.is_nan() {
        None
    } else if x < y {
        Some(-1)
    } else if x > y {
        Some(1)
    } else {
        Some(0)
    }
}

/// ORDER BY comparator: asc puts NULLs last, desc puts them first (PG default).
pub fn order_cmp(a: &Value, b: &Value, desc: bool, nulls_first: bool) -> R<std::cmp::Ordering> {
    use std::cmp::Ordering::*;
    let null_a = matches!(a, Value::Null);
    let null_b = matches!(b, Value::Null);
    if null_a || null_b {
        if null_a && null_b {
            return Ok(Equal);
        }
        return Ok(match (null_a, nulls_first) {
            (true, false) => Greater, // nulls last
            (true, true) => Less,     // nulls first
            (false, false) => Less,
            (false, true) => Greater,
        });
    }
    let base = match compare(a, b)? {
        Some(0) => None,
        Some(-1) => Some(Less),
        Some(_) => Some(Greater),
        None => None,
    };
    let base = base.unwrap_or(Equal);
    Ok(if desc { base.reverse() } else { base })
}

/// PG-style temporal arithmetic. Returns Ok(None) when the operand pair is
/// not temporal (fall through to numeric paths); temporal operands with a
/// non-temporal operator (interval * int) fall to None too and error later.
pub fn temporal_arith(a: &Value, op: &str, b: &Value) -> R<Option<Value>> {
    use Value::*;
    let ov = || PqError::new("22003", "date/time value out of range");
    let date_ts = |d: i32| d as i64 * temporal::US_PER_DAY;
    // PG's timestamp_mi justifies its result: |time| >= 24h spills into
    // the days field (truncating division, like C)
    let iv_justified = |micros: i64| -> R<Value> {
        let days = micros / temporal::US_PER_DAY;
        let rem = micros % temporal::US_PER_DAY;
        let d = i32::try_from(days).map_err(|_| ov())?;
        Ok(Interval { months: 0, days: d, micros: rem })
    };
    let ts_add_iv = |x: i64, m: i32, d: i32, u: i64, sign: i32| -> R<Value> {
        let base = temporal::add_months(x, sign * m);
        let delta = (d as i64)
            .checked_mul(temporal::US_PER_DAY)
            .and_then(|v| v.checked_add(u))
            .ok_or_else(ov)?;
        let r = if sign > 0 {
            base.checked_add(delta)
        } else {
            base.checked_sub(delta)
        }
        .ok_or_else(ov)?;
        Ok(Timestamp(r))
    };
    let iv_parts = |m: i32, d: i32, u: i64| -> R<Value> {
        Ok(Interval { months: m, days: d, micros: u })
    };
    match (a, b, op) {
        // date - date -> int days; date ± int -> date
        (Date(x), Date(y), "-") => Ok(Some(Int(*x as i64 - *y as i64))),
        (Date(x), Int(y), "+") | (Int(y), Date(x), "+") => {
            let dy = i32::try_from(*y).map_err(|_| ov())?;
            Ok(Some(Date(x.checked_add(dy).ok_or_else(ov)?)))
        }
        (Date(x), Int(y), "-") => {
            let dy = i32::try_from(*y).map_err(|_| ov())?;
            Ok(Some(Date(x.checked_sub(dy).ok_or_else(ov)?)))
        }
        // timestamp - timestamp / time - time -> interval (justified)
        (Timestamp(x), Timestamp(y), "-") => {
            iv_justified(x.checked_sub(*y).ok_or_else(ov)?).map(Some)
        }
        (Time(x), Time(y), "-") => {
            iv_justified(x.checked_sub(*y).ok_or_else(ov)?).map(Some)
        }
        // timestamp/date ± interval -> timestamp
        (Timestamp(x), Interval { months, days, micros }, "+") => {
            ts_add_iv(*x, *months, *days, *micros, 1).map(Some)
        }
        (Date(x), Interval { months, days, micros }, "+") => {
            ts_add_iv(date_ts(*x), *months, *days, *micros, 1).map(Some)
        }
        (Interval { months, days, micros }, Timestamp(x), "+") => {
            ts_add_iv(*x, *months, *days, *micros, 1).map(Some)
        }
        (Timestamp(x), Interval { months, days, micros }, "-") => {
            ts_add_iv(*x, *months, *days, *micros, -1).map(Some)
        }
        (Date(x), Interval { months, days, micros }, "-") => {
            ts_add_iv(date_ts(*x), *months, *days, *micros, -1).map(Some)
        }
        // interval ± interval -> interval (component-wise)
        (Interval { months: m1, days: d1, micros: u1 },
         Interval { months: m2, days: d2, micros: u2 },
         "+") => {
            let months = m1.checked_add(*m2).ok_or_else(ov)?;
            let days = d1.checked_add(*d2).ok_or_else(ov)?;
            let micros = u1.checked_add(*u2).ok_or_else(ov)?;
            iv_parts(months, days, micros).map(Some)
        }
        (Interval { months: m1, days: d1, micros: u1 },
         Interval { months: m2, days: d2, micros: u2 },
         "-") => {
            let months = m1.checked_sub(*m2).ok_or_else(ov)?;
            let days = d1.checked_sub(*d2).ok_or_else(ov)?;
            let micros = u1.checked_sub(*u2).ok_or_else(ov)?;
            iv_parts(months, days, micros).map(Some)
        }
        // time ± interval -> time (wraps at 24h like PG)
        (Time(x), Interval { days, micros, .. }, "+")
        | (Interval { days, micros, .. }, Time(x), "+") => {
            let delta = (*days as i64)
                .checked_mul(temporal::US_PER_DAY)
                .and_then(|v| v.checked_add(*micros))
                .ok_or_else(ov)?;
            Ok(Some(Time((x + delta).rem_euclid(temporal::US_PER_DAY))))
        }
        (Time(x), Interval { days, micros, .. }, "-") => {
            let delta = (*days as i64)
                .checked_mul(temporal::US_PER_DAY)
                .and_then(|v| v.checked_add(*micros))
                .ok_or_else(ov)?;
            Ok(Some(Time((x - delta).rem_euclid(temporal::US_PER_DAY))))
        }
        // mixed date/timestamp subtraction -> interval (justified)
        (Timestamp(x), Date(y), "-") => {
            iv_justified(x.checked_sub(date_ts(*y)).ok_or_else(ov)?).map(Some)
        }
        (Date(x), Timestamp(y), "-") => {
            iv_justified(date_ts(*x).checked_sub(*y).ok_or_else(ov)?).map(Some)
        }
        _ => Ok(None),
    }
}

pub fn arith(a: &Value, op: &str, b: &Value) -> R<Value> {
    if matches!(a, Value::Null) || matches!(b, Value::Null) {
        return Ok(Value::Null);
    }
    let (a, b) = coerce_text(a, b)?;
    if let Some(v) = temporal_arith(&a, op, &b)? {
        return Ok(v);
    }
    match (&a, &b) {
        (Value::Int(x), Value::Int(y)) => {
            let r = match op {
                "+" => x.checked_add(*y),
                "-" => x.checked_sub(*y),
                "*" => x.checked_mul(*y),
                "/" => {
                    if *y == 0 {
                        return Err(PqError::new("22012", "division by zero"));
                    }
                    x.checked_div(*y)
                }
                "%" => {
                    if *y == 0 {
                        return Err(PqError::new("22012", "division by zero"));
                    }
                    x.checked_rem(*y)
                }
                _ => None,
            };
            r.map(Value::Int)
                .ok_or_else(|| PqError::new("22003", "integer out of range"))
        }
        _ => {
            let x = as_float(&a)?;
            let y = as_float(&b)?;
            let v = match op {
                "+" => x + y,
                "-" => x - y,
                "*" => x * y,
                "/" => {
                    if y == 0.0 {
                        return Err(PqError::new("22012", "division by zero"));
                    }
                    x / y
                }
                "%" => x % y,
                _ => return Err(PqError::syntax("bad op")),
            };
            Ok(Value::Float(v))
        }
    }
}

fn as_float(v: &Value) -> R<f64> {
    match v {
        Value::Int(i) => Ok(*i as f64),
        Value::Float(f) => Ok(*f),
        Value::Bool(b) => Ok(*b as i32 as f64),
        Value::Text(s) => s
            .trim()
            .parse::<f64>()
            .map_err(|_| PqError::new("22P02", format!("invalid input syntax for type double precision: \"{}\"", s))),
        Value::Date(_) | Value::Time(_) | Value::Timestamp(_) | Value::Interval { .. } => {
            Err(PqError::wrong_type(format!(
                "cannot use {} as a number",
                type_name(v)
            )))
        }
        Value::Null => Ok(f64::NAN), // unreachable (handled by caller)
    }
}

/// Coercion of a value to a target type (INSERT, UPDATE, CAST).
pub fn coerce(v: Value, ty: Ty) -> R<Value> {
    match (&v, ty) {
        (Value::Null, _) => Ok(Value::Null),
        (Value::Int(_), Ty::Int) => Ok(v),
        (Value::Bool(_), Ty::Bool) => Ok(v),
        (Value::Text(_), Ty::Text) => Ok(v),
        (Value::Float(_), Ty::Float) => Ok(v),
        (Value::Int(i), Ty::Float) => Ok(Value::Float(*i as f64)),
        (Value::Float(f), Ty::Int) => Ok(Value::Int(f.round() as i64)),
        (Value::Bool(b), Ty::Int) => Ok(Value::Int(*b as i64)),
        (Value::Int(i), Ty::Bool) => Ok(Value::Bool(*i != 0)),
        (_, Ty::Text) => Ok(Value::Text(value_to_text(&v).unwrap_or_default())),
        (Value::Text(s), Ty::Int) => s
            .trim()
            .parse::<i64>()
            .map(Value::Int)
            .map_err(|_| PqError::new("22P02", format!("invalid input syntax for type bigint: \"{}\"", s))),
        (Value::Text(s), Ty::Float) => s
            .trim()
            .parse::<f64>()
            .map(Value::Float)
            .map_err(|_| PqError::new("22P02", format!("invalid input syntax for type double precision: \"{}\"", s))),
        (Value::Text(s), Ty::Bool) => match s.trim().to_lowercase().as_str() {
            "t" | "true" | "yes" | "1" => Ok(Value::Bool(true)),
            "f" | "false" | "no" | "0" => Ok(Value::Bool(false)),
            _ => Err(PqError::new("22P02", format!("invalid input syntax for type boolean: \"{}\"", s))),
        },
        (Value::Text(s), Ty::Date) => temporal::parse_date(s)
            .map(Value::Date)
            .map_err(|e| PqError::new("22P02", e)),
        (Value::Text(s), Ty::Time) => temporal::parse_time(s)
            .map(Value::Time)
            .map_err(|e| PqError::new("22P02", e)),
        (Value::Text(s), Ty::Timestamp) => temporal::parse_timestamp(s)
            .map(Value::Timestamp)
            .map_err(|e| PqError::new("22P02", e)),
        (Value::Text(s), Ty::Interval) => temporal::parse_interval(s)
            .map(|(months, days, micros)| Value::Interval { months, days, micros })
            .map_err(|e| PqError::new("22P02", e)),
        (Value::Date(_), Ty::Date)
        | (Value::Time(_), Ty::Time)
        | (Value::Timestamp(_), Ty::Timestamp)
        | (Value::Interval { .. }, Ty::Interval) => Ok(v),
        (Value::Date(d), Ty::Timestamp) => {
            Ok(Value::Timestamp(*d as i64 * temporal::US_PER_DAY))
        }
        (Value::Timestamp(us), Ty::Date) => {
            Ok(Value::Date((us / temporal::US_PER_DAY) as i32))
        }
        (Value::Timestamp(us), Ty::Time) => {
            Ok(Value::Time(us.rem_euclid(temporal::US_PER_DAY)))
        }
        (Value::Date(_), Ty::Time) => Ok(Value::Time(0)),
        _ => Err(PqError::wrong_type(format!(
            "cannot store {} in {} column",
            type_name(&v),
            ty.name()
        ))),
    }
}

pub fn expr_name(e: &Expr) -> String {
    match e {
        Expr::Col(c) => c.clone(),
        Expr::QCol(_, c) => c.clone(),
        Expr::Lit(Value::Bool(_)) => "bool".into(),
        Expr::Lit(Value::Int(_)) => "int8".into(),
        Expr::Lit(Value::Float(_)) => "float8".into(),
        Expr::Lit(Value::Text(_)) => "text".into(),
        Expr::Lit(Value::Null) => "unknown".into(),
        Expr::Param(_) => "?column?".into(),
        _ => "?column?".into(),
    }
}

pub fn infer_ty(e: &Expr, scope: &Scope) -> Ty {
    match e {
        Expr::Subquery(_) => Ty::Text,
        Expr::Lit(Value::Int(_)) => Ty::Int,
        Expr::Lit(Value::Float(_)) => Ty::Float,
        Expr::Lit(Value::Bool(_)) => Ty::Bool,
        Expr::Lit(Value::Date(_)) => Ty::Date,
        Expr::Lit(Value::Time(_)) => Ty::Time,
        Expr::Lit(Value::Timestamp(_)) => Ty::Timestamp,
        Expr::Lit(Value::Interval { .. }) => Ty::Interval,
        Expr::Lit(Value::Text(_)) | Expr::Lit(Value::Null) | Expr::Param(_) => Ty::Text,
        Expr::Col(c) => match scope.resolve(c) {
            Ok(i) => scope.col_ty(i),
            Err(_) => Ty::Text,
        },
        Expr::QCol(t, c) => match scope.resolve_q(t, c) {
            Ok(i) => scope.col_ty(i),
            Err(_) => Ty::Text,
        },
        Expr::AggRef(_) => Ty::Text,
        Expr::Agg(kind, arg, _) => match kind {
            AggKind::CountStar | AggKind::Count => Ty::Int,
            AggKind::Avg => Ty::Float,
            AggKind::Sum => {
                match infer_ty(arg, scope) {
                    Ty::Int => Ty::Int,
                    Ty::Interval => Ty::Interval,
                    _ => Ty::Float,
                }
            }
            AggKind::Min | AggKind::Max => infer_ty(arg, scope),
        },
        Expr::Neg(inner) => infer_ty(inner, scope),
        Expr::Not(_) | Expr::IsNull(..) | Expr::Like(..) | Expr::InList(..)
        | Expr::Between(..) => Ty::Bool,
        Expr::Bin(_, op, _) => match *op {
            "+" | "-" | "*" | "/" | "%" => {
                fn all_int(e: &Expr, scope: &Scope) -> bool {
                    match e {
                        Expr::Lit(Value::Int(_)) => true,
                        Expr::Col(_) | Expr::QCol(..) => infer_ty(e, scope) == Ty::Int,
                        Expr::Neg(i) => all_int(i, scope),
                        Expr::Bin(_, op, r) => {
                            matches!(*op, "+" | "-" | "*" | "/" | "%") && all_int(r, scope)
                        }
                        _ => false,
                    }
                }
                let l = e_l(e);
                let r = e_r(e);
                if all_int(l, scope) && all_int(r, scope) {
                    Ty::Int
                } else if matches!(*op, "+" | "-") {
                    // temporal results
                    let lt = infer_ty(l, scope);
                    let rt = infer_ty(r, scope);
                    match (lt, rt) {
                        (Ty::Date, Ty::Int) | (Ty::Int, Ty::Date) => Ty::Date,
                        (Ty::Date, Ty::Date) => Ty::Int,
                        (Ty::Timestamp, Ty::Interval) | (Ty::Interval, Ty::Timestamp)
                        | (Ty::Date, Ty::Interval) | (Ty::Interval, Ty::Date) => Ty::Timestamp,
                        (Ty::Timestamp, Ty::Timestamp) | (Ty::Time, Ty::Time) => Ty::Interval,
                        (Ty::Time, Ty::Interval) | (Ty::Interval, Ty::Time) => Ty::Time,
                        (Ty::Interval, Ty::Interval) => Ty::Interval,
                        _ if lt.is_temporal() || rt.is_temporal() => Ty::Timestamp,
                        _ => Ty::Float,
                    }
                } else {
                    Ty::Float
                }
            }
            "||" => Ty::Text,
            _ => Ty::Bool,
        },
        Expr::Cast(_, t) => *t,
        Expr::Case { when_then, else_, .. } => when_then
            .first()
            .map(|(_, t)| infer_ty(t, scope))
            .or_else(|| else_.as_ref().map(|e| infer_ty(e, scope)))
            .unwrap_or(Ty::Text),
        Expr::Func(name, args) => match name.as_str() {
            "now" | "current_timestamp" => Ty::Timestamp,
            "current_lsn" => Ty::Int,
            "current_date" => Ty::Date,
            "current_time" => Ty::Time,
            "__extract" | "date_part" => Ty::Float,
            "date_trunc" => Ty::Timestamp,
            "age" => Ty::Interval,
            "to_char" => Ty::Text,
            "upper" | "lower" | "concat" => Ty::Text,
            "length" | "char_length" | "character_length" => Ty::Int,
            "round" if args.len() == 1 && infer_ty(&args[0], scope) == Ty::Int => Ty::Int,
            "round" | "floor" | "ceil" | "ceiling" | "sqrt" => Ty::Float,
            "abs" => args.first().map(|a| infer_ty(a, scope)).unwrap_or(Ty::Int),
            "coalesce" | "nullif" | "greatest" | "least" => {
                args.iter()
                    .find(|a| infer_ty(a, scope) != Ty::Text)
                    .map(|a| infer_ty(a, scope))
                    .unwrap_or(Ty::Text)
            }
            _ => Ty::Text,
        },
    }
}

fn e_l(e: &Expr) -> &Expr {
    match e {
        Expr::Bin(l, _, _) => l,
        _ => e,
    }
}
fn e_r(e: &Expr) -> &Expr {
    match e {
        Expr::Bin(_, _, r) => r,
        _ => e,
    }
}

// ----------------------------------------------------------------- helpers --

fn same_schema(a: &Table, b: &Table) -> bool {
    a.cols.len() == b.cols.len()
        && a.cols.iter().zip(b.cols.iter()).all(|(x, y)| {
            x.name == y.name && x.ty == y.ty && x.not_null == y.not_null
        })
}

/// Records that transform table `old` into table `new` (used by apply_txn).
fn diff_recs(old: &Table, new: &Table) -> Vec<WalRec> {
    use std::collections::HashSet;
    let mut recs = Vec::new();
    let old_map: HashMap<u64, Vec<Value>> =
        (0..old.nrows()).map(|i| (old.rowids[i], old.row(i))).collect();
    let new_ids: HashSet<u64> = new.rowids.iter().copied().collect();
    for i in 0..old.nrows() {
        if !new_ids.contains(&old.rowids[i]) {
            recs.push(WalRec::Delete { rowid: old.rowids[i] });
        }
    }
    for i in 0..new.nrows() {
        let id = new.rowids[i];
        match old_map.get(&id) {
            None => recs.push(WalRec::Insert {
                rowid: id,
                cells: new.row(i),
            }),
            Some(oc) if *oc != new.columns.iter().map(|c| c[i].clone()).collect::<Vec<_>>() => {
                recs.push(WalRec::Update {
                    rowid: id,
                    cells: new.row(i),
                })
            }
            _ => {}
        }
    }
    recs
}

/// PG-style GROUP BY coverage: a subexpression that equals a GROUP BY item is
/// fully covered; otherwise every column leaf inside must be a standalone
/// GROUP BY item. Aggregate calls are fine anywhere.
fn uncovered_col<'a>(e: &'a Expr, groups: &[Expr]) -> Option<&'a Expr> {
    if groups.iter().any(|g| g == e) {
        return None;
    }
    match e {
        Expr::Col(_) | Expr::QCol(..) => Some(e),
        Expr::Lit(_) | Expr::Param(_) | Expr::AggRef(_) | Expr::Agg(..)
        | Expr::Subquery(_) => None,
        Expr::Neg(a) | Expr::Not(a) | Expr::IsNull(a, _) | Expr::Cast(a, _) => {
            uncovered_col(a, groups)
        }
        Expr::Like(a, b, _) => uncovered_col(a, groups).or_else(|| uncovered_col(b, groups)),
        Expr::Between(a, lo, hi, _) => uncovered_col(a, groups)
            .or_else(|| uncovered_col(lo, groups))
            .or_else(|| uncovered_col(hi, groups)),
        Expr::InList(a, l, _) => uncovered_col(a, groups)
            .or_else(|| l.iter().find_map(|x| uncovered_col(x, groups))),
        Expr::Bin(a, _, b) => {
            uncovered_col(a, groups).or_else(|| uncovered_col(b, groups))
        }
        Expr::Case { operand, when_then, else_ } => {
            operand.as_ref().and_then(|o| uncovered_col(o, groups))
                .or_else(|| {
                    when_then
                        .iter()
                        .find_map(|(w, t)| uncovered_col(w, groups).or_else(|| uncovered_col(t, groups)))
                })
                .or_else(|| else_.as_ref().and_then(|x| uncovered_col(x, groups)))
        }
        Expr::Func(_, args) => args.iter().find_map(|a| uncovered_col(a, groups)),
    }
}

// ---------------------------------------------------- shared normalization --

/// GROUP BY positions refer to the output list; a bare name resolves as a
/// source column first, then as an output alias (PG rule).
pub fn normalize_group_by(
    sel: &SelectStmt,
    items: &[(Expr, Option<String>)],
    out_cols: &[(String, Ty)],
    scope: &Scope,
) -> Vec<Expr> {
    sel.group_by
        .iter()
        .map(|g| match g {
            Expr::Lit(Value::Int(n)) if *n >= 1 && (*n as usize) <= items.len() => {
                items[(*n - 1) as usize].0.clone()
            }
            Expr::Col(c) => {
                if scope.resolve(c).is_ok() {
                    g.clone()
                } else if let Some(pos) = out_cols.iter().position(|(n, _)| n == c) {
                    items[pos].0.clone()
                } else {
                    g.clone()
                }
            }
            _ => g.clone(),
        })
        .collect()
}

// -------------------------------------------------------- vectorized path --

/// One equi-join conjunct: left-side expr and right-side expr.
struct EquiJoin<'a> {
    table: &'a TableRef,
    kind: JoinKind,
    lkeys: Vec<&'a Expr>,
    rkeys: Vec<&'a Expr>,
    /// non-equi conjuncts (INNER joins only: applied as a post-join filter)
    residual: Vec<&'a Expr>,
}

impl Db {
    /// Vectorized SELECT: single-table scans and INNER/LEFT equi-join chains.
    /// Returns Ok(None) when the query shape isn't covered (caller falls back
    /// to the reference executor).
    fn select_vectorized(&self, sel: &SelectStmt, params: &[Value]) -> R<Option<StmtResult>> {
        let dbg = std::env::var("PUDDLE_VECX_DEBUG").is_ok();
        if sel.from_items.len() != 1 {
            if dbg { eprintln!("[vecx] fallback: multiple from_items"); }
            return Ok(None);
        }
        let item = &sel.from_items[0];
        if item.joins.is_empty() {
            // single table: pure borrowed-columns scan
            let guard = self.tables.read().unwrap_or_else(|e| e.into_inner());
            let tab = match guard.get(&item.base.name) {
                Some(t) => t,
                // not a user table: catalog relation or missing — let the
                // reference path serve it / produce the error
                None => return Ok(None),
            };
            let mut scope = Scope::empty();
            scope.push(
                item.base.effective_name(),
                apply_col_aliases(
                    tab.cols.iter().map(|c| (c.name.clone(), c.ty)).collect(),
                    &item.base,
                ),
            );
            let src = VSrc::Borrowed {
                cols: tab.columns.iter().map(|c| &c[..]).collect(),
                n: tab.nrows(),
            };
            // secondary-index prefilter (point / range / IN) on the base table
            let pre = index_prefilter(sel.where_.as_ref(), tab, &scope, params);
            return self.vexec(sel, &scope, &src, params, &[], pre);
        }
        // join chain: analyze ON shapes first (no locks held, no side effects)
        let guard = self.tables.read().unwrap_or_else(|e| e.into_inner());
        let base = match guard.get(&item.base.name) {
            Some(t) => t,
            // catalog relations join via the reference path
            None => return Ok(None),
        };
        let mut scope = Scope::empty();
        scope.push(
            item.base.effective_name(),
            apply_col_aliases(
                base.cols.iter().map(|c| (c.name.clone(), c.ty)).collect(),
                &item.base,
            ),
        );
        let mut left_width = scope.width();
        let mut plans: Vec<EquiJoin> = Vec::new();
        for j in &item.joins {
            let right = match guard.get(&j.table.name) {
                Some(t) => t,
                None => return Ok(None), // catalog relation: reference path
            };
            let on = match &j.on {
                Some(o) => o,
                None => { if dbg { eprintln!("[vecx] fallback: join without ON"); } return Ok(None); } // CROSS JOIN via JOIN without ON
            };
            // push the right table into the scope FIRST so ON-side columns
            // resolve; the left/right boundary is the pre-push width.
            scope.push(
                j.table.effective_name(),
                apply_col_aliases(
                    right.cols.iter().map(|c| (c.name.clone(), c.ty)).collect(),
                    &j.table,
                ),
            );
            let boundary = left_width;
            // flatten conjuncts
            let mut conjuncts: Vec<&Expr> = Vec::new();
            fn flatten<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
                if let Expr::Bin(a, "AND", b) = e {
                    flatten(a, out);
                    flatten(b, out);
                } else {
                    out.push(e);
                }
            }
            flatten(on, &mut conjuncts);
            let mut lkeys = Vec::new();
            let mut rkeys = Vec::new();
            let mut residual = Vec::new();
            for c in conjuncts {
                let pair = match c {
                    Expr::Bin(a, "=", b) => Some((a, b)),
                    _ => None,
                };
                let mut matched = false;
                if let Some((a, b)) = pair {
                    if dbg { eprintln!("[vecx] conjunct: {:?} vs {:?}", a, b); }
                    // classify each side: which side do ALL its columns live on?
                    let side_of = |e: &Expr| -> Option<usize> {
                        let mut cols = Vec::new();
                        e.collect_cols(&mut cols);
                        if cols.is_empty() {
                            return None; // constant: belongs to either side
                        }
                        let mut all_left = true;
                        let mut all_right = true;
                        for ce in cols {
                            let idx = match ce {
                                Expr::Col(name) => scope.resolve(name).ok()?,
                                Expr::QCol(t, c) => scope.resolve_q(t, c).ok()?,
                                _ => None?,
                            };
                            if idx < boundary {
                                all_right = false;
                            } else {
                                all_left = false;
                            }
                        }
                        if all_left && !all_right {
                            Some(0)
                        } else if all_right && !all_left {
                            Some(1)
                        } else {
                            None
                        }
                    };
                    let (sa, sb) = (side_of(a), side_of(b));
                    if dbg { eprintln!("[vecx]   sides: {:?} {:?}", sa, sb); }
                    match (sa, sb) {
                        (Some(0), Some(1)) => {
                            lkeys.push(&**a);
                            rkeys.push(&**b);
                            matched = true;
                        }
                        (Some(1), Some(0)) => {
                            lkeys.push(&**b);
                            rkeys.push(&**a);
                            matched = true;
                        }
                        _ => {}
                    }
                }
                if !matched {
                    if j.kind == JoinKind::Inner {
                        residual.push(c);
                    } else {
                        if dbg { eprintln!("[vecx] fallback: LEFT join non-equi conjunct"); }
                        return Ok(None); // LEFT JOIN needs pure equi conditions
                    }
                }
            }
            if lkeys.is_empty() {
                if dbg { eprintln!("[vecx] fallback: no equi keys"); }
                return Ok(None); // no equi keys at all -> reference path
            }
            plans.push(EquiJoin { table: &j.table, kind: j.kind, lkeys, rkeys, residual });
            left_width = scope.width();
        }
        // run the join chain
        let mut src = VSrc::Borrowed {
            cols: base.columns.iter().map(|c| &c[..]).collect(),
            n: base.nrows(),
        };
        for plan in &plans {
            let right = guard
                .get(&plan.table.name)
                .ok_or_else(|| PqError::undefined_table(&plan.table.name))?;
            let rn = right.nrows();
            // right keys: evaluate against a right-only view (full-scope
            // indices; left entries are empty slices). The view spans only
            // the left width + this join's right width.
            let lw = src.width();
            let rw = right.columns.len();
            let right_view = VSrc::Borrowed {
                cols: (0..lw + rw)
                    .map(|i| {
                        if i < lw {
                            &[][..]
                        } else {
                            &right.columns[i - lw][..]
                        }
                    })
                    .collect(),
                n: rn,
            };
            let right_keys: Vec<Vec<Value>> = plan
                .rkeys
                .iter()
                .map(|e| veval(e, &right_view, &(0..rn).collect::<Vec<_>>(), &scope, params))
                .collect::<R<Vec<_>>>()?;
            let ln = src.nrows();
            let left_keys: Vec<Vec<Value>> = plan
                .lkeys
                .iter()
                .map(|e| veval(e, &src, &(0..ln).collect::<Vec<_>>(), &scope, params))
                .collect::<R<Vec<_>>>()?;
            let pairs = hash_join(ln, &left_keys, &right_keys, plan.kind == JoinKind::Left);
            // materialize the joined source
            let lw = src.width();
            let rw = right.columns.len();
            let mut cols: Vec<Vec<Value>> = Vec::with_capacity(lw + rw);
            for c in 0..lw {
                let old = src.col(c);
                cols.push(pairs.iter().map(|(i, _)| old[*i].clone()).collect());
            }
            for c in 0..rw {
                cols.push(
                    pairs
                        .iter()
                        .map(|(_, j)| match j {
                            Some(j) => right.columns[c][*j].clone(),
                            None => Value::Null,
                        })
                        .collect(),
                );
            }
            src = VSrc::Owned { cols };
        }
        // residual non-equi conjuncts (INNER joins only) filter the joined
        // rows — exactly ON-filter semantics for inner joins
        let residuals: Vec<Expr> = plans
            .iter()
            .flat_map(|p| p.residual.iter().map(|e| (*e).clone()))
            .collect();
        self.vexec(sel, &scope, &src, params, &residuals, None)
    }

    /// Common vectorized pipeline over a prepared source: WHERE (+ residual
    /// INNER-join conditions, applied post-join) -> items / aggregates ->
    /// HAVING -> DISTINCT -> ORDER BY -> LIMIT.
    #[allow(clippy::too_many_arguments)]
    fn vexec(
        &self,
        sel: &SelectStmt,
        scope: &Scope,
        src: &VSrc,
        params: &[Value],
        extra_filters: &[Expr],
        pre_sel: Option<Vec<usize>>,
    ) -> R<Option<StmtResult>> {
        let n = src.nrows();
        // Expand stars + output description (same helpers as the reference).
        let items: Vec<(Expr, Option<String>)> = expand_items(&sel.items, scope)?;
        let mut out_cols: Vec<(String, Ty)> = Vec::new();
        for (e, alias) in &items {
            let name = match alias {
                Some(a) => a.clone(),
                None => expr_name(e),
            };
            out_cols.push((name, infer_ty(e, scope)));
        }
        let group_by = normalize_group_by(sel, &items, &out_cols, scope);

        // split aggregates out of items / HAVING / ORDER BY
        let mut specs: Vec<AggSpec> = Vec::new();
        let item_t: Vec<(Expr, Option<String>)> = items
            .iter()
            .map(|(e, a)| (split_aggs(e, &mut specs), a.clone()))
            .collect();
        let having_t = sel.having.as_ref().map(|h| split_aggs(h, &mut specs));
        let order_t: Vec<(Expr, bool, bool)> = sel
            .order_by
            .iter()
            .map(|(e, d, nf)| (split_aggs(e, &mut specs), *d, *nf))
            .collect();

        // WHERE (+ residual join conditions) -> selection. A row survives
        // only if every predicate is Bool(true) (Null fails, like SQL AND).
        // `pre_sel` (secondary-index candidates) replaces the full row range;
        // the full predicate set still runs, so the index only prunes work —
        // it can never change results.
        let all_rows: Vec<usize> = match pre_sel {
            Some(p) => p,
            None => (0..n).collect(),
        };
        let mut predicates: Vec<&Expr> = Vec::new();
        if let Some(w) = &sel.where_ {
            predicates.push(w);
        }
        predicates.extend(extra_filters.iter());
        let sel_idx: Vec<usize> = if predicates.is_empty() {
            all_rows
        } else {
            let nsel0 = all_rows.len();
            let mut keep = vec![true; nsel0];
            for p in &predicates {
                let v = veval(p, src, &all_rows, scope, params)?;
                for j in 0..nsel0 {
                    if keep[j] && !matches!(v[j], Value::Bool(true)) {
                        keep[j] = false;
                    }
                }
            }
            (0..nsel0).filter(|&j| keep[j]).map(|j| all_rows[j]).collect()
        };
        let nsel = sel_idx.len();

        let mut out_rows: Vec<Vec<Value>> = Vec::new();
        let mut order_keys: Vec<Vec<Value>> = Vec::new();

        let agg_mode = !specs.is_empty() || !group_by.is_empty();
        if !agg_mode {
            // ---------------- plain projection ----------------
            let ocols: Vec<Vec<Value>> = item_t
                .iter()
                .map(|(e, _)| veval(e, src, &sel_idx, scope, params))
                .collect::<R<Vec<_>>>()?;
            for j in 0..nsel {
                out_rows.push(ocols.iter().map(|c| c[j].clone()).collect());
            }
            if !order_t.is_empty() {
            let mut keycols: Vec<Vec<Value>> = Vec::with_capacity(order_t.len());
            for (e, _, _) in &order_t {
                keycols.push(match e {
                    Expr::Lit(Value::Int(k)) if *k >= 1 && (*k as usize) <= ocols.len() => {
                        ocols[(*k - 1) as usize].clone()
                    }
                    Expr::Col(c) => {
                        if let Some(pos) = out_cols.iter().position(|(nm, _)| nm == c) {
                            ocols[pos].clone()
                        } else {
                            veval(e, src, &sel_idx, scope, params)?
                        }
                    }
                    _ => veval(e, src, &sel_idx, scope, params)?,
                });
            }
                for j in 0..nsel {
                    order_keys.push(keycols.iter().map(|c| c[j].clone()).collect());
                }
            }
        } else {
            // ---------------- aggregation ----------------
            // same coverage rules as the reference executor
            let mut to_check: Vec<&Expr> = Vec::new();
            for (e, _) in &item_t {
                to_check.push(e);
            }
            if let Some(h) = &having_t {
                to_check.push(h);
            }
            for e in to_check {
                if let Some(c) = uncovered_col(e, &group_by) {
                    return Err(PqError::new(
                        "42803",
                        format!(
                            "column \"{}\" must appear in the GROUP BY clause or be used in an aggregate function",
                            col_display(c)
                        ),
                    ));
                }
            }
            // vectorized aggregate arguments + group keys over the selection
            let arg_vals: Vec<Vec<Value>> = specs
                .iter()
                .map(|s| veval(&s.arg, src, &sel_idx, scope, params))
                .collect::<R<Vec<_>>>()?;
            let gvals: Vec<Vec<Value>> = group_by
                .iter()
                .map(|g| veval(g, src, &sel_idx, scope, params))
                .collect::<R<Vec<_>>>()?;
            let mut index: HashMap<Vec<GK>, usize> = HashMap::new();
            let mut group_of: Vec<usize> = vec![0; nsel];
            let mut rep: Vec<Option<usize>> = Vec::new(); // rep sel-position per group
            for j in 0..nsel {
                let key: Vec<GK> = gvals.iter().map(|g| gkey(&g[j])).collect();
                let gi = *index.entry(key).or_insert_with(|| {
                    rep.push(Some(j));
                    rep.len() - 1
                });
                group_of[j] = gi;
            }
            let ngroups = rep.len();
            // empty input without GROUP BY = one empty group (matches PG)
            if nsel == 0 && group_by.is_empty() {
                rep.push(None);
            }
            let ngroups = rep.len().max(ngroups);
            let mut agg_vals: Vec<Vec<Value>> = vec![Vec::new(); ngroups];
            for (si, spec) in specs.iter().enumerate() {
                let mut by_group: Vec<Vec<&Value>> = vec![Vec::new(); ngroups];
                for j in 0..nsel {
                    by_group[group_of[j]].push(&arg_vals[si][j]);
                }
                for gi in 0..ngroups {
                    let v = compute_agg_vals(spec, &by_group[gi])?;
                    agg_vals[gi].push(v);
                }
            }
            // per group: representative row + scalar assembly (cheap: #groups)
            let empty_row: Vec<Value> = Vec::new();
            for gi in 0..ngroups {
                let rep_row: Vec<Value> = match rep[gi] {
                    Some(j) => src.row(sel_idx[j]),
                    None => Vec::new(),
                };
                let ctx = Ctx {
                    scope,
                    row: &rep_row,
                    aggs: &agg_vals[gi],
                    params,
                };
                if let Some(h) = &having_t {
                    match eval(h, &ctx)? {
                        Value::Bool(true) => {}
                        _ => continue,
                    }
                }
                let r: R<Vec<Value>> = item_t.iter().map(|(e, _)| eval(e, &ctx)).collect();
                let r = r?;
                if !order_t.is_empty() {
                    let mut keys = Vec::with_capacity(order_t.len());
                    for (e, _, _) in &order_t {
                        keys.push(match e {
                            Expr::Lit(Value::Int(k)) if *k >= 1 && (*k as usize) <= r.len() => {
                                r[(*k - 1) as usize].clone()
                            }
                            Expr::Col(c) => {
                                if let Some(pos) = out_cols.iter().position(|(nm, _)| nm == c) {
                                    r[pos].clone()
                                } else {
                                    eval(e, &ctx)?
                                }
                            }
                            _ => eval(e, &ctx)?,
                        });
                    }
                    order_keys.push(keys);
                }
                out_rows.push(r);
            }
            let _ = empty_row;
        }

        // ---------------- DISTINCT ----------------
        if sel.distinct {
            let mut seen = std::collections::HashSet::new();
            let mut deduped = Vec::with_capacity(out_rows.len());
            for r in out_rows {
                let key: Vec<GK> = r.iter().map(gkey).collect();
                if seen.insert(key) {
                    deduped.push(r);
                }
            }
            out_rows = deduped;
        }

        // ---------------- ORDER BY ----------------
        if !order_t.is_empty() {
            let descs: Vec<(bool, bool)> =
                sel.order_by.iter().map(|(_, d, nf)| (*d, *nf)).collect();
            let mut idx: Vec<usize> = (0..out_rows.len()).collect();
            idx.sort_by(|&a, &b| {
                for (i, (desc, nf)) in descs.iter().enumerate() {
                    match order_cmp(&order_keys[a][i], &order_keys[b][i], *desc, *nf) {
                        Ok(std::cmp::Ordering::Equal) => continue,
                        Ok(o) => return o,
                        Err(_) => return std::cmp::Ordering::Equal,
                    }
                }
                std::cmp::Ordering::Equal
            });
            out_rows = idx.into_iter().map(|i| out_rows[i].clone()).collect();
        }

        // ---------------- LIMIT / OFFSET ----------------
        let mut start = 0usize;
        if let Some(o) = &sel.offset {
            let ov = eval(o, &Ctx { scope, row: &[], aggs: &[], params })?;
            if let Value::Int(i) = ov {
                start = i.max(0) as usize;
            }
        }
        if let Some(l) = &sel.limit {
            let lv = eval(l, &Ctx { scope, row: &[], aggs: &[], params })?;
            if let Value::Int(i) = lv {
                let end = start.saturating_add(i.max(0) as usize);
                out_rows.truncate(end);
            }
        }
        out_rows.drain(..start.min(out_rows.len()));

        Ok(Some(StmtResult::Rows {
            cols: out_cols,
            rows: out_rows,
            tag: "SELECT".into(),
        }))
    }
}

// ---------------------------------------------------------- index planning --

/// True if `e` references no columns (constant per query: literals, params,
/// functions of them).
fn is_const_expr(e: &Expr) -> bool {
    let mut cols = Vec::new();
    e.collect_cols(&mut cols);
    cols.is_empty() && !matches!(e, Expr::Agg(..) | Expr::AggRef(_))
}

/// Resolves `e` to a base-table column position if it is a plain (possibly
/// qualified) column reference within a single-table scope.
fn as_col_pos(e: &Expr, scope: &Scope) -> Option<usize> {
    match e {
        Expr::Col(name) => scope.resolve(name).ok(),
        Expr::QCol(t, c) => scope.resolve_q(t, c).ok(),
        _ => None,
    }
}

/// Does the constant's runtime type match the column domain exactly?
/// (Anything else falls back to a full scan so compare()/coerce semantics
/// stay identical to the reference executor.)
fn key_of_typed(v: &Value, col_ty: Ty) -> Option<IKey> {
    match (v, col_ty) {
        (Value::Bool(_), Ty::Bool)
        | (Value::Int(_), Ty::Int)
        | (Value::Float(_), Ty::Float)
        | (Value::Text(_), Ty::Text) => IKey::of(v),
        _ => None,
    }
}

/// Flattens an AND chain into conjuncts.
fn and_conjuncts<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::Bin(a, "AND", b) = e {
        and_conjuncts(a, out);
        and_conjuncts(b, out);
    } else {
        out.push(e);
    }
}

/// Uses secondary indexes to compute the candidate row positions for a WHERE
/// clause. Returns None when no index applies (caller scans everything).
/// The result is always a SUBSET of the rows the full predicate would keep,
/// so correctness never depends on the index — only speed does.
fn index_prefilter(
    w: Option<&Expr>,
    tab: &Table,
    scope: &Scope,
    params: &[Value],
) -> Option<Vec<usize>> {
    let w = w?;
    if matches!(w, Expr::Agg(..)) {
        return None;
    }
    let mut conjuncts: Vec<&Expr> = Vec::new();
    and_conjuncts(w, &mut conjuncts);
    let ctx = Ctx {
        scope,
        row: &[],
        aggs: &[],
        params,
    };
    let mut best: Option<Vec<usize>> = None;
    for c in conjuncts {
        // positions for this conjunct, if an index serves it
        let positions: Option<Vec<usize>> = (|| {
            match c {
                // col op const / const op col
                Expr::Bin(a, op, b) if matches!(*op, "=" | "<" | "<=" | ">" | ">=") => {
                    let (col_e, const_e, flipped) =
                        if as_col_pos(a, scope).is_some() && is_const_expr(b) {
                            (a, b, false)
                        } else if is_const_expr(a) && as_col_pos(b, scope).is_some() {
                            (b, a, true)
                        } else {
                            return None;
                        };
                    let col = as_col_pos(col_e, scope)?;
                    let cv = eval(const_e, &ctx).ok()?;
                    let (ix_name, ix) = tab.indexes.iter().find(|(_, ix)| ix.col == col)?;
                    let _ = ix_name;
                    let k = key_of_typed(&cv, tab.cols[col].ty)?;
                    let rowids: Vec<u64> = match (ix.method, *op) {
                        (crate::sql::IdxMethod::Hash, "=") => ix.lookup(&k).to_vec(),
                        (crate::sql::IdxMethod::BTree, "=") => ix.lookup(&k).to_vec(),
                        (crate::sql::IdxMethod::BTree, "<") => {
                            if flipped {
                                ix.greater_than(&k, false)
                            } else {
                                ix.less_than(&k, false)
                            }
                        }
                        (crate::sql::IdxMethod::BTree, "<=") => {
                            if flipped {
                                ix.greater_than(&k, true)
                            } else {
                                ix.less_than(&k, true)
                            }
                        }
                        (crate::sql::IdxMethod::BTree, ">") => {
                            if flipped {
                                ix.less_than(&k, false)
                            } else {
                                ix.greater_than(&k, false)
                            }
                        }
                        (crate::sql::IdxMethod::BTree, ">=") => {
                            if flipped {
                                ix.less_than(&k, true)
                            } else {
                                ix.greater_than(&k, true)
                            }
                        }
                        _ => return None, // hash index can only serve =
                    };
                    Some(rowids_to_positions(tab, rowids))
                }
                // col BETWEEN const AND const (btree only)
                Expr::Between(x, lo, hi, false) => {
                    let col = as_col_pos(x, scope)?;
                    if !is_const_expr(lo) || !is_const_expr(hi) {
                        return None;
                    }
                    let lov = eval(lo, &ctx).ok()?;
                    let hiv = eval(hi, &ctx).ok()?;
                    let (_n, ix) = tab.indexes.iter().find(|(_, ix)| ix.col == col)?;
                    if ix.method != crate::sql::IdxMethod::BTree {
                        return None;
                    }
                    let klo = key_of_typed(&lov, tab.cols[col].ty)?;
                    let khi = key_of_typed(&hiv, tab.cols[col].ty)?;
                    // [lo, hi] inclusive. If lo > hi the predicate is never
                    // true (SQL BETWEEN) — and BTreeMap::range would PANIC on
                    // reversed bounds, so short-circuit to empty.
                    if klo > khi {
                        return Some(Vec::new());
                    }
                    let rowids = ix.range(
                        std::ops::Bound::Included(&klo),
                        std::ops::Bound::Included(&khi),
                    );
                    Some(rowids_to_positions(tab, rowids))
                }
                // col IN (const, ...) — hash or btree point lookups
                Expr::InList(x, list, false) => {
                    let col = as_col_pos(x, scope)?;
                    if !list.iter().all(is_const_expr) {
                        return None;
                    }
                    let (_n, ix) = tab.indexes.iter().find(|(_, ix)| ix.col == col)?;
                    let mut rowids = Vec::new();
                    for e in list {
                        let v = eval(e, &ctx).ok()?;
                        // NULL list entries contribute nothing (NULL = x is
                        // never true) — matches eval_in
                        if let Some(k) = key_of_typed(&v, tab.cols[col].ty) {
                            rowids.extend_from_slice(ix.lookup(&k));
                        }
                    }
                    Some(rowids_to_positions(tab, rowids))
                }
                _ => None,
            }
        })();
        if let Some(p) = positions {
            best = match best {
                None => Some(p),
                Some(b) => Some(if p.len() < b.len() { p } else { b }),
            };
        }
    }
    best
}

/// rowids -> row positions (rowids are kept sorted ascending; a miss means
/// the row is gone — impossible in practice, but stay safe).
fn rowids_to_positions(tab: &Table, rowids: Vec<u64>) -> Vec<usize> {
    let mut pos: Vec<usize> = rowids
        .iter()
        .filter_map(|rid| tab.rowids.binary_search(rid).ok())
        .collect();
    pos.sort_unstable();
    pos.dedup();
    pos
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tab() -> Table {
        let cols = vec![
            ColumnDef { name: "a".into(), ty: Ty::Int, not_null: false },
            ColumnDef { name: "b".into(), ty: Ty::Text, not_null: false },
        ];
        let mut t = Table::empty(cols, 1);
        for i in 0..100i64 {
            let cells = vec![
                if i % 10 == 0 { Value::Null } else { Value::Int(i) },
                Value::Text(format!("v{}", i % 3)),
            ];
            let id = t.next_rowid;
            t.next_rowid += 1;
            for ix in t.indexes.values_mut() {
                if let Some(k) = IKey::of(&cells[ix.col]) {
                    ix.insert(&k, id);
                }
            }
            t.push_row(id, cells);
        }
        t
    }

    #[test]
    fn ikey_total_order() {
        // negatives < zero < positives, NaN sorts last (PG btree style)
        let mut ks = [-1.5f64, 2.5, 0.0, f64::NAN, -0.0, 1.5];
        ks.sort_by(|a, b| IKey::of(&Value::Float(*a)).unwrap().cmp(&IKey::of(&Value::Float(*b)).unwrap()));
        assert!(ks[0] == -1.5 && ks[1] == -0.0 && ks[2] == 0.0 && ks[3] == 1.5 && ks[4] == 2.5);
        assert!(ks[5].is_nan());
        // cross-type ordering is deterministic (bool < int < float < text)
        assert!(IKey::Bool(true) < IKey::Int(5));
        assert!(IKey::Int(5) < IKey::of(&Value::Float(0.0)).unwrap());
        assert!(IKey::of(&Value::Float(9.9)).unwrap() < IKey::Text("a".into()));
    }

    #[test]
    fn index_build_and_lookup() {
        let t = tab();
        let ix = t.indexes.get("missing"); // no indexes registered in tab()
        assert!(ix.is_none());
        let bt = TableIndex::build(&t, 0, crate::sql::IdxMethod::BTree);
        // 100 rows, 10 NULL keys excluded
        assert_eq!(bt.lookup(&IKey::Int(5)).len(), 1);
        assert_eq!(bt.lookup(&IKey::Int(55)).len(), 1);
        assert_eq!(bt.lookup(&IKey::Int(-5)).len(), 0);
        // keys are 1..99 minus the multiples of 10 (those are NULLs)
        assert_eq!(bt.less_than(&IKey::Int(10), false).len(), 9); // 1..9
        assert_eq!(bt.less_than(&IKey::Int(10), true).len(), 9); // 1..9 (10 is NULL)
        assert_eq!(bt.greater_than(&IKey::Int(90), false).len(), 9); // 91..99
        assert_eq!(
            bt.range(
                std::ops::Bound::Included(&IKey::Int(10)),
                std::ops::Bound::Included(&IKey::Int(19))
            ).len(),
            9 // 11..19 (10 is NULL)
        );
        let hs = TableIndex::build(&t, 1, crate::sql::IdxMethod::Hash);
        assert_eq!(hs.lookup(&IKey::Text("v0".into())).len(), 34); // 100 values, 3 distinct
    }

    #[test]
    fn index_maintenance() {
        let mut t = tab();
        let ix = TableIndex::build(&t, 0, crate::sql::IdxMethod::BTree);
        let name = "ix".to_string();
        t.indexes.insert(name.clone(), ix);
        // insert
        let id = t.next_rowid;
        let cells = vec![Value::Int(500), Value::Text("v0".into())];
        for ix in t.indexes.values_mut() {
            if let Some(k) = IKey::of(&cells[ix.col]) {
                ix.insert(&k, id);
            }
        }
        t.push_row(id, cells);
        assert_eq!(t.indexes[&name].lookup(&IKey::Int(500)).len(), 1);
        // update 500 -> 501
        let pos = t.rowids.binary_search(&id).unwrap();
        let newcells = vec![Value::Int(501), Value::Text("v0".into())];
        for ix in t.indexes.values_mut() {
            let c = ix.col;
            let old_k = IKey::of(&t.columns[c][pos]);
            let new_k = IKey::of(&newcells[c]);
            if old_k != new_k {
                if let Some(k) = &old_k { ix.remove(k, id); }
                if let Some(k) = &new_k { ix.insert(k, id); }
            }
        }
        for (c, v) in newcells.into_iter().enumerate() { t.columns[c][pos] = v; }
        assert_eq!(t.indexes[&name].lookup(&IKey::Int(500)).len(), 0);
        assert_eq!(t.indexes[&name].lookup(&IKey::Int(501)).len(), 1);
        // delete
        for ix in t.indexes.values_mut() {
            if let Some(k) = IKey::of(&t.columns[ix.col][pos]) { ix.remove(&k, id); }
        }
        assert_eq!(t.indexes[&name].lookup(&IKey::Int(501)).len(), 0);
    }
}
