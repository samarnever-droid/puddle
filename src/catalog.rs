//! pg_catalog / information_schema: synthesized read-only views over the
//! live catalog so real tools (psql `\d`-family, DBeaver, Metabase, ORMs)
//! can introspect the database through standard PostgreSQL queries.
//!
//! These are NOT stored tables: every lookup rebuilds rows from the current
//! in-memory catalog, so DDL is reflected instantly and there is nothing to
//! persist. OIDs are deterministic (FNV-1a of the name), stable within a
//! process, which is all introspection tools need.
//!
//! Catalog tables are plain row sets served through the reference executor
//! (the vectorized path falls back for them — they are tiny).

use crate::engine::{Table, Ty, Value};
use std::collections::HashMap;
use std::sync::Mutex;

/// The database name most recently connected with (pg_database/current
/// catalog strings). puddle accepts any dbname per connection; tools list
/// what they connected to.
static DBNAME: Mutex<String> = Mutex::new(String::new());

pub fn set_dbname(name: &str) {
    let mut g = DBNAME.lock().unwrap_or_else(|e| e.into_inner());
    *g = name.to_string();
}

pub fn dbname() -> String {
    DBNAME.lock().unwrap_or_else(|e| e.into_inner()).clone()
}

fn fnv(s: &str) -> i64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    (h & 0x7fffffff) as i64 | (1 << 31)
}

// namespace oids (PG's real ones)
const NS_PUBLIC: i64 = 2200;
const NS_PG_CATALOG: i64 = 11;
const NS_INFORMATION_SCHEMA: i64 = 13071;

/// type oids — PG's real ones, so drivers map types correctly
pub fn type_oid(t: Ty) -> i64 {
    match t {
        Ty::Bool => 16,
        Ty::Int => 23,    // int4
        Ty::Float => 701, // float8
        Ty::Text => 25,
        Ty::Date => 1082,
        Ty::Time => 1083,
        Ty::Timestamp => 1114,
        Ty::Interval => 1186,
    }
}

fn type_name(t: Ty) -> &'static str {
    match t {
        Ty::Bool => "boolean",
        Ty::Int => "integer",
        Ty::Float => "double precision",
        Ty::Text => "text",
        Ty::Date => "date",
        Ty::Time => "time without time zone",
        Ty::Timestamp => "timestamp without time zone",
        Ty::Interval => "interval",
    }
}

/// PG builtin types tools expect to find in pg_type.
const BUILTIN_TYPES: &[(i64, &str, i16, &str, &str)] = &[
    // (oid, typname, typlen, typtype, typcategory)
    (16, "bool", 1, "b", "B"),
    (17, "bytea", -1, "b", "U"),
    (18, "char", 1, "b", "Z"),
    (19, "name", 64, "b", "S"),
    (20, "int8", 8, "b", "N"),
    (21, "int2", 2, "b", "N"),
    (23, "int4", 4, "b", "N"),
    (25, "text", -1, "b", "S"),
    (26, "oid", 4, "b", "N"),
    (114, "json", -1, "b", "U"),
    (3802, "jsonb", -1, "b", "U"),
    (700, "float4", 4, "b", "N"),
    (701, "float8", 8, "b", "N"),
    (1042, "bpchar", -1, "b", "S"),
    (1043, "varchar", -1, "b", "S"),
    (1082, "date", 4, "b", "D"),
    (1083, "time", 8, "b", "D"),
    (1114, "timestamp", 8, "b", "D"),
    (1184, "timestamptz", 8, "b", "D"),
    (1186, "interval", 16, "b", "T"),
    (1700, "numeric", -1, "b", "N"),
    (2205, "regclass", 4, "b", "N"),
    (2950, "uuid", 16, "b", "U"),
];

const AM_HEAP: i64 = 2;
const AM_BTREE: i64 = 403;
const AM_HASH: i64 = 405;

type Cols = Vec<(String, Ty)>;
type Rows = Vec<Vec<Value>>;

struct CatBuilder {
    cols: Cols,
    rows: Rows,
}

impl CatBuilder {
    fn new(cols: &[(&str, Ty)]) -> Self {
        CatBuilder {
            cols: cols.iter().map(|(n, t)| (n.to_string(), *t)).collect(),
            rows: Vec::new(),
        }
    }
    fn row(&mut self, vals: Vec<Value>) {
        self.rows.push(vals);
    }
    fn finish(self) -> (Cols, Rows) {
        (self.cols, self.rows)
    }
}

fn int(i: i64) -> Value {
    Value::Int(i)
}
fn txt(s: &str) -> Value {
    Value::Text(s.to_string())
}

/// Splits "schema.table" (or bare) into (schema, table) for routing.
fn split_qual(name: &str) -> (&str, &str) {
    match name.rfind('.') {
        Some(i) => (&name[..i], &name[i + 1..]),
        None => ("", name),
    }
}

/// Looks up a catalog relation by (possibly schema-qualified, possibly
/// bare) name. Returns None for non-catalog names.
pub fn lookup(name: &str, tables: &HashMap<String, Table>) -> Option<(Cols, Rows)> {
    let (schema, bare) = split_qual(name);
    match schema {
        "pg_catalog" => pg_catalog(bare, tables),
        "information_schema" => information_schema(bare, tables),
        // bare name: pg_catalog first, then information_schema (user tables
        // were already checked by the caller)
        "" => pg_catalog(bare, tables).or_else(|| information_schema(bare, tables)),
        _ => None,
    }
}

fn pg_catalog(name: &str, tables: &HashMap<String, Table>) -> Option<(Cols, Rows)> {
    // stable iteration order for deterministic output
    let mut names: Vec<&String> = tables.keys().collect();
    names.sort();
    match name {
        "pg_namespace" => {
            let mut b = CatBuilder::new(&[
                ("oid", Ty::Int),
                ("nspname", Ty::Text),
                ("nspowner", Ty::Int),
            ]);
            b.row(vec![int(NS_PUBLIC), txt("public"), int(10)]);
            b.row(vec![int(NS_PG_CATALOG), txt("pg_catalog"), int(10)]);
            b.row(vec![int(NS_INFORMATION_SCHEMA), txt("information_schema"), int(10)]);
            Some(b.finish())
        }
        "pg_database" => {
            let mut b = CatBuilder::new(&[
                ("oid", Ty::Int),
                ("datname", Ty::Text),
                ("datdba", Ty::Int),
                ("encoding", Ty::Int),
                ("datlocprovider", Ty::Text),
                ("datcollate", Ty::Text),
                ("datctype", Ty::Text),
                ("daticulocale", Ty::Text),
                ("daticurules", Ty::Text),
                ("datacl", Ty::Text),
                ("datallowconn", Ty::Bool),
            ]);
            b.row(vec![
                int(fnv(&dbname()) & 0x7fffffff),
                txt(&dbname()),
                int(10),
                int(6), // UTF8
                txt("b"),
                txt("C"),
                txt("C"),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Bool(true),
            ]);
            Some(b.finish())
        }
        "pg_am" => {
            let mut b = CatBuilder::new(&[
                ("oid", Ty::Int),
                ("amname", Ty::Text),
                ("amhandler", Ty::Text),
                ("amtype", Ty::Text),
            ]);
            b.row(vec![int(AM_HEAP), txt("heap"), txt("heap_tableam_handler"), txt("t")]);
            b.row(vec![int(AM_BTREE), txt("btree"), txt("bthandler"), txt("i")]);
            b.row(vec![int(AM_HASH), txt("hash"), txt("hashhandler"), txt("i")]);
            Some(b.finish())
        }
        "pg_class" => {
            let mut b = CatBuilder::new(&[
                ("oid", Ty::Int),
                ("relname", Ty::Text),
                ("relnamespace", Ty::Int),
                ("reltype", Ty::Int),
                ("relowner", Ty::Int),
                ("relam", Ty::Int),
                ("relkind", Ty::Text),
                ("relnatts", Ty::Int),
                ("relhasindex", Ty::Bool),
                ("reltuples", Ty::Int),
                ("relpages", Ty::Int),
                ("relispartition", Ty::Bool),
                ("relchecks", Ty::Int),
                ("relhasrules", Ty::Bool),
                ("relhastriggers", Ty::Bool),
                ("relrowsecurity", Ty::Bool),
                ("relforcerowsecurity", Ty::Bool),
                ("reloftype", Ty::Int),
                ("relpersistence", Ty::Text),
                ("relreplident", Ty::Text),
                ("reltoastrelid", Ty::Int),
                ("reltablespace", Ty::Int),
            ]);
            for tname in &names {
                let t = &tables[tname.as_str()];
                let oid = fnv(tname);
                b.row(vec![
                    int(oid),
                    txt(tname),
                    int(NS_PUBLIC),
                    int(fnv(&format!("{}_type", tname))),
                    int(10),
                    int(AM_HEAP),
                    txt("r"),
                    int(t.cols.len() as i64),
                    Value::Bool(!t.indexes.is_empty()),
                    int(t.nrows() as i64),
                    int((t.nrows() / 128).max(1) as i64),
                    Value::Bool(false),
                    int(0),                // relchecks
                    Value::Bool(false),    // relhasrules
                    Value::Bool(false),    // relhastriggers
                    Value::Bool(false),    // relrowsecurity
                    Value::Bool(false),    // relforcerowsecurity
                    int(0),                // reloftype
                    txt("p"),              // relpersistence
                    txt("d"),              // relreplident
                    int(0),                // reltoastrelid
                    int(0),                // reltablespace
                ]);
                // index relations
                let mut inames: Vec<&String> = t.indexes.keys().collect();
                inames.sort();
                for iname in inames {
                    let ix = &t.indexes[iname.as_str()];
                    b.row(vec![
                        int(fnv(iname)),
                        txt(iname),
                        int(NS_PUBLIC),
                        int(0),
                        int(10),
                        int(if ix.method == crate::sql::IdxMethod::BTree {
                            AM_BTREE
                        } else {
                            AM_HASH
                        }),
                        txt("i"),
                        int(1),
                        Value::Bool(false),
                        int(t.nrows() as i64),
                        int((t.nrows() / 128).max(1) as i64),
                        Value::Bool(false),
                        int(0),             // relchecks
                        Value::Bool(false), // relhasrules
                        Value::Bool(false), // relhastriggers
                        Value::Bool(false), // relrowsecurity
                        Value::Bool(false), // relforcerowsecurity
                        int(0),             // reloftype
                        txt("p"),           // relpersistence
                        txt("d"),           // relreplident
                        int(0),             // reltoastrelid
                        int(0),             // reltablespace
                    ]);
                }
            }
            Some(b.finish())
        }
        "pg_attribute" => {
            let mut b = CatBuilder::new(&[
                ("attrelid", Ty::Int),
                ("attname", Ty::Text),
                ("atttypid", Ty::Int),
                ("attlen", Ty::Int),
                ("attnum", Ty::Int),
                ("attndims", Ty::Int),
                ("attcacheoff", Ty::Int),
                ("atttypmod", Ty::Int),
                ("attnotnull", Ty::Bool),
                ("atthasdef", Ty::Bool),
                ("attidentity", Ty::Text),
                ("attgenerated", Ty::Text),
                ("attisdropped", Ty::Bool),
                ("attislocal", Ty::Bool),
                ("attstorage", Ty::Text),
            ]);
            for tname in &names {
                let t = &tables[tname.as_str()];
                let oid = fnv(tname);
                for (i, c) in t.cols.iter().enumerate() {
                    let typlen = match c.ty {
                        Ty::Bool => 1,
                        Ty::Int => 4,
                        Ty::Float => 8,
                        Ty::Text => -1,
                        Ty::Date => 4,
                        Ty::Time => 8,
                        Ty::Timestamp => 8,
                        Ty::Interval => 16,
                    };
                    b.row(vec![
                        int(oid),
                        txt(&c.name),
                        int(type_oid(c.ty)),
                        int(typlen as i64),
                        int(i as i64 + 1),
                        int(0),
                        int(-1),
                        int(-1),
                        Value::Bool(c.not_null),
                        Value::Bool(false),
                        txt(""),
                        txt(""),
                        Value::Bool(false),
                        Value::Bool(true),
                        txt("p"),
                    ]);
                }
            }
            Some(b.finish())
        }
        "pg_attrdef" => {
            let b = CatBuilder::new(&[
                ("oid", Ty::Int),
                ("adrelid", Ty::Int),
                ("adnum", Ty::Int),
                ("adbin", Ty::Text),
            ]);
            Some(b.finish())
        }
        // roles: a single implicit superuser "puddle" (\du, \dg probes)
        "pg_roles" => {
            let mut b = CatBuilder::new(&[
                ("rolname", Ty::Text),
                ("rolsuper", Ty::Bool),
                ("rolinherit", Ty::Bool),
                ("rolcreaterole", Ty::Bool),
                ("rolcreatedb", Ty::Bool),
                ("rolcanlogin", Ty::Bool),
                ("rolreplication", Ty::Bool),
                ("rolconnlimit", Ty::Int),
                ("rolvaliduntil", Ty::Text),
                ("oid", Ty::Int),
            ]);
            b.row(vec![
                txt("puddle"),
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(true),
                Value::Bool(false),
                int(-1),
                Value::Null,
                int(10),
            ]);
            Some(b.finish())
        }
        // role membership: empty (no GRANT support)
        "pg_auth_members" => {
            let b = CatBuilder::new(&[
                ("roleid", Ty::Int),
                ("member", Ty::Int),
                ("grantor", Ty::Int),
            ]);
            Some(b.finish())
        }
        // extended statistics: not represented; empty keeps psql's \d
        // statistics probe well-formed
        "pg_statistic_ext" => {
            let b = CatBuilder::new(&[
                ("oid", Ty::Int),
                ("stxrelid", Ty::Int),
                ("stxname", Ty::Text),
                ("stxnamespace", Ty::Int),
                ("stxstattarget", Ty::Int),
                ("stxkind", Ty::Text),
            ]);
            Some(b.finish())
        }
        "pg_statistic_ext_data" => {
            let b = CatBuilder::new(&[("stxoid", Ty::Int)]);
            Some(b.finish())
        }
        // table inheritance / partitioning: not represented; empty keeps
        // psql's \d probes well-formed
        "pg_inherits" => {
            let b = CatBuilder::new(&[
                ("inhrelid", Ty::Int),
                ("inhparent", Ty::Int),
                ("inhseqno", Ty::Int),
                ("inhdetachpending", Ty::Bool),
            ]);
            Some(b.finish())
        }
        // logical replication publications: not represented; empty keeps
        // psql's \d publication probe well-formed
        "pg_publication" => {
            let b = CatBuilder::new(&[
                ("oid", Ty::Int),
                ("pubname", Ty::Text),
                ("puballtables", Ty::Bool),
                ("pubinsert", Ty::Bool),
                ("pubupdate", Ty::Bool),
                ("pubdelete", Ty::Bool),
            ]);
            Some(b.finish())
        }
        "pg_publication_namespace" => {
            let b = CatBuilder::new(&[("pnpubid", Ty::Int), ("pnnspid", Ty::Int)]);
            Some(b.finish())
        }
        "pg_publication_rel" => {
            let b = CatBuilder::new(&[
                ("oid", Ty::Int),
                ("prpubid", Ty::Int),
                ("prrelid", Ty::Int),
                ("prqual", Ty::Text),
                ("prattrs", Ty::Text),
            ]);
            Some(b.finish())
        }
        // row security policies are not represented: empty keeps psql's
        // \d policy probe well-formed
        "pg_policy" => {
            let b = CatBuilder::new(&[
                ("oid", Ty::Int),
                ("polname", Ty::Text),
                ("polpermissive", Ty::Bool),
                ("polroles", Ty::Int),
                ("polrelid", Ty::Int),
                ("polcmd", Ty::Text),
                ("polqual", Ty::Text),
                ("polwithcheck", Ty::Text),
            ]);
            Some(b.finish())
        }
        // constraints are not represented yet: an empty relation keeps
        // psql's \d index/constraint probes well-formed (they LEFT JOIN it)
        "pg_constraint" => {
            let b = CatBuilder::new(&[
                ("oid", Ty::Int),
                ("conname", Ty::Text),
                ("connamespace", Ty::Int),
                ("contype", Ty::Text),
                ("conrelid", Ty::Int),
                ("conindid", Ty::Int),
                ("confrelid", Ty::Int),
                ("condeferrable", Ty::Bool),
                ("condeferred", Ty::Bool),
            ]);
            Some(b.finish())
        }
        "pg_type" => {
            let mut b = CatBuilder::new(&[
                ("oid", Ty::Int),
                ("typname", Ty::Text),
                ("typnamespace", Ty::Int),
                ("typowner", Ty::Int),
                ("typlen", Ty::Int),
                ("typtype", Ty::Text),
                ("typcategory", Ty::Text),
                ("typispreferred", Ty::Bool),
                ("typdelim", Ty::Text),
                ("typrelid", Ty::Int),
                ("typelem", Ty::Int),
                ("typarray", Ty::Int),
            ]);
            for (oid, name, len, typtype, category) in BUILTIN_TYPES {
                b.row(vec![
                    int(*oid),
                    txt(name),
                    int(NS_PG_CATALOG),
                    int(10),
                    int(*len as i64),
                    txt(typtype),
                    txt(category),
                    Value::Bool(*name == "text"),
                    txt(","),
                    int(0),
                    int(0),
                    int(fnv(&format!("{}_array", name))),
                ]);
            }
            Some(b.finish())
        }
        "pg_index" => {
            let mut b = CatBuilder::new(&[
                ("indexrelid", Ty::Int),
                ("indrelid", Ty::Int),
                ("indnatts", Ty::Int),
                ("indisunique", Ty::Bool),
                ("indisprimary", Ty::Bool),
                ("indisvalid", Ty::Bool),
                ("indkey", Ty::Int),
                ("indnkeyatts", Ty::Int),
                ("indisclustered", Ty::Bool),
                ("indisreplident", Ty::Bool),
            ]);
            for tname in &names {
                let t = &tables[tname.as_str()];
                let relid = fnv(tname);
                let mut inames: Vec<&String> = t.indexes.keys().collect();
                inames.sort();
                for iname in inames {
                    let ix = &t.indexes[iname.as_str()];
                    b.row(vec![
                        int(fnv(iname)),
                        int(relid),
                        int(1),
                        Value::Bool(false),
                        Value::Bool(false),
                        Value::Bool(true),
                        int(ix.col as i64 + 1),
                        int(1),
                        Value::Bool(false),
                        Value::Bool(false),
                    ]);
                }
            }
            Some(b.finish())
        }
        "pg_tables" => {
            let mut b = CatBuilder::new(&[
                ("schemaname", Ty::Text),
                ("tablename", Ty::Text),
                ("tableowner", Ty::Text),
                ("tablespace", Ty::Text),
                ("hasindexes", Ty::Bool),
                ("hasrules", Ty::Bool),
                ("hastriggers", Ty::Bool),
                ("rowsecurity", Ty::Bool),
            ]);
            for tname in &names {
                let t = &tables[tname.as_str()];
                b.row(vec![
                    txt("public"),
                    txt(tname),
                    txt("puddle"),
                    Value::Null,
                    Value::Bool(!t.indexes.is_empty()),
                    Value::Bool(false),
                    Value::Bool(false),
                    Value::Bool(false),
                ]);
            }
            Some(b.finish())
        }
        "pg_indexes" => {
            let mut b = CatBuilder::new(&[
                ("schemaname", Ty::Text),
                ("tablename", Ty::Text),
                ("indexname", Ty::Text),
                ("tablespace", Ty::Text),
                ("indexdef", Ty::Text),
            ]);
            for tname in &names {
                let t = &tables[tname.as_str()];
                let mut inames: Vec<&String> = t.indexes.keys().collect();
                inames.sort();
                for iname in inames {
                    let ix = &t.indexes[iname.as_str()];
                    let method = match ix.method {
                        crate::sql::IdxMethod::BTree => "btree",
                        crate::sql::IdxMethod::Hash => "hash",
                    };
                    let col = t.cols[ix.col].name.clone();
                    b.row(vec![
                        txt("public"),
                        txt(tname),
                        txt(iname),
                        Value::Null,
                        txt(&format!(
                            "CREATE INDEX {} ON public.{} USING {} ({})",
                            iname, tname, method, col
                        )),
                    ]);
                }
            }
            Some(b.finish())
        }
        "pg_description" => {
            let b = CatBuilder::new(&[
                ("objoid", Ty::Int),
                ("classoid", Ty::Int),
                ("objsubid", Ty::Int),
                ("description", Ty::Text),
            ]);
            Some(b.finish())
        }
        "pg_collation" => {
            let mut b = CatBuilder::new(&[
                ("oid", Ty::Int),
                ("collname", Ty::Text),
                ("collnamespace", Ty::Int),
                ("collencoding", Ty::Int),
            ]);
            // intentionally empty: psql's \d attcollation probe is a
            // correlated subquery over pg_collation; an empty table makes
            // it correctly yield NULL (column collation is not reported)
            let _ = &mut b;
            Some(b.finish())
        }
        "pg_proc" => {
            let b = CatBuilder::new(&[
                ("oid", Ty::Int),
                ("proname", Ty::Text),
            ]);
            Some(b.finish())
        }
        _ => None,
    }
}

fn information_schema(name: &str, tables: &HashMap<String, Table>) -> Option<(Cols, Rows)> {
    let mut names: Vec<&String> = tables.keys().collect();
    names.sort();
    let db = dbname();
    match name {
        "tables" => {
            let mut b = CatBuilder::new(&[
                ("table_catalog", Ty::Text),
                ("table_schema", Ty::Text),
                ("table_name", Ty::Text),
                ("table_type", Ty::Text),
            ]);
            for tname in &names {
                b.row(vec![txt(&db), txt("public"), txt(tname), txt("BASE TABLE")]);
            }
            Some(b.finish())
        }
        "columns" => {
            let mut b = CatBuilder::new(&[
                ("table_catalog", Ty::Text),
                ("table_schema", Ty::Text),
                ("table_name", Ty::Text),
                ("column_name", Ty::Text),
                ("ordinal_position", Ty::Int),
                ("column_default", Ty::Text),
                ("is_nullable", Ty::Text),
                ("data_type", Ty::Text),
                ("character_maximum_length", Ty::Int),
                ("numeric_precision", Ty::Int),
            ]);
            for tname in &names {
                let t = &tables[tname.as_str()];
                for (i, c) in t.cols.iter().enumerate() {
                    let (maxlen, prec) = match c.ty {
                        Ty::Text => (Value::Null, Value::Null),
                        Ty::Int => (Value::Null, int(32)),
                        Ty::Float => (Value::Null, int(53)),
                        Ty::Bool => (Value::Null, Value::Null),
                        Ty::Date => (Value::Null, Value::Null),
                        Ty::Time => (Value::Null, int(6)),
                        Ty::Timestamp => (Value::Null, int(6)),
                        Ty::Interval => (Value::Null, Value::Null),
                    };
                    b.row(vec![
                        txt(&db),
                        txt("public"),
                        txt(tname),
                        txt(&c.name),
                        int(i as i64 + 1),
                        Value::Null,
                        txt(if c.not_null { "NO" } else { "YES" }),
                        txt(type_name(c.ty)),
                        maxlen,
                        prec,
                    ]);
                }
            }
            Some(b.finish())
        }
        "schemata" => {
            let mut b = CatBuilder::new(&[
                ("catalog_name", Ty::Text),
                ("schema_name", Ty::Text),
                ("schema_owner", Ty::Text),
            ]);
            b.row(vec![txt(&db), txt("public"), txt("puddle")]);
            b.row(vec![txt(&db), txt("pg_catalog"), txt("puddle")]);
            b.row(vec![txt(&db), txt("information_schema"), txt("puddle")]);
            Some(b.finish())
        }
        _ => None,
    }
}

/// Maps a type oid (from pg_type) to its SQL display name for
/// format_type().
pub fn format_type(oid: i64) -> Option<&'static str> {
    for (o, name, _, _, _) in BUILTIN_TYPES {
        if *o == oid {
            return Some(match *name {
                "bool" => "boolean",
                "int2" => "smallint",
                "int4" => "integer",
                "int8" => "bigint",
                "float4" => "real",
                "float8" => "double precision",
                "bpchar" => "character",
                "timestamptz" => "timestamp with time zone",
                "time" => "time without time zone",
                "timestamp" => "timestamp without time zone",
                other => other,
            });
        }
    }
    None
}
