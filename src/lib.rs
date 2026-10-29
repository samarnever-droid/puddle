//! puddle — a lightweight lakehouse that speaks PostgreSQL and lives in
//! an S3 bucket. Library crate: the same engine the server binary runs,
//! linkable in-process (no TCP) or over the wire.
//!
//! Three ways to embed:
//!   * `open_memory()` — pure in-RAM database, dies with the handle
//!   * `open_storage("file:///dir", ..)` — durable local directory
//!   * `open_storage("s3://bucket/prefix", ..)` — an S3 bucket as state
//!
//! plus the C ABI in `ffi.rs` (cdylib) for any FFI-capable language.

pub mod auth;
pub mod catalog;
pub mod copy;
pub mod crc32c;
pub mod engine;
pub mod ffi;
pub mod part;
pub mod persist;
pub mod s3;
pub mod sql;
pub mod store;
pub mod temporal;
pub mod vecx;
pub mod wal;
pub mod wire;

use engine::Db;
use std::sync::Arc;

/// Single source of truth for the puddle version.
pub const VERSION: &str = "2.2.0";

/// A pure in-memory database: nothing is ever written anywhere, the data
/// dies with the handle (SQLite `:memory:` semantics).
pub fn open_memory() -> Arc<Db> {
    Arc::new(Db::new())
}

/// Opens a durable database from a `file://` directory or `s3://bucket/
/// prefix` URL, running recovery. This is exactly the bootstrap the
/// server binary performs (including the relaxed-ack WAL flusher thread
/// when `ack = "relaxed"`).
#[allow(clippy::too_many_arguments)]
pub fn open_storage(
    url: &str,
    endpoint: Option<&str>,
    region: Option<&str>,
    access_key: Option<&str>,
    secret_key: Option<&str>,
    ack: &str,
) -> Result<Arc<Db>, String> {
    let store: Box<dyn store::ObjectStore + Send + Sync> = if let Some(rest) =
        url.strip_prefix("file://")
    {
        let dir = rest.trim_end_matches('/');
        std::fs::create_dir_all(dir).map_err(|e| format!("cannot create data dir {}: {}", dir, e))?;
        Box::new(
            store::FileStore::new(dir).map_err(|e| format!("cannot open data dir {}: {}", dir, e))?,
        )
    } else if let Some(rest) = url.strip_prefix("s3://") {
        let (bucket, prefix) = match rest.split_once('/') {
            Some((b, p)) => (b, p),
            None => (rest, ""),
        };
        let endpoint = endpoint.unwrap_or("https://s3.amazonaws.com");
        let region = region.unwrap_or("us-east-1");
        let ak = access_key.unwrap_or("");
        let sk = secret_key.unwrap_or("");
        if ak.is_empty() || sk.is_empty() {
            return Err("s3 storage needs access and secret keys".into());
        }
        Box::new(s3::S3Store::new(endpoint, bucket, prefix, region, ak, sk))
    } else {
        return Err(format!(
            "storage url must be file:///dir or s3://bucket/prefix (got {})",
            url
        ));
    };
    let ack_level = match ack {
        "relaxed" => persist::AckLevel::Relaxed,
        "committed" | "" => persist::AckLevel::Committed,
        other => return Err(format!("ack must be relaxed|committed (got {})", other)),
    };
    let (p, tables) =
        persist::Persist::open(store, ack_level).map_err(|e| format!("recovery failed: {}", e))?;
    let n: usize = tables.values().map(|t| t.nrows()).sum();
    eprintln!("* recovered {} tables, {} rows", tables.len(), n);
    let p = Arc::new(p);
    if ack_level == persist::AckLevel::Relaxed {
        let fp = p.clone();
        std::thread::Builder::new()
            .name("wal-flusher".into())
            .spawn(move || loop {
                std::thread::sleep(std::time::Duration::from_millis(500));
                if let Err(e) = fp.flush_all() {
                    eprintln!("! wal flusher: {}", e);
                }
            })
            .ok();
    }
    Ok(Arc::new(Db::from_parts(tables, p)))
}
