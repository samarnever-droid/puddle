//! Object store abstraction: one interface, three backends.
//! - FileStore: local directory with S3-like semantics (dev, tests, "embedded")
//! - S3Store: any S3-compatible endpoint over HTTP(S) with SigV4 (src/s3.rs)
//!
//! Keys are logical ("t/<id>/manifest/3.json"); backends may map them under a
//! prefix. Conditional writes (create-if-absent / compare-and-swap on etag)
//! are the commit primitives from ARCHITECTURE.md.

use sha2::{Digest, Sha256};
#[cfg(test)] // MemStore (test-only backend)
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;
#[cfg(test)] // MemStore (test-only backend)
use std::sync::RwLock;

#[derive(Debug)]
pub enum StoreError {
    NotFound(String),
    PreconditionFailed(String),
    Io(String),
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::NotFound(k) => write!(f, "object not found: {}", k),
            StoreError::PreconditionFailed(k) => write!(f, "precondition failed: {}", k),
            StoreError::Io(m) => write!(f, "storage error: {}", m),
        }
    }
}

pub type StoreResult<T> = Result<T, StoreError>;

pub trait ObjectStore: Send + Sync {
    /// Plain put (immutable objects).
    fn put(&self, key: &str, data: &[u8]) -> StoreResult<()>;
    /// Create only if absent (S3 If-None-Match: *). Ok(false) on 412.
    fn put_if_absent(&self, key: &str, data: &[u8]) -> StoreResult<bool>;
    /// Compare-and-swap on etag (S3 If-Match). Ok(false) on 412.
    fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> StoreResult<bool>;
    fn get(&self, key: &str) -> StoreResult<Vec<u8>>;
    fn head(&self, key: &str) -> StoreResult<(String, u64)>;
    fn delete(&self, key: &str) -> StoreResult<()>;
    /// Keys under a prefix (recursive), sorted.
    fn list(&self, prefix: &str) -> StoreResult<Vec<String>>;
    /// Range read [start, end) — end exclusive; end > size clamps.
    /// Exercised by the store unit tests only (kept out of release builds).
    #[cfg(test)]
    fn get_range(&self, key: &str, start: u64, end: u64) -> StoreResult<Vec<u8>>;
}

fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    let d = h.finalize();
    let mut s = String::with_capacity(64);
    for b in d {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

// ------------------------------------------------------------------- memory --

// --------------------------------------------------------------------- file --

/// Local directory with S3-like semantics: immutable objects, atomic
/// create-if-absent / CAS on sha256 etags, tmp-file + rename writes.
pub struct FileStore {
    root: PathBuf,
}

impl FileStore {
    pub fn new(dir: &str) -> std::io::Result<FileStore> {
        let root = PathBuf::from(dir);
        std::fs::create_dir_all(&root)?;
        Ok(FileStore { root })
    }

    fn path_of(&self, key: &str) -> PathBuf {
        // logical keys never contain ".." (we generate them), but be safe
        let mut p = self.root.clone();
        for seg in key.split('/') {
            if seg == ".." || seg.is_empty() {
                continue;
            }
            p.push(seg);
        }
        p
    }

    fn etag_of(path: &std::path::Path) -> std::io::Result<String> {
        let mut f = std::fs::File::open(path)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        Ok(sha256_hex(&buf))
    }

    /// tmp + fsync + rename: readers see the old object or the new one
    fn atomic_write(&self, key: &str, data: &[u8]) -> std::io::Result<()> {
        let path = self.path_of(key);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let tmp = path.with_extension("tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(data)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }
}

impl ObjectStore for FileStore {
    fn put(&self, key: &str, data: &[u8]) -> StoreResult<()> {
        self.atomic_write(key, data).map_err(|e| StoreError::Io(e.to_string()))
    }
    fn put_if_absent(&self, key: &str, data: &[u8]) -> StoreResult<bool> {
        let path = self.path_of(key);
        if path.exists() {
            return Ok(false);
        }
        // create-if-absent: O_EXCL on the tmp file would still race the
        // rename; use a lock-free trick: write tmp, try link, unlink tmp
        let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| StoreError::Io(e.to_string()))?;
        }
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp).map_err(|e| StoreError::Io(e.to_string()))?;
            f.write_all(data).map_err(|e| StoreError::Io(e.to_string()))?;
            f.sync_all().map_err(|e| StoreError::Io(e.to_string()))?;
        }
        // hard link fails if target exists (atomic create-if-absent)
        match std::fs::hard_link(&tmp, &path) {
            Ok(_) => {
                let _ = std::fs::remove_file(&tmp);
                Ok(true)
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let _ = std::fs::remove_file(&tmp);
                Ok(false)
            }
            Err(e) => Err(StoreError::Io(e.to_string())),
        }
    }
    fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> StoreResult<bool> {
        let path = self.path_of(key);
        // absent object behaves as etag "" (CAS-on-new-key with an empty
        // expected etag is a create; a non-empty expectation fails cleanly)
        let cur = match Self::etag_of(&path) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(e) => return Err(StoreError::Io(e.to_string())),
        };
        if cur != etag {
            return Ok(false);
        }
        self.atomic_write(key, data).map_err(|e| StoreError::Io(e.to_string()))?;
        Ok(true)
    }
    fn get(&self, key: &str) -> StoreResult<Vec<u8>> {
        std::fs::read(self.path_of(key)).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StoreError::NotFound(key.to_string())
            } else {
                StoreError::Io(e.to_string())
            }
        })
    }
    fn head(&self, key: &str) -> StoreResult<(String, u64)> {
        let path = self.path_of(key);
        let len = std::fs::metadata(&path)
            .map_err(|_| StoreError::NotFound(key.to_string()))?
            .len();
        let etag = Self::etag_of(&path).map_err(|e| StoreError::Io(e.to_string()))?;
        Ok((etag, len))
    }
    fn delete(&self, key: &str) -> StoreResult<()> {
        match std::fs::remove_file(self.path_of(key)) {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StoreError::Io(e.to_string())),
        }
    }
    fn list(&self, prefix: &str) -> StoreResult<Vec<String>> {
        fn walk(dir: &std::path::Path, acc: &mut Vec<String>, root: &std::path::Path) {
            if let Ok(rd) = std::fs::read_dir(dir) {
                for ent in rd.flatten() {
                    let p = ent.path();
                    if p.is_dir() {
                        walk(&p, acc, root);
                    } else if let Ok(rel) = p.strip_prefix(root) {
                        acc.push(rel.to_string_lossy().replace('\\', "/"));
                    }
                }
            }
        }
        let mut acc = Vec::new();
        walk(&self.root, &mut acc, &self.root);
        acc.retain(|k| k.starts_with(prefix));
        acc.sort();
        Ok(acc)
    }
    #[cfg(test)]
    fn get_range(&self, key: &str, start: u64, end: u64) -> StoreResult<Vec<u8>> {
        use std::io::{Read as _, Seek as _};
        let mut f = std::fs::File::open(self.path_of(key))
            .map_err(|e| StoreError::Io(e.to_string()))?;
        let len = f
            .metadata()
            .map_err(|e| StoreError::Io(e.to_string()))?
            .len();
        let end = end.min(len);
        if end <= start {
            return Ok(Vec::new());
        }
        f.seek(std::io::SeekFrom::Start(start))
            .map_err(|e| StoreError::Io(e.to_string()))?;
        let mut buf = vec![0u8; (end - start) as usize];
        f.read_exact(&mut buf).map_err(|e| StoreError::Io(e.to_string()))?;
        Ok(buf)
    }
}

// ------------------------------------------------------------------- memory --
// Test-only in-process backend: exercises the ObjectStore contract (CAS,
// ranges, listing) without touching the filesystem or network. The server
// itself always runs on FileStore or S3Store, so this is compiled out of
// release builds entirely.

#[cfg(test)]
#[derive(Default)]
pub struct MemStore {
    map: RwLock<HashMap<String, Vec<u8>>>,
}

#[cfg(test)]
impl MemStore {
    pub fn new() -> MemStore {
        MemStore::default()
    }
}

#[cfg(test)]
impl ObjectStore for MemStore {
    fn put(&self, key: &str, data: &[u8]) -> StoreResult<()> {
        self.map
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key.to_string(), data.to_vec());
        Ok(())
    }
    fn put_if_absent(&self, key: &str, data: &[u8]) -> StoreResult<bool> {
        let mut m = self.map.write().unwrap_or_else(|e| e.into_inner());
        if m.contains_key(key) {
            return Ok(false);
        }
        m.insert(key.to_string(), data.to_vec());
        Ok(true)
    }
    fn put_if_match(&self, key: &str, data: &[u8], etag: &str) -> StoreResult<bool> {
        let mut m = self.map.write().unwrap_or_else(|e| e.into_inner());
        match m.get(key) {
            Some(cur) => {
                if sha256_hex(cur) != etag {
                    return Ok(false);
                }
                m.insert(key.to_string(), data.to_vec());
                Ok(true)
            }
            None => Ok(false),
        }
    }
    fn get(&self, key: &str) -> StoreResult<Vec<u8>> {
        self.map
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .cloned()
            .ok_or_else(|| StoreError::NotFound(key.to_string()))
    }
    fn get_range(&self, key: &str, start: u64, end: u64) -> StoreResult<Vec<u8>> {
        let m = self.map.read().unwrap_or_else(|e| e.into_inner());
        let v = m
            .get(key)
            .ok_or_else(|| StoreError::NotFound(key.to_string()))?;
        let s = (start as usize).min(v.len());
        let e = (end as usize).min(v.len());
        Ok(v[s..e].to_vec())
    }
    fn head(&self, key: &str) -> StoreResult<(String, u64)> {
        let m = self.map.read().unwrap_or_else(|e| e.into_inner());
        let v = m
            .get(key)
            .ok_or_else(|| StoreError::NotFound(key.to_string()))?;
        Ok((sha256_hex(v), v.len() as u64))
    }
    fn delete(&self, key: &str) -> StoreResult<()> {
        self.map
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(key);
        Ok(())
    }
    fn list(&self, prefix: &str) -> StoreResult<Vec<String>> {
        let m = self.map.read().unwrap_or_else(|e| e.into_inner());
        let mut ks: Vec<String> = m
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        ks.sort();
        Ok(ks)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exercise(store: &dyn ObjectStore) {
        assert!(store.put_if_absent("a/1", b"one").unwrap());
        assert!(!store.put_if_absent("a/1", b"one-again").unwrap()); // exists
        assert_eq!(store.get("a/1").unwrap(), b"one");
        let (etag, size) = store.head("a/1").unwrap();
        assert_eq!(size, 3);
        // CAS success
        assert!(store.put_if_match("a/1", b"two", &etag).unwrap());
        assert_eq!(store.get("a/1").unwrap(), b"two");
        // CAS with stale etag fails
        assert!(!store.put_if_match("a/1", b"three", &etag).unwrap());
        assert_eq!(store.get("a/1").unwrap(), b"two");
        store.put("a/2", b"0123456789").unwrap();
        assert_eq!(store.get_range("a/2", 2, 5).unwrap(), b"234");
        store.put("b/3", b"x").unwrap();
        assert_eq!(store.list("a/").unwrap(), vec!["a/1", "a/2"]);
        assert_eq!(store.list("").unwrap().len(), 3);
        store.delete("a/2").unwrap();
        assert!(store.get("a/2").is_err());
        store.delete("nope").unwrap(); // idempotent
    }

    #[test]
    fn mem_store() {
        exercise(&MemStore::new());
    }

    #[test]
    fn file_store() {
        let dir = std::env::temp_dir().join(format!("puddle-test-{}", std::process::id()));
        let s = FileStore::new(dir.to_str().unwrap()).unwrap();
        exercise(&s);
        std::fs::remove_dir_all(&dir).ok();
    }

}
