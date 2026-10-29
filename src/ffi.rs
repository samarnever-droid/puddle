//! C ABI for embedding puddle in-process without any socket (the
//! "inlink"): link against the cdylib (libpuddle.so / puddle.dll) or the
//! rlib from Rust. SQLite-style surface:
//!
//! ```c
//! puddle_db *db  = puddle_open("/path/to/dir");   // NULL = in-memory
//! puddle_result *r = puddle_exec(db, "SELECT 1 + 2");
//! if (puddle_ok(r)) {
//!     for (uint64_t i = 0; i < puddle_rows(r); i++)
//!         printf("%s\n", puddle_get(r, i, 0));    // text-encoded
//! }
//! puddle_result_free(r);
//! puddle_close(db);
//! ```
//!
//! Notes:
//!   * `puddle_exec` runs every statement in the SQL string (like
//!     PQexec); the result set is that of the LAST statement, the tag of
//!     the last statement overall.
//!   * values are text-encoded exactly as the PostgreSQL wire would
//!     (a NULL cell is a NULL pointer; SQL NULL is representable).
//!   * one database handle is safe to use from multiple threads
//!     concurrently (the engine is RwLock-guarded), but a single
//!     `puddle_result` belongs to whoever created it.
//!   * parameter binding is not part of this ABI — interpolate and
//!     quote carefully, or use the Rust API (`Db::execute` takes typed
//!     parameters) from the rlib.

use crate::engine::{value_to_text, Db, StmtResult};
use std::ffi::{c_char, CStr, CString};
use std::sync::Arc;

/// Opaque database handle.
pub struct PuddleDb {
    db: Arc<Db>,
}

/// Result of `puddle_exec`. Owned entirely by the caller after return;
/// release with `puddle_result_free`.
#[repr(C)]
pub struct PuddleResult {
    ok: i32,
    /// null when ok != 0; "SQLSTATE: message" otherwise
    error: *mut c_char,
    /// command tag of the last statement ("SELECT", "INSERT 0 3", ...)
    tag: *mut c_char,
    n_cols: u64,
    n_rows: u64,
    /// n_cols column names (owned)
    col_names: *mut *mut c_char,
    /// n_rows * n_cols cells, row-major (owned); a null cell is SQL NULL
    cells: *mut *mut c_char,
}

fn cstr(s: &str) -> *mut c_char {
    match CString::new(s) {
        Ok(c) => c.into_raw(),
        // interior NUL cannot occur in our encodings; fall back to empty
        Err(_) => CString::new("").map(|c| c.into_raw()).unwrap_or(null_mut_helper()),
    }
}

fn null_mut_helper() -> *mut c_char {
    std::ptr::null_mut()
}

fn empty_result() -> PuddleResult {
    PuddleResult {
        ok: 0,
        error: std::ptr::null_mut(),
        tag: std::ptr::null_mut(),
        n_cols: 0,
        n_rows: 0,
        col_names: std::ptr::null_mut(),
        cells: std::ptr::null_mut(),
    }
}

/// Opens a database. `path == NULL` or `""` opens a pure in-memory
/// database. Otherwise `path` is a directory (created if missing) or a
/// `file://` URL; recovery runs before the handle is returned. Returns
/// NULL on failure.
/// # Safety
/// `path` must be NULL or a valid NUL-terminated C string.
#[no_mangle]
pub unsafe extern "C" fn puddle_open(path: *const c_char) -> *mut PuddleDb {
    if path.is_null() {
        return Box::into_raw(Box::new(PuddleDb {
            db: crate::open_memory(),
        }));
    }
    let p = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => s,
        Err(_) => return std::ptr::null_mut(),
    };
    let url = if p.is_empty() {
        return Box::into_raw(Box::new(PuddleDb {
            db: crate::open_memory(),
        }));
    } else if p.starts_with("file://") || p.starts_with("s3://") {
        p.to_string()
    } else if p.contains("://") {
        // some other scheme (ftp://, postgres://, ...) — a typo, not a
        // directory name; refuse rather than creating a weird directory
        return std::ptr::null_mut();
    } else {
        format!("file://{}", p)
    };
    match crate::open_storage(&url, None, None, None, None, "committed") {
        Ok(db) => Box::into_raw(Box::new(PuddleDb { db })),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Closes a database handle opened with `puddle_open`. NULL is accepted
/// and ignored. Outstanding results stay valid until freed.
/// # Safety
/// `db` must be NULL or a handle from `puddle_open` that has not been
/// closed yet (closing twice is UB).
#[no_mangle]
pub unsafe extern "C" fn puddle_close(db: *mut PuddleDb) {
    if !db.is_null() {
        unsafe { drop(Box::from_raw(db)) };
    }
}

/// Runs every statement in `sql` against the database. Never panics and
/// never blocks longer than the statements themselves; errors are
/// reported through the result, not by returning NULL.
/// # Safety
/// `db` must be a live handle from `puddle_open`; `sql` must be a valid
/// NUL-terminated C string. The returned result must be freed with
/// `puddle_result_free` exactly once.
#[no_mangle]
pub unsafe extern "C" fn puddle_exec(db: *mut PuddleDb, sql: *const c_char) -> *mut PuddleResult {
    if db.is_null() || sql.is_null() {
        let mut r = empty_result();
        r.error = cstr("XX000: null handle or null sql to puddle_exec");
        return Box::into_raw(Box::new(r));
    }
    let this = unsafe { &*db };
    let sql_text = match unsafe { CStr::from_ptr(sql) }.to_str() {
        Ok(s) => s,
        Err(_) => {
            let mut r = empty_result();
            r.error = cstr("XX000: sql is not valid UTF-8");
            return Box::into_raw(Box::new(r));
        }
    };
    let toks = match crate::sql::lex(sql_text) {
        Ok(t) => t,
        Err(e) => {
            let mut r = empty_result();
            r.error = cstr(&format!("42601: {}", e));
            return Box::into_raw(Box::new(r));
        }
    };
    let stmts = match crate::sql::Parser::new(toks).parse_all() {
        Ok(s) => s,
        Err(e) => {
            let mut r = empty_result();
            r.error = cstr(&format!("42601: {}", e));
            return Box::into_raw(Box::new(r));
        }
    };
    let mut last: Option<StmtResult> = None;
    for st in &stmts {
        match this.db.execute(st, &[]) {
            Ok(res) => last = Some(res),
            Err(e) => {
                let mut r = empty_result();
                r.error = cstr(&format!("{}: {}", e.code, e.msg));
                return Box::into_raw(Box::new(r));
            }
        }
    }
    let mut r = empty_result();
    r.ok = 1;
    match last {
        None => {
            r.tag = cstr("EMPTY");
        }
        Some(StmtResult::Tag(tag)) => {
            r.tag = cstr(&tag);
        }
        Some(StmtResult::Rows { cols, rows, tag }) => {
            r.tag = cstr(&tag);
            r.n_cols = cols.len() as u64;
            r.n_rows = rows.len() as u64;
            let mut names: Vec<*mut c_char> = Vec::with_capacity(cols.len());
            for (n, _) in &cols {
                names.push(cstr(n));
            }
            r.col_names = if names.is_empty() {
                std::ptr::null_mut()
            } else {
                Box::into_raw(names.into_boxed_slice()) as *mut *mut c_char
            };
            let mut cells: Vec<*mut c_char> = Vec::with_capacity(rows.len() * cols.len());
            for row in &rows {
                for v in row {
                    cells.push(match value_to_text(v) {
                        Some(t) => cstr(&t),
                        None => std::ptr::null_mut(),
                    });
                }
            }
            r.cells = if cells.is_empty() {
                std::ptr::null_mut()
            } else {
                Box::into_raw(cells.into_boxed_slice()) as *mut *mut c_char
            };
        }
    }
    Box::into_raw(Box::new(r))
}

/// 1 when the exec succeeded, 0 when it errored.
/// # Safety
/// `r` must be NULL or a live result from `puddle_exec` not yet freed.
#[no_mangle]
pub unsafe extern "C" fn puddle_ok(r: *const PuddleResult) -> i32 {
    if r.is_null() {
        0
    } else {
        unsafe { (*r).ok }
    }
}

/// Error text ("SQLSTATE: message") or NULL. Valid until
/// `puddle_result_free`.
/// # Safety
/// `r` must be NULL or a live result from `puddle_exec` not yet freed.
#[no_mangle]
pub unsafe extern "C" fn puddle_error(r: *const PuddleResult) -> *const c_char {
    if r.is_null() {
        std::ptr::null()
    } else {
        unsafe { (*r).error }
    }
}

/// Command tag of the last statement ("SELECT", "INSERT 0 3", ...).
/// # Safety
/// `r` must be NULL or a live result from `puddle_exec` not yet freed.
#[no_mangle]
pub unsafe extern "C" fn puddle_tag(r: *const PuddleResult) -> *const c_char {
    if r.is_null() {
        std::ptr::null()
    } else {
        unsafe { (*r).tag }
    }
}

/// # Safety
/// `r` must be NULL or a live result from `puddle_exec` not yet freed.
#[no_mangle]
pub unsafe extern "C" fn puddle_cols(r: *const PuddleResult) -> u64 {
    if r.is_null() {
        0
    } else {
        unsafe { (*r).n_cols }
    }
}

/// # Safety
/// `r` must be NULL or a live result from `puddle_exec` not yet freed.
#[no_mangle]
pub unsafe extern "C" fn puddle_rows(r: *const PuddleResult) -> u64 {
    if r.is_null() {
        0
    } else {
        unsafe { (*r).n_rows }
    }
}

/// Column name `i`, or NULL when out of range.
/// # Safety
/// `r` must be NULL or a live result from `puddle_exec` not yet freed.
#[no_mangle]
pub unsafe extern "C" fn puddle_col_name(r: *const PuddleResult, i: u64) -> *const c_char {
    if r.is_null() || i >= unsafe { (*r).n_cols } {
        return std::ptr::null();
    }
    unsafe {
        let names = std::slice::from_raw_parts((*r).col_names, (*r).n_cols as usize);
        names.get(i as usize).copied().unwrap_or(std::ptr::null_mut())
    }
}

/// Cell (row-major) text, or NULL for SQL NULL / out of range.
/// # Safety
/// `r` must be NULL or a live result from `puddle_exec` not yet freed.
#[no_mangle]
pub unsafe extern "C" fn puddle_get(r: *const PuddleResult, row: u64, col: u64) -> *const c_char {
    if r.is_null() {
        return std::ptr::null();
    }
    unsafe {
        if col >= (*r).n_cols || row >= (*r).n_rows {
            return std::ptr::null();
        }
        let n = (*r).n_rows * (*r).n_cols;
        let cells = std::slice::from_raw_parts((*r).cells, n as usize);
        cells[row as usize * (*r).n_cols as usize + col as usize]
    }
}

/// Frees a result and everything it owns. NULL is accepted and ignored.
/// # Safety
/// `r` must be NULL or a result from `puddle_exec` not yet freed;
/// freeing twice is a double free.
#[no_mangle]
pub unsafe extern "C" fn puddle_result_free(r: *mut PuddleResult) {
    if r.is_null() {
        return;
    }
    let owned = unsafe { Box::from_raw(r) };
    unsafe {
        if !owned.error.is_null() {
            drop(CString::from_raw(owned.error));
        }
        if !owned.tag.is_null() {
            drop(CString::from_raw(owned.tag));
        }
        if !owned.col_names.is_null() {
            let names = std::slice::from_raw_parts_mut(owned.col_names, owned.n_cols as usize);
            for p in names.iter() {
                if !(*p).is_null() {
                    drop(CString::from_raw(*p));
                }
            }
            drop(Box::from_raw(names as *mut [*mut c_char]));
        }
        if !owned.cells.is_null() {
            let n = (owned.n_rows * owned.n_cols) as usize;
            let cells = std::slice::from_raw_parts_mut(owned.cells, n);
            for p in cells.iter() {
                if !(*p).is_null() {
                    drop(CString::from_raw(*p));
                }
            }
            drop(Box::from_raw(cells as *mut [*mut c_char]));
        }
    }
}
