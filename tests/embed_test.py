#!/usr/bin/env python3
"""Embedded-mode tests: link libpuddle.so via ctypes, no TCP anywhere.

Covers the C ABI in src/ffi.rs: open (memory + durable file), exec,
results (cols/rows/cells/NULL cells), tags, errors with SQLSTATEs, and
result freeing. Run after `cargo build --release`.
"""
import ctypes as C
import os
import shutil
import sys
import tempfile

LIB = os.path.join(os.path.dirname(__file__), "..", "target", "release", "libpuddle.so")
if not os.path.exists(LIB):
    print("FAIL: build first: cargo build --release (need %s)" % LIB)
    sys.exit(1)

l = C.CDLL(LIB)


class PuddleResult(C.Structure):
    _fields_ = [
        ("ok", C.c_int),
        ("error", C.c_char_p),
        ("tag", C.c_char_p),
        ("n_cols", C.c_uint64),
        ("n_rows", C.c_uint64),
        ("col_names", C.POINTER(C.c_char_p)),
        ("cells", C.POINTER(C.c_char_p)),
    ]


l.puddle_open.restype = C.c_void_p
l.puddle_open.argtypes = [C.c_char_p]
l.puddle_close.argtypes = [C.c_void_p]
l.puddle_exec.restype = C.POINTER(PuddleResult)
l.puddle_exec.argtypes = [C.c_void_p, C.c_char_p]
l.puddle_result_free.argtypes = [C.POINTER(PuddleResult)]
l.puddle_ok.restype = C.c_int
l.puddle_ok.argtypes = [C.POINTER(PuddleResult)]
l.puddle_error.restype = C.c_char_p
l.puddle_error.argtypes = [C.POINTER(PuddleResult)]
l.puddle_tag.restype = C.c_char_p
l.puddle_tag.argtypes = [C.POINTER(PuddleResult)]
l.puddle_cols.restype = C.c_uint64
l.puddle_cols.argtypes = [C.POINTER(PuddleResult)]
l.puddle_rows.restype = C.c_uint64
l.puddle_rows.argtypes = [C.POINTER(PuddleResult)]
l.puddle_col_name.restype = C.c_char_p
l.puddle_col_name.argtypes = [C.POINTER(PuddleResult), C.c_uint64]
l.puddle_get.restype = C.c_char_p
l.puddle_get.argtypes = [C.POINTER(PuddleResult), C.c_uint64, C.c_uint64]

PASS = FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print("ok %2d  %s" % (PASS, name))
    else:
        FAIL += 1
        print("FAIL %2d  %s  %s" % (FAIL, name, detail))
        sys.exit(1)


def exec_(db, sql):
    return l.puddle_exec(db, sql.encode())


def rows_of(res):
    """[(c0, c1, ...), ...] with None for SQL NULL."""
    out = []
    for r in range(res.contents.n_rows):
        out.append(
            tuple(
                l.puddle_get(res, r, c)
                for c in range(res.contents.n_cols)
            )
        )
    return out


def q1(db, sql):
    res = exec_(db, sql)
    assert res.contents.ok == 1, res.contents.error
    v = rows_of(res)
    l.puddle_result_free(res)
    return v


# ------------------------------------------------------------------ memory
db = l.puddle_open(None)
check("open(NULL) = in-memory db", db != 0)

res = exec_(db, "CREATE TABLE t (a int, b text)")
check("CREATE TABLE ok", res.contents.ok == 1)
check("CREATE tag", res.contents.tag == b"CREATE TABLE")
l.puddle_result_free(res)

res = exec_(db, "INSERT INTO t VALUES (1,'one'), (2,'two'), (3,NULL)")
check("INSERT ok", res.contents.ok == 1)
check("INSERT tag reports n", res.contents.tag == b"INSERT 0 3")
l.puddle_result_free(res)

res = exec_(db, "SELECT a, b FROM t ORDER BY a")
check("SELECT ok", res.contents.ok == 1)
check("SELECT tag", res.contents.tag == b"SELECT")
check("cols", res.contents.n_cols == 2)
check("rows", res.contents.n_rows == 3)
check("col names", [l.puddle_col_name(res, i) for i in range(2)] == [b"a", b"b"])
check(
    "cells text-encoded + NULL cell",
    rows_of(res) == [(b"1", b"one"), (b"2", b"two"), (b"3", None)],
    rows_of(res),
)
l.puddle_result_free(res)

# multi-statement: result of the LAST statement
res = exec_(db, "INSERT INTO t VALUES (4,'four'); SELECT count(*) FROM t")
check("multi-statement exec", res.contents.ok == 1 and rows_of(res) == [(b"4",)])
l.puddle_result_free(res)

# empty string / non-row statements
res = exec_(db, "")
check("empty sql: ok with EMPTY tag", res.contents.ok == 1 and res.contents.tag == b"EMPTY")
l.puddle_result_free(res)

# error paths carry SQLSTATEs
res = exec_(db, "SELECT * FROM nope")
check("42P01 undefined table", res.contents.ok == 0 and res.contents.error.startswith(b"42P01"), res.contents.error)
l.puddle_result_free(res)
res = exec_(db, "SELEKT 1")
check("42601 syntax error", res.contents.ok == 0 and res.contents.error.startswith(b"42601"), res.contents.error)
l.puddle_result_free(res)
res = exec_(db, "SELECT 1/0")
check("22012 division by zero", res.contents.ok == 0 and res.contents.error.startswith(b"22012"), res.contents.error)
l.puddle_result_free(res)

# the engine really is the full server engine: joins/windows/CTEs/vecx
res = exec_(db, """
    WITH s AS (SELECT a, a * 10 AS ten FROM t WHERE b IS NOT NULL)
    SELECT sum(ten) OVER (ORDER BY a) FROM s ORDER BY a DESC LIMIT 1
""")
check("CTE + window + LIMIT", res.contents.ok == 1 and rows_of(res) == [(b"70",)], rows_of(res))
l.puddle_result_free(res)

res = exec_(db, "SELECT b FROM t WHERE b LIKE 'on%' ORDER BY a")
check("LIKE pushdown", rows_of(res) == [(b"one",)])
l.puddle_result_free(res)

l.puddle_close(db)

# two memory handles at once are independent: table in one is not in the other
dbA = l.puddle_open(None)
dbB = l.puddle_open(None)
q1(dbA, "CREATE TABLE only_here (a int)")
check("handle A sees its table", q1(dbA, "SELECT count(*) FROM only_here") == [(b"0",)])
res = exec_(dbB, "SELECT * FROM only_here")
check("handle B does not (independent)", res.contents.ok == 0 and res.contents.error.startswith(b"42P01"))
l.puddle_result_free(res)
l.puddle_close(dbA)
l.puddle_close(dbB)

# ------------------------------------------------------------------ durable
tmp = tempfile.mkdtemp(prefix="puddle-embed-")
path = os.path.join(tmp, "db").encode()
dbd = l.puddle_open(path)
check("open(dir) durable", dbd != 0)
res = exec_(dbd, "CREATE TABLE d (x int); INSERT INTO d VALUES (42)")
check("durable write", res.contents.ok == 1)
l.puddle_result_free(res)
l.puddle_close(dbd)

# reopen: recovery must bring the table back
dbd = l.puddle_open(path)
check(
    "reopen after close recovers",
    q1(dbd, "SELECT x FROM d") == [(b"42",)],
)
l.puddle_close(dbd)

# file:// URL spelling works too
dbd = l.puddle_open(b"file://" + path)
check("file:// url open", q1(dbd, "SELECT count(*) FROM d") == [(b"1",)])
l.puddle_close(dbd)

# bad url -> NULL handle
check("open(bad scheme) returns NULL", not l.puddle_open(b"ftp://x"))

shutil.rmtree(tmp)

# ------------------------------------------------------------------ free loop
# free must handle: error results, EMPTY results, wide/NULL results, repeats
db = l.puddle_open(None)
for sql in ["SELECT 1", "SELECT 1/0", "", "SELECT NULL, 'x', 1.5", "CREATE TABLE z(a int)"]:
    res = exec_(db, sql)
    l.puddle_result_free(res)
check("result_free over mixed results", True)
l.puddle_close(db)

print("\n%d passed, %d failed" % (PASS, FAIL))
sys.exit(1 if FAIL else 0)
