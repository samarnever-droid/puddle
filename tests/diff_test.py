#!/usr/bin/env python3
"""Differential test: vectorized executor vs reference executor.

Starts two puddle servers with identical data — one with the vectorized
SELECT path enabled, one with it disabled (pure reference oracle) — then runs
a battery of queries and compares results (or errors) byte-for-byte.

usage: python3 diff_test.py  (ports 5531/5532, data dirs under /tmp)
"""
import os
import shutil
import subprocess
import sys
import time

import psycopg2

BIN = os.path.join(os.path.dirname(__file__), "..", "target", "release", "puddle")
PORT_A = 5531  # vectorized ON
PORT_B = 5532  # vectorized OFF
DATA_A = "/tmp/puddle-diff-a"
DATA_B = "/tmp/puddle-diff-b"

def build_setup():
    t1_rows = []
    for i in range(400):
        a = None if i == 399 else i % 40
        b = None if i % 17 == 0 else "v%d" % (i % 7)
        c = None if i % 11 == 0 else (i * 1.5) % 100
        d = None if i % 13 == 0 else (i % 3 == 0)
        t1_rows.append((a, b, c, d))
    t2_rows = [(i % 40, "e%d" % (i % 5)) for i in range(200)]
    t2_rows += [(None, "nullkey"), (77, "lonely")]
    dim_rows = [(0, "zero"), (1, "one"), (2, "two"), (3, "three"), (None, "nil")]
    # temporal column: dates spanning months/years, timestamps with times,
    # times, intervals; NULLs sprinkled
    cal_rows = []
    for i in range(400):
        d = None if i % 19 == 0 else "DATE '2026-%02d-%02d'" % (i % 12 + 1, i % 28 + 1)
        ts = None if i % 23 == 0 else "TIMESTAMP '202%d-%02d-%02d %02d:%02d:00'" % (
            4 + i % 3, i % 12 + 1, i % 28 + 1, i % 24, (i * 7) % 60)
        iv = None if i % 29 == 0 else "INTERVAL '%d days %d hours'" % (i % 40, i % 25)
        tm = None if i % 31 == 0 else "TIME '%02d:%02d:00'" % (i % 24, (i * 11) % 60)
        cal_rows.append((d, ts, iv, tm))
    stmts = [
        "CREATE TABLE t1 (a int, b text, c float, d bool)",
        "CREATE TABLE t2 (a int, e text)",
        "CREATE TABLE dim (k int, v text)",
        "CREATE TABLE cal (d date, ts timestamp, iv interval, t time)",
    ]
    def lit(v):
        if v is None:
            return "NULL"
        if v is True:
            return "true"
        if v is False:
            return "false"
        if isinstance(v, str):
            return "'" + v.replace("'", "''") + "'"
        return repr(v)
    yield from stmts
    def batch(sql, rows, ch=100):
        for base in range(0, len(rows), ch):
            chunk = rows[base:base + ch]
            yield sql + " VALUES " + ",".join(
                "(" + ",".join(lit(v) for v in r) + ")" for r in chunk)
    yield from batch("INSERT INTO t1", t1_rows)
    yield from batch("INSERT INTO t2", t2_rows)
    yield from batch("INSERT INTO dim", dim_rows)
    for r in cal_rows:
        yield "INSERT INTO cal VALUES (%s)" % ", ".join(v if v is not None else "NULL" for v in r)
    # v2.0: snapshot the loaded state, then mutate past it
    yield "CREATE SNAPSHOT diffsnap"
    yield "DELETE FROM t1 WHERE a = 7"
    yield "INSERT INTO t1 VALUES (7, 'after-snap', 50.0, true)"
    yield "UPDATE cal SET d = DATE '2027-12-25' WHERE d = DATE '2026-01-01'"

SETUP = list(build_setup())

# sanity: the setup must produce exactly this many rows, else the whole
# differential run is comparing empty tables (a lesson we only need to learn once)
# t1: 400 loaded, 10 rows with a=7 deleted after the snapshot, 1 re-inserted
EXPECT = {"t1": 391, "t2": 202, "dim": 5, "cal": 400}

QUERIES = [
    # ---- plain scans / projections ----
    "SELECT * FROM t1",
    "SELECT a, b FROM t1",
    "SELECT t1.a, t1.b FROM t1",
    "SELECT a AS x, b AS y FROM t1",
    "SELECT count(*) FROM t1",
    "SELECT count(*), count(c), count(d) FROM t1",
    # ---- WHERE: every expression family ----
    "SELECT a, b FROM t1 WHERE a < 10",
    "SELECT a FROM t1 WHERE a >= 30 AND c IS NOT NULL",
    "SELECT a FROM t1 WHERE a < 5 OR b LIKE 'v3'",
    "SELECT a FROM t1 WHERE b NOT LIKE 'v%'",
    "SELECT a FROM t1 WHERE a IN (1, 3, 5)",
    "SELECT a FROM t1 WHERE a NOT IN (1, 2, NULL)",
    "SELECT a FROM t1 WHERE a BETWEEN 10 AND 12",
    "SELECT a FROM t1 WHERE a NOT BETWEEN 10 AND 35",
    "SELECT a FROM t1 WHERE c IS NULL",
    "SELECT a FROM t1 WHERE NOT (a < 20)",
    "SELECT a FROM t1 WHERE -a < -30",
    "SELECT a FROM t1 WHERE a + 1 * 2 = 9",
    "SELECT a FROM t1 WHERE a % 4 = 1 AND c / 2 > 10",
    "SELECT a FROM t1 WHERE b || '!' = 'v2!'",
    "SELECT a FROM t1 WHERE d = true",
    "SELECT a FROM t1 WHERE d IS NULL AND a > 5",
    "SELECT a FROM t1 WHERE length(b) > 1",
    "SELECT a FROM t1 WHERE upper(b) = 'V2'",
    "SELECT a FROM t1 WHERE abs(a - 20) <= 2",
    "SELECT a FROM t1 WHERE coalesce(c, -1.0) < 0",
    "SELECT a FROM t1 WHERE CASE WHEN a % 2 = 0 THEN true ELSE false END",
    "SELECT a FROM t1 WHERE CASE a WHEN 1 THEN true WHEN 2 THEN true ELSE false END",
    "SELECT a FROM t1 WHERE c::text IS NOT NULL",
    "SELECT a FROM t1 WHERE a::float > 38.5",
    # ---- expressions in projection ----
    "SELECT a + 1, b || b, -a, NOT d FROM t1",
    "SELECT CASE WHEN a > 20 THEN 'big' WHEN a > 10 THEN 'mid' ELSE 'small' END FROM t1",
    "SELECT CASE a WHEN 0 THEN 'zero' WHEN 1 THEN 'one' END FROM t1",
    "SELECT a::text FROM t1",
    "SELECT c::int FROM t1 WHERE c IS NOT NULL",
    "SELECT length(b), upper(b), substr(b, 2, 2) FROM t1",
    # ---- aggregates ----
    "SELECT sum(a), min(b), max(c), avg(a) FROM t1",
    "SELECT count(DISTINCT a) FROM t1",
    "SELECT sum(DISTINCT a) FROM t1",
    "SELECT min(a), max(a) FROM t1 WHERE a < 20",
    # ---- GROUP BY ----
    "SELECT b, count(*) FROM t1 GROUP BY b",
    "SELECT a % 10, count(*), sum(c) FROM t1 GROUP BY a % 10",
    "SELECT b, a, count(*) FROM t1 GROUP BY b, a",
    "SELECT b, count(*) FROM t1 WHERE a < 25 GROUP BY b",
    "SELECT a + 1, count(*) FROM t1 GROUP BY a + 1",
    "SELECT b AS grp, count(*) FROM t1 GROUP BY grp",
    "SELECT b AS grp, count(*) FROM t1 GROUP BY 1",
    "SELECT count(*) FROM t1 GROUP BY d",
    "SELECT b, sum(c), avg(c), min(c), max(c), count(c) FROM t1 GROUP BY b",
    # ---- HAVING ----
    "SELECT b, count(*) FROM t1 GROUP BY b HAVING count(*) > 60",
    "SELECT a % 10, sum(a) FROM t1 GROUP BY a % 10 HAVING sum(a) > 100",
    "SELECT b, count(*) FROM t1 GROUP BY b HAVING b LIKE 'v%'",
    # ---- ORDER BY ----
    "SELECT a, c FROM t1 ORDER BY a, c",
    "SELECT a, c FROM t1 ORDER BY a DESC, c DESC",
    "SELECT a, c FROM t1 ORDER BY c",
    "SELECT a, c FROM t1 ORDER BY c DESC",
    "SELECT a FROM t1 ORDER BY 1",
    "SELECT a AS x FROM t1 ORDER BY x",
    "SELECT b, count(*) FROM t1 GROUP BY b ORDER BY count(*) DESC, b",
    "SELECT a FROM t1 ORDER BY c NULLS FIRST",
    "SELECT a FROM t1 ORDER BY c NULLS LAST",
    "SELECT a FROM t1 ORDER BY d DESC NULLS LAST, a",
    "SELECT a % 7 AS m FROM t1 GROUP BY a % 7 ORDER BY m",
    # ---- DISTINCT ----
    "SELECT DISTINCT b FROM t1",
    "SELECT DISTINCT a, b FROM t1 ORDER BY a, b",
    "SELECT DISTINCT a % 5 FROM t1 ORDER BY 1",
    # ---- LIMIT / OFFSET ----
    "SELECT a, b FROM t1 ORDER BY a, b LIMIT 5",
    "SELECT a, b FROM t1 ORDER BY a, b LIMIT 5 OFFSET 10",
    "SELECT a, b FROM t1 ORDER BY a, b LIMIT 0",
    # ---- params ----
    "SELECT a, b FROM t1 WHERE a = $1",
    "SELECT count(*) FROM t1 WHERE b = $1 AND c > $2",
    "SELECT b, count(*) FROM t1 WHERE a < $1 GROUP BY b HAVING count(*) > $2 ORDER BY b",
    # ---- joins: equi (hash join path) ----
    "SELECT t1.a, t2.e FROM t1 JOIN t2 ON t1.a = t2.a",
    "SELECT t1.a, t2.e FROM t1 INNER JOIN t2 ON t1.a = t2.a WHERE t1.a < 10",
    "SELECT t1.a, t2.e FROM t1 LEFT JOIN t2 ON t1.a = t2.a",
    "SELECT t1.a, t2.e FROM t1 LEFT JOIN t2 ON t1.a = t2.a WHERE t2.e IS NULL",
    "SELECT t1.a, t2.e FROM t1 JOIN t2 ON t1.a = t2.a AND t1.a > 30",
    "SELECT t1.a, t2.e FROM t1 LEFT JOIN t2 ON t1.a = t2.a AND t2.e = 'e1'",
    "SELECT count(*) FROM t1 JOIN t2 ON t1.a = t2.a",
    "SELECT t2.a, count(*) FROM t1 JOIN t2 ON t1.a = t2.a GROUP BY t2.a ORDER BY t2.a",
    "SELECT t1.b, sum(t2.a) FROM t1 JOIN t2 ON t1.a = t2.a GROUP BY t1.b ORDER BY t1.b",
    "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a",
    "SELECT t1.*, t2.e FROM t1 JOIN t2 ON t1.a = t2.a",
    # multi-key equi
    "SELECT * FROM t1 a JOIN t1 b ON a.a = b.a AND a.b = b.b",
    # multi-join chain
    "SELECT t1.a, t2.e, dim.v FROM t1 JOIN t2 ON t1.a = t2.a JOIN dim ON t1.a % 4 = dim.k",
    "SELECT t1.a, dim.v FROM t1 LEFT JOIN dim ON t1.a % 4 = dim.k ORDER BY t1.a, dim.v",
    # join with residual non-equi (INNER only)
    "SELECT t1.a, t2.a FROM t1 JOIN t2 ON t1.a = t2.a AND t2.a > t1.a - 5",
    # ---- non-equi / cross joins: must fall back and still be right ----
    "SELECT t1.a, dim.k FROM t1, dim WHERE t1.a < 5",
    "SELECT t1.a, dim.k FROM t1 CROSS JOIN dim WHERE t1.a < 3 AND dim.k < 2",
    "SELECT t1.a, t2.a FROM t1 JOIN t2 ON t1.a < t2.a WHERE t1.a < 4 AND t2.a < 4",
    "SELECT t1.a, t2.a FROM t1 LEFT JOIN t2 ON t1.a < t2.a WHERE t1.a < 3",
    # ---- star over joins (ambiguity handling) ----
    "SELECT * FROM t1 JOIN dim ON t1.a = dim.k",
    "SELECT t1.*, dim.v FROM t1 JOIN dim ON t1.a = dim.k",
    "SELECT count(*) FROM t1 JOIN dim ON t1.a = dim.k",
    # ---- 3-way join chains ----
    "SELECT t1.a, t2.e, dim.v FROM t1 JOIN t2 ON t1.a = t2.a JOIN dim ON t1.a = dim.k",
    "SELECT t1.b, dim.v, count(*) FROM t1 JOIN t2 ON t1.a = t2.a JOIN dim ON t1.a = dim.k GROUP BY t1.b, dim.v",
    "SELECT count(*) FROM t1 a JOIN t2 b ON a.a = b.a JOIN dim c ON b.a = c.k JOIN t2 d ON b.a = d.a",
    # ---- empty results ----
    "SELECT a FROM t1 WHERE a > 1000",
    "SELECT b, count(*) FROM t1 WHERE a > 1000 GROUP BY b",
    "SELECT sum(a) FROM t1 WHERE a > 1000",
    "SELECT b, count(*) FROM t1 WHERE a > 1000 GROUP BY b HAVING count(*) > 1",
    "SELECT t1.a, t2.e FROM t1 JOIN t2 ON t1.a = t2.a AND t1.a > 1000",
    # ---- errors must match too ----
    "SELECT nope FROM t1",
    "SELECT a FROM t1 WHERE nope = 1",
    "SELECT a FROM t1 WHERE c = 'zzz'",
    "SELECT sum(b) FROM t1",
    "SELECT a FROM t1 GROUP BY a HAVING b = 'x'",
    "SELECT a FROM nosuch",
    "SELECT * FROM t1 JOIN nosuch ON t1.a = nosuch.a",
    "SELECT a FROM t1 WHERE a IN (1, 2) AND a = 'x'",
    "SELECT 1/0",
]

TEMPORAL_QUERIES = [
    "SELECT d FROM cal WHERE d > DATE '2026-06-15' ORDER BY d",
    "SELECT count(*) FROM cal WHERE ts >= TIMESTAMP '2025-06-01' AND ts < TIMESTAMP '2026-01-01'",
    "SELECT iv FROM cal WHERE iv > INTERVAL '10 days' ORDER BY iv",
    "SELECT t FROM cal WHERE t BETWEEN TIME '06:00:00' AND TIME '18:00:00' ORDER BY t",
    "SELECT d, count(*) FROM cal GROUP BY d ORDER BY d, 2",
    "SELECT date_trunc('month', ts), count(*) FROM cal GROUP BY 1 ORDER BY 1",
    "SELECT extract(year from ts), extract(dow from d), count(*) FROM cal GROUP BY 1, 2 ORDER BY 1, 2",
    "SELECT to_char(d, 'Dy Mon DD') FROM cal WHERE d IS NOT NULL ORDER BY d",
    "SELECT ts + iv FROM cal ORDER BY ts",
    "SELECT ts - d FROM cal ORDER BY ts",
    "SELECT max(d), min(d), max(ts), min(iv) FROM cal",
    "SELECT sum(iv) FROM cal",
    "SELECT a, d FROM t1 JOIN cal ON t1.a = extract(day from cal.d) WHERE d IS NOT NULL ORDER BY a, d",
    "SELECT CASE WHEN d < DATE '2026-06-01' THEN 'h1' ELSE 'h2' END, count(*) "
    "FROM cal WHERE d IS NOT NULL GROUP BY 1 ORDER BY 1",
    "SELECT age(ts) FROM cal WHERE ts IS NOT NULL ORDER BY ts",
    "SELECT count(*) FROM cal WHERE ts::date = d",
    "SELECT d::text, ts::time, iv::text FROM cal WHERE d IS NOT NULL ORDER BY d, ts",
    "SELECT count(*) FROM cal WHERE ts - iv > TIMESTAMP '2025-01-01'",
    "SELECT DISTINCT extract(quarter from d) FROM cal WHERE d IS NOT NULL ORDER BY 1",
    "SELECT t + iv FROM cal WHERE t IS NOT NULL AND iv IS NOT NULL ORDER BY t",
]

PARAM_QUERIES = {
    "SELECT a, b FROM t1 WHERE a = $1": [7],
    "SELECT count(*) FROM t1 WHERE b = $1 AND c > $2": ["v2", 10.0],
    "SELECT b, count(*) FROM t1 WHERE a < $1 GROUP BY b HAVING count(*) > $2 ORDER BY b": [25, 5],
    "SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.b = $1": ["v3"],
    "SELECT a FROM t1 WHERE b LIKE $1": ["v_"],
    "SELECT count(*) FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.b = $1": ["v3"],
    "SELECT dim.v, sum(t1.c) FROM t1 JOIN dim ON t1.a = dim.k WHERE t1.a < $1 GROUP BY dim.v ORDER BY dim.v": [30],
}


def start(port, data, extra, wipe=True):
    if wipe:
        shutil.rmtree(data, ignore_errors=True)
        os.makedirs(data, exist_ok=True)
    log = open(f"/tmp/puddle-diff-{port}.log", "w")
    p = subprocess.Popen(
        [BIN, "--port", str(port), "--data", f"file://{data}"] + extra,
        stdout=log, stderr=subprocess.STDOUT,
    )
    for _ in range(100):
        try:
            c = psycopg2.connect(host="127.0.0.1", port=port, dbname="d", user="u",
                                 connect_timeout=1)
            c.close()
            return p
        except Exception:
            time.sleep(0.05)
    raise RuntimeError(f"server on {port} did not start")


def fetch(port, sql, params=None):
    c = psycopg2.connect(host="127.0.0.1", port=port, dbname="d", user="u")
    c.autocommit = True
    cur = c.cursor()
    try:
        cur.execute(sql, params)
        if cur.description is None:
            r = ("ok",)
        else:
            r = tuple(cur.fetchall())
    except psycopg2.Error as ex:
        code = ex.pgcode or "?????"
        r = ("ERROR", code, str(ex).split("\n")[0])
    finally:
        c.close()
    return r


def main():
    pa = start(PORT_A, DATA_A, ["--vectorized", "on"])
    pb = start(PORT_B, DATA_B, ["--vectorized", "off"])
    fails = 0
    total = 0
    try:
        for s in SETUP:
            ra = fetch(PORT_A, s)
            rb = fetch(PORT_B, s)
            if ra != rb or (isinstance(ra, tuple) and ra and ra[0] == "ERROR"):
                print(f"[SETUP FAIL] {s}")
                print(f"   A: {ra}")
                print(f"   B: {rb}")
                sys.exit(1)
        for t, want in EXPECT.items():
            for port in (PORT_A, PORT_B):
                r = fetch(port, f"SELECT count(*) FROM {t}")
                n = r[0][0] if r and r[0] else None
                if n != want:
                    print(f"[SETUP FAIL] {t} on {port}: {n} rows, expected {want}")
                    sys.exit(1)
        # v2.0 time travel: identical statement streams on both servers
        # assign identical LSNs, so AS OF reads must agree too
        snap_lsn = fetch(PORT_A, "SELECT lsn FROM puddle_snapshots WHERE name = 'diffsnap'")[0][0]
        snap_lsn_b = fetch(PORT_B, "SELECT lsn FROM puddle_snapshots WHERE name = 'diffsnap'")[0][0]
        total += 1
        if snap_lsn != snap_lsn_b:
            fails += 1
            print(f"[FAIL] snapshot lsn differs: {snap_lsn} vs {snap_lsn_b}")
        SNAP_QUERIES = [
            f"SELECT count(*) FROM t1 AS OF LSN {snap_lsn}",
            "SELECT count(*) FROM t1 AS OF SNAPSHOT diffsnap",
            "SELECT sum(c) FROM t1 AS OF SNAPSHOT diffsnap",
            "SELECT a, b FROM t1 AS OF SNAPSHOT diffsnap WHERE a = 7",
            "SELECT d FROM cal AS OF SNAPSHOT diffsnap WHERE d IS NOT NULL ORDER BY d, ts",
            "SELECT count(*) FROM cal AS OF SNAPSHOT diffsnap WHERE d = DATE '2027-12-25'",
            "SELECT t1.a, h.b FROM t1 JOIN t1 AS OF SNAPSHOT diffsnap h ON t1.a = h.a "
            "WHERE t1.a < 5 ORDER BY 1",
            "SELECT extract(year from d), count(*) FROM cal AS OF SNAPSHOT diffsnap "
            "WHERE d IS NOT NULL GROUP BY 1 ORDER BY 1",
        ]
        for q in QUERIES + TEMPORAL_QUERIES + SNAP_QUERIES:
            total += 1
            ra = fetch(PORT_A, q)
            rb = fetch(PORT_B, q)
            if ra != rb:
                fails += 1
                print(f"[FAIL] {q}")
                print(f"   vec : {str(ra)[:300]}")
                print(f"   ref : {str(rb)[:300]}")
        # ---- M5: secondary indexes on both servers, then index-shaped queries.
        # The vectorized path uses the indexes; the reference path full-scans —
        # results must stay byte-identical.
        for s in [
            "CREATE INDEX t1_a_bt ON t1 USING btree (a)",
            "CREATE INDEX t1_b_hash ON t1 USING hash (b)",
            "CREATE INDEX t2_a_bt ON t2 USING btree (a)",
            "CREATE INDEX t1_c_bt ON t1 USING btree (c)",
        ]:
            ra = fetch(PORT_A, s)
            rb = fetch(PORT_B, s)
            total += 1
            if ra != rb:
                fails += 1
                print(f"[FAIL] {s}: {ra} vs {rb}")
        for q in [
            "SELECT a, b FROM t1 WHERE a = 7",
            "SELECT count(*) FROM t1 WHERE a = 7",
            "SELECT count(*) FROM t1 WHERE a > 30",
            "SELECT count(*) FROM t1 WHERE a >= 30",
            "SELECT count(*) FROM t1 WHERE a < 5",
            "SELECT count(*) FROM t1 WHERE a <= 5",
            "SELECT count(*) FROM t1 WHERE a BETWEEN 10 AND 19",
            "SELECT count(*) FROM t1 WHERE a IN (1, 3, 99, NULL)",
            "SELECT count(*) FROM t1 WHERE b = 'v3'",
            "SELECT count(*) FROM t1 WHERE b = 'nope'",
            "SELECT count(*) FROM t1 WHERE c > 90",
            "SELECT count(*) FROM t1 WHERE c = 50.0",
            "SELECT a FROM t1 WHERE a = 399",          # a IS NULL there
            "SELECT count(*) FROM t1 WHERE 15 = a",
            "SELECT count(*) FROM t1 WHERE 10 < a",
            "SELECT count(*) FROM t1 WHERE a = 7 AND b LIKE 'v%'",
            "SELECT b, count(*) FROM t1 WHERE a > 20 GROUP BY b HAVING count(*) > 5",
            "SELECT t1.a, t2.e FROM t1 JOIN t2 ON t1.a = t2.a WHERE t1.a = 5",
            "SELECT count(*) FROM t1 WHERE a BETWEEN 20 AND 10",  # empty range
            "SELECT count(*) FROM t1 WHERE a > 'x'",               # type mismatch
            # DML under indexes: maintenance must keep lookups exact
            "INSERT INTO t1 VALUES (41, 'v-new', 41.5, true)",
            "SELECT count(*) FROM t1 WHERE a = 41",
            "SELECT count(*) FROM t1 WHERE b = 'v-new'",
            "UPDATE t1 SET a = 42 WHERE a = 41",
            "SELECT count(*) FROM t1 WHERE a = 41",
            "SELECT count(*) FROM t1 WHERE a = 42",
            "DELETE FROM t1 WHERE a = 42",
            "SELECT count(*) FROM t1 WHERE a = 42",
            "SELECT count(*) FROM t1 WHERE b = 'v-new'",
            "SELECT count(*) FROM t1",  # sanity: back to 400
        ]:
            total += 1
            ra = fetch(PORT_A, q)
            rb = fetch(PORT_B, q)
            if ra != rb:
                fails += 1
                print(f"[FAIL] {q}")
                print(f"   vec : {str(ra)[:300]}")
                print(f"   ref : {str(rb)[:300]}")
        # ---- restart one server: indexes must rebuild and stay correct
        pa.terminate()
        pa.wait()
        # restart WITHOUT wiping the data dir (durability: index defs +
        # rebuild-on-recovery)
        pa = start(PORT_A, DATA_A, ["--vectorized", "on"], wipe=False)
        for q in [
            "SELECT count(*) FROM t1 WHERE a = 7",
            "SELECT count(*) FROM t1 WHERE a BETWEEN 10 AND 19",
            "SELECT count(*) FROM t1 WHERE b = 'v3'",
            "SELECT count(*) FROM t1 WHERE a > 30",
        ]:
            total += 1
            ra = fetch(PORT_A, q)
            rb = fetch(PORT_B, q)
            if ra != rb:
                fails += 1
                print(f"[FAIL after restart] {q}")
                print(f"   vec : {str(ra)[:300]}")
                print(f"   ref : {str(rb)[:300]}")
        for q, ps in PARAM_QUERIES.items():
            total += 1
            ra = fetch(PORT_A, q, ps)
            rb = fetch(PORT_B, q, ps)
            if ra != rb:
                fails += 1
                print(f"[FAIL] {q} {ps}")
                print(f"   vec : {str(ra)[:300]}")
                print(f"   ref : {str(rb)[:300]}")
    finally:
        pa.terminate()
        pb.terminate()
    print(f"\n{total - fails}/{total} matched")
    if fails:
        print("DIFFERENTIAL TESTS FAILED")
        sys.exit(1)
    print("DIFFERENTIAL TESTS PASSED (vectorized == reference)")


if __name__ == "__main__":
    main()
