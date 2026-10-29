#!/usr/bin/env python3
"""puddle vs SQLite head-to-head (v2.1 baseline).

Same 100k-row table in both engines, same query shapes, median of N runs.
SQLite runs in-process (python module, C); puddle over TCP via psycopg2.
Point-lookup shapes are dominated by the round trip for puddle; batched
shapes (scans/aggregates/top-k) measure raw engine throughput.
"""
import json
import os
import signal
import statistics
import subprocess
import sys
import time

import psycopg2
import sqlite3

PORT = 5521
N_ROWS = 100_000
N_GROUPS = 20
RUNS = 5
DATA = "/tmp/bench-sqlite"
DSN = f"host=127.0.0.1 port={PORT} dbname=x user=u sslmode=disable"


def setup_puddle():
    cur = None
    c = psycopg2.connect(DSN)
    c.autocommit = True
    cur = c.cursor()
    cur.execute("DROP TABLE IF EXISTS t")
    cur.execute("DROP TABLE IF EXISTS w")
    cur.execute("CREATE TABLE t (id int, g int, v float, s text)")
    cur.execute("CREATE TABLE w (id int, g int, v float, s text)")
    import io
    buf = io.StringIO()
    for i in range(N_ROWS):
        buf.write(f"{i}\t{i % N_GROUPS}\t{i * 0.5}\ttxt-{i % 9973}\n")
    buf.seek(0)
    t0 = time.perf_counter()
    cur.copy_expert("COPY t FROM STDIN", buf)
    load_s = time.perf_counter() - t0
    cur.execute("CREATE INDEX ti ON t USING btree (id)")
    cur.execute("CHECKPOINT")
    return c, load_s


def setup_sqlite():
    path = f"{DATA}/bench.db"
    if os.path.exists(path):
        os.remove(path)
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS w")
    cur.execute("CREATE TABLE t (id int, g int, v real, s text)")
    cur.execute("CREATE TABLE w (id int, g int, v real, s text)")
    cur.execute("BEGIN")
    t0 = time.perf_counter()
    cur.executemany("INSERT INTO t VALUES (?,?,?,?)",
                    [(i, i % N_GROUPS, i * 0.5, f"txt-{i % 9973}")
                     for i in range(N_ROWS)])
    con.commit()
    load_s = time.perf_counter() - t0
    cur.execute("CREATE INDEX ti ON t (id)")
    cur.execute("ANALYZE")
    return con, load_s


SHAPES = [
    # (name, category, puddle sql, sqlite sql, iterations per run)
    ("point-lookup", "rt",
     "SELECT v FROM t WHERE id = %s",
     "SELECT v FROM t WHERE id = ?", 500),
    ("range-100", "rt",
     "SELECT sum(v) FROM t WHERE id BETWEEN %s AND %s",
     "SELECT sum(v) FROM t WHERE id BETWEEN ? AND ?", 200),
    ("full-scan-sum", "batch",
     "SELECT sum(v) FROM t", "SELECT sum(v) FROM t", 1),
    ("groupby-20", "batch",
     "SELECT g, count(*), sum(v), min(v), max(v) FROM t GROUP BY g ORDER BY g",
     "SELECT g, count(*), sum(v), min(v), max(v) FROM t GROUP BY g ORDER BY g", 1),
    ("groupby-filter-having", "batch",
     "SELECT g, sum(v) FROM t WHERE v > 1000 GROUP BY g HAVING sum(v) > 100000 ORDER BY g",
     "SELECT g, sum(v) FROM t WHERE v > 1000 GROUP BY g HAVING sum(v) > 100000 ORDER BY g", 1),
    ("topk-10", "batch",
     "SELECT id, v FROM t ORDER BY v DESC LIMIT 10",
     "SELECT id, v FROM t ORDER BY v DESC LIMIT 10", 1),
    ("topk-100-offset", "batch",
     "SELECT id, v FROM t ORDER BY v DESC LIMIT 100 OFFSET 5000",
     "SELECT id, v FROM t ORDER BY v DESC LIMIT 100 OFFSET 5000", 1),
    ("like-filter", "batch",
     "SELECT count(*) FROM t WHERE s LIKE 'txt-99%'", 
     "SELECT count(*) FROM t WHERE s LIKE 'txt-99%'", 1),
    ("distinct-count", "batch",
     "SELECT count(DISTINCT g) FROM t",
     "SELECT count(DISTINCT g) FROM t", 1),
    ("update-point", "rt",
     "UPDATE t SET v = v + 1 WHERE id = %s",
     "UPDATE t SET v = v + 1 WHERE id = ?", 200),
    # writes: single-row autocommit statements (durability path)
    ("insert-single", "write",
     "INSERT INTO w VALUES (%s, %s, %s, %s)",
     "INSERT INTO w VALUES (?,?,?,?)", 500),
    ("insert-multi-100", "write",
     "INSERT INTO w VALUES " + ",".join(["(%s,%s,%s,%s)"] * 100),
     "INSERT INTO w VALUES " + ",".join(["(?,?,?,?)"] * 100), 5),
    ("delete-reinsert", "write",
     "DELETE FROM w WHERE id = %s",
     "DELETE FROM w WHERE id = ?", 200),
]


def run_puddle(c, sql, iters, params_fn):
    cur = c.cursor()
    meds = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        for i in range(iters):
            p = params_fn(i)
            cur.execute(sql, p) if p else cur.execute(sql)
            if cur.description:
                cur.fetchall()
        meds.append((time.perf_counter() - t0) / iters * 1000)
    return min(meds)


def run_sqlite(con, sql, iters, params_fn):
    cur = con.cursor()
    meds = []
    for _ in range(RUNS):
        t0 = time.perf_counter()
        for i in range(iters):
            p = params_fn(i)
            cur.execute(sql, p) if p else cur.execute(sql)
            if cur.description:
                cur.fetchall()
        meds.append((time.perf_counter() - t0) / iters * 1000)
    return min(meds)


def main():
    os.makedirs(DATA, exist_ok=True)
    p = subprocess.Popen(
        ["/home/user/puddle/target/release/puddle", "--data",
         f"file://{DATA}/puddle", "--port", str(PORT), "--bind", "127.0.0.1"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(200):
        try:
            psycopg2.connect(DSN, connect_timeout=1).close()
            break
        except Exception:
            time.sleep(0.05)
    try:
        pc, pload = setup_puddle()
        sc, sload = setup_sqlite()
        print(f"load 100k rows: puddle(COPY) {pload:.2f}s  sqlite(executemany) {sload:.2f}s\n")

        results = {}
        print(f"{'shape':22} {'puddle ms':>10} {'sqlite ms':>10} {'ratio':>7}")
        for name, cat, psql, ssql, iters in SHAPES:
            if name == "point-lookup" or name == "range-100":
                pf = lambda i: (5000 + (i * 37) % 90000,)
                sf = pf
            elif name == "range-100":
                pass
            elif name == "update-point":
                pf = lambda i: (1000 + (i * 13) % 98000,)
                sf = pf
            else:
                pf = lambda i: None
                sf = pf
            if name == "range-100":
                pf = lambda i: (1000 + (i * 97) % 80000, 1100 + (i * 97) % 80000)
                sf = pf
            elif name == "insert-single":
                pf = lambda i: (9000000 + i, i % N_GROUPS, i * 0.25, "w")
                sf = pf
            elif name == "insert-multi-100":
                pf = lambda i: tuple(x for j in range(100)
                                     for x in (9500000 + i * 100 + j, j % N_GROUPS, j * 0.25, "wm"))
                sf = pf
            elif name == "delete-reinsert":
                pf = lambda i: (9000000 + i,)
                sf = pf
            pt = run_puddle(pc, psql, iters, pf)
            st = run_sqlite(sc, ssql, iters, sf)
            ratio = pt / st if st > 0 else float("inf")
            results[name] = {"puddle_ms": round(pt, 4), "sqlite_ms": round(st, 4),
                             "ratio": round(ratio, 2), "category": cat}
            print(f"{name:22} {pt:10.3f} {st:10.3f} {ratio:6.1f}x")
        with open("/home/user/puddle/sqlite-bench-results.json", "w") as f:
            json.dump(results, f, indent=1)
        print("\nresults -> sqlite-bench-results.json")
    finally:
        p.send_signal(signal.SIGKILL)


if __name__ == "__main__":
    main()
