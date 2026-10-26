#!/usr/bin/env python3
"""puddle benchmark — the M8 bench tool.

Self-contained: starts its own server (file store, fresh dir), loads the
standard dataset, runs the executor battery, then the index battery, then a
concurrent point-select OLTP probe (QPS + p50/p99 latency), then a kill -9
restart sanity check. Prints a table with speedups vs the recorded M3
reference-executor baseline (same shapes, same rows) and appends JSON.

Usage: python3 tests/bench.py [--rows 200000] [--quick] [--port 5595]
       --quick = 20k rows, short concurrency probe (smoke runs)
"""
import json
import os
import signal
import subprocess
import sys
import threading
import time

import psycopg2

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
BIN = f"{ROOT}/target/release/puddle"
PORT = 5595
ROWS = 200_000
DSN = f"host=127.0.0.1 port={PORT} dbname=x user=u sslmode=disable"
DATA = "/tmp/puddle-bench"

# M3 baseline (reference executor, pre-vectorization, pre-index), same 7
# shapes, same 200K rows — recorded 2026-10 (see ARCHITECTURE.md).
M3_BASELINE = {
    "scan+filter+count": 108.11,
    "group-by+aggs": 193.68,
    "scan+like+aggs": 106.38,
    "equi-join+agg": 3840.44,
    "order-by top-k": 223.93,
    "left-join anti": 4183.95,
    "case+mod+group": 221.93,
}

QUERIES = [
    ("scan+filter+count", "SELECT count(*) FROM bench_big WHERE a > {half}"),
    ("group-by+aggs",
     "SELECT g, count(*), avg(x), min(a), max(a) FROM bench_big GROUP BY g ORDER BY g LIMIT 5"),
    ("scan+like+aggs", "SELECT count(*), sum(x) FROM bench_big WHERE label LIKE 'row-1%'"),
    ("equi-join+agg",
     "SELECT d.gname, count(*), sum(b.x) FROM bench_big b JOIN bench_dim d ON b.g = d.g "
     "GROUP BY d.gname ORDER BY d.gname LIMIT 5"),
    ("order-by top-k", "SELECT a, x, label FROM bench_big ORDER BY x DESC LIMIT 10"),
    ("left-join anti",
     "SELECT count(*) FROM bench_big b LEFT JOIN bench_dim d ON b.g = d.g WHERE d.gname IS NULL"),
    ("case+mod+group",
     "SELECT CASE WHEN a % 10 = 0 THEN 'ten' WHEN a % 5 = 0 THEN 'five' ELSE 'other' END, "
     "count(*) FROM bench_big GROUP BY 1 ORDER BY 2 DESC LIMIT 3"),
]

IDX_QUERIES = [
    ("point-select", "SELECT a, x, label FROM bench_big WHERE a = {k}"),
    ("range-1000", "SELECT count(*) FROM bench_big WHERE a BETWEEN {half} AND {half2}"),
    ("in-list-10", "SELECT count(*) FROM bench_big WHERE a IN ({lst})"),
]


def start_server():
    subprocess.run(["pgrep", "-x", "puddle"], capture_output=True)
    p = subprocess.Popen([BIN, "--data", f"file://{DATA}", "--port", str(PORT),
                          "--bind", "127.0.0.1"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(200):
        try:
            psycopg2.connect(DSN, connect_timeout=1).close()
            return p
        except Exception:
            time.sleep(0.05)
    raise RuntimeError("bench server did not start")


def run_query(sql, n, timeout=180):
    try:
        r = subprocess.run(
            [sys.executable, f"{HERE}/bench_worker.py", str(PORT), sql, str(n)],
            capture_output=True, text=True, timeout=timeout)
        if r.returncode != 0:
            return None, (r.stderr.strip().splitlines() or ["error"])[-1][:120]
        lines = r.stdout.strip().splitlines()
        return float(lines[0]), lines[1]
    except subprocess.TimeoutExpired:
        return None, f"TIMEOUT(>{timeout}s)"


def fmt(ms):
    return f"{ms:9.2f} ms" if ms is not None else "      DNF "


def main():
    global ROWS, PORT, DSN
    args = sys.argv[1:]
    if "--quick" in args:
        ROWS = 20_000
    if "--rows" in args:
        ROWS = int(args[args.index("--rows") + 1])
    if "--port" in args:
        PORT = int(args[args.index("--port") + 1])
    DSN = f"host=127.0.0.1 port={PORT} dbname=x user=u sslmode=disable"
    sub = {"half": ROWS // 2, "half2": ROWS // 2 + 999,
           "k": ROWS // 2 + 7, "lst": ", ".join(str(ROWS // 2 + i) for i in range(10))}

    subprocess.run(f"rm -rf {DATA}", shell=True, check=False)
    p = start_server()
    out = {"rows": ROWS, "ms": {}}

    # ---- load ----
    t0 = time.perf_counter()
    r = subprocess.run([sys.executable, f"{HERE}/bench_setup.py", str(PORT), str(ROWS)],
                       capture_output=True, text=True, timeout=1800)
    if r.returncode != 0:
        print(r.stderr[-600:])
        sys.exit(1)
    load_s = time.perf_counter() - t0
    out["load_s"] = load_s
    print(f"puddle bench — {ROWS:,} rows loaded in {load_s:.2f}s "
          f"({ROWS / load_s:,.0f} rows/s)")

    # ---- executor battery ----
    print(f"\n{'query':22s} {'this run':>12s}   {'M3 baseline':>11s}   speedup")
    print("-" * 62)
    for name, q in QUERIES:
        ms, res = run_query(q.format(**sub), 5)
        out["ms"][name] = ms
        base = M3_BASELINE.get(name)
        sp = f"{base / ms:7.1f}x" if (ms and base) else "     --"
        print(f"{name:22s} {fmt(ms)}   {fmt(base)}   {sp}   {str(res)[:34]}")

    # ---- index battery: ALL no-index numbers first, then indexes, then all
    # indexed numbers (creating an index mid-loop would poison the later
    # 'no index' measurements)
    print(f"\n{'indexed shape':22s} {'no index':>12s}   {'indexed':>12s}   speedup")
    print("-" * 62)
    no_idx = {}
    for name, q in IDX_QUERIES:
        ms0, _ = run_query(q.format(**sub), 5)
        no_idx[name] = ms0
        out["ms"][f"{name}-noindex"] = ms0
    t0 = time.perf_counter()
    run_query("CREATE INDEX bench_a ON bench_big USING btree (a)", 1, 300)
    out["index_build_ms"] = (time.perf_counter() - t0) * 1000
    run_query("CREATE INDEX bench_g ON bench_dim USING hash (g)", 1, 300)
    for name, q in IDX_QUERIES:
        ms1, res = run_query(q.format(**sub), 5)
        out["ms"][f"{name}-indexed"] = ms1
        ms0 = no_idx[name]
        sp = f"{ms0 / ms1:7.1f}x" if (ms0 and ms1) else "     --"
        print(f"{name:22s} {fmt(ms0)}   {fmt(ms1)}   {sp}   {str(res)[:30]}")

    # ---- concurrent point-select OLTP probe ----
    seconds = 3 if ROWS <= 20_000 else 5
    nthreads = 8
    lats = []
    lock = threading.Lock()
    stop_evt = threading.Event()

    def hammer(tid):
        rng = __import__("random").Random(1234 + tid)
        c = psycopg2.connect(DSN)
        c.autocommit = True
        cur = c.cursor()
        mine = []
        q = "SELECT a, x, label FROM bench_big WHERE a = %s"
        while not stop_evt.is_set():
            t = time.perf_counter()
            cur.execute(q, (rng.randrange(ROWS),))
            cur.fetchall()
            mine.append(time.perf_counter() - t)
        c.close()
        with lock:
            lats.extend(mine)

    ths = [threading.Thread(target=hammer, args=(i,)) for i in range(nthreads)]
    t0 = time.perf_counter()
    for t in ths:
        t.start()
    time.sleep(seconds)
    stop_evt.set()
    for t in ths:
        t.join()
    wall = time.perf_counter() - t0
    lats.sort()
    qps = len(lats) / wall
    p50 = lats[len(lats) // 2] * 1000
    p99 = lats[int(len(lats) * 0.99)] * 1000
    out["qps"] = qps
    out["p50_ms"] = p50
    out["p99_ms"] = p99
    print(f"\nconcurrent point-selects ({nthreads} threads, {seconds:.0f}s, "
          f"btree index):")
    print(f"  throughput : {qps:,.0f} queries/s")
    print(f"  latency    : p50 {p50:.3f} ms   p99 {p99:.3f} ms")

    # ---- durability spot check: kill -9 mid-flight, restart, verify ----
    os.kill(p.pid, signal.SIGKILL)
    p.wait()
    p = start_server()
    c = psycopg2.connect(DSN)
    c.autocommit = True
    cur = c.cursor()
    cur.execute("SELECT count(*) FROM bench_big")
    n = cur.fetchone()[0]
    c.close()
    ok = n == ROWS
    out["restart_exact"] = ok
    print(f"\nkill -9 + restart: {n:,} rows recovered "
          f"{'(exact)' if ok else '(MISMATCH!)'}")

    os.kill(p.pid, signal.SIGKILL)
    p.wait()

    with open(f"{ROOT}/bench-results.json", "a") as f:
        f.write(json.dumps(out) + "\n")
    print(f"\nresults appended to bench-results.json")
    if not ok:
        sys.exit(1)


if __name__ == "__main__":
    main()
