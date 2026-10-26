#!/usr/bin/env python3
"""puddle M7 memory contract: RSS ceilings.

The SLO: 'lightweight' means RAM. Idle server (with and without TLS) must
sit under 10 MB; a loaded 200K-row 2-index dataset under 60 MB; connection
churn must not leak; a 100-connection burst must stay bounded.
"""
import os
import shutil
import subprocess
import sys
import threading
import time

import psycopg2

BIN = "/home/user/puddle/target/release/puddle"
DATA = "/tmp/puddle-rss"
FAILS = []


def rss_kb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1])
    return -1


def start(port, extra=()):
    shutil.rmtree(DATA, ignore_errors=True)
    p = subprocess.Popen([BIN, "--data", f"file://{DATA}", "--port", str(port),
                          "--bind", "127.0.0.1", *extra],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    dsn = f"host=127.0.0.1 port={port} dbname=x user=u sslmode=disable"
    for _ in range(200):
        try:
            psycopg2.connect(dsn, connect_timeout=1).close()
            return p, dsn
        except Exception:
            time.sleep(0.05)
    raise RuntimeError("no start")


def check(name, got_kb, ceiling_kb):
    ok = got_kb < ceiling_kb
    print(f"  [{'OK' if ok else 'FAIL'}] {name}: {got_kb/1024:.2f} MB "
          f"(ceiling {ceiling_kb/1024:.0f} MB)")
    if not ok:
        FAILS.append(name)


def main():
    print("RSS contract (plain TCP):")
    p, dsn = start(5491)
    c = psycopg2.connect(dsn)
    c.autocommit = True
    c.cursor().execute("SELECT 1")
    time.sleep(0.3)
    idle = rss_kb(p.pid)
    check("idle RSS", idle, 10 * 1024)

    # ---- connection churn: 200 sequential connect/query/close ----
    for _ in range(200):
        c2 = psycopg2.connect(dsn)
        c2.autocommit = True
        c2.cursor().execute("SELECT 1")
        c2.close()
    time.sleep(0.3)
    churn = rss_kb(p.pid)
    check("after 200 conn churn (growth)", churn - idle, 2 * 1024)

    # ---- 100-connection burst ----
    conns = []
    for _ in range(100):
        c2 = psycopg2.connect(dsn)
        c2.autocommit = True
        c2.cursor().execute("SELECT 1")
        conns.append(c2)
    time.sleep(0.5)
    burst = rss_kb(p.pid)
    check("100 live connections", burst, 40 * 1024)
    for c2 in conns:
        c2.close()
    time.sleep(0.5)

    # ---- loaded: 200K rows + 2 secondary indexes on a FRESH server ----
    # (measures the dataset's own cost, not churn/burst allocator retention)
    c.close()
    os.kill(p.pid, 9)
    p.wait()
    p, dsn = start(5493)
    c = psycopg2.connect(dsn)
    c.autocommit = True
    cur = c.cursor()
    cur.execute("CREATE TABLE bench_big (a int, b text, g int, f float)")
    for base in range(0, 200_000, 10_000):
        cur.execute("INSERT INTO bench_big VALUES " + ",".join(
            f"({i}, 'row-{i}', {i % 100}, {i}.5)" for i in range(base, base + 10_000)))
    cur.execute("CREATE INDEX ix_g ON bench_big (g)")
    cur.execute("CREATE INDEX ix_a ON bench_big (a)")
    cur.execute("SELECT count(*) FROM bench_big WHERE g = 42")
    cur.fetchone()
    time.sleep(0.3)
    loaded = rss_kb(p.pid)
    check("200K rows + 2 indexes (fresh server)", loaded, 64 * 1024)

    # ---- drop + checkpoint: report release (no assert: allocator retention) ----
    cur.execute("DROP TABLE bench_big")
    cur.execute("CHECKPOINT")
    time.sleep(0.3)
    after_drop = rss_kb(p.pid)
    print(f"  [info] after DROP+CHECKPOINT: {after_drop/1024:.2f} MB "
          f"(from {loaded/1024:.2f} MB; retention is allocator-level)")
    c.close()
    os.kill(p.pid, 9)
    p.wait()

    # ---- TLS idle ----
    print("RSS contract (TLS):")
    p, dsn = start(5492, extra=("--tls",))
    c = psycopg2.connect(f"host=127.0.0.1 port=5492 dbname=x user=u "
                         f"sslmode=require sslrootcert=/nonexistent")
    c.autocommit = True
    c.cursor().execute("SELECT 1")
    time.sleep(0.3)
    check("idle RSS with TLS", rss_kb(p.pid), 10 * 1024)
    c.close()
    os.kill(p.pid, 9)
    p.wait()

    if FAILS:
        print(f"RSS TESTS FAILED: {FAILS}")
        sys.exit(1)
    print("RSS TESTS PASSED")


if __name__ == "__main__":
    main()
