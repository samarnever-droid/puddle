#!/usr/bin/env python3
"""puddle M7 soak: 12 threads x ~30s of contended mixed DML.

Invariants checked DURING and AFTER the run:
  * sum(acc.bal) == 100,000 after EVERY committed snapshot read (transfers
    are atomic; a torn read or half-applied transfer breaks this instantly)
  * count(acc) == 1,000 forever
  * final log rows == acked inserts - acked deletes, exactly (no loss, no
    resurrection)
  * OCC 40001 serialization failures MUST occur (hot 32-account set) and
    MUST be retryable to completion
  * CHECKPOINT runs concurrently with the workload (part writes + WAL GC
    under load); durability contract unchanged
  * server RSS stays under the loaded ceiling
"""
import os
import random
import shutil
import subprocess
import sys
import threading
import time

import psycopg2
from psycopg2 import errors as pgerrors

BIN = "/home/user/puddle/target/release/puddle"
DATA = "/tmp/puddle-soak"
PORT = 5481
DSN = f"host=127.0.0.1 port={PORT} dbname=x user=u sslmode=disable"
THREADS = 12
DURATION = 30.0
RSS_CEILING_MB = 150.0
random.seed(20261023)

FAILS = []
stop = threading.Event()
stats = {
    "transfers": 0, "retries": 0, "inserts": 0, "deletes": 0,
    "reads": 0, "bad_sums": 0,
}
lock = threading.Lock()


def bump(k, n=1):
    with lock:
        stats[k] += n


def rss_mb(pid):
    with open(f"/proc/{pid}/status") as f:
        for line in f:
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) / 1024.0
    return -1.0


def wait_listening():
    for _ in range(200):
        try:
            psycopg2.connect(DSN, connect_timeout=1).close()
            return
        except Exception:
            time.sleep(0.05)
    raise RuntimeError("server did not start")


def connect():
    c = psycopg2.connect(DSN)
    c.autocommit = True
    return c


def worker(wid):
    rng = random.Random(1000 + wid)
    c = connect()
    cur = c.cursor()
    deadline = time.time() + DURATION
    while not stop.is_set() and time.time() < deadline:
        r = rng.random()
        try:
            if r < 0.70:
                # transfer between two hot accounts (32-account hot set)
                for attempt in range(200):
                    a, b = rng.sample(range(32), 2)
                    amt = rng.randint(1, 20)
                    try:
                        cur.execute("BEGIN")
                        cur.execute("UPDATE acc SET bal = bal + %s WHERE id = %s", (amt, a))
                        cur.execute("UPDATE acc SET bal = bal - %s WHERE id = %s", (amt, b))
                        cur.execute("COMMIT")
                        bump("transfers")
                        break
                    except pgerrors.TransactionRollbackError:
                        bump("retries")
                        try:
                            cur.execute("ROLLBACK")
                        except Exception:
                            pass
                        # randomized backoff decorrelates the 12 threads
                        # (immediate retry loops livelock under OCC)
                        time.sleep(rng.uniform(0.0005, 0.004))
                        if attempt == 199:
                            FAILS.append(f"worker {wid}: transfer never committed after 200 tries")
                    except pgerrors.Error as e:
                        FAILS.append(f"worker {wid}: transfer failed: {str(e)[:80]}")
                        try:
                            cur.execute("ROLLBACK")
                        except Exception:
                            pass
                        break
            elif r < 0.85:
                cur.execute("INSERT INTO log (sensor, note) VALUES (%s, %s)",
                            (rng.randint(0, 15), f"w{wid}-{rng.randint(0, 9999)}"))
                bump("inserts")
            elif r < 0.95:
                cur.execute("DELETE FROM log WHERE sensor = %s AND note LIKE %s",
                            (rng.randint(0, 15), f"w{wid}-%"))
                bump("deletes", cur.rowcount)
            else:
                # snapshot-consistency canary: sum must be exact right now
                cur.execute("SELECT sum(bal), count(*) FROM acc")
                s, n = cur.fetchone()
                bump("reads")
                if s != 100000 or n != 1000:
                    bump("bad_sums")
                    FAILS.append(f"worker {wid}: INVARIANT BROKEN sum={s} count={n}")
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            FAILS.append(f"worker {wid}: connection died: {str(e)[:80]}")
            return
        except pgerrors.Error as e:
            # statement-level errors that are NOT rollback-class: log + continue
            try:
                cur.execute("ROLLBACK")
            except Exception:
                pass
            bump("stmt_errors")
            if not any(f"stmt err {str(e)[:40]}" in f for f in FAILS):
                FAILS.append(f"worker {wid}: stmt err {str(e)[:80]}")
    c.close()


def checkpointer(pid):
    c = connect()
    cur = c.cursor()
    while not stop.is_set():
        time.sleep(5.0)
        if stop.is_set():
            break
        try:
            cur.execute("CHECKPOINT")
        except Exception as e:
            FAILS.append(f"checkpointer: {str(e)[:80]}")
            return
    c.close()


def main():
    shutil.rmtree(DATA, ignore_errors=True)
    p = subprocess.Popen([BIN, "--data", f"file://{DATA}", "--port", str(PORT),
                          "--bind", "127.0.0.1"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_listening()
    c = connect()
    cur = c.cursor()
    cur.execute("CREATE TABLE acc (id int, bal int)")
    cur.execute("CREATE TABLE log (sensor int, note text)")
    cur.execute("INSERT INTO acc VALUES " +
                ",".join(f"({i}, 100)" for i in range(1000)))
    cur.execute("SELECT sum(bal), count(*) FROM acc")
    assert cur.fetchone() == (100000, 1000)
    c.close()

    idle_rss = rss_mb(p.pid)
    peak_rss = idle_rss

    ths = [threading.Thread(target=worker, args=(i,), daemon=True)
           for i in range(THREADS)]
    ck = threading.Thread(target=checkpointer, args=(p.pid,), daemon=True)
    t0 = time.time()
    for t in ths:
        t.start()
    ck.start()

    while any(t.is_alive() for t in ths):
        time.sleep(1.0)
        peak_rss = max(peak_rss, rss_mb(p.pid))
    stop.set()
    ck.join(timeout=10)
    for t in ths:
        t.join(timeout=10)
    wall = time.time() - t0

    # ---- final exact accounting ----
    c = connect()
    cur = c.cursor()
    cur.execute("SELECT sum(bal), count(*) FROM acc")
    s, n = cur.fetchone()
    cur.execute("SELECT count(*) FROM log")
    logn = cur.fetchone()[0]
    expected_log = stats["inserts"] - stats["deletes"]
    c.close()

    print(f"soak: {THREADS} threads x {wall:.0f}s "
          f"(+background CHECKPOINT every 5s)")
    print(f"  transfers ok      : {stats['transfers']}")
    print(f"  OCC 40001 retries : {stats['retries']}")
    print(f"  log inserts/deletes: {stats['inserts']} / {stats['deletes']}")
    print(f"  consistency reads : {stats['reads']} (bad sums: {stats['bad_sums']})")
    print(f"  final acc         : sum={s} count={n} (want 100000 / 1000)")
    print(f"  final log rows    : {logn} (want exactly {expected_log})")
    print(f"  RSS idle={idle_rss:.1f}MB peak={peak_rss:.1f}MB "
          f"(ceiling {RSS_CEILING_MB:.0f}MB)")

    if s != 100000:
        FAILS.append(f"final sum(bal)={s} != 100000")
    if n != 1000:
        FAILS.append(f"final count(acc)={n} != 1000")
    if logn != expected_log:
        FAILS.append(f"final log rows={logn} != acked {expected_log} "
                     f"(lost or resurrected rows)")
    if stats["retries"] < 10:
        FAILS.append(f"only {stats['retries']} OCC retries — contention path "
                     f"not exercised")
    if stats["reads"] < 10:
        FAILS.append("consistency canary reads never ran")
    if peak_rss > RSS_CEILING_MB:
        FAILS.append(f"peak RSS {peak_rss:.1f}MB over ceiling {RSS_CEILING_MB}MB")

    os.kill(p.pid, 15)
    p.wait(timeout=10)
    # clean shutdown then restart: everything must still be exact
    p = subprocess.Popen([BIN, "--data", f"file://{DATA}", "--port", str(PORT),
                          "--bind", "127.0.0.1"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_listening()
    c = connect()
    cur = c.cursor()
    cur.execute("SELECT sum(bal), count(*) FROM acc")
    s2, n2 = cur.fetchone()
    cur.execute("SELECT count(*) FROM log")
    logn2 = cur.fetchone()[0]
    c.close()
    os.kill(p.pid, 9)
    p.wait()
    if (s2, n2, logn2) != (s, n, logn):
        FAILS.append(f"post-restart mismatch: {(s2, n2, logn2)} != {(s, n, logn)}")
    else:
        print(f"  post-restart       : exact match (sum={s2}, acc={n2}, log={logn2})")

    if FAILS:
        print("SOAK FAILED:")
        for f in FAILS[:20]:
            print("  -", f)
        sys.exit(1)
    print("SOAK PASSED (invariants held throughout, exact final counts, "
          "OCC retried, RSS under ceiling)")


if __name__ == "__main__":
    main()
