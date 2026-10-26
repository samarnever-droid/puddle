#!/usr/bin/env python3
"""puddle M7 crash-loop: SIGKILL the server mid-concurrent-writes, ~20 rounds.

Contract under test (AckLevel=Committed, the default):
  * every insert the client SAW acked is durable after kill -9 + restart
  * per-writer k-space is a CONTIGUOUS prefix after every crash — a hole
    means a committed txn was lost (durability bug); a middle row missing
    while later rows exist means a txn was torn (atomicity bug)
  * no duplicate k ever (a resurrected txn would duplicate)
  * a static table's contents are never disturbed
Writers own disjoint k ranges (thread t owns k = t*1_000_000 + n). A writer
that survives a crash resumes from its observed prefix end, so durable-but-
unread acks (commit applied, response lost) are legal and absorbed.
"""
import hashlib
import os
import random
import signal
import subprocess
import sys
import threading
import time
import shutil

import psycopg2

BIN = "/home/user/puddle/target/release/puddle"
DATA = "/tmp/puddle-crash"
PORT = 5441
DSN = f"host=127.0.0.1 port={PORT} dbname=x user=u sslmode=disable"
ROUNDS = 20
WRITERS = 6
SPACE = 1_000_000
random.seed(20261023)

FAILS = []


def start():
    p = subprocess.Popen([BIN, "--data", f"file://{DATA}", "--port", str(PORT),
                          "--bind", "127.0.0.1"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(200):
        try:
            c = psycopg2.connect(DSN)
            c.close()
            return p
        except Exception:
            time.sleep(0.05)
    raise RuntimeError("server did not come back after crash (recovery failed?)")


def sql(*stmts, fetch=True):
    c = psycopg2.connect(DSN)
    c.autocommit = True
    cur = c.cursor()
    out = []
    for s in stmts:
        cur.execute(s)
        if fetch and cur.description:
            out.append(cur.fetchall())
        else:
            out.append(None)
    c.close()
    return out


def frozen_checksum():
    rows = sql("SELECT tag, payload FROM frozen ORDER BY tag")[0]
    h = hashlib.sha256()
    for t, p in rows:
        h.update(repr((t, p)).encode())
    return h.hexdigest(), len(rows)


def ensure_schema(want_checksum):
    """(Re)create schema if a crash landed during iteration-0 setup."""
    try:
        n = sql("SELECT count(*) FROM frozen")[0][0][0]
        if n:
            return want_checksum if want_checksum else frozen_checksum()
    except psycopg2.Error:
        pass
    sql("CREATE TABLE seq (k int, v text)",
        "CREATE TABLE frozen (tag text, payload text)")
    for i in range(300):
        sql(f"INSERT INTO frozen VALUES ('tag-{i:03d}', "
            f"'payload-{i:03d}-{'x' * (i % 37)}')")
    ck, _ = frozen_checksum()
    if want_checksum and ck != want_checksum:
        FAILS.append("frozen table mutated by crash during setup")
    return ck


class Writer(threading.Thread):
    def __init__(self, tid):
        super().__init__(daemon=True)
        self.tid = tid
        self.acked = 0   # rows this thread got acks for, this process
        self.err = None

    def run(self):
        base = self.tid * SPACE
        try:
            c = psycopg2.connect(DSN)
            c.autocommit = True
            cur = c.cursor()
            # resume from observed durable prefix end (absorbs unread acks)
            cur.execute("SELECT max(k) FROM seq WHERE k > %s AND k < %s",
                        (base, base + SPACE))
            m = cur.fetchone()[0]
            n = (m - base) if m is not None else 0
            while True:
                k = base + n + 1
                tag = f"t{self.tid}-{k}"
                try:
                    if random.random() < 0.2:
                        # 3-row explicit txn: all-or-nothing after crash
                        c.autocommit = False
                        cur.execute(f"BEGIN")
                        for j in range(3):
                            cur.execute("INSERT INTO seq VALUES (%s, %s)",
                                        (k + j, tag))
                        c.commit()
                        c.autocommit = True
                        n += 3
                        self.acked += 3
                    else:
                        cur.execute("INSERT INTO seq VALUES (%s, %s)", (k, tag))
                        n += 1
                        self.acked += 1
                except (psycopg2.OperationalError, psycopg2.InterfaceError):
                    return  # server killed under us
                except psycopg2.Error as e:
                    # serialized/retried statement errors: stop (harsh env)
                    self.err = str(e).strip()[:80]
                    return
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            return  # server dead at connect
        except Exception as e:  # noqa: BLE001
            self.err = f"{type(e).__name__}: {e}"[:80]


def verify(round_no, base_counts):
    """All per-thread k sets must be contiguous prefixes; no dups; frozen ok."""
    rows = sql("SELECT k FROM seq")[0]
    if len(rows) != len(set(rows)):
        FAILS.append(f"round {round_no}: DUPLICATE k values "
                     f"({len(rows)} rows, {len(set(rows))} distinct)")
    per = {}
    for (k,) in rows:
        per.setdefault((k - 1) // SPACE, []).append(k - ((k - 1) // SPACE) * SPACE)
    total = 0
    for t, ks in sorted(per.items()):
        s = sorted(ks)
        if s != list(range(1, len(s) + 1)):
            holes = sorted(set(range(1, s[-1] + 1)) - set(s))[:5]
            FAILS.append(f"round {round_no}: thread {t} prefix NOT contiguous "
                         f"(len {len(s)}, max {s[-1]}, first holes {holes})")
        total += len(s)
    if total != len(rows):
        FAILS.append(f"round {round_no}: k outside writer ranges")
    grew = {t: len(per.get(t, [])) - base_counts.get(t, 0)
            for t in per}
    return {t: len(ks) for t, ks in per.items()}, grew


def main():
    shutil.rmtree(DATA, ignore_errors=True)
    ck = None
    counts = {}
    total_acked = 0
    print(f"crash-loop: {ROUNDS} rounds x SIGKILL mid-write, {WRITERS} writers")
    t0 = time.time()
    for rnd in range(1, ROUNDS + 1):
        p = start()
        ck = ensure_schema(ck)
        writers = [Writer(t) for t in range(WRITERS)]
        for w in writers:
            w.start()
        time.sleep(random.uniform(0.35, 1.1))
        os.kill(p.pid, signal.SIGKILL)
        p.wait()
        for w in writers:
            w.join(timeout=10)
        total_acked += sum(w.acked for w in writers)
        for w in writers:
            if w.err:
                FAILS.append(f"round {rnd}: writer {w.tid} hit {w.err}")
        p = start()  # recovery
        counts, grew = verify(rnd, counts)
        ck2, nfr = frozen_checksum()
        if ck2 != ck or nfr != 300:
            FAILS.append(f"round {rnd}: frozen table checksum changed")
        os.kill(p.pid, signal.SIGKILL)
        p.wait()
        alive = {t: g for t, g in grew.items() if g}
        print(f"  round {rnd:2d}: acked+running "
              f"{sum(w.acked for w in writers):5d} | totals/thread "
              f"{ {t: counts[t] for t in sorted(counts)} } | grew {alive or '-'}"
              + ("  <<< FAIL" if FAILS else ""))
        if FAILS:
            break
    dur = time.time() - t0
    if not FAILS:
        # final clean check
        p = start()
        final = sql("SELECT count(*), count(DISTINCT k) FROM seq")[0][0]
        print(f"final: {final[0]} rows, {final[1]} distinct, "
              f"{total_acked} client-acked, {dur:.1f}s wall")
        if final[0] != final[1]:
            FAILS.append("final: duplicate k survived")
        if final[0] < total_acked:
            FAILS.append(f"final: DURABILITY HOLE — {total_acked} acked but "
                         f"{final[0]} durable")
        os.kill(p.pid, signal.SIGKILL)
        p.wait()
    if FAILS:
        print("CRASH-LOOP FAILED:")
        for f in FAILS:
            print("  -", f)
        sys.exit(1)
    print("CRASH-LOOP PASSED (acked high-water durable, contiguous prefixes, "
          "no dups, frozen table intact)")


if __name__ == "__main__":
    main()
