#!/usr/bin/env python3
"""puddle M7 corruption tests: bit-flips, truncation, garbage, deletion in
every on-disk structure. The contract (M3 rules, now enforced):
  * STRUCTURAL damage (CRC mismatch, bad magic, unparseable meta/manifest,
    missing referenced part, torn non-latest WAL segment, lost catalog with
    data present) => LOUD refusal, exit 1. Never start on top of damage.
  * a torn/corrupt LATEST WAL segment => consistent prefix: the whole
    segment is ONE transaction that was never acked, so it is discarded
    whole — a half-applied transaction must never be visible.
  * garbage objects with unparseable names are ignored; data stays exact.
  * in every case where the server DOES start, the data served must be
    exactly the checkpointed state or the full state — never wrong rows.
"""
import glob
import os
import shutil
import subprocess
import sys
import time

import psycopg2

BIN = "/home/user/puddle/target/release/puddle"
PRISTINE = "/tmp/puddle-corrupt-pristine"
PORT = 5470
FAILS = []

# ------------------------------------------------------- build pristine dir --


def build_pristine():
    shutil.rmtree(PRISTINE, ignore_errors=True)
    p = subprocess.Popen([BIN, "--data", f"file://{PRISTINE}", "--port", str(PORT),
                          "--bind", "127.0.0.1"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    wait_listening()
    c = psycopg2.connect(dsn())
    c.autocommit = True
    cur = c.cursor()
    cur.execute("CREATE TABLE keep (id int, v text)")
    for base in range(0, 500, 100):
        cur.execute("INSERT INTO keep VALUES " +
                    ",".join(f"({i},'v{i}')" for i in range(base, base + 100)))
    cur.execute("CHECKPOINT")
    cur.execute("INSERT INTO keep VALUES " +
                ",".join(f"({500 + i},'post{i}')" for i in range(50)))
    cur.execute("SELECT count(*) FROM keep")
    assert cur.fetchone() == (550,)
    c.close()
    os.kill(p.pid, 9)
    p.wait()


def dsn(port=None):
    return f"host=127.0.0.1 port={port or PORT} dbname=x user=u sslmode=disable"


def wait_listening(port=None):
    for _ in range(100):
        try:
            psycopg2.connect(dsn(port), connect_timeout=1).close()
            return True
        except Exception:
            time.sleep(0.05)
    return False


def flip(path, off):
    b = bytearray(open(path, "rb").read())
    b[off] ^= 0x01
    open(path, "wb").write(b)


CASE_N = 0


def case(name, mutate, expect):
    """expect: 'refuse' | 'full' (550 exact) | 'prefix' (500 exact)."""
    global CASE_N
    CASE_N += 1
    port = PORT + CASE_N
    d = f"/tmp/pc-case{CASE_N}"
    shutil.rmtree(d, ignore_errors=True)
    shutil.copytree(PRISTINE, d)
    mutate(d)
    p = subprocess.Popen([BIN, "--data", f"file://{d}", "--port", str(port),
                          "--bind", "127.0.0.1"],
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    started = wait_listening(port)
    if not started:
        rc = p.poll()
        out, _ = p.communicate(timeout=5)
        p.wait()
        tail = out.decode().strip().splitlines()
        if expect == "refuse":
            detail = tail[-1] if tail else ""
            ok = rc != 0 and ("recovery failed" in out.decode() or "refus" in out.decode().lower())
            print(f"  [{'OK' if ok else 'FAIL'}] {name}: refused rc={rc} :: {detail[:90]}")
            if not ok:
                FAILS.append(name)
        else:
            print(f"  [FAIL] {name}: expected {expect} but server refused: "
                  f"{tail[-1] if tail else ''}"[:140])
            FAILS.append(name)
        return
    try:
        c = psycopg2.connect(dsn(port))
        c.autocommit = True
        cur = c.cursor()
        cur.execute("SELECT count(*), count(DISTINCT id), min(id), max(id) FROM keep")
        n, dist, mn, mx = cur.fetchone()
        cur.execute("SELECT count(*) FROM keep WHERE id >= 500")
        post = cur.fetchone()[0]
        # value invariant: v = 'v'+id (checkpointed rows) / 'post'+(id-500)
        cur.execute("SELECT count(*) FROM keep WHERE v <> "
                    "CASE WHEN id >= 500 THEN 'post' || CAST(id - 500 AS text) "
                    "ELSE 'v' || CAST(id AS text) END")
        badv = cur.fetchone()[0]
        c.close()
        if expect == "full":
            ok = (n, dist, mn, mx, post, badv) == (550, 550, 0, 549, 50, 0)
        elif expect == "prefix":
            ok = (n, dist, mn, mx, post, badv) == (500, 500, 0, 499, 0, 0)
        else:
            ok = False
        print(f"  [{'OK' if ok else 'FAIL'}] {name}: started rows={n} distinct={dist} "
              f"({expect})" + ("" if ok else f" MISMATCH post={post} badv={badv}"))
        if not ok:
            FAILS.append(name)
    except Exception as e:
        print(f"  [FAIL] {name}: started but query failed: {str(e)[:80]}")
        FAILS.append(name)
    finally:
        p.kill()
        p.wait()


def part(d):
    return glob.glob(f"{d}/t/1/part/*.part")[0]


def wal(d):
    return glob.glob(f"{d}/wal/*.wal")[0]


def main():
    build_pristine()
    print(f"corruption cases (pristine: 500 checkpointed + 50 in WAL):")
    # ---- meta.json ----
    case("meta.json garbage", lambda d: open(f"{d}/meta.json", "w").write("{{{nz0!!"), "refuse")
    case("meta.json deleted (data exists)", lambda d: os.remove(f"{d}/meta.json"), "refuse")
    case("meta.json bit-flip", lambda d: flip(f"{d}/meta.json", 30), "refuse")
    # ---- manifest ----
    case("manifest garbage", lambda d: open(f"{d}/t/1/manifest/1.json", "w").write("nope"), "refuse")
    case("manifest deleted (parts exist)", lambda d: os.remove(f"{d}/t/1/manifest/1.json"), "refuse")
    case("manifest truncated", lambda d: open(f"{d}/t/1/manifest/1.json", "r+").truncate(40), "refuse")

    def lie(d):
        b = open(f"{d}/t/1/manifest/1.json").read()
        open(f"{d}/t/1/manifest/1.json", "w").write(b.replace('"rows":500', '"rows":499'))
    case("manifest rows field lies (data must still be exact)", lie, "full")
    # ---- parts ----
    case("part truncated (bad magic)", lambda d: open(part(d), "wb").write(open(part(d), "rb").read()[:200]), "refuse")
    case("part bit-flip mid-payload (CRC)", lambda d: flip(part(d), len(open(part(d), "rb").read()) // 2), "refuse")
    case("part bit-flip header", lambda d: flip(part(d), 12), "refuse")
    case("part deleted (manifest dangles)", lambda d: os.remove(part(d)), "refuse")
    # ---- WAL ----
    case("latest WAL truncated -> consistent prefix",
         lambda d: open(wal(d), "wb").write(open(wal(d), "rb").read()[:40]), "prefix")
    case("latest WAL bit-flip -> whole unacked txn discarded",
         lambda d: flip(wal(d), 60), "prefix")
    case("stray garbage object in wal/ ignored",
         lambda d: open(f"{d}/wal/zzz-notasq.wal", "wb").write(b"\xde\xad" * 25), "full")

    def torn_nonlatest(d):
        # make seq 7 torn (keep the first 40 bytes) and add a valid seq 8
        # copy so that 7 is NOT the latest segment anymore
        w = wal(d)
        head = open(w, "rb").read()[:40]
        open(w, "wb").write(head)
        shutil.copy(glob.glob(f"{PRISTINE}/wal/*.wal")[0],
                    f"{d}/wal/0000000000000008-deadbeefdeadbeefdeadbeefdeadbeef.wal")
    case("torn NON-latest WAL segment -> refuse", torn_nonlatest, "refuse")

    if FAILS:
        print(f"CORRUPTION TESTS FAILED ({len(FAILS)}):")
        for f in FAILS:
            print("  -", f)
        sys.exit(1)
    print(f"CORRUPTION TESTS PASSED ({CASE_N} cases: loud refusal or exact "
          "prefix, never silent wrong data)")


if __name__ == "__main__":
    main()
