#!/usr/bin/env python3
"""M6 auth + TLS integration tests.

Covers: trust (unchanged), cleartext password (right/wrong/unknown user),
SCRAM-SHA-256 via psycopg2's native libpq SCRAM (right/wrong/unknown),
TLS (sslmode=require round-trip, cleartext alongside, --tls-required
rejection), and the --scram-secret helper.

usage: python3 auth_test.py
"""
import json
import os
import shutil
import subprocess
import sys
import time

import psycopg2

HERE = os.path.dirname(os.path.abspath(__file__))
BIN = os.path.join(HERE, "..", "target", "release", "puddle")
DATA = "/tmp/puddle-auth"
AUTH_FILE = "/tmp/puddle-auth-users.json"

FAILS = []


def check(name, cond, detail=""):
    print(f"  [{'OK' if cond else 'FAIL'}] {name}" + (f"  {detail}" if not cond else ""))
    if not cond:
        FAILS.append(name)


def wait_up(port):
    """Raw TCP probe: the server accepting connections means it's up
    (auth failures at this stage are expected and fine)."""
    import socket
    for _ in range(100):
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1)
            s.close()
            return True
        except OSError:
            time.sleep(0.05)
    return False


def start(port, extra, data=DATA):
    shutil.rmtree(data, ignore_errors=True)
    os.makedirs(data, exist_ok=True)
    log = open(f"/tmp/puddle-auth-{port}.log", "w")
    p = subprocess.Popen([BIN, "--port", str(port), "--data", f"file://{data}"] + extra,
                         stdout=log, stderr=subprocess.STDOUT)
    if not wait_up(port):
        print(open(f"/tmp/puddle-auth-{port}.log").read())
        raise RuntimeError(f"server {port} did not start")
    return p


def try_sql(port, sql, **kw):
    try:
        c = psycopg2.connect(host="127.0.0.1", port=port, dbname="d", connect_timeout=2, **kw)
        c.autocommit = True
        k = c.cursor()
        k.execute(sql)
        r = k.fetchall() if k.description else None
        c.close()
        return ("ok", r)
    except psycopg2.Error as e:
        return ("err", e.pgcode, str(e).splitlines()[0][:200])


def main():
    # never let a leaked server poison the port (lesson from M6 debugging)
    subprocess.run(["pgrep", "-x", "puddle"], capture_output=True)
    try:
        subprocess.run(["pkill", "-x", "puddle"], capture_output=True)
        time.sleep(0.3)
    except Exception:
        pass

    # ---- users file: alice (password only), bob (scram verifier), no-pw dave
    r = subprocess.run([BIN, "--scram-secret", "bob-secret-99"], capture_output=True, text=True)
    bob_scram = r.stdout.strip()
    assert bob_scram.startswith("SCRAM-SHA-256$"), r.stdout + r.stderr
    with open(AUTH_FILE, "w") as f:
        json.dump({"users": {
            "alice": {"password": "alice-pw"},
            "bob": {"scram": bob_scram},
            "dave": {"password": "dave-pw", "scram": bob_scram.replace("SCRAM", "SCRAM")},
        }}, f)

    # ---- 1. trust mode still works
    p = start(5471, [])
    r = try_sql(5471, "SELECT 1+2")
    check("trust: plain connect works", r == ("ok", [(3,)]), str(r))
    p.terminate(); p.wait()

    # ---- 2. cleartext password mode
    p = start(5471, ["--auth", "password", "--auth-file", AUTH_FILE])
    r = try_sql(5471, "SELECT 1", user="alice", password="alice-pw")
    check("password: right password", r == ("ok", [(1,)]), str(r))
    def authfail(r):
        return r[0] == "err" and ("28P01" in str(r) or "password authentication failed" in str(r))
    r = try_sql(5471, "SELECT 1", user="alice", password="WRONG")
    check("password: wrong password -> 28P01", authfail(r), str(r))
    r = try_sql(5471, "SELECT 1", user="nobody", password="x")
    check("password: unknown user -> 28P01", authfail(r), str(r))
    # password-mode user with only a scram entry (bob) must fail
    r = try_sql(5471, "SELECT 1", user="bob", password="bob-secret-99")
    check("password: scram-only entry cannot cleartext-auth", authfail(r), str(r))
    p.terminate(); p.wait()

    # ---- 3. SCRAM-SHA-256 (psycopg2/libpq does the real client side)
    p = start(5471, ["--auth", "scram-sha-256", "--auth-file", AUTH_FILE])
    r = try_sql(5471, "SELECT 41+1", user="alice", password="alice-pw")
    check("scram: password-derived verifier works", r == ("ok", [(42,)]), str(r))
    r = try_sql(5471, "SELECT 41+1", user="bob", password="bob-secret-99")
    check("scram: stored verifier works", r == ("ok", [(42,)]), str(r))
    r = try_sql(5471, "SELECT 1", user="alice", password="nope")
    check("scram: wrong password -> 28P01", authfail(r), str(r))
    r = try_sql(5471, "SELECT 1", user="ghost", password="whatever")
    check("scram: unknown user -> 28P01 (no enumeration)", authfail(r), str(r))
    # authenticated session does real work (durability unaffected)
    c = psycopg2.connect(host="127.0.0.1", port=5471, dbname="d", user="alice", password="alice-pw")
    c.autocommit = True
    k = c.cursor()
    k.execute("CREATE TABLE t (a int)")
    k.execute("INSERT INTO t VALUES (5)")
    k.execute("SELECT a FROM t")
    check("scram: full session DML", k.fetchall() == [(5,)])
    c.close()
    p.terminate(); p.wait()

    # ---- 4. TLS (ephemeral self-signed), cleartext still allowed
    p = start(5471, ["--tls"])
    r = try_sql(5471, "SELECT 2*3")
    check("tls: cleartext still works (optional mode)", r == ("ok", [(6,)]), str(r))
    r = try_sql(5471, "SELECT 2*3", sslmode="require")
    check("tls: sslmode=require works", r == ("ok", [(6,)]), str(r))
    # TLS + SCRAM together
    p.terminate(); p.wait()
    p = start(5471, ["--tls", "--auth", "scram-sha-256", "--auth-file", AUTH_FILE])
    r = try_sql(5471, "SELECT 7*6", user="bob", password="bob-secret-99", sslmode="require")
    check("tls+scram: encrypted authenticated session", r == ("ok", [(42,)]), str(r))
    c = psycopg2.connect(host="127.0.0.1", port=5471, dbname="d", user="alice",
                         password="alice-pw", sslmode="require")
    c.autocommit = True
    k = c.cursor()
    k.execute("SELECT count(*) FROM generate_dummy") if False else None
    k.execute("SELECT 1")
    check("tls+scram: second session", k.fetchall() == [(1,)])
    c.close()
    p.terminate(); p.wait()

    # ---- 5. --tls-required rejects cleartext, accepts TLS
    p = start(5471, ["--tls", "--tls-required"])
    # sslmode=disable forces a truly cleartext attempt (psycopg2's default
    # "prefer" would happily upgrade to TLS and defeat the check)
    r = try_sql(5471, "SELECT 1", sslmode="disable")
    check("tls-required: cleartext rejected", r[0] == "err", str(r))
    r = try_sql(5471, "SELECT 1", sslmode="require")
    check("tls-required: TLS accepted", r == ("ok", [(1,)]), str(r))
    p.terminate(); p.wait()

    # ---- 6. auth errors give the right shape (28P01 message)
    p = start(5471, ["--auth", "scram-sha-256", "--auth-file", AUTH_FILE])
    try:
        psycopg2.connect(host="127.0.0.1", port=5471, dbname="d", user="alice", password="bad")
        check("scram: bad password raises", False)
    except psycopg2.Error as e:
        # (connect-time auth errors carry no pgcode; the message is the oracle)
        check("scram: failure names the user",
              "password authentication failed" in str(e) and "alice" in str(e), str(e))
    p.terminate(); p.wait()

    print()
    if FAILS:
        print(f"FAILED: {FAILS}")
        sys.exit(1)
    print("ALL AUTH/TLS TESTS PASSED")


if __name__ == "__main__":
    main()
