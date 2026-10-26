#!/usr/bin/env python3
"""puddle psql compatibility suite (v1.1).

Drives the real psql client binary (simple query protocol, \copy wire COPY,
backslash catalog commands) against a dedicated server instance:

  - \l  \dt  \d <table> render from the synthesized catalog
  - server-side COPY FROM/TO STDIN (text + csv, quoting, NULL, col subset)
  - client-side \copy round-trips through files
  - COPY (query) TO STDOUT
  - error paths: bad int (22P02), field count, NOT NULL (23502),
    duplicate column (42701), COPY inside txn (0A000)
  - ALTER TABLE ADD COLUMN + kill -9 restart: schema and rows exact
  - generate_series in FROM, UNION [ALL], special registers

Exit code 0 = all green.
"""

import os
import shutil
import signal
import subprocess
import sys
import time

import psycopg2

PORT = 5503
PSQL = "/usr/bin/psql"
DSN = f"host=127.0.0.1 port={PORT} dbname=z user=puddle"
BIN = os.path.join(os.path.dirname(__file__), "..", "target", "release", "puddle")
DATA = "/tmp/puddle-psql-test"

PASS = 0
FAIL = 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  [OK] {name}")
    else:
        FAIL += 1
        print(f"  [FAIL] {name} {detail}")


def psql(args, stdin=None):
    """Runs psql with -X (no psqlrc) and returns (rc, stdout+stderr)."""
    cmd = [PSQL, "-X", DSN] + args
    r = subprocess.run(cmd, input=stdin, capture_output=True, text=True, timeout=60)
    return r.returncode, (r.stdout + r.stderr)


def q(sql, stdin=None):
    """Single -c query, tuple mode; returns (rc, output)."""
    return psql(["-tAc", sql], stdin=stdin)


def start_server():
    env = dict(os.environ, RUST_BACKTRACE="1")
    p = subprocess.Popen(
        [BIN, "--data", f"file://{DATA}", "--port", str(PORT)],
        stdout=open("/tmp/puddle-psql-test.log", "w"),
        stderr=subprocess.STDOUT,
        env=env,
    )
    for _ in range(100):
        rc, out = q("SELECT 1")
        if rc == 0:
            return p
        if p.poll() is not None:
            print("server died:", open("/tmp/puddle-psql-test.log").read()[-2000:])
            sys.exit(1)
        time.sleep(0.1)
    print("server never came up")
    sys.exit(1)


def kill9(p):
    p.send_signal(signal.SIGKILL)
    p.wait()
    time.sleep(0.3)


def main():
    if not shutil.which("psql") and not os.path.exists(PSQL):
        print("[install] postgresql-client missing, installing...")
        subprocess.run(
            ["sudo", "-n", "apt-get", "install", "-y", "postgresql-client"],
            check=True,
        )

    shutil.rmtree(DATA, ignore_errors=True)
    os.makedirs(DATA)
    proc = start_server()

    try:
        # ------------------------------------------------ setup ----------
        rc, out = psql(["-c", "CREATE TABLE lakes (name text NOT NULL, depth int, area float)"])
        check("create table", rc == 0, out)
        rc, out = psql(["-c", "CREATE INDEX ix_depth ON lakes USING btree (depth)"])
        check("create index", rc == 0, out)
        rc, out = psql(["-c", "INSERT INTO lakes VALUES ('Baikal', 1642, 31500), ('Vostok', 500, 12500)"])
        check("insert", rc == 0, out)

        # ------------------------------------------- backslash commands --
        rc, out = psql(["-c", r"\l"])
        check(r"\l lists the database", rc == 0 and " z " in out and "puddle" in out, out)
        check(r"\l shows encoding", "UTF8" in out, out)

        rc, out = psql(["-c", r"\dt"])
        check(r"\dt lists public.lakes", rc == 0 and "lakes" in out and "table" in out, out)

        rc, out = psql(["-c", r"\d lakes"])
        check(r"\d shows columns", rc == 0 and "name" in out and "depth" in out and "area" in out, out)
        check(r"\d shows types", rc == 0 and "integer" in out and "double precision" in out, out)
        check(r"\d shows NOT NULL", rc == 0 and "not null" in out, out)
        check(r"\d shows the index", rc == 0 and "ix_depth" in out, out)

        rc, out = psql(["-c", r"\d nosuchtable"])
        check(r"\d missing table errors", rc != 0, out)

        # --------------------------------------- server-side COPY IN -----
        data = "Victoria\t80\t68800\nTanganyika\t1470\t32600\n"
        rc, out = psql(["-c", "COPY lakes FROM STDIN"], stdin=data)
        check("copy in text", rc == 0, out)
        rc, out = q("SELECT count(*) FROM lakes")
        check("copy in text rows", out.strip() == "4", out)

        rc, out = psql(["-c", "COPY lakes FROM STDIN WITH (FORMAT csv)"],
                       stdin="Superior,403,82100\nErie,64,25700\n")
        check("copy in csv", rc == 0, out)
        rc, out = q("SELECT count(*) FROM lakes")
        check("copy in csv rows", out.strip() == "6", out)

        # column subset: depth stays NULL
        rc, out = psql(["-c", "COPY lakes (name, area) FROM STDIN"], stdin="Huron\t60000\n")
        check("copy in column subset", rc == 0, out)
        rc, out = q("SELECT depth IS NULL FROM lakes WHERE name = 'Huron'")
        check("subset leaves depth NULL", out.strip() == "t", out)

        # --------------------------------------- server-side COPY OUT ----
        rc, out = q("COPY lakes TO STDOUT")
        lines = [l for l in out.splitlines() if l]
        check("copy out text rows", len(lines) == 7, out)
        check("copy out tab separated", any("\t" in l for l in lines), out)

        rc, out = q("COPY lakes TO STDOUT WITH (FORMAT csv, HEADER true)")
        lines = out.splitlines()
        check("copy out csv header", lines[0] == "name,depth,area", out)
        check("copy out csv rows", len(lines) == 8, out)

        rc, out = q("COPY (SELECT name, depth FROM lakes WHERE depth > 1000 ORDER BY depth DESC) TO STDOUT")
        check("copy (query) out", out == "Baikal\t1642\nTanganyika\t1470\n", repr(out))

        # quoting round-trip through the server
        rc, out = q("COPY (SELECT 'a,b' AS c1, E'x\ny' AS c2) TO STDOUT WITH (FORMAT csv)")
        check("csv quoting", out == '"a,b","x\ny"\n', repr(out))

        # ------------------------------------------- client-side \copy ---
        path = "/tmp/puddle-psql-in.txt"
        with open(path, "w") as f:
            f.write("Ontario\t244\t19000\nMichigan\t281\t58000\n")
        rc, out = psql(["-c", f"\\copy lakes FROM '{path}'"])
        check(r"\copy from file", rc == 0, out)
        rc, out = q("SELECT count(*) FROM lakes")
        check(r"\copy from rows", out.strip() == "9", out)

        outpath = "/tmp/puddle-psql-out.csv"
        rc, out = psql(["-c", f"\\copy (SELECT * FROM lakes ORDER BY name) TO '{outpath}' WITH (FORMAT csv, HEADER true)"])
        check(r"\copy to file", rc == 0, out)
        with open(outpath) as f:
            content = f.read()
        check(r"\copy to file header", content.startswith("name,depth,area\n"), repr(content[:40]))
        check(r"\copy to file rows", content.count("\n") == 10, repr(content))

        # ----------------------------------------------- COPY errors -----
        rc, out = psql(["-c", "COPY lakes FROM STDIN"], stdin="foo\tbar\t1\n")
        check("copy bad int 22P02", rc != 0 and "22P02" not in out and "invalid input syntax" in out, out)

        rc, out = psql(["-c", "COPY lakes FROM STDIN"], stdin="a\tb\n")
        check("copy field count error", rc != 0 and "2 fields but 3 were expected" in out, out)

        rc, out = psql(["-c", "COPY lakes FROM STDIN"], stdin="\\N\t5\t1\n")
        check("copy not-null 23502", rc != 0 and "not-null constraint" in out, out)

        # server must still be healthy after failed copies
        rc, out = q("SELECT count(*) FROM lakes")
        check("server alive after copy errors", rc == 0 and out.strip() == "9", out)

        # COPY inside an explicit transaction
        rc, out = psql(["-c", "BEGIN; COPY lakes FROM STDIN;"], stdin="x\t1\t1\n")
        check("copy in txn 0A000", rc != 0 and "transaction" in out, out)
        rc, out = q("SELECT count(*) FROM lakes")
        check("txn copy left no rows", rc == 0 and out.strip() == "9", out)

        # ------------------------------------------------ ALTER ----------
        rc, out = psql(["-c", "ALTER TABLE lakes ADD COLUMN volume float"])
        check("alter add column", rc == 0, out)
        rc, out = q("SELECT volume FROM lakes WHERE name = 'Baikal'")
        check("new column NULL-padded", out.strip() == "", out)

        rc, out = psql(["-c", "ALTER TABLE lakes ADD COLUMN volume float"])
        check("alter duplicate 42701", rc != 0 and "already exists" in out, out)
        rc, out = psql(["-c", "ALTER TABLE lakes ADD COLUMN IF NOT EXISTS volume float"])
        check("alter if not exists noop", rc == 0, out)
        rc, out = psql(["-c", "ALTER TABLE IF EXISTS nosuch ADD COLUMN z int"])
        check("alter missing table noop", rc == 0, out)
        rc, out = psql(["-c", "ALTER TABLE nosuch ADD COLUMN z int"])
        check("alter missing table 42P01", rc != 0 and "does not exist" in out, out)

        rc, out = psql(["-c", "UPDATE lakes SET volume = 23615.7 WHERE name = 'Baikal'"])
        check("update new column", rc == 0, out)

        # ----------------------------- kill -9 restart: ALTER + COPY ------
        before = q("SELECT count(*), count(volume), sum(area)::text FROM lakes")[1].strip()
        kill9(proc)
        proc = start_server()
        after = q("SELECT count(*), count(volume), sum(area)::text FROM lakes")[1].strip()
        check("kill-9 restart exact (rows+schema)", before == after, f"{before!r} vs {after!r}")

        rc, out = psql(["-c", r"\d lakes"])
        check(r"\d after restart shows volume", rc == 0 and "volume" in out, out)

        # ------------------------------------ misc tool-facing SQL -------
        rc, out = q("SELECT current_database(), current_schema(), current_user")
        check("special registers", out.strip() == "z|public|puddle", out)

        rc, out = q("SELECT x FROM generate_series(1, 5) AS x WHERE x <> 3")
        check("generate_series in FROM", out.replace("\n", " ").split() == ["1", "2", "4", "5"], out)

        rc, out = q("SELECT 1 UNION SELECT 2 UNION SELECT 2 ORDER BY 1")
        check("union distinct", out.split() == ["1", "2"], out)
        rc, out = q("SELECT 1 UNION ALL SELECT 1")
        check("union all", out.split() == ["1", "1"], out)

        rc, out = q("SELECT n.nspname FROM pg_catalog.pg_namespace n WHERE n.nspname = 'public'")
        check("catalog query via psql", out.strip() == "public", out)

        rc, out = q("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY 1")
        check("information_schema via psql", out.strip() == "lakes", out)

        rc, out = q("SELECT name FROM lakes WHERE depth = 1642")
        check("index lookup after restart", out.strip() == "Baikal", out)

        # ------------------------- psycopg2 copy_expert (driver path) ----
        conn = psycopg2.connect(DSN)
        conn.autocommit = True  # v1.1: COPY inside an explicit txn is 0A000
        cur = conn.cursor()
        cur.execute("CREATE TABLE big (id int, val text)")
        import io
        payload = "".join(f"{i}\tvalue-{i}\n" for i in range(20000))
        cur.copy_expert("COPY big FROM STDIN", io.BytesIO(payload.encode()))
        cur.execute("SELECT count(*), sum(id) FROM big")
        n, sm = cur.fetchone()
        check("psycopg2 copy_expert count", (n, sm) == (20000, 199990000), f"{n},{sm}")
        conn.close()

        # ------------------------- kill -9 DURING a large COPY ----------
        # the batch is a single WAL record group: mid-flight death must
        # leave the table exactly at its pre-copy state (all or nothing)
        rc, out = q("SELECT count(*) FROM big")
        pre = int(out.strip())
        conn = psycopg2.connect(DSN)
        conn.autocommit = True
        cur = conn.cursor()
        huge = "".join(f"{i}\tx\n" for i in range(500000)).encode()

        import threading

        result = {}

        def do_copy():
            try:
                cur.copy_expert("COPY big FROM STDIN", io.BytesIO(huge))
                result["done"] = True
            except Exception as e:  # noqa: BLE001
                result["err"] = str(e)[:80]

        t = threading.Thread(target=do_copy)
        t.start()
        time.sleep(0.05)  # let the stream reach the server mid-flight
        kill9(proc)
        proc = None
        t.join(timeout=30)
        proc = start_server()
        rc, out = q("SELECT count(*) FROM big")
        post = int(out.strip())
        check(
            "kill-9 mid-COPY is all-or-nothing",
            post in (pre, pre + 500000),
            f"pre={pre} post={post}",
        )
        check("mid-COPY kill left no partial row", (post - pre) % 500000 == 0, f"{post - pre}")

    finally:
        kill9(proc)

    print()
    if FAIL == 0:
        print(f"PSQL TESTS PASSED ({PASS} checks: \\l \\dt \\d, server+client COPY, "
              "ALTER + kill -9 restart, catalog, specials)")
        return 0
    print(f"PSQL TESTS FAILED: {FAIL} failed / {PASS} passed")
    return 1


if __name__ == "__main__":
    sys.exit(main())
