#!/usr/bin/env python3
"""puddle M3 durability E2E: file store, kill -9 crash loops at every stage."""
import os, signal, subprocess, sys, time, shutil
import psycopg2

BIN = "/home/user/puddle/target/release/puddle"
DATA = "/tmp/puddle-dur"
PORT = 5437
FAILS = []

def start():
    p = subprocess.Popen([BIN, "--data", f"file://{DATA}", "--port", str(PORT),
                          "--bind", "127.0.0.1"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            c = psycopg2.connect(f"host=127.0.0.1 port={PORT} dbname=x user=u")
            c.close()
            return p
        except Exception:
            time.sleep(0.05)
    raise RuntimeError("server did not start")

def kill9(p):
    os.kill(p.pid, signal.SIGKILL)
    p.wait()

def sql(*stmts, fetch=True):
    c = psycopg2.connect(f"host=127.0.0.1 port={PORT} dbname=x user=u")
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

def check(name, got, want):
    ok = got == want
    print(f"  [{'OK' if ok else 'FAIL'}] {name}: {got!r}" + ("" if ok else f" != {want!r}"))
    if not ok:
        FAILS.append(name)

shutil.rmtree(DATA, ignore_errors=True)

print("== round 1: build state ==")
p = start()
sql("""CREATE TABLE lakes (name text NOT NULL, depth int, area float, country text)""",
    """INSERT INTO lakes VALUES ('Baikal', 1642, 31500, 'RU'), ('Tanganyika', 1470, 32900, 'TZ'),
        ('Crater Lake', 594, 53, 'US'), ('Loch Ness', 230, 56, 'GB'), ('Vostok', 500, 12500, 'AQ')""",
    """CREATE TABLE readings (sensor int, temp float, note text)""",
    """INSERT INTO readings VALUES (1, 4.5, 'cold'), (2, 12.25, 'mild'), (3, NULL, 'n/a')""",
    "UPDATE lakes SET depth = depth + 1 WHERE name = 'Baikal'",
    "DELETE FROM readings WHERE sensor = 2", fetch=False)
# committed transaction
c = psycopg2.connect(f"host=127.0.0.1 port={PORT} dbname=x user=u"); c.autocommit = False
cur = c.cursor()
cur.execute("INSERT INTO readings VALUES (4, 20.0, 'txn')")
cur.execute("UPDATE lakes SET country = 'KZ' WHERE name = 'Vostok'")
c.commit(); c.close()
# rolled-back transaction must NOT survive
c = psycopg2.connect(f"host=127.0.0.1 port={PORT} dbname=x user=u"); c.autocommit = False
cur = c.cursor()
cur.execute("INSERT INTO readings VALUES (5, 99.0, 'rolled back')")
c.rollback(); c.close()
kill9(p)

print("== round 2: recover after kill -9 (no checkpoint) ==")
p = start()
r = sql("SELECT name, depth FROM lakes ORDER BY name")
check("lakes after crash", r[0], [('Baikal', 1643), ('Crater Lake', 594), ('Loch Ness', 230),
                                  ('Tanganyika', 1470), ('Vostok', 500)])
r = sql("SELECT sensor, temp FROM readings ORDER BY sensor")
check("readings after crash", r[0], [(1, 4.5), (3, None), (4, 20.0)])
r = sql("SELECT country FROM lakes WHERE name = 'Vostok'")
check("txn update survived", r[0], [('KZ',)])
r = sql("SELECT count(*) FROM readings WHERE sensor = 5")
check("rollback lost", r[0], [(0,)])
# partial-statement atomicity: multi-row INSERT where 2nd row violates NOT NULL
try:
    sql("INSERT INTO lakes VALUES ('X', 1, 1, 'XX'), (NULL, 2, 2, 'YY')", fetch=False)
    check("multi-INSERT rejects bad row", "no error", "NotNullViolation")
except Exception as e:
    check("multi-INSERT rejects bad row", type(e).__name__ == "NotNullViolation", True)
r = sql("SELECT count(*) FROM lakes WHERE name = 'X'")
check("failed multi-INSERT fully rolled back", r[0], [(0,)])
# then mutate more and crash again
sql("INSERT INTO readings VALUES (5, 30.5, 'after-recovery')", fetch=False)
kill9(p)

print("== round 3: recover, then CHECKPOINT, then crash ==")
p = start()
r = sql("SELECT sensor FROM readings ORDER BY sensor")
check("5 rows incl new", [x[0] for x in r[0]], [1, 3, 4, 5])
sql("CHECKPOINT", fetch=False)
kill9(p)
print("== round 4: recover from checkpointed parts ==")
p = start()
r = sql("SELECT name, depth FROM lakes ORDER BY name")
check("lakes from parts", r[0], [('Baikal', 1643), ('Crater Lake', 594), ('Loch Ness', 230),
                                 ('Tanganyika', 1470), ('Vostok', 500)])
r = sql("SELECT sensor, temp, note FROM readings ORDER BY sensor")
check("readings from parts", r[0], [(1, 4.5, 'cold'), (3, None, 'n/a'), (4, 20.0, 'txn'), (5, 30.5, 'after-recovery')])
# post-checkpoint mutations + crash (WAL after parts)
sql("UPDATE readings SET note = 'postcp' WHERE sensor = 5",
    "INSERT INTO lakes VALUES ('Titicaca', 281, 8372, 'PE')", fetch=False)
kill9(p)
print("== round 5: parts + wal replay mix ==")
p = start()
r = sql("SELECT note FROM readings WHERE sensor = 5")
check("wal-after-part update", r[0], [('postcp',)])
r = sql("SELECT count(*) FROM lakes")
check("wal-after-part insert", r[0], [(6,)])
# drop and crash
sql("DROP TABLE readings", fetch=False)
kill9(p)
print("== round 6: dropped table stays dropped ==")
p = start()
r = sql("SELECT count(*) FROM information_schema.tables" if False else "SELECT 1", fetch=True)
try:
    sql("SELECT * FROM readings")
    check("dropped table gone", "no error", "error")
except Exception as e:
    check("dropped table gone", "42P01" in str(e) or "does not exist" in str(e), True)
r = sql("SELECT count(*) FROM lakes")
check("survivor intact", r[0], [(6,)])
# drop + recreate same name (schema change) and crash
sql("DROP TABLE lakes", "CREATE TABLE lakes (id int, label text)",
    "INSERT INTO lakes VALUES (1, 'new-incarnation')", fetch=False)
kill9(p)
print("== round 7: drop+recreate convergence ==")
p = start()
r = sql("SELECT id, label FROM lakes")
check("recreated table", r[0], [(1, 'new-incarnation')])
r = sql("SELECT name FROM lakes ORDER BY name") if False else None
# rowid reuse sanity: insert after recovery
sql("INSERT INTO lakes VALUES (2, 'second')", fetch=False)
r = sql("SELECT id, label FROM lakes ORDER BY id")
check("ids unique after recovery", r[0], [(1, 'new-incarnation'), (2, 'second')])
kill9(p)

# ---- round 8 (M5): indexes survive kill -9 and rebuild on recovery ----
p = start()
sql("DROP TABLE IF EXISTS lakes", "CREATE TABLE lakes (id int, label text, v float)", fetch=False)
sql("INSERT INTO lakes VALUES " + ",".join(f"({i}, 'l{i%7}', {i*0.5})" for i in range(500)), fetch=False)
sql("CREATE INDEX lakes_id_bt ON lakes USING btree (id)",
    "CREATE INDEX lakes_label_hash ON lakes USING hash (label)", fetch=False)
r = sql("SELECT count(*) FROM lakes WHERE id = 250")
check("pre-crash btree point", r[0], [(1,)])
r = sql("SELECT count(*) FROM lakes WHERE id BETWEEN 100 AND 199")
check("pre-crash btree range", r[0], [(100,)])
r = sql("SELECT count(*) FROM lakes WHERE label = 'l3'")
check("pre-crash hash point", r[0], [(71,)])  # i%7==3 for i<500
# mutate under indexes, then crash mid-stream
sql("INSERT INTO lakes VALUES (1000, 'l-new', 1.0)", fetch=False)
sql("UPDATE lakes SET id = 1001 WHERE id = 1000", fetch=False)
sql("DELETE FROM lakes WHERE id < 10", fetch=False)
kill9(p)
print("== round 8: index durability ==")
p = start()
r = sql("SELECT count(*) FROM lakes WHERE id = 1001")
check("recovered btree point (mutated)", r[0], [(1,)])
r = sql("SELECT count(*) FROM lakes WHERE id = 1000")
check("recovered btree point (old key gone)", r[0], [(0,)])
r = sql("SELECT count(*) FROM lakes WHERE id < 10")
check("recovered btree range (deleted)", r[0], [(0,)])
r = sql("SELECT count(*) FROM lakes WHERE id BETWEEN 100 AND 199")
check("recovered btree range", r[0], [(100,)])
r = sql("SELECT count(*) FROM lakes WHERE label = 'l3'")
check("recovered hash point", r[0], [(70,)])  # 71 - deleted i=3 (id<10)
r = sql("SELECT count(*) FROM lakes WHERE label = 'l-new'")
check("recovered hash point (inserted)", r[0], [(1,)])
r = sql("SELECT count(*) FROM lakes")
check("row count after mutations", r[0], [(491,)])
# drop the table: index defs must die with it (heal check)
sql("DROP TABLE lakes", "CREATE TABLE lakes (id int, label text, v float)", fetch=False)
kill9(p)
p = start()
r = sql("SELECT count(*) FROM lakes WHERE id = 5")
check("fresh incarnation full-scan", r[0], [(0,)])
sql("CREATE INDEX lakes_id_bt ON lakes USING btree (id)", fetch=False)
r = sql("SELECT count(*) FROM lakes WHERE id = 5")
check("re-indexed fresh incarnation", r[0], [(0,)])
kill9(p)

# ---- round 9 (v2.0): snapshots + AS OF survive kill -9 ----
p = start()
r = sql("SELECT count(*) FROM lakes")
check("lakes live before snap", r[0], [(0,)])
sql("CREATE SNAPSHOT dursnap", fetch=False)
sql("INSERT INTO lakes VALUES (1,'pre',1.0), (2,'pre',2.0)", fetch=False)
r = sql("SELECT count(*) FROM lakes AS OF SNAPSHOT dursnap")
check("asof pre-crash", r[0], [(0,)])
lsn = sql("SELECT current_lsn()")[0][0]
sql("CHECKPOINT", fetch=False)
kill9(p)
p = start()
r = sql("SELECT count(*) FROM lakes")
check("live after crash+ckpt", r[0], [(2,)])
r = sql("SELECT count(*) FROM lakes AS OF SNAPSHOT dursnap")
check("asof after crash+ckpt", r[0], [(0,)])
r = sql("SELECT lsn FROM puddle_snapshots WHERE name = 'dursnap'")
check("snapshot survives crash", len(r[0]), 1)
r = sql("SELECT current_lsn()")
check("lsn watermark durable", r[0][0] >= lsn, True)
sql("DROP SNAPSHOT dursnap", fetch=False)
sql("CHECKPOINT", fetch=False)
r = sql("SELECT count(*) FROM puddle_snapshots")
check("snapshot dropped after crash", r[0], [(0,)])
kill9(p)

print()
if FAILS:
    print(f"FAILED: {FAILS}")
    sys.exit(1)
print("ALL DURABILITY TESTS PASSED")
