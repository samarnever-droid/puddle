#!/usr/bin/env python3
"""puddle M2+M3 regression battery (psycopg2). Run against a live server."""
import sys
import datetime
import psycopg2

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 5433
FAILS = []
N = [0]

def conn(autocommit=True):
    c = psycopg2.connect(f"host=127.0.0.1 port={PORT} dbname=x user=u")
    c.autocommit = autocommit
    return c

def q(sql, params=None, cur=None):
    own = cur is None
    if own:
        c = conn()
        cur = c.cursor()
    cur.execute(sql, params)
    r = cur.fetchall() if cur.description else None
    if own:
        c.close()
    return r

def check(name, got, want):
    N[0] += 1
    ok = got == want
    if not ok:
        FAILS.append(name)
        print(f"  [FAIL] {name}: {got!r} != {want!r}")
    return ok

def expect_err(name, sql, code):
    N[0] += 1
    try:
        q(sql)
        FAILS.append(name)
        print(f"  [FAIL] {name}: no error raised")
    except psycopg2.Error as e:
        ok = e.pgcode == code
        if not ok:
            FAILS.append(name)
            print(f"  [FAIL] {name}: pgcode {e.pgcode} != {code} ({e})")

# ---- setup
q("DROP TABLE IF EXISTS lakes")
q("DROP TABLE IF EXISTS readings")
q("""CREATE TABLE lakes (name text NOT NULL, depth int, area float, country text)""")
q("""INSERT INTO lakes VALUES
    ('Baikal', 1642, 31500, 'RU'), ('Tanganyika', 1470, 32900, 'TZ'),
    ('Crater Lake', 594, 53, 'US'), ('Loch Ness', 230, 56, 'GB'),
    ('Vostok', 500, 12500, 'AQ'), ('Caspian Sea', 1025, 371000, 'KZ')""")
q("""CREATE TABLE readings (sensor int, temp float, note text)""")
q("""INSERT INTO readings VALUES (1, 4.5, 'cold'), (2, 12.25, 'mild'), (3, NULL, 'n/a')""")

# ---- basic select / types / 3VL
check("select all", q("SELECT count(*) FROM lakes"), [(6,)])
check("int avg", q("SELECT avg(depth) FROM lakes"), [(5461/6,)])
check("3VL null eq", q("SELECT count(*) FROM readings WHERE temp = NULL"), [(0,)])
check("3VL null is", q("SELECT sensor FROM readings WHERE temp IS NULL"), [(3,)])
check("is not null", q("SELECT count(*) FROM readings WHERE note IS NOT NULL"), [(3,)])
check("bool ops", q("SELECT 1 WHERE 1 < 2 AND NOT (3 = 4)"), [(1,)])
check("float text", q("SELECT temp::text FROM readings WHERE sensor = 2"), [("12.25",)])
check("distinct", sorted(q("SELECT DISTINCT country FROM lakes")[0] and q("SELECT DISTINCT country FROM lakes")), sorted([('AQ',), ('GB',), ('KZ',), ('RU',), ('TZ',), ('US',)]))

# ---- aggregates / group by / having
check("group by", q("SELECT country, count(*), max(depth) FROM lakes GROUP BY country ORDER BY country"),
      [('AQ', 1, 500), ('GB', 1, 230), ('KZ', 1, 1025), ('RU', 1, 1642), ('TZ', 1, 1470), ('US', 1, 594)])
check("having", q("SELECT country, sum(area) FROM lakes GROUP BY country HAVING sum(area) > 50000 ORDER BY country"),
      [('KZ', 371000.0)])
check("count distinct", q("SELECT count(DISTINCT country) FROM lakes"), [(6,)])
check("agg no rows", q("SELECT count(*), sum(depth) FROM lakes WHERE name = 'nope'"), [(0, None)])

# ---- joins
check("inner join", q("""SELECT l.name, r.temp FROM lakes l JOIN readings r ON l.depth > r.temp
                          ORDER BY l.name, r.temp LIMIT 3"""),
      [('Baikal', 4.5), ('Baikal', 12.25), ('Caspian Sea', 4.5)])
check("left join nulls", q("""SELECT r.sensor, l.name FROM readings r LEFT JOIN lakes l ON false
                               ORDER BY r.sensor"""), [(1, None), (2, None), (3, None)])
check("join on eq", q("""SELECT count(*) FROM lakes a JOIN lakes b ON a.country = b.country"""), [(6,)])

# ---- expressions
check("case", q("SELECT name, CASE WHEN depth > 1000 THEN 'deep' WHEN depth > 400 THEN 'mid' ELSE 'shallow' END FROM lakes ORDER BY name LIMIT 3"),
      [('Baikal', 'deep'), ('Caspian Sea', 'deep'), ('Crater Lake', 'mid')])
check("like", q("SELECT count(*) FROM lakes WHERE name LIKE '%a%'"), [(4,)])
check("in", q("SELECT name FROM lakes WHERE country IN ('RU', 'KZ') ORDER BY name"), [('Baikal',), ('Caspian Sea',)])
check("between", q("SELECT count(*) FROM lakes WHERE depth BETWEEN 500 AND 1100"), [(3,)])
check("funcs", q("SELECT upper(name), length(name), abs(-5), round(2.567, 2) FROM lakes WHERE name = 'Vostok'"),
      [('VOSTOK', 6, 5, 2.57)])
check("coalesce", q("SELECT coalesce(temp, -99.0) FROM readings ORDER BY sensor"), [(4.5,), (12.25,), (-99.0,)])

# ---- order by variants / limit offset
check("order alias", q("SELECT name AS n, depth d FROM lakes ORDER BY d DESC LIMIT 2"),
      [('Baikal', 1642), ('Tanganyika', 1470)])
check("order position", q("SELECT name, depth FROM lakes ORDER BY 2 LIMIT 1"), [('Loch Ness', 230)])
check("order nulls last", q("SELECT sensor FROM readings ORDER BY temp NULLS LAST"), [(1,), (2,), (3,)])
check("order nulls first", q("SELECT sensor FROM readings ORDER BY temp NULLS FIRST"), [(3,), (1,), (2,)])
check("order desc default nulls first", q("SELECT sensor FROM readings ORDER BY temp DESC"), [(3,), (2,), (1,)])
check("order asc default nulls last", q("SELECT sensor FROM readings ORDER BY temp"), [(1,), (2,), (3,)])
check("order desc nulls last", q("SELECT sensor FROM readings ORDER BY temp DESC NULLS LAST"), [(2,), (1,), (3,)])
check("limit offset", q("SELECT name FROM lakes ORDER BY name LIMIT 2 OFFSET 2"), [('Crater Lake',), ('Loch Ness',)])

# ---- update / delete / returning
check("update returning", q("UPDATE lakes SET depth = depth + 1 WHERE country = 'RU' RETURNING name, depth"),
      [('Baikal', 1643)])
check("update applied", q("SELECT depth FROM lakes WHERE name = 'Baikal'"), [(1643,)])
check("delete returning", q("DELETE FROM readings WHERE sensor = 2 RETURNING sensor, note"), [(2, 'mild')])
check("delete applied", q("SELECT count(*) FROM readings"), [(2,)])

# ---- errors
expect_err("undefined table", "SELECT * FROM nope", "42P01")
expect_err("undefined column", "SELECT wat FROM lakes", "42703")
expect_err("syntax", "SELEC 1", "42601")
expect_err("dup table", "CREATE TABLE lakes (a int)", "42P07")
expect_err("not null", "INSERT INTO lakes (name) VALUES (NULL)", "23502")
expect_err("missing col not null", "INSERT INTO lakes (depth) VALUES (1)", "23502")
expect_err("bad col", "INSERT INTO lakes (wat) VALUES (1)", "42703")
check("col-list insert", q("INSERT INTO readings (sensor, note) VALUES (9, 'partial')", None) or
      q("SELECT sensor, temp, note FROM readings WHERE sensor = 9"), [(9, None, 'partial')])
check("col-list reorder", q("INSERT INTO readings (note, sensor) VALUES ('reordered', 10)", None) or
      q("SELECT sensor, note FROM readings WHERE sensor = 10"), [(10, 'reordered')])
expect_err("div zero", "SELECT 1/0", "22012")
expect_err("ambig col", "SELECT name FROM lakes a JOIN lakes b ON a.country = b.country", "42702")

# ---- transactions
c1 = conn(autocommit=False); cur1 = c1.cursor()
cur1.execute("INSERT INTO readings VALUES (7, 1.5, 't1')")
check("txn sees own write", q("SELECT count(*) FROM readings", cur=cur1), [(5,)])
c1.commit(); c1.close()
check("committed visible", q("SELECT count(*) FROM readings"), [(5,)])
c2 = conn(autocommit=False); cur2 = c2.cursor()
cur2.execute("INSERT INTO readings VALUES (8, 2.5, 'rb')")
c2.rollback(); c2.close()
check("rolled back gone", q("SELECT count(*) FROM readings"), [(5,)])

# serialization conflict: two writers, second commit must 40001
ca = conn(autocommit=False); cb = conn(autocommit=False)
a, b = ca.cursor(), cb.cursor()
a.execute("UPDATE lakes SET depth = 1 WHERE name = 'Vostok'")
b.execute("UPDATE lakes SET depth = 2 WHERE name = 'Vostok'")
ca.commit()
N[0] += 1
try:
    cb.commit()
    FAILS.append("serialization"); print("  [FAIL] serialization: second commit succeeded")
except psycopg2.Error as e:
    if e.pgcode != "40001":
        FAILS.append("serialization"); print(f"  [FAIL] serialization: {e.pgcode} != 40001")
ca.close(); cb.close()
check("winner value", q("SELECT depth FROM lakes WHERE name = 'Vostok'"), [(1,)])

# aborted txn: 25P02 until COMMIT/ROLLBACK
c3 = conn(autocommit=False); k = c3.cursor()
try:
    k.execute("SELECT 1/0")
except psycopg2.Error:
    pass
N[0] += 1
try:
    k.execute("SELECT 1")
    FAILS.append("25P02"); print("  [FAIL] 25P02: statement ran in aborted txn")
except psycopg2.Error as e:
    if e.pgcode != "25P02":
        FAILS.append("25P02"); print(f"  [FAIL] 25P02: {e.pgcode}")
c3.rollback()
check("recovered after rollback", q("SELECT 1", cur=c3.cursor()), [(1,)])
c3.close()

# protocol-level prepared statements: psycopg2 auto-prepares after 5 runs
c4 = conn(); k4 = c4.cursor()
for i in range(7):
    k4.execute("SELECT %s::int + %s", (i, 100))
    r = k4.fetchall()
    check(f"prepared round {i}", r, [(100 + i,)])

# ------------------------------------------------------------------ temporal --
print("\n-- temporal (v1.2) --")

# literals
check("date literal", q("SELECT DATE '2026-10-24'::text"), [("2026-10-24",)])
check("date literal str", q("SELECT DATE '2026-10-24'::text"), [("2026-10-24",)])
check("time literal", q("SELECT TIME '17:30:45'::text"), [("17:30:45",)])
check("time frac", q("SELECT TIME '04:05:06.123456'::text"), [("04:05:06.123456",)])
check("timestamp literal", q("SELECT TIMESTAMP '2026-10-24 17:30:45'::text"),
      [("2026-10-24 17:30:45",)])
check("timestamp T form", q("SELECT TIMESTAMP '2026-10-24T08:09:10'::text"),
      [("2026-10-24 08:09:10",)])
check("timestamp date-only", q("SELECT TIMESTAMP '2026-10-24'::text"),
      [("2026-10-24 00:00:00",)])
check("interval literal", q("SELECT INTERVAL '1 day 2 hours'::text"), [("1 day 02:00:00",)])
check("interval months", q("SELECT INTERVAL '2 years 3 mons'::text"), [("2 years 3 mons",)])
check("interval neg", q("SELECT INTERVAL '-2 days'::text"), [("-2 days",)])
check("interval time form", q("SELECT INTERVAL '04:05:06'::text"), [("04:05:06",)])
check("interval fractional", q("SELECT INTERVAL '1.5 hours'::text"), [("01:30:00",)])
check("interval empty", q("SELECT INTERVAL ''::text"), [("00:00:00",)])
expect_err("bad date", "SELECT DATE '2026-02-30'", "42601")
expect_err("bad time", "SELECT TIME '25:00:00'", "42601")
expect_err("bad interval", "SELECT INTERVAL '1 fortnight'", "42601")

# comparisons
check("date < date", q("SELECT DATE '2026-02-28' < DATE '2026-03-01'"), [(True,)])
check("ts <= ts", q("SELECT TIMESTAMP '2026-01-01' <= TIMESTAMP '2026-01-01'"), [(True,)])
check("date vs ts promotion",
      q("SELECT DATE '2026-10-25' > TIMESTAMP '2026-10-24 23:59:59'"), [(True,)])
check("interval compare", q("SELECT INTERVAL '2 hours' > INTERVAL '30 minutes'"), [(True,)])
check("text literal vs ts",
      q("SELECT TIMESTAMP '2026-10-24 17:30:00' = '2026-10-24 17:30:00'"), [(True,)])
check("between dates",
      q("SELECT DATE '2026-06-15' BETWEEN DATE '2026-01-01' AND DATE '2026-12-31'"),
      [(True,)])

# arithmetic
check("date - date", q("SELECT DATE '2026-03-01' - DATE '2026-01-31'"), [(29,)])
check("date + int", q("SELECT (DATE '2026-02-28' + 1)::text"), [("2026-03-01",)])
check("int + date", q("SELECT (1 + DATE '2026-02-28')::text"), [("2026-03-01",)])
check("date - int", q("SELECT (DATE '2026-03-01' - 1)::text"), [("2026-02-28",)])
check("month clamp (jan 31)", q("SELECT (TIMESTAMP '2026-01-31 12:00:00' + INTERVAL '1 month')::text"),
      [("2026-02-28 12:00:00",)])
check("leap clamp", q("SELECT (TIMESTAMP '2024-01-31' + INTERVAL '1 month')::text"),
      [("2024-02-29 00:00:00",)])
check("ts + interval", q("SELECT (TIMESTAMP '2026-10-24 10:00:00' + INTERVAL '90 minutes')::text"),
      [("2026-10-24 11:30:00",)])
check("ts - interval", q("SELECT (TIMESTAMP '2026-10-24 10:00:00' - INTERVAL '1 day 2 hours')::text"),
      [("2026-10-23 08:00:00",)])
check("ts - ts", q("SELECT (TIMESTAMP '2026-10-24 17:30:00' - TIMESTAMP '2026-10-20 09:00:00')::text"),
      [("4 days 08:30:00",)])
check("ts - ts timedelta", q("SELECT TIMESTAMP '2026-10-24 17:30:00' - TIMESTAMP '2026-10-20 09:00:00'"),
      [(datetime.timedelta(days=4, seconds=30600),)])
check("time - time", q("SELECT (TIME '18:00:00' - TIME '15:30:00')::text"), [("02:30:00",)])
check("time + interval wraps", q("SELECT (TIME '23:00:00' + INTERVAL '2 hours')::text"),
      [("01:00:00",)])
check("interval + interval", q("SELECT (INTERVAL '1 day 2 hours' + INTERVAL '30 minutes')::text"),
      [("1 day 02:30:00",)])
# PG subtracts intervals component-wise: 2d - (1d 12h) = 1 day -12:00:00
check("interval - interval", q("SELECT (INTERVAL '2 days' - INTERVAL '1 day 12 hours')::text"),
      [("1 day -12:00:00",)])
check("neg interval", q("SELECT (- INTERVAL '1 day 2 hours')::text"), [("-1 days -02:00:00",)])
check("interval + month",
      q("SELECT (TIMESTAMP '2026-12-15' + INTERVAL '2 months')::text"), [("2027-02-15 00:00:00",)])

# casts
check("cast text date", q("SELECT '2026-10-24'::date::text"), [("2026-10-24",)])
check("cast ts date", q("SELECT TIMESTAMP '2026-10-24 17:30:00'::date::text"), [("2026-10-24",)])
check("cast ts time", q("SELECT TIMESTAMP '2026-10-24 17:30:00'::time::text"), [("17:30:00",)])
check("cast date ts", q("SELECT DATE '2026-10-24'::timestamp::text"),
      [("2026-10-24 00:00:00",)])
check("cast interval text", q("SELECT INTERVAL '1 day'::text"), [("1 day",)])
expect_err("cast bad date text", "SELECT 'not-a-date'::date", "22P02")

# functions
check("extract year", q("SELECT extract(year from TIMESTAMP '2026-10-24 17:30:00')"), [(2026.0,)])
check("extract second frac",
      q("SELECT extract(second from TIMESTAMP '2026-10-24 17:30:45.5')"), [(45.5,)])
check("extract dow", q("SELECT extract(dow from DATE '2026-10-24')"), [(6.0,)])
check("extract doy", q("SELECT extract(doy from DATE '2024-03-01')"), [(61.0,)])
check("extract epoch",
      q("SELECT extract(epoch from TIMESTAMP '1970-01-02 00:00:01')"), [(86401.0,)])
check("extract from interval", q("SELECT extract(day from INTERVAL '3 days 4 hours')"), [(3.0,)])
check("date_part alias", q("SELECT date_part('year', DATE '2026-10-24')"), [(2026.0,)])
check("date_trunc month",
      q("SELECT date_trunc('month', TIMESTAMP '2026-10-24 17:30:45')::text"),
      [("2026-10-01 00:00:00",)])
check("date_trunc week",
      q("SELECT date_trunc('week', TIMESTAMP '2026-10-24 17:30:45')::text"),
      [("2026-10-19 00:00:00",)])
check("date_trunc minute",
      q("SELECT date_trunc('minute', TIMESTAMP '2026-10-24 17:30:45')::text"),
      [("2026-10-24 17:30:00",)])
check("to_char basic", q("SELECT to_char(TIMESTAMP '2026-10-24 17:30:45', 'YYYY-MM-DD')"),
      [("2026-10-24",)])
check("to_char names", q("SELECT to_char(DATE '2026-10-24', 'Day, Mon DD YYYY')"),
      [("Saturday, Oct 24 2026",)])
check("to_char time", q("SELECT to_char(TIME '17:30:45', 'HH12:MI AM')"), [("05:30 PM",)])
check("to_char us", q("SELECT to_char(TIMESTAMP '2026-01-02 03:04:05.123456', 'HH24:MI:SS.US')"),
      [("03:04:05.123456",)])
check("age 2-arg", q("SELECT age(TIMESTAMP '2026-03-01', TIMESTAMP '2026-01-31')::text"),
      [("1 mon 1 day",)])
check("age 2-arg neg", q("SELECT age(TIMESTAMP '2026-01-31', TIMESTAMP '2026-03-01')::text"),
      [("-1 mons -1 days",)])
check("now sane", q("SELECT now() > TIMESTAMP '2026-01-01'"), [(True,)])
check("current_date type", q("SELECT current_date >= DATE '2026-10-26'"), [(True,)])
check("current_time type", q("SELECT current_time BETWEEN TIME '00:00:00' AND TIME '23:59:59'"),
      [(True,)])

# storage: table roundtrip, index, COPY, durability
q("DROP TABLE IF EXISTS tcal")
q("CREATE TABLE tcal (id int, d date, t time, ts timestamp, iv interval)")
for i in range(1, 9):
    q("INSERT INTO tcal VALUES (%s, %s::date, %s::time, %s::timestamp, %s::interval)",
      (i, f"2026-10-0{i}", f"0{i}:00:00", f"2026-10-0{i} 10:30:00", f"{i} days"))
check("temporal table roundtrip", q(
      "SELECT id, d::text, t::text, ts::text, iv::text FROM tcal WHERE id = 3"),
      [(3, "2026-10-03", "03:00:00", "2026-10-03 10:30:00", "3 days")])
check("temporal native types", q("SELECT d, t, ts, iv FROM tcal WHERE id = 3"),
      [(datetime.date(2026, 10, 3), datetime.time(3, 0),
        datetime.datetime(2026, 10, 3, 10, 30), datetime.timedelta(days=3))])
check("temporal where", q("SELECT count(*) FROM tcal WHERE ts >= TIMESTAMP '2026-10-05'"), [(4,)])
check("temporal min max", q("SELECT min(d)::text, max(d)::text FROM tcal"),
      [("2026-10-01", "2026-10-08")])
check("temporal order by", q("SELECT d::text FROM tcal ORDER BY d DESC LIMIT 2"),
      [("2026-10-08",), ("2026-10-07",)])
check("temporal group by", q("SELECT to_char(d, 'Mon'), count(*) FROM tcal GROUP BY 1"),
      [("Oct", 8)])
check("sum interval", q("SELECT sum(iv)::text FROM tcal"), [("36 days",)])
check("temporal in list",
      q("SELECT count(*) FROM tcal WHERE d IN (DATE '2026-10-02', DATE '2026-10-04')"), [(2,)])
q("CREATE INDEX tcal_ix ON tcal USING btree (ts)")
check("temporal index point",
      q("SELECT id FROM tcal WHERE ts = TIMESTAMP '2026-10-05 10:30:00'"), [(5,)])
check("temporal index range",
      q("SELECT count(*) FROM tcal WHERE ts >= TIMESTAMP '2026-10-06'"), [(3,)])
q("DROP TABLE IF EXISTS tcal2")
q("CREATE TABLE tcal2 (id int, d date, t time, ts timestamp, iv interval)")
import io
buf = io.StringIO()
cur5 = conn().cursor()
cur5.copy_expert("COPY tcal TO STDOUT", buf)
cur5.copy_expert("COPY tcal2 FROM STDIN", io.StringIO(buf.getvalue()))
check("temporal COPY roundtrip", q(
      "SELECT count(*), min(d)::text, max(iv)::text FROM tcal2"),
      [(8, "2026-10-01", "8 days")])

# ---------------------------------------------------------------- snapshots --
print("\n-- snapshots + AS OF (v2.0) --")

q("DROP TABLE IF EXISTS tt")
q("CREATE TABLE tt (a int, b text, d date)")
q("INSERT INTO tt VALUES (1,'one', DATE '2026-01-01'), (2,'two', DATE '2026-02-01'), (3,'three', DATE '2026-03-01')")
q("CREATE SNAPSHOT snap1")
q("INSERT INTO tt VALUES (4,'four', DATE '2026-04-01'), (5,'five', DATE '2026-05-01')")
q("UPDATE tt SET b = 'TWO!' WHERE a = 2")
q("DELETE FROM tt WHERE a = 1")

check("asof sees snapshot state",
      q("SELECT a, b FROM tt AS OF SNAPSHOT snap1 ORDER BY a"),
      [(1, "one"), (2, "two"), (3, "three")])
check("asof where/order", q("SELECT a FROM tt AS OF SNAPSHOT snap1 WHERE a >= 2 ORDER BY a DESC"),
      [(3,), (2,)])
check("asof agg", q("SELECT count(*), sum(a) FROM tt AS OF SNAPSHOT snap1"), [(3, 6)])
check("asof join with live",
      q("SELECT l.a, h.b FROM tt l JOIN tt AS OF SNAPSHOT snap1 h ON l.a = h.a ORDER BY l.a"),
      [(2, "two"), (3, "three")])
check("asof temporal col", q("SELECT d::text FROM tt AS OF SNAPSHOT snap1 ORDER BY a"),
      [("2026-01-01",), ("2026-02-01",), ("2026-03-01",)])
check("asof group by", q("SELECT extract(month from d), count(*) FROM tt AS OF SNAPSHOT snap1 GROUP BY 1 ORDER BY 1"),
      [(1.0, 1), (2.0, 1), (3.0, 1)])
check("asof alias", q("SELECT h.b FROM tt AS OF SNAPSHOT snap1 AS h WHERE h.a = 2"),
      [("two",)])
lsn_snap = q("SELECT lsn FROM puddle_snapshots WHERE name = 'snap1'")[0][0]
lsn_now = q("SELECT current_lsn()")[0][0]
check("current_lsn positive int", isinstance(lsn_now, int) and lsn_now > 0, True)
check("snap lsn listed", isinstance(lsn_snap, int) and 0 < lsn_snap <= lsn_now, True)
check("asof lsn boundary",
      q(f"SELECT a, b FROM tt AS OF LSN {lsn_snap} ORDER BY a"),
      [(1, "one"), (2, "two"), (3, "three")])
check("asof lsn latest", len(q(f"SELECT a FROM tt AS OF LSN {lsn_now}")), 4)
# the listed lsn is the last INCLUDED txn: AS OF it == the snapshot
check("asof count at snap", q(f"SELECT count(*) FROM tt AS OF LSN {lsn_snap}"), [(3,)])
# a snapshot pins every table registered at creation time (whole-db LSN)
nreg = q("SELECT count(*) FROM pg_tables WHERE schemaname = 'public'")[0][0]
check("snapshot listing row",
      q("SELECT name, pinned_tables FROM puddle_snapshots WHERE name = 'snap1'"),
      [("snap1", nreg)])
check("snapshot created_at type",
      q("SELECT created_at < now() FROM puddle_snapshots WHERE name = 'snap1'"), [(True,)])

# retention: checkpoints must never break time travel
q("CHECKPOINT")
check("asof survives checkpoint",
      q("SELECT a, b FROM tt AS OF SNAPSHOT snap1 ORDER BY a"),
      [(1, "one"), (2, "two"), (3, "three")])
q("INSERT INTO tt VALUES (6,'six', DATE '2026-06-01')")
q("CHECKPOINT")
check("asof survives 2nd checkpoint",
      q("SELECT count(*) FROM tt AS OF SNAPSHOT snap1"), [(3,)])
check("asof lsn survives checkpoints",
      q(f"SELECT count(*) FROM tt AS OF LSN {lsn_snap}"), [(3,)])

# rollback isolation: uncommitted writes never reach any snapshot
c6 = conn(autocommit=False); k6 = c6.cursor()
k6.execute("INSERT INTO tt VALUES (99,'rolled', DATE '2026-09-09')")
k6.execute("SELECT count(*) FROM tt AS OF SNAPSHOT snap1")
check("in-txn asof pre-commit", k6.fetchall(), [(3,)])
c6.rollback()
check("rollback leaves live intact", q("SELECT count(*) FROM tt"), [(5,)])
q("CREATE SNAPSHOT snap2")
check("rollback not in later snapshot", q("SELECT count(*), sum(a) FROM tt AS OF SNAPSHOT snap2"),
      [(5, 20)])
try:
    k6 = c6.cursor(); k6.execute("CREATE SNAPSHOT in_txn"); c6.rollback()
    check("create snapshot in txn refused", "no error", "25001")
except psycopg2.Error as e:
    check("create snapshot in txn refused", e.pgcode, "25001")
    c6.rollback()
c6.close()

# drop + recreate: snapshots see the historical incarnation
q("DROP TABLE tt")
expect_err("dropped table live read", "SELECT * FROM tt", "42P01")
check("asof reads dropped table", q("SELECT count(*), sum(a) FROM tt AS OF SNAPSHOT snap1"),
      [(3, 6)])
check("asof2 reads dropped table", q("SELECT count(*), sum(a) FROM tt AS OF SNAPSHOT snap2"),
      [(5, 20)])
q("CREATE TABLE tt (a int, b text, d date)")
q("INSERT INTO tt VALUES (42,'reborn', DATE '2027-01-01')")
check("reborn live", q("SELECT a, b FROM tt"), [(42, "reborn")])
check("asof sees pre-drop incarnation",
      q("SELECT a, b FROM tt AS OF SNAPSHOT snap1 ORDER BY a"),
      [(1, "one"), (2, "two"), (3, "three")])
check("new table not at old snapshot",
      q("SELECT count(*) FROM tt AS OF SNAPSHOT snap1 WHERE a = 42"), [(0,)])

# error cases
expect_err("unknown snapshot", "SELECT * FROM tt AS OF SNAPSHOT nope", "42704")
expect_err("duplicate snapshot", "CREATE SNAPSHOT snap1", "42710")
expect_err("future lsn", f"SELECT * FROM tt AS OF LSN {lsn_now + 100}", "22023")
expect_err("asof on insert", "INSERT INTO tt AS OF SNAPSHOT snap1 VALUES (1,'x', DATE '2026-01-01')", "42601")
expect_err("asof on delete", "DELETE FROM tt AS OF SNAPSHOT snap1", "42601")
expect_err("asof on srf", "SELECT * FROM generate_series(1,3) AS OF SNAPSHOT snap1", "0A000")
expect_err("asof garbage", "SELECT * FROM tt AS OF WHATEVER 1", "42601")

# drop snapshot semantics
q("DROP SNAPSHOT IF EXISTS never_existed")
expect_err("drop missing snapshot", "DROP SNAPSHOT never_existed", "42704")
q("DROP SNAPSHOT snap1")
q("CHECKPOINT")
expect_err("dropped snapshot unreadable", "SELECT * FROM tt AS OF SNAPSHOT snap1", "42704")
check("other snapshot unaffected", q("SELECT count(*) FROM tt AS OF SNAPSHOT snap2"), [(5,)])
q("DROP SNAPSHOT snap2")
q("CHECKPOINT")
expect_err("all dropped unreadable", "SELECT * FROM tt AS OF SNAPSHOT snap2", "42704")
check("listing empty", q("SELECT count(*) FROM puddle_snapshots"), [(0,)])
q("DROP TABLE IF EXISTS tt")

print(f"\n{N[0] - len(FAILS)}/{N[0]} passed")
if FAILS:
    print("FAILED:", FAILS)
    sys.exit(1)
print("REGRESSION BATTERY PASSED")
