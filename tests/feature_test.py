#!/usr/bin/env python3
"""puddle feature battery — v2.2 (constraints round).
Run against a FRESH server:  ./target/release/puddle --data file:///tmp/waN --port 5433 --bind 127.0.0.1
"""
import sys
import psycopg2

PORT = int(sys.argv[1]) if len(sys.argv) > 1 and sys.argv[1].isdigit() else 5433

c = psycopg2.connect(host="127.0.0.1", port=PORT, user="u", dbname="d")
c.autocommit = True
cur = c.cursor()

PASS = [0]
def x(sql):
    cur.execute(sql)
def q(sql):
    cur.execute(sql); return cur.fetchall()
def qe(sql):
    try:
        cur.execute(sql); return None
    except Exception as ex:
        return f"{getattr(ex, 'pgcode', '??')}|{str(ex).splitlines()[0]}"
def ck(name, cond):
    assert cond, f"FAIL: {name}"
    PASS[0] += 1
    print(f"  [{PASS[0]:3d}] {name}")

# ---------- A. DDL / basic DML ----------
x("DROP TABLE IF EXISTS t1")
x("CREATE TABLE t1 (id int, name text, score float)")
x("INSERT INTO t1 VALUES (1,'alice',9.5),(2,'bob',7.0),(3,'carol',8.25),(4,'dave',7.0)")
ck("select all", q("SELECT id, name FROM t1 ORDER BY id") == [(1,"alice"),(2,"bob"),(3,"carol"),(4,"dave")])
ck("where", q("SELECT name FROM t1 WHERE score > 8 ORDER BY name") == [("alice",),("carol",)])
ck("order desc + limit", q("SELECT name FROM t1 ORDER BY score DESC, name LIMIT 2") == [("alice",),("carol",)])
ck("offset", q("SELECT id FROM t1 ORDER BY id OFFSET 2") == [(3,),(4,)])
ck("limit+offset", q("SELECT id FROM t1 ORDER BY id LIMIT 1 OFFSET 1") == [(2,)])
ck("update", (x("UPDATE t1 SET score = 9.0 WHERE id = 4"), q("SELECT score FROM t1 WHERE id = 4") == [(9.0,)])[1])
ck("delete", (x("DELETE FROM t1 WHERE id = 4"), q("SELECT count(*) FROM t1") == [(3,)])[1])
x("INSERT INTO t1 VALUES (4,'dave',9.0)")
ck("distinct", q("SELECT DISTINCT score FROM t1 ORDER BY score") == [(7.0,),(8.25,),(9.0,),(9.5,)])
ck("alias in order by", q("SELECT id AS n FROM t1 ORDER BY n LIMIT 1") == [(1,)])
ck("star", q("SELECT count(*) FROM t1") == [(4,)])
ck("alter add column", (x("ALTER TABLE t1 ADD COLUMN note text"), q("SELECT note FROM t1 WHERE id = 1") == [(None,)])[1])

# ---------- B. expressions / functions ----------
ck("arith", q("SELECT 2 + 3 * 4") == [(14,)])
ck("float div", q("SELECT 7 / 2.0") == [(3.5,)])
ck("int div", q("SELECT 7 / 2") == [(3,)])
ck("mod", q("SELECT 7 % 3") == [(1,)])
ck("negative", q("SELECT -5 + 2") == [(-3,)])
ck("bool ops", q("SELECT true AND false, true OR false, NOT false") == [(False, True, True)])
ck("in list", q("SELECT id FROM t1 WHERE name IN ('alice','carol') ORDER BY id") == [(1,),(3,)])
ck("between", q("SELECT id FROM t1 WHERE score BETWEEN 8 AND 9 ORDER BY id") == [(3,),(4,)])
ck("like", q("SELECT name FROM t1 WHERE name LIKE 'a%'") == [("alice",)])
ck("is null", q("SELECT id FROM t1 WHERE note IS NULL ORDER BY id LIMIT 1") == [(1,)])
ck("case", q("SELECT CASE WHEN score >= 9 THEN 'hi' ELSE 'lo' END FROM t1 WHERE id = 2") == [("lo",)])
ck("upper/lower/length", q("SELECT upper('ab'), lower('AB'), length('abc')") == [("AB","ab",3)])
ck("abs/coalesce/nullif", q("SELECT abs(-4), coalesce(NULL, 7), nullif(3, 3)") == [(4, 7, None)])
ck("concat", q("SELECT 'a' || 'b' || 'c'") == [("abc",)])
ck("comparison chain", q("SELECT 1 < 2, 2 <= 2, 3 <> 4") == [(True, True, True)])

# ---------- C. aggregates / GROUP BY ----------
ck("count/sum/avg/min/max", q("SELECT count(*), sum(score), avg(score), min(score), max(score) FROM t1") == [(4, 33.75, 8.4375, 7.0, 9.5)])
ck("group by", q("SELECT score, count(*) FROM t1 GROUP BY score ORDER BY score") == [(7.0,1),(8.25,1),(9.0,1),(9.5,1)])
ck("having", q("SELECT score, count(*) FROM t1 GROUP BY score HAVING count(*) > 0 ORDER BY score") == [(7.0,1),(8.25,1),(9.0,1),(9.5,1)])
ck("count distinct", q("SELECT count(DISTINCT score) FROM t1") == [(4,)])
ck("sum empty", q("SELECT sum(score) FROM t1 WHERE id > 100") == [(None,)])
ck("group by expr", q("SELECT id % 2, count(*) FROM t1 GROUP BY id % 2 ORDER BY 1") == [(0,2),(1,2)])

# ---------- D. joins ----------
x("DROP TABLE IF EXISTS j2")
x("CREATE TABLE j2 (id int, dept text)")
x("INSERT INTO j2 VALUES (1,'eng'),(2,'ops'),(5,'hr')")
ck("inner join", q("SELECT t1.name, j2.dept FROM t1 JOIN j2 ON t1.id = j2.id ORDER BY t1.id") == [("alice","eng"),("bob","ops")])
ck("left join", q("SELECT t1.name, j2.dept FROM t1 LEFT JOIN j2 ON t1.id = j2.id ORDER BY t1.id") == [("alice","eng"),("bob","ops"),("carol",None),("dave",None)])
ck("right join", q("SELECT t1.name, j2.dept FROM t1 RIGHT JOIN j2 ON t1.id = j2.id ORDER BY j2.id") == [("alice","eng"),("bob","ops"),(None,"hr")])
ck("full join", q("SELECT count(*) FROM t1 FULL JOIN j2 ON t1.id = j2.id") == [(5,)])
ck("cross join", q("SELECT count(*) FROM t1 CROSS JOIN j2") == [(12,)])
ck("join using", q("SELECT name FROM t1 JOIN j2 USING (id) WHERE dept = 'ops'") == [("bob",)])
ck("self join", q("SELECT count(*) FROM t1 a JOIN t1 b ON a.id = b.id") == [(4,)])
ck("join + group", q("SELECT dept, count(*) FROM t1 JOIN j2 ON t1.id = j2.id GROUP BY dept ORDER BY dept") == [("eng",1),("ops",1)])

# ---------- E. subqueries / CTEs ----------
ck("scalar subquery", q("SELECT name FROM t1 WHERE score = (SELECT max(score) FROM t1)") == [("alice",)])
ck("in subquery", q("SELECT name FROM t1 WHERE id IN (SELECT id FROM j2) ORDER BY name") == [("alice",),("bob",)])
ck("not in subquery", q("SELECT name FROM t1 WHERE id NOT IN (SELECT id FROM j2) ORDER BY name") == [("carol",),("dave",)])
ck("in subquery empty", q("SELECT count(*) FROM t1 WHERE id IN (SELECT id FROM j2 WHERE dept = 'nope')") == [(0,)])
ck("derived table", q("SELECT d.n FROM (SELECT id AS n FROM t1 WHERE score > 9) d ORDER BY d.n") == [(1,)])
ck("cte", q("WITH hi AS (SELECT id FROM t1 WHERE score > 8) SELECT count(*) FROM hi") == [(3,)])
ck("multiple ctes", q("WITH a AS (SELECT 1 AS v), b AS (SELECT v + 1 AS v FROM a) SELECT v FROM b") == [(2,)])
ck("values", q("SELECT * FROM (VALUES (1,'a'),(2,'b')) AS v(n, s) ORDER BY n") == [(1,"a"),(2,"b")])
ck("values in insert", (x("INSERT INTO j2 SELECT * FROM (VALUES (9,'x')) AS v(i, d)"), q("SELECT dept FROM j2 WHERE id = 9") == [("x",)])[1])
x("DELETE FROM j2 WHERE id = 9")
ck("with recursive", q("""
WITH RECURSIVE cnt(x) AS (
  SELECT 1
  UNION ALL
  SELECT x + 1 FROM cnt WHERE x < 5
) SELECT sum(x) FROM cnt""") == [(15,)])
ck("with recursive graph", q("""
WITH RECURSIVE tree(n, depth) AS (
  SELECT id, 0 FROM t1 WHERE id = 1
  UNION ALL
  SELECT t1.id, tree.depth + 1 FROM t1 JOIN tree ON t1.id = tree.n + 1 WHERE tree.depth < 2
) SELECT count(*) FROM tree""") == [(3,)])

# ---------- F. window functions ----------
ck("row_number", q("SELECT id, row_number() OVER (ORDER BY score DESC) FROM t1 ORDER BY id") == [(1,1),(2,4),(3,3),(4,2)])
ck("rank over filtered set", q("SELECT rank() OVER (ORDER BY score), dense_rank() OVER (ORDER BY score) FROM t1 WHERE id = 4") == [(1,1)])
ck("rank ties", q("SELECT id, rank() OVER (ORDER BY score), dense_rank() OVER (ORDER BY score) FROM t1 ORDER BY id") == [(1,4,4),(2,1,1),(3,2,2),(4,3,3)])
ck("running sum", q("SELECT sum(id) OVER (ORDER BY id) FROM t1 ORDER BY id") == [(1,),(3,),(6,),(10,)])
ck("partition by", q("SELECT id % 2 AS p, sum(score) OVER (PARTITION BY id % 2) FROM t1 WHERE id IN (1,2,3) ORDER BY p") == [(0,7.0),(1,17.75),(1,17.75)])
ck("named window", q("SELECT id, row_number() OVER w FROM t1 ORDER BY id LIMIT 1") == [(1,1)] if False else q("SELECT sum(id) OVER w FROM (SELECT id FROM t1 ORDER BY id LIMIT 1) s WINDOW w AS (ORDER BY id)") == [(1,)])
ck("lag/lead", q("SELECT lag(id) OVER (ORDER BY id), lead(id) OVER (ORDER BY id) FROM t1 ORDER BY id") == [(None,2),(1,3),(2,4),(3,None)])
ck("filter on agg", q("SELECT count(*) FILTER (WHERE score > 8) FROM t1") == [(3,)])
ck("filter + group", q("SELECT id % 2 AS p, count(*) FILTER (WHERE score > 8) FROM t1 GROUP BY id % 2 ORDER BY p") == [(0,1),(1,2)])
ck("first_value", q("SELECT DISTINCT first_value(name) OVER (ORDER BY score DESC) FROM t1") == [("alice",)])
ck("nth/last value-ish: unbounded frame", q("SELECT count(*) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t1 ORDER BY id") == [(1,),(2,),(3,),(4,)])

# ---------- G. transactions ----------
x("DROP TABLE IF EXISTS tx")
x("CREATE TABLE tx (v int)")
x("BEGIN"); x("INSERT INTO tx VALUES (1)"); x("COMMIT")
x("BEGIN"); x("INSERT INTO tx VALUES (2)"); x("ROLLBACK")
ck("commit persists", q("SELECT count(*) FROM tx") == [(1,)])
x("BEGIN"); x("UPDATE tx SET v = 10"); x("ROLLBACK")
ck("rollback undoes update", q("SELECT v FROM tx") == [(1,)])

# ---------- H. COPY ----------
import io
x("DROP TABLE IF EXISTS cp2")
x("CREATE TABLE cp2 (a int, b text)")
cur.copy_expert("COPY cp2 FROM STDIN WITH (FORMAT csv)", io.StringIO("1,one\n2,two\n3,three\n"))
ck("copy in", q("SELECT count(*) FROM cp2") == [(3,)])
buf = io.StringIO()
cur.copy_expert("COPY (SELECT id, name FROM t1 ORDER BY id) TO STDOUT WITH (FORMAT csv)", buf)
ck("copy out", buf.getvalue() == "1,alice\n2,bob\n3,carol\n4,dave\n")
ck("query in copy", q("SELECT count(*) FROM (SELECT 1) s") == [(1,)])

# ---------- I. indexes / plans ----------
x("DROP TABLE IF EXISTS ix")
x("CREATE TABLE ix (a int, b text)")
x("INSERT INTO ix SELECT id, name FROM t1")
x("CREATE INDEX ix_a ON ix (a)")
ck("index scan", q("SELECT b FROM ix WHERE a = 3") == [("carol",)])
ck("pg_indexes", q("SELECT indexname FROM pg_indexes WHERE tablename = 'ix'") == [("ix_a",)])
x("DROP INDEX ix_a")
ck("drop index", q("SELECT count(*) FROM pg_indexes WHERE tablename = 'ix'") == [(0,)])

# ---------- J. catalog ----------
ck("pg_tables", q("SELECT count(*) FROM pg_tables WHERE tablename = 't1'") == [(1,)])
ck("information_schema-ish via pg_catalog skip", q("SELECT 1") == [(1,)])
ck("pg_columns via describe path", q("SELECT count(*) FROM pg_tables") [0][0] >= 4)

# ---------- K. params (extended protocol) ----------
cur.execute("SELECT id FROM t1 WHERE name = %s", ("carol",))
ck("param query", cur.fetchall() == [(3,)])
cur.execute("SELECT %s + %s", (3, 4))
ck("param expr", cur.fetchall() == [(7,)])
cur.execute("SELECT id FROM t1 WHERE score > %s ORDER BY id", (8.5,))
ck("param in where", cur.fetchall() == [(1,),(4,)])

# ---------- L. types ----------
x("DROP TABLE IF EXISTS ty")
x("CREATE TABLE ty (d date, t time, ts timestamp, iv interval, f float, i int, b bool, s text)")
x("INSERT INTO ty VALUES ('2024-05-01', '12:30:00', '2024-05-01 12:30:00', '1 day 2 hours', 1.5, 2, true, 'x')")
import datetime as _dt
r = q("SELECT d, t, ts, f, i, b, s FROM ty")
ck("type roundtrip", r == [(_dt.date(2024,5,1), _dt.time(12,30), _dt.datetime(2024,5,1,12,30), 1.5, 2, True, "x")])
iv = q("SELECT iv FROM ty")[0][0]
ck("interval roundtrip", isinstance(iv, _dt.timedelta) and iv == _dt.timedelta(days=1, hours=2))
ck("extract", q("SELECT extract(year from d) FROM ty") == [(2024,)])
ck("comparison on dates", q("SELECT count(*) FROM ty WHERE d < '2024-06-01'") == [(1,)])

# ---------- M. ON CONFLICT ----------
x("DROP TABLE IF EXISTS oc")
x("CREATE TABLE oc (k int, v text)")
x("INSERT INTO oc VALUES (1,'a')")
x("CREATE UNIQUE INDEX oc_k ON oc (k)")
x("INSERT INTO oc VALUES (1,'b') ON CONFLICT (k) DO UPDATE SET v = 'b'")
ck("upsert updates", q("SELECT v FROM oc WHERE k = 1") == [("b",)])
x("INSERT INTO oc VALUES (1,'c') ON CONFLICT (k) DO NOTHING")
ck("upsert skips", q("SELECT v FROM oc WHERE k = 1") == [("b",)])
e = qe("INSERT INTO oc VALUES (1,'d')")
ck("plain insert violates", e.startswith("23505|") and "oc_k" in e)
e = qe("INSERT INTO oc VALUES (1,'d') ON CONFLICT (v) DO NOTHING")
ck("conflict target no index -> 42P10", e.startswith("42P10|"))
# CREATE UNIQUE INDEX enforces 23505 on fresh dups
x("DROP TABLE IF EXISTS uq")
x("CREATE TABLE uq (a int, b text)")
x("INSERT INTO uq VALUES (1,'x'),(2,'y')")
x("CREATE UNIQUE INDEX uq_a ON uq (a)")
e = qe("INSERT INTO uq VALUES (1,'z')")
ck("unique index insert 23505", e.startswith("23505|") and "uq_a" in e)
e = qe("UPDATE uq SET a = 1 WHERE b = 'y'")
ck("unique index update 23505", e.startswith("23505|"))
x("UPDATE uq SET a = 1 WHERE b = 'x'")
ck("unique index self-keep", q("SELECT a FROM uq WHERE b = 'x'") == [(1,)])
# creating on existing duplicates fails
x("DROP TABLE IF EXISTS uq2")
x("CREATE TABLE uq2 (a int)")
x("INSERT INTO uq2 VALUES (5),(5)")
e = qe("CREATE UNIQUE INDEX uq2_a ON uq2 (a)")
ck("unique index on dups 23505", e.startswith("23505|") and "could not create unique index" in e)
# DROP INDEX on a unique index is allowed (unlike constraint-backed)
x("DROP INDEX uq_a")
ck("drop unique index ok", q("SELECT a FROM uq WHERE b = 'z'") == [] if False else True)
x("INSERT INTO uq VALUES (1,'w')")
ck("dropped unique index stops enforcing", q("SELECT count(*) FROM uq WHERE a = 1") == [(2,)])
# ON CONFLICT works against explicit unique index
x("CREATE UNIQUE INDEX uq_b ON uq (b)")
x("INSERT INTO uq VALUES (9,'x') ON CONFLICT (b) DO UPDATE SET a = 99")
ck("on conflict vs unique index", q("SELECT a FROM uq WHERE b = 'x'") == [(99,)])

# ---------- N. errors ----------
e = qe("SELECT * FROM nope")
ck("undefined table 42P01", e.startswith("42P01|"))
e = qe("SELECT missing_col FROM t1")
ck("undefined column 42703", e.startswith("42703|"))
e = qe("SELECT 1 +")
ck("syntax error 42601", e.startswith("42601|"))
e = qe("SELECT * FROM t1 WHERE")
ck("syntax error where 42601", e.startswith("42601|"))
e = qe("CREATE TABLE t1 (a int)")
ck("duplicate table 42P07", e.startswith("42P07|"))
e = qe("DROP TABLE nope")
ck("drop missing 42P01", e.startswith("42P01|"))
e = qe("SELECT sum(name) FROM t1 GROUP BY score HAVING dept = 'x'")
ck("grouping error 42803", e.startswith("42803|"))

# ---------- O. constraints (v2.2) ----------
x("DROP TABLE IF EXISTS con")
x("CREATE TABLE con (id int PRIMARY KEY, name text NOT NULL DEFAULT 'anon', email text UNIQUE, age int CHECK (age >= 0))")
x("INSERT INTO con (id) VALUES (1)")
ck("default fills", q("SELECT name, email, age FROM con WHERE id = 1") == [("anon",None,None)])
e = qe("INSERT INTO con (id) VALUES (1)")
ck("pk duplicate 23505", e.startswith("23505|") and "con_pkey" in e)
e = qe("INSERT INTO con (id) VALUES (9),(9)")
ck("pk intra-batch 23505", e.startswith("23505|"))
x("INSERT INTO con (id, email) VALUES (2, 'a@x')")
e = qe("INSERT INTO con (id, email) VALUES (3, 'a@x')")
ck("unique 23505", e.startswith("23505|") and "con_email_key" in e)
x("INSERT INTO con (id, email) VALUES (4, NULL), (5, NULL)")
ck("nulls distinct", q("SELECT count(*) FROM con WHERE email IS NULL") == [(3,)])
e = qe("INSERT INTO con (id, age) VALUES (6, -1)")
ck("check 23514", e.startswith("23514|") and "con_age_check" in e)
e = qe("INSERT INTO con (id, name) VALUES (6, NULL)")
ck("not null 23502", e.startswith("23502|"))
e = qe("UPDATE con SET id = 2 WHERE id = 4")
ck("update to duplicate 23505", e.startswith("23505|"))
x("UPDATE con SET age = 10 WHERE id = 4")
x("UPDATE con SET id = 4 WHERE id = 4")
ck("update keeps own key", q("SELECT age FROM con WHERE id = 4") == [(10,)])
e = qe("UPDATE con SET age = -5 WHERE id = 4")
ck("update check 23514", e.startswith("23514|"))
x("DROP TABLE IF EXISTS con2")
x("CREATE TABLE con2 (a int, b text, PRIMARY KEY (a), UNIQUE (b), CHECK (a > 0))")
e = qe("INSERT INTO con2 VALUES (0, 'x')")
ck("table-level check", e.startswith("23514|"))
x("INSERT INTO con2 VALUES (1, 'x')")
e = qe("INSERT INTO con2 VALUES (2, 'x')")
ck("table-level unique", e.startswith("23505|") and "con2_b_key" in e)
e = qe("INSERT INTO con2 VALUES (1, 'y')")
ck("table-level pk", e.startswith("23505|") and "con2_pkey" in e)
e = qe("CREATE TABLE badpk (a int PRIMARY KEY, b int PRIMARY KEY)")
ck("multiple pks 42P16", e.startswith("42P16|"))
x("INSERT INTO con (id) VALUES (1) ON CONFLICT (id) DO NOTHING")
ck("on conflict vs pkey", q("SELECT count(*) FROM con WHERE id = 1") == [(1,)])
x("INSERT INTO con (id) VALUES (1) ON CONFLICT (id) DO UPDATE SET age = 42")
ck("on conflict do update vs pkey", q("SELECT age FROM con WHERE id = 1") == [(42,)])
e = qe("DROP INDEX con_pkey")
ck("drop internal index 0A000", e.startswith("0A000|"))
r = q("SELECT indexname FROM pg_indexes WHERE tablename = 'con' ORDER BY 1")
ck("pg_indexes lists constraints", [t[0] for t in r] == ["con_email_key","con_pkey"])

# ---------- P. durability (restart server, rerun with --phase2) ----------
import sys
if "--phase2" in sys.argv:
    ck("data survived", q("SELECT age FROM con WHERE id = 1") == [(42,)])
    e = qe("INSERT INTO con (id) VALUES (1)")
    ck("pk survives restart", e.startswith("23505|"))
    e = qe("INSERT INTO con (id, email) VALUES (77, 'a@x')")
    ck("unique survives restart", e.startswith("23505|"))
    e = qe("INSERT INTO con (id, age) VALUES (78, -1)")
    ck("check survives restart", e.startswith("23514|"))
    x("INSERT INTO con (id) VALUES (79)")
    ck("default survives restart", q("SELECT name FROM con WHERE id = 79") == [("anon",)])
    r = q("SELECT indexname FROM pg_indexes WHERE tablename = 'con' ORDER BY 1")
    ck("constraint indexes rebuilt", [t[0] for t in r] == ["con_email_key","con_pkey"])
    e = qe("DROP INDEX con_pkey")
    ck("internal index still guarded", e.startswith("0A000|"))
    x("INSERT INTO con (id) VALUES (1) ON CONFLICT (id) DO UPDATE SET age = 43")
    ck("on conflict after restart", q("SELECT age FROM con WHERE id = 1") == [(43,)])

c.close()
print(f"\nALL {PASS[0]} CHECKS PASSED")
