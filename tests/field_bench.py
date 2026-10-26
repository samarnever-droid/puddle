#!/usr/bin/env python3
"""puddle vs the field: PostgreSQL, MariaDB, SQLite, MongoDB, Redis.

Same dataset and query shapes as tests/bench.py (the M8 methodology), so
puddle's numbers stay comparable with the recorded baselines. One engine at
a time: start -> load -> verify -> query battery -> index battery ->
concurrency probe -> stop -> cleanup.

Fairness rules (also disclosed in BENCHMARKS.md):
  - identical dataset (bench_big 200K rows, bench_dim 100 rows) and shapes
  - identical load pattern: 5000-row batches, one commit per batch
    (MongoDB: insert_many batches; Redis: pipelined HSET batches)
  - drivers: psycopg2 (C) for BOTH puddle and PostgreSQL -- the headline
    comparison shares client and wire protocol; mariadb connector (C) for
    MariaDB; stdlib sqlite3 (C) for SQLite; pymongo / redis-py (Python)
    for MongoDB / Redis
  - per-engine deviations (auth off, cache caps, WAL pragmas, Redis
    persistence off) are recorded in each engine's `note`
  - memory = sum of PSS over the server process tree (smaps_rollup,
    VmRSS fallback), sampled idle / after-load / peak-during-probe
  - puddle's own outputs are the verification reference: every other
    engine must return the same rows (float tolerance 1e-6 rel) for every
    shared shape

Usage: python3 tests/field_bench.py [engines...]
       engines: puddle postgres mariadb sqlite mongo redis (default: all)
"""
import json
import os
import random
import shutil
import socket
import statistics
import subprocess
import sys
import threading
import time

ROWS = 200_000
CHUNK = 5_000
PROBE_SECS = 10.0
PROBE_THREADS = 8
PG_BIN = "/usr/lib/postgresql/17/bin"
MONGO_BIN = "/home/user/mongo/bin/mongod"
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
PUDDLE_BIN = f"{ROOT}/target/release/puddle"

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
     "count(*) FROM bench_big GROUP BY 1 ORDER BY 2 DESC, 1 ASC LIMIT 3"),
]


def gen_row(i):
    return (i, i * 0.5, i % 100, f"row-{i % 997}")


DIM = [(g, f"group-{g}") for g in range(100)]


# ------------------------------------------------------------------ helpers --

def wait_port(port, timeout=60.0):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=0.5)
            s.close()
            return time.time() - t0
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"port {port} never came up")


def proc_tree_pids(pid):
    out = [pid]
    try:
        kids = subprocess.run(["pgrep", "-P", str(pid)],
                              capture_output=True, text=True)
        for k in kids.stdout.split():
            out += proc_tree_pids(int(k))
    except Exception:
        pass
    return out


def tree_pss_mb(pid):
    """Sum of PSS across the process tree -- honest physical memory."""
    total = 0.0
    for p in proc_tree_pids(pid):
        got = False
        try:
            with open(f"/proc/{p}/smaps_rollup") as f:
                for line in f:
                    if line.startswith("Pss:"):
                        total += int(line.split()[1]) / 1024.0
                        got = True
                        break
        except OSError:
            pass
        if not got:
            try:
                with open(f"/proc/{p}/status") as f:
                    for line in f:
                        if line.startswith("VmRSS:"):
                            total += int(line.split()[1]) / 1024.0
                            break
            except OSError:
                pass
    return round(total, 1)


def norm(rows):
    out = []
    for r in rows:
        out.append(tuple(round(v, 6) if isinstance(v, float) else v for v in r))
    return out


def rows_close(a, b):
    if len(a) != len(b):
        return False
    for ra, rb in zip(a, b):
        if len(ra) != len(rb):
            return False
        for va, vb in zip(ra, rb):
            if isinstance(va, (int, float)) and isinstance(vb, (int, float)):
                if abs(va - vb) > 1e-6 * max(1.0, abs(va), abs(vb)):
                    return False
            elif str(va) != str(vb):
                return False
    return True


# ------------------------------------------------------------------ engines --

class Engine:
    name = "?"
    note = ""
    can_analytics = True   # 7 SQL shapes
    can_range = True       # range / in-list probes

    def start(self):
        self.start_s = 0.0
        self.idle_mb = 0.0

    def stop(self):
        pass

    def load(self):
        raise NotImplementedError

    def create_index(self):
        raise NotImplementedError

    def timed_query(self, conn, sql):
        """One query on an existing connection; returns (ms, rows)."""
        raise NotImplementedError

    def point_lookup_ms(self, conn, k):
        raise NotImplementedError

    def open_probe_conn(self):
        raise NotImplementedError

    def close_conn(self, c):
        try:
            c.close()
        except Exception:
            pass


class PsycopgEngine(Engine):
    """puddle and PostgreSQL share the driver, the protocol and the SQL."""

    port = None

    def _conn(self):
        import psycopg2
        c = psycopg2.connect(self.dsn())
        c.autocommit = True
        return c

    def dsn(self):
        raise NotImplementedError

    def start(self):
        super().start()
        self.t0 = time.time()
        self.spawn()
        wait_port(self.port)
        self.start_s = time.time() - self.t0
        self.idle_mb = tree_pss_mb(self.rss_pid())

    def spawn(self):
        raise NotImplementedError

    def rss_pid(self):
        return self.proc.pid

    def load(self):
        c = self._conn()
        k = c.cursor()
        k.execute("DROP TABLE IF EXISTS bench_big")
        k.execute("DROP TABLE IF EXISTS bench_dim")
        k.execute("CREATE TABLE bench_big (a int, x float, g int, label text)")
        t0 = time.perf_counter()
        for base in range(0, ROWS, CHUNK):
            vals = ",".join(
                k.mogrify("(%s,%s,%s,%s)", gen_row(i)).decode()
                for i in range(base, min(base + CHUNK, ROWS)))
            k.execute(f"INSERT INTO bench_big VALUES {vals}")
        load_s = time.perf_counter() - t0
        k.execute("CREATE TABLE bench_dim (g int, gname text)")
        k.executemany("INSERT INTO bench_dim VALUES (%s, %s)", DIM)
        c.close()
        return load_s

    def create_index(self):
        c = self._conn()
        k = c.cursor()
        t0 = time.perf_counter()
        k.execute("CREATE INDEX bench_ix ON bench_big USING btree (a)")
        c.close()
        return (time.perf_counter() - t0) * 1000.0

    def timed_query(self, conn, sql):
        cur = conn.cursor()
        t0 = time.perf_counter()
        cur.execute(sql)
        rows = cur.fetchall()
        return (time.perf_counter() - t0) * 1000.0, rows

    def point_lookup_ms(self, conn, k):
        cur = conn.cursor()
        t0 = time.perf_counter()
        cur.execute("SELECT a, x, label FROM bench_big WHERE a = %s", (k,))
        cur.fetchall()
        return (time.perf_counter() - t0) * 1000.0

    def open_probe_conn(self):
        return self._conn()

    def point_noindex_ms(self, k):
        return self.point_lookup_ms(self._conn(), k)


class Puddle(PsycopgEngine):
    name = "puddle"
    note = ("default config, committed acks, vectorized executor, btree index; "
            "psycopg2 driver")
    port = 5550

    def dsn(self):
        return f"host=127.0.0.1 port={self.port} dbname=x user=u sslmode=disable"

    def spawn(self):
        shutil.rmtree("/tmp/fb-puddle", ignore_errors=True)
        os.makedirs("/tmp/fb-puddle", exist_ok=True)
        self.log = open("/tmp/fb-puddle.log", "w")
        self.proc = subprocess.Popen(
            [PUDDLE_BIN, "--port", str(self.port), "--data", "file:///tmp/fb-puddle"],
            stdout=self.log, stderr=subprocess.STDOUT)

    def stop(self):
        self.proc.kill()
        self.proc.wait()
        self.log.close()


class Postgres(PsycopgEngine):
    name = "postgresql"
    note = ("PostgreSQL 17, initdb defaults (shared_buffers 128MB), trust auth, "
            "tmpfs datadir; psycopg2 driver -- same client+protocol as puddle")
    port = 5551
    datadir = "/tmp/fb-pg"

    def dsn(self):
        return (f"host=127.0.0.1 port={self.port} dbname=bench "
                f"user={os.getenv('USER', 'user')} sslmode=disable")

    def spawn(self):
        shutil.rmtree(self.datadir, ignore_errors=True)
        subprocess.run([f"{PG_BIN}/initdb", "-D", self.datadir, "-A", "trust"],
                       capture_output=True, check=True)
        self.log = open("/tmp/fb-pg.log", "w")
        self.proc = subprocess.Popen(
            [f"{PG_BIN}/postgres", "-D", self.datadir, "-p", str(self.port),
             "-k", self.datadir],
            stdout=self.log, stderr=subprocess.STDOUT)
        wait_port(self.port)
        c = self._conn_admin()
        c.cursor().execute("CREATE DATABASE bench")
        c.close()

    def _conn_admin(self):
        import psycopg2
        c = psycopg2.connect(
            f"host=127.0.0.1 port={self.port} dbname=postgres "
            f"user={os.getenv('USER', 'user')} sslmode=disable")
        c.autocommit = True
        return c

    def start(self):
        # spawn() already waited for the port (needed for CREATE DATABASE);
        # re-measure so start_s covers exec..first-usable
        Engine.start(self)
        self.t0 = time.time()
        self.spawn()
        self.start_s = time.time() - self.t0
        self.idle_mb = tree_pss_mb(self.rss_pid())

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.log.close()
        shutil.rmtree(self.datadir, ignore_errors=True)


class MariaDB(Engine):
    name = "mariadb"
    note = ("MariaDB 11.8 (MySQL family), InnoDB, default buffer pool, "
            "skip-grant-tables (auth only), tmpfs datadir; mariadb C connector")
    port = 5552
    datadir = "/tmp/fb-maria"

    def start(self):
        Engine.start(self)
        shutil.rmtree(self.datadir, ignore_errors=True)
        os.makedirs(self.datadir, exist_ok=True)
        subprocess.run(["mariadb-install-db", "--datadir", self.datadir,
                        "--auth-root-authentication-method=normal"],
                       capture_output=True, check=True)
        self.log = open("/tmp/fb-maria.log", "w")
        self.t0 = time.time()
        self.proc = subprocess.Popen(
            ["/usr/sbin/mariadbd", "--datadir", self.datadir, "--port", str(self.port),
             "--socket", f"{self.datadir}/m.sock", "--skip-grant-tables",
             "--bind-address", "127.0.0.1", "--skip-log-bin",
             "--pid-file", f"{self.datadir}/mariadbd.pid"],
            stdout=self.log, stderr=subprocess.STDOUT)
        wait_port(self.port)
        self.start_s = time.time() - self.t0
        import mariadb
        c0 = mariadb.connect(host="127.0.0.1", port=self.port, user="root")
        c0.autocommit = True
        c0.cursor().execute("CREATE DATABASE IF NOT EXISTS bench")
        c0.close()
        self.idle_mb = tree_pss_mb(self.rss_pid())

    def rss_pid(self):
        return self.proc.pid

    def _conn(self):
        import mariadb
        c = mariadb.connect(host="127.0.0.1", port=self.port,
                            user="root", database="bench")
        c.autocommit = True
        return c

    def load(self):
        c = self._conn()
        k = c.cursor()
        k.execute("DROP TABLE IF EXISTS bench_big")
        k.execute("DROP TABLE IF EXISTS bench_dim")
        k.execute("CREATE TABLE bench_big (a INT, x DOUBLE, g INT, label VARCHAR(16))")
        t0 = time.perf_counter()
        for base in range(0, ROWS, CHUNK):
            k.executemany("INSERT INTO bench_big VALUES (?,?,?,?)",
                          [gen_row(i) for i in range(base, min(base + CHUNK, ROWS))])
        load_s = time.perf_counter() - t0
        k.execute("CREATE TABLE bench_dim (g INT, gname VARCHAR(16))")
        k.executemany("INSERT INTO bench_dim VALUES (?,?)", DIM)
        c.close()
        return load_s

    def create_index(self):
        c = self._conn()
        k = c.cursor()
        t0 = time.perf_counter()
        k.execute("CREATE INDEX bench_ix ON bench_big (a)")
        c.close()
        return (time.perf_counter() - t0) * 1000.0

    def timed_query(self, conn, sql):
        cur = conn.cursor()
        t0 = time.perf_counter()
        cur.execute(sql)
        rows = cur.fetchall()
        return (time.perf_counter() - t0) * 1000.0, rows

    def point_lookup_ms(self, conn, k):
        cur = conn.cursor()
        t0 = time.perf_counter()
        cur.execute("SELECT a, x, label FROM bench_big WHERE a = ?", (k,))
        cur.fetchall()
        return (time.perf_counter() - t0) * 1000.0

    def open_probe_conn(self):
        return self._conn()

    def point_noindex_ms(self, k):
        return self.point_lookup_ms(self._conn(), k)

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.log.close()
        shutil.rmtree(self.datadir, ignore_errors=True)


class Sqlite(Engine):
    name = "sqlite"
    note = ("SQLite (stdlib sqlite3), file DB on tmpfs, WAL + "
            "synchronous=NORMAL, in-process (RSS = python PSS delta); C driver")

    def start(self):
        Engine.start(self)
        shutil.rmtree("/tmp/fb-sqlite", ignore_errors=True)
        os.makedirs("/tmp/fb-sqlite", exist_ok=True)
        self.start_s = 0.0  # in-process: no server to boot
        import sqlite3
        self._sqlite3 = sqlite3
        self._base_pss = tree_pss_mb(os.getpid())
        self.idle_mb = 0.0  # in-process: report deltas only (see rss_mb)

    def _conn(self):
        c = self._sqlite3.connect("/tmp/fb-sqlite/bench.db", isolation_level=None)
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("PRAGMA synchronous=NORMAL")
        return c

    def rss_mb(self):
        return round(tree_pss_mb(os.getpid()) - self._base_pss, 1)

    def load(self):
        c = self._conn()
        c.execute("DROP TABLE IF EXISTS bench_big")
        c.execute("DROP TABLE IF EXISTS bench_dim")
        c.execute("CREATE TABLE bench_big (a INT, x REAL, g INT, label TEXT)")
        t0 = time.perf_counter()
        for base in range(0, ROWS, CHUNK):
            c.execute("BEGIN")
            c.executemany("INSERT INTO bench_big VALUES (?,?,?,?)",
                          [gen_row(i) for i in range(base, min(base + CHUNK, ROWS))])
            c.execute("COMMIT")
        load_s = time.perf_counter() - t0
        c.execute("CREATE TABLE bench_dim (g INT, gname TEXT)")
        c.executemany("INSERT INTO bench_dim VALUES (?,?)", DIM)
        c.close()
        return load_s

    def create_index(self):
        c = self._conn()
        t0 = time.perf_counter()
        c.execute("CREATE INDEX bench_ix ON bench_big (a)")
        c.close()
        return (time.perf_counter() - t0) * 1000.0

    def timed_query(self, conn, sql):
        t0 = time.perf_counter()
        rows = conn.execute(sql).fetchall()
        return (time.perf_counter() - t0) * 1000.0, rows

    def point_lookup_ms(self, conn, k):
        t0 = time.perf_counter()
        conn.execute("SELECT a, x, label FROM bench_big WHERE a = ?", (k,)).fetchall()
        return (time.perf_counter() - t0) * 1000.0

    def open_probe_conn(self):
        return self._conn()

    def point_noindex_ms(self, k):
        c = self._conn()
        dt = self.point_lookup_ms(c, k)
        c.close()
        return dt


class Mongo(Engine):
    name = "mongodb"
    note = ("MongoDB 8.0, WiredTiger cache capped 256MB (sandbox RAM), tmpfs "
            "dbpath, indexes on bench_big.a + bench_dim.g; pymongo driver")
    port = 5553
    datadir = "/tmp/fb-mongo"

    def start(self):
        Engine.start(self)
        shutil.rmtree(self.datadir, ignore_errors=True)
        os.makedirs(self.datadir, exist_ok=True)
        self.log = open("/tmp/fb-mongo.log", "w")
        self.t0 = time.time()
        self.proc = subprocess.Popen(
            [MONGO_BIN, "--port", str(self.port), "--dbpath", self.datadir,
             "--wiredTigerCacheSizeGB", "0.25", "--bind_ip", "127.0.0.1",
             "--setParameter", "diagnosticDataCollectionEnabled=false"],
            stdout=self.log, stderr=subprocess.STDOUT)
        wait_port(self.port)
        self.start_s = time.time() - self.t0
        self._cl = None
        self.idle_mb = tree_pss_mb(self.rss_pid())

    def rss_pid(self):
        return self.proc.pid

    def cl(self):
        if self._cl is None:
            import pymongo
            self._pymongo = pymongo
            self._cl = pymongo.MongoClient(
                f"mongodb://127.0.0.1:{self.port}/bench",
                serverSelectionTimeoutMS=10000)
        return self._cl

    def load(self):
        db = self.cl()["bench"]
        t0 = time.perf_counter()
        for base in range(0, ROWS, CHUNK):
            db.bench_big.insert_many(
                [dict(zip(("a", "x", "g", "label"), gen_row(i)))
                 for i in range(base, min(base + CHUNK, ROWS))], ordered=False)
        load_s = time.perf_counter() - t0
        db.bench_dim.insert_many([{"g": g, "gname": gn} for g, gn in DIM])
        return load_s

    def create_index(self):
        db = self.cl()["bench"]
        t0 = time.perf_counter()
        db.bench_big.create_index([("a", 1)])
        db.bench_dim.create_index([("g", 1)])
        return (time.perf_counter() - t0) * 1000.0

    def run_shape(self, tag):
        db = self.cl()["bench"]
        col, dim = db.bench_big, db.bench_dim
        half = ROWS // 2
        if tag == "scan+filter+count":
            return [(d["n"],) for d in col.aggregate(
                [{"$match": {"a": {"$gt": half}}}, {"$count": "n"}])]
        if tag == "group-by+aggs":
            cur = col.aggregate([
                {"$group": {"_id": "$g", "c": {"$sum": 1}, "av": {"$avg": "$x"},
                            "mn": {"$min": "$a"}, "mx": {"$max": "$a"}}},
                {"$sort": {"_id": 1}}, {"$limit": 5}])
            return [(d["_id"], d["c"], d["av"], d["mn"], d["mx"]) for d in cur]
        if tag == "scan+like+aggs":
            cur = col.aggregate([
                {"$match": {"label": {"$regex": "^row-1"}}},
                {"$group": {"_id": None, "c": {"$sum": 1}, "s": {"$sum": "$x"}}}])
            return [(d["c"], d["s"]) for d in cur]
        if tag == "equi-join+agg":
            cur = col.aggregate([
                {"$lookup": {"from": "bench_dim", "localField": "g",
                             "foreignField": "g", "as": "d"}},
                {"$unwind": "$d"},
                {"$group": {"_id": "$d.gname", "c": {"$sum": 1}, "s": {"$sum": "$x"}}},
                {"$sort": {"_id": 1}}, {"$limit": 5}])
            return [(d["_id"], d["c"], d["s"]) for d in cur]
        if tag == "order-by top-k":
            cur = col.find({}, {"_id": 0, "a": 1, "x": 1, "label": 1}) \
                      .sort("x", -1).limit(10)
            return [(d["a"], d["x"], d["label"]) for d in cur]
        if tag == "left-join anti":
            cur = col.aggregate([
                {"$lookup": {"from": "bench_dim", "localField": "g",
                             "foreignField": "g", "as": "d"}},
                {"$match": {"d": {"$size": 0}}},
                {"$count": "n"}])
            # $count emits no document when the count is zero
            return [(d["n"],) for d in cur] or [(0,)]
        if tag == "case+mod+group":
            cur = col.aggregate([
                {"$addFields": {"bucket": {"$switch": {"branches": [
                    {"case": {"$eq": [{"$mod": ["$a", 10]}, 0]}, "then": "ten"},
                    {"case": {"$eq": [{"$mod": ["$a", 5]}, 0]}, "then": "five"}],
                    "default": "other"}}}},
                {"$group": {"_id": "$bucket", "c": {"$sum": 1}}},
                {"$sort": {"c": -1, "_id": 1}}, {"$limit": 3}])
            return [(d["_id"], d["c"]) for d in cur]
        raise RuntimeError(tag)

    def timed_query(self, conn, sql):
        # unused for mongo (shape dispatch is tag-based)
        raise RuntimeError("mongo uses run_shape")

    def point_lookup_ms(self, conn, k):
        t0 = time.perf_counter()
        conn.bench.bench_big.find_one(
            {"a": k}, {"_id": 0, "a": 1, "x": 1, "label": 1})
        return (time.perf_counter() - t0) * 1000.0

    def open_probe_conn(self):
        import pymongo
        return pymongo.MongoClient(f"mongodb://127.0.0.1:{self.port}/bench")

    def point_noindex_ms(self, k):
        return self.point_lookup_ms(self.cl(), k)

    def range_ms(self, conn, k):
        t0 = time.perf_counter()
        n = conn.bench.bench_big.count_documents(
            {"a": {"$gte": k, "$lt": k + 1000}})
        return (time.perf_counter() - t0) * 1000.0, n

    def inlist_ms(self, conn, k):
        t0 = time.perf_counter()
        n = conn.bench.bench_big.count_documents(
            {"a": {"$in": [k + i * 7 for i in range(10)]}})
        return (time.perf_counter() - t0) * 1000.0, n

    def stop(self):
        if self._cl is not None:
            self._cl.close()
        self.proc.terminate()
        try:
            self.proc.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.log.close()
        shutil.rmtree(self.datadir, ignore_errors=True)


class Redis(Engine):
    name = "redis"
    note = ("Redis 8.0, in-memory, persistence OFF (--save '' --appendonly no); "
            "rows as hashes big:<a>; analytics/range N/A by data model; "
            "redis-py driver")
    can_analytics = False
    can_range = False

    def start(self):
        Engine.start(self)
        self.log = open("/tmp/fb-redis.log", "w")
        self.t0 = time.time()
        self.proc = subprocess.Popen(
            ["redis-server", "--port", "5554", "--save", "", "--appendonly", "no",
             "--bind", "127.0.0.1"],
            stdout=self.log, stderr=subprocess.STDOUT)
        wait_port(5554)
        self.start_s = time.time() - self.t0
        import redis as redispy
        self._redis = redispy
        r = self.r()
        r.ping()
        self.idle_mb = tree_pss_mb(self.rss_pid())

    def rss_pid(self):
        return self.proc.pid

    def r(self):
        return self._redis.Redis(host="127.0.0.1", port=5554)

    def load(self):
        r = self.r()
        t0 = time.perf_counter()
        for base in range(0, ROWS, CHUNK):
            p = r.pipeline(transaction=False)
            for i in range(base, min(base + CHUNK, ROWS)):
                _, x, g, label = gen_row(i)
                p.hset(f"big:{i}", mapping={"a": i, "x": x, "g": g, "label": label})
            p.execute()
        load_s = time.perf_counter() - t0
        assert r.dbsize() == ROWS, r.dbsize()
        r.close()
        return load_s

    def create_index(self):
        return 0.0  # key-addressed by design

    def timed_query(self, conn, sql):
        raise RuntimeError("redis is point-lookup only")

    def point_lookup_ms(self, conn, k):
        t0 = time.perf_counter()
        conn.hgetall(f"big:{k}")
        return (time.perf_counter() - t0) * 1000.0

    def open_probe_conn(self):
        return self.r()

    def point_noindex_ms(self, k):
        r = self.r()
        dt = self.point_lookup_ms(r, k)
        r.close()
        return dt

    def stop(self):
        self.proc.terminate()
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        self.log.close()


# ------------------------------------------------------------------- driver --

def bench_engine(eng, verify):
    print(f"\n=== {eng.name} ===")
    print(f"  note: {eng.note}")
    res = {"engine": eng.name, "note": eng.note}
    try:
        return _bench_engine_inner(eng, verify, res)
    finally:
        try:
            eng.stop()
        except Exception:
            pass


def _bench_engine_inner(eng, verify, res):
    eng.start()
    res["start_s"] = round(eng.start_s, 2)
    res["idle_mb"] = eng.idle_mb
    print(f"  start: {eng.start_s:.2f}s   idle RSS: {eng.idle_mb} MB")

    load_s = eng.load()
    res["load_s"] = round(load_s, 3)
    res["load_rps"] = int(ROWS / load_s)
    print(f"  load: {load_s:.2f}s ({int(ROWS / load_s):,} rows/s)")

    # point-select WITHOUT index (full scan), before the index exists
    rng = random.Random(4242)
    if not isinstance(eng, Redis):  # redis is key-addressed; no scan exists
        noindex = [eng.point_noindex_ms(rng.randrange(ROWS)) for _ in range(20)]
        res["point_noindex_ms"] = round(statistics.median(noindex), 3)
        print(f"  point (no index, median of 20): {res['point_noindex_ms']:.2f} ms")

    idx_ms = eng.create_index()
    res["index_build_ms"] = round(idx_ms, 1)
    print(f"  index build: {idx_ms:.0f} ms")

    loaded_mb = eng.rss_mb() if isinstance(eng, Sqlite) else tree_pss_mb(eng.rss_pid())
    res["loaded_mb"] = loaded_mb
    print(f"  RSS after load+index: {loaded_mb} MB")

    # ---- 7 query shapes (median of 5) ----
    res["ms"] = {}
    for tag, sql in QUERIES:
        if not eng.can_analytics:
            continue
        times, out = [], None
        for _ in range(5):
            if isinstance(eng, Mongo):
                t0 = time.perf_counter()
                out = eng.run_shape(tag)
                times.append((time.perf_counter() - t0) * 1000.0)
            else:
                c = eng.open_probe_conn()
                dt, out = eng.timed_query(c, sql.format(half=ROWS // 2))
                times.append(dt)
                eng.close_conn(c)
        res["ms"][tag] = round(statistics.median(times), 3)
        print(f"  {tag}: {res['ms'][tag]:8.2f} ms (median of 5)")

        if verify is not None:
            if tag not in verify:
                verify[tag] = norm(out)
            elif not rows_close(verify[tag], norm(out)):
                print(f"    !! MISMATCH vs puddle: {norm(out)[:2]} != {verify[tag][:2]}")
                res.setdefault("mismatches", []).append(tag)

    # ---- indexed probes (median of n) ----
    res["idx"] = {}
    k0 = rng.randrange(1000, ROWS - 2000)

    # point-select: one connection, many lookups
    c = eng.open_probe_conn()
    lats = [eng.point_lookup_ms(c, rng.randrange(ROWS)) for _ in range(200)]
    eng.close_conn(c)
    res["idx"]["point-select"] = round(statistics.median(lats), 3)

    if eng.can_range:
        # range-1000
        if isinstance(eng, Mongo):
            c = eng.open_probe_conn()
            lats = [eng.range_ms(c, k0)[0] for _ in range(50)]
            eng.close_conn(c)
        else:
            c = eng.open_probe_conn()
            lats = []
            for _ in range(50):
                dt, _rows = eng.timed_query(
                    c, f"SELECT count(*) FROM bench_big WHERE a BETWEEN {k0} "
                       f"AND {k0 + 999}")
                lats.append(dt)
            eng.close_conn(c)
        res["idx"]["range-1000"] = round(statistics.median(lats), 3)

        # in-list-10
        if isinstance(eng, Mongo):
            c = eng.open_probe_conn()
            lats = [eng.inlist_ms(c, k0)[0] for _ in range(50)]
            eng.close_conn(c)
        else:
            lst = ",".join(str(k0 + i * 7) for i in range(10))
            c = eng.open_probe_conn()
            lats = []
            for _ in range(50):
                dt, _rows = eng.timed_query(
                    c, f"SELECT count(*) FROM bench_big WHERE a IN ({lst})")
                lats.append(dt)
            eng.close_conn(c)
        res["idx"]["in-list-10"] = round(statistics.median(lats), 3)
    print("  indexed probes (median): " +
          "  ".join(f"{k}={v:.3f}ms" for k, v in res["idx"].items()))

    # ---- concurrency probe: 8 threads, indexed point selects, 10 s ----
    lat_lists = [[] for _ in range(PROBE_THREADS)]
    stop_flag = threading.Event()

    def worker(i):
        rngw = random.Random(1000 + i)
        conn = eng.open_probe_conn()
        local = lat_lists[i]
        while not stop_flag.is_set():
            local.append(eng.point_lookup_ms(conn, rngw.randrange(ROWS)))
        eng.close_conn(conn)

    threads = [threading.Thread(target=worker, args=(i,))
               for i in range(PROBE_THREADS)]
    t0 = time.time()
    for t in threads:
        t.start()
    time.sleep(PROBE_SECS)
    stop_flag.set()
    for t in threads:
        t.join()
    wall = time.time() - t0

    all_lats = sorted(x for l in lat_lists for x in l)
    res["qps"] = int(len(all_lats) / wall)
    res["p50_ms"] = round(all_lats[len(all_lats) // 2], 3)
    res["p99_ms"] = round(all_lats[int(len(all_lats) * 0.99)], 3)
    print(f"  {PROBE_THREADS}-thread point-select: {res['qps']:,} qps, "
          f"p50 {res['p50_ms']:.3f} ms, p99 {res['p99_ms']:.3f} ms")

    peak_mb = eng.rss_mb() if isinstance(eng, Sqlite) else tree_pss_mb(eng.rss_pid())
    res["peak_mb"] = peak_mb
    print(f"  peak RSS during probe: {peak_mb} MB")

    return res


def _ancestor_pids():
    pids = set()
    p = os.getpid()
    while p > 1:
        pids.add(p)
        try:
            with open(f"/proc/{p}/stat") as f:
                p = int(f.read().split()[3])
        except (OSError, ValueError):
            break
    return pids


def sweep_stale():
    """kill leftover servers from crashed runs so they cannot hold our
    ports or answer readiness probes. Never touches our own ancestors:
    pgrep -f matches the full cmdline, which includes this script and the
    shell that launched it."""
    protect = _ancestor_pids()
    for pat in ("mariadbd", "mongod", "redis-server", "postgres",
                "puddle --port 55"):
        try:
            out = subprocess.run(["pgrep", "-f", pat],
                                 capture_output=True, text=True)
            for pid in out.stdout.split():
                if int(pid) not in protect:
                    try:
                        os.kill(int(pid), 9)
                    except OSError:
                        pass
        except Exception:
            pass
    time.sleep(0.3)


def main():
    which = sys.argv[1:] or ["puddle", "postgresql", "mariadb", "sqlite",
                             "mongo", "redis"]
    sweep_stale()
    makers = {"puddle": Puddle, "postgresql": Postgres, "mariadb": MariaDB,
              "sqlite": Sqlite, "mongo": Mongo, "redis": Redis}
    print(f"field bench: {ROWS:,} rows, load batches of {CHUNK:,}, "
          f"{PROBE_THREADS} threads x {PROBE_SECS:.0f}s point-select probe")

    verify = {}
    results = []
    for name in which:
        if name not in makers:
            sys.exit(f"unknown engine {name}")
        r = bench_engine(makers[name](), verify)
        results.append(r)

    out = os.path.join(ROOT, "field-bench-results.json")
    with open(out, "w") as f:
        json.dump(results, f, indent=1)
    print(f"\nwrote {out}")
    if verify:
        print(f"cross-engine verification: {len(verify)} shapes compared against puddle")


if __name__ == "__main__":
    main()
