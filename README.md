# puddle

**A lightweight lakehouse that speaks PostgreSQL and lives in an S3 bucket.**

puddle is a single ~7 MB Rust binary that implements the PostgreSQL wire
protocol (v3) end-to-end — real libpq clients (psql, psycopg2, JDBC, pgx…)
just work — and keeps all of its state in either a local directory or an
S3-compatible bucket. Idle RSS is ~1.8 MB. "Lightweight" is a RAM contract,
not a toy label: bounded memory, durable by default, loud about damage.

```
$ puddle --data s3://my-bucket/warehouse --endpoint https://s3.amazonaws.com \
         --access-key AKIA... --secret-key ... --tls --auth scram-sha-256
$ psql -h localhost -p 5433 -U alice warehouse
```

## Quick start

```
cargo build --release
./target/release/puddle --data file:///tmp/puddle --port 5433
psql "host=127.0.0.1 port=5433 dbname=x user=u"        # trust mode
```

S3 mode (the bucket is the source of truth; puddle itself is stateless —
kill it, restart it, point a second one at the prefix after the first dies):

```
./target/release/puddle --data s3://bucket/prefix \
    --endpoint http://127.0.0.1:9000 --region us-east-1 \
    --access-key KEY --secret-key SECRET
```

TLS + SCRAM (dual-mode port: TLS and cleartext on 5433, or require TLS):

```
./target/release/puddle --data file:///tmp/puddle \
    --tls --auth scram-sha-256 --auth-file users.json
# --tls-cert/--tls-key for your PEM; --tls-required to reject cleartext
# --scram-secret PASS prints a PG-format SCRAM verifier for the file
```

## What it speaks

* **Wire**: PostgreSQL v3 — simple and extended protocol, named
  statements/portals, parameters (`$1`), TEXT result formats, SSLRequest
  upgrade. Verified against real libpq.
* **Auth**: `trust`, cleartext `password`, and full **SCRAM-SHA-256**
  (RFC 5802 — hand-rolled PBKDF2/proofs, gs2 `n,,`/`y,,`, channel-binding
  safe); unknown users get a dummy exchange so failures are
  indistinguishable. TLS via rustls, self-signed or your cert.
* **SQL**: `SELECT` with INNER/LEFT JOIN, GROUP BY + HAVING, aggregates
  (count/sum/avg/min/max, DISTINCT), ORDER BY + NULLS FIRST/LAST,
  LIMIT/OFFSET, CASE, LIKE, IN, BETWEEN, IS NULL, CAST, scalar functions,
  correlated arithmetic, scalar subqueries (uncorrelated), `UNION [ALL]`,
  `generate_series` in FROM — plus `INSERT`/`UPDATE`/`DELETE` (with
  RETURNING), `CREATE/DROP TABLE`, `CREATE/DROP INDEX`,
  `ALTER TABLE ... ADD COLUMN` (crash-safe, NULL-pads old segments),
  `BEGIN/COMMIT/ROLLBACK` (snapshot isolation, OCC, 40001 on conflict),
  `CHECKPOINT`.
* **Temporal (v1.2)**: native `DATE`, `TIME`, `TIMESTAMP`, `INTERVAL`
  types — real PG oids (1082/1083/1114/1186) so psycopg2/psql decode
  them as Python `date`/`time`/`datetime`/`timedelta` natively. Typed
  literals (`DATE '2026-10-24'`), `CURRENT_DATE`/`CURRENT_TIME`/
  `CURRENT_TIMESTAMP`, `EXTRACT`/`date_part`, `date_trunc`, `to_char`,
  `age`, temporal arithmetic with PG's month-end clamping
  (Jan 31 + 1 mon → Feb 28), `time + interval` day-wrap, component-wise
  interval arithmetic with PG's exact display conventions, `sum(interval)`,
  casts, comparisons, ORDER BY/GROUP BY/indexes over temporal columns —
  all matching real PostgreSQL 17 output (verified side-by-side).
* **Snapshots + time travel (v2.0)**: `CREATE SNAPSHOT name` pins an
  LSN-addressed, immutable view of the whole database in one meta.json CAS
  (Neon-style: the snapshot is just a pointer; data is never copied).
  `SELECT ... FROM t AS OF SNAPSHOT name` or `AS OF LSN n` reconstructs
  any table's state — pinned manifest + parts, then exact WAL replay up to
  the target LSN. Snapshots survive kill -9, see through DROP TABLE and
  drop/recreate cycles (incarnation ids), and stay readable across
  CHECKPOINTs: GC is snapshot-aware (a segment dies only when no snapshot
  needs it as a replay base). `DROP SNAPSHOT` reclaims. `current_lsn()`,
  the `puddle_snapshots` catalog table, and restore-by-query
  (`INSERT INTO t SELECT * FROM t AS OF SNAPSHOT s`) round it out.
* **COPY**: `COPY ... FROM/TO STDIN/STDOUT`, text and CSV formats,
  column lists, `DELIMITER`/`NULL`/`QUOTE`/`HEADER` options,
  `COPY (query) TO STDOUT`, client-side `\copy` in psql, psycopg2
  `copy_expert`. Wire-driven (CopyInResponse/CopyData/CopyDone), one
  atomic WAL batch per COPY — kill -9 mid-COPY is all-or-nothing.
* **Catalog**: a synthesized `pg_catalog` + `information_schema` (driven
  off real table metadata, fnv-hashed oids, empty stubs where a feature
  doesn't exist) so **psql `\l`, `\dt`, `\d` render real output** without
  any stored system tables. `current_user`/`current_schema`/
  `current_database` special registers included.
* **Indexes**: btree (ranges, ORDER BY keys) and hash (equality, IN lists);
  the planner uses them as a *prefilter only* — an index can never change
  a query's results.

## Performance (200K rows, single core of a dev sandbox)

| shape | time | vs reference executor |
|---|---|---|
| scan + filter + count | 10.1 ms | 10.7× |
| group-by + aggs | 28.9 ms | 6.7× |
| equi-join + agg | 59.7 ms | 64.3× |
| left-join anti | 57.1 ms | 73.3× |
| point select (btree) | 0.06 ms | 134× vs no index |
| range-1000 (btree) | 0.13 ms | 83× |
| IN-list-10 (btree) | 0.09 ms | 496× |
| **8-thread point selects** | **49,000 q/s** | p50 0.11 ms, p99 0.85 ms |

Load rate 161K rows/s. Full run: `python3 tests/bench.py`.

## Memory contract

Idle **1.4 MB** (1.6 MB with TLS). 100 live connections: 4.2 MB. 200K rows
+ 2 indexes: 59.5 MB. 200-connection churn growth: <0.05 MB. Enforced by
`tests/rss_test.py`, not just claimed.

## Durability & safety model

* One transaction = one immutable WAL segment object, CRC32C-framed.
  **An acked commit is on the storage before the client hears "COMMIT"**
  (`--ack committed`, the default; `--ack relaxed` trades this for speed).
* Checkpoints materialize columnar parts (dict/RLE/LZ4-encoded, zone
  stats) with create-once manifests; the WAL up to the checkpoint is then
  GC'd. Kill -9 at any point: acked high-water always recovers, a torn
  segment is one unacked transaction discarded *whole*.
* **Corruption is loud, never silent**: bad CRC / truncated part / lost
  manifest / missing catalog ⇒ the server refuses to start, exactly. A
  damaged latest WAL segment ⇒ exact consistent prefix, never half a
  transaction. Verified by 15 corruption cases and 20-round kill-loops.
* Single writer per prefix: two processes writing one prefix collide on WAL
  seq and the next recovery *refuses* rather than silently dropping either.

## No stubs, ever

Enforced by the compiler, not by convention: `Cargo.toml` carries a
`[lints.clippy]` block that **forbids** `todo!` / `unimplemented!` /
`dbg!` / `unreachable!` — a stub cannot build (`cargo clippy` fails with
exit 101). The gate is:

```
cargo clippy --release --all-targets -- -D warnings   # zero warnings
cargo test --release                                  # 18 unit tests
```

Semantically: anything unsupported returns a SQLSTATE error (`0A000`
feature-not-supported, `22P02` bad input, …) — the server never panics on
user input and never silently returns placeholder data. (The empty
`pg_constraint`-style catalog relations are *correct*, not stubs: with no
constraints defined, real PostgreSQL also returns zero rows there.)

## The test suite (M7: "harder than Silicon tests")

15 suites, all green: 58-check SQL regression, 53-check **psql battery**
(real `psql` binary: `\l`/`\dt`/`\d`, server- and client-side COPY
round-trips, ALTER + kill -9 restart, psycopg2 `copy_expert`, kill -9
*mid-COPY*), 156-check differential
(vectorized vs reference executor, errors included), seeded differential
**fuzzer** (type-chaos + typed generators, 7,500+ queries), 20-round
**kill -9 crash-loop** under 6 concurrent writers, **corruption injection**
(bit-flip/truncate/garbage/delete in every on-disk structure), 12-thread
**soak** with invariants on every concurrent read and 65K OCC retries,
**RSS ceilings**, **hostile-socket** resilience, S3 mode against a test
server that independently verifies SigV4 on every request, and a
**7-layer hostile fuzzer** (directed kill-probes, startup/message/extended-
protocol/SCRAM corruption, grammar generation, session-replay mutation —
hard oracle: process alive, framing valid, RSS ceilings, unguessable
canary byte-exact; ddmin minimizer on any violation). The suite found
9 real bugs before they shipped (see ARCHITECTURE.md). A cross-engine
benchmark — puddle vs PostgreSQL, MariaDB, SQLite, MongoDB and Redis on
identical workloads with results verified identical before timing —
lives in [BENCHMARKS.md](BENCHMARKS.md).

## CLI

```
puddle [--bind IP] [--port N] --data file:///DIR | s3://BUCKET/PREFIX
       [--endpoint URL] [--region R] [--access-key K] [--secret-key S]
       [--ack committed|relaxed] [--vectorized on|off]
       [--auth trust|password|scram-sha-256] [--auth-file FILE]
       [--scram-secret PASS]           # print a verifier, exit
       [--tls] [--tls-cert PEM] [--tls-key PEM] [--tls-required]
```

## Limitations (honest list)

* Single writer per data prefix (no active-active; violations refuse loudly).
* Binary parameter format is rejected (`0A000`); binary *results* work.
* COPY uses the simple protocol only (extended-protocol COPY → `0A000`),
  and COPY inside an explicit transaction is `0A000` in v1.1.
* Scalar subqueries are uncorrelated-only (a correlated reference fails
  with a clean `42703`, never a silent wrong answer).
* No window functions, no CTEs, no `INSERT ... SELECT`, no array values.
* Temporal limits: `timestamptz` parses as plain timestamp (no zone
  math), `extract` returns double (PG returns numeric), typed literals
  fail with `42601` instead of PG's `22007`, nominal-equal intervals
  collide in unique indexes.
* Snapshot limits: AS OF reads use the reference executor (no vecx, no
  secondary-index prefilter) and reconstruct per query (no cache); AS OF
  LSN resolves the table's *current* incarnation (only named snapshots
  track drop/recreate); CREATE/DROP SNAPSHOT is refused inside explicit
  transactions (`25001`); a snapshot taken before a table's first
  CHECKPOINT pins the whole WAL (checkpoint first, then snapshot, for
  cheap retention).
* `--ack relaxed` explicitly trades durability for throughput.

## Docs

* `ARCHITECTURE.md` — milestones, design decisions, the M7 bug ledger.
* `PAPERS.md` — the research this is built on (Aurora, Neon, X100,
  MonetDB, Snowflake micro-partitions, S3 conditional writes…).
* `tests/` — the whole suite; every claim above has a harness behind it.
