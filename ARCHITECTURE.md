# puddle — Architecture

*A small body of standing water: a lightweight lakehouse that speaks PostgreSQL and lives in an S3 bucket.*

Design forced by `PAPERS.md`. RAM discipline, safety, TLS, and test brutality are
non-negotiable requirements (user contract), not properties we hope to emerge.

```
                            psql / any PG driver
                                   │  PostgreSQL wire protocol v3
                                   ▼
                    ┌──────────────────────────────┐
                    │  front-end (wire.rs)          │  thread per connection
                    │  auth (trust / cleartext /    │  simple + extended query,
                    │  SCRAM-SHA-256)               │  portals, COPY
                    └──────────────┬───────────────┘
                                   ▼
                    ┌──────────────────────────────┐
                    │  SQL: lexer → parser → plan   │  hand-written recursive descent
                    └──────────────┬───────────────┘
                                   ▼
                    ┌──────────────────────────────┐
                    │  vectorized executor          │  batches of ~2048 values;
                    │  scan / filter / hash-agg /   │  zone-stat pruning; late
                    │  hash join / sort / dml       │  materialization
                    └───────┬──────────────┬───────┘
                            ▼              ▼
                ┌───────────────┐   ┌─────────────────────────┐
                │ table state   │   │ bounded LRU chunk cache │  byte-budgeted,
                │ (manifest-    │   │ (the only big RAM user) │  pin/unpin, hit stats
                │  pinned)      │   └────────────┬────────────┘
                └───────┬───────┘                │
                        ▼                        ▼
        ┌───────────────────────────────────────────────────┐
        │  storage: S3-compatible object store over TLS     │
        │  SigV4 client (ureq+rustls), range GETs, multipart │
        └───────────────────────────────────────────────────┘
```

## Bucket layout

```
s3://<bucket>/<prefix>/
  puddle.meta.json                    # global: schema-format version, uuid
  <table_id>/
    manifest/<n>.json                 # versioned table state (CAS-committed)
    wal/<lsn>-<uuid>.wal              # immutable WAL segments (the truth)
    part/<uuid>.col/<col>-<chunk>.bin # immutable columnar parts
    idx/<index_id>/<uuid>.bin         # immutable index segments
```

Objects other than manifests are **immutable and content-named** (UUID), so they are written
exactly once with a plain PUT. Only manifests are ever conditionally overwritten.

## Formats (v1)

**manifest/N.json** — `schema_version, table_name, columns[{name,type,nullable}]`,
`parts[{uuid, rows, lsn_range, zone_stats{min,max,nnz} per column, crc32c, bytes}]`,
`wal_truncate_lsn`, `indexes[...]`, `created_lsn`. JSON (serde_json) so a human can read a
table's state with any S3 console. The commit CAS guards exactly this object.

**WAL segment** — frame: `[u32 len][u32 crc32c(payload)][u8 type][u64 lsn][payload]`,
torn-tail tolerant (stop at first bad CRC — a partial final upload is expected and safe).
Records: `Insert(row)`, `Update(rowid, new_row)`, `Delete(rowid)`, `Truncate`. Segments roll
at ~8 MB or on commit boundaries. Every record LSN-stamped.

**Columnar part** — 65,536 rows (last part smaller). Per column: a *chunk* = header
(`encoding, nrows, nnz, min/max, len`) + payload in one of: dictionary (dict + u32 idx),
RLE (value,run), raw (i64/f64/bool/varint-str). Each chunk CRC32C'd. Chunks are laid out
column-contiguous so **one S3 range GET fetches exactly one column of one part** — Snowflake
micro-partitions meet Parquet column chunks. LZ4-block compression per chunk when it wins.

**Rowid** — implicit u64 per row, dense per part, never reused; UPDATE = delete(rowid) +
insert(new row). This is POSTGRES-style no-overwrite (#10) at bucket granularity.

## Durability & the commit protocol (safety core) — implemented in M3

Two ack levels, per transaction:

- `relaxed` — records buffer in RAM; a background flusher uploads one segment every
  500 ms. A crash may lose the last window; everything flushed is durable.
- `committed` (**default**) — the whole transaction is uploaded as one WAL segment
  *before* the statement/commit is acknowledged. Survives anything but loss of the
  bucket itself.

`paranoid` (manifest CAS per statement) is deferred: while committed segments are
replayable, per-statement manifest CAS is redundant for v1's single-writer model.
Revisit when readers pin manifests without replay (M4+).

The truth layer is a **global WAL stream** (`wal/<seq>-<uuid>.wal`): one transaction =
one immutable segment object, so multi-table commits are atomic for free (the object
exists or it doesn't). `CHECKPOINT` materializes every table into a fresh columnar
part, CAS-commits `manifest/<n+1>.json` per table (`If-Match:` on the etag we tracked
since load), then best-effort deletes superseded parts and covered segments. The
`meta.json` registry (name → table id) is itself CAS-maintained on CREATE/DROP, and
recovery self-heals it against replayed truth.

Recovery (on boot): load newest manifest per registered table (LIST `manifest/`,
pick max n, verify part CRCs), replay ALL segments in seq order — **idempotently**
(Insert with an existing rowid is skipped; Update/Delete of an unknown rowid are
skipped) — so a crash between part-write and segment-delete converges. A torn tail
(truncation / CRC mismatch) is tolerated only in the highest-seq segment; a torn
non-latest segment or structural garbage refuses to start, loudly.

Every frame and every part carries CRC32C. Corrupt-input handling is *refuse and
report*, never best-effort.

## SQL surface (v1, "go as far as possible")

`CREATE TABLE` (BOOL, INT, BIGINT, REAL/DOUBLE, TEXT, DATE, TIME, TIMESTAMP,
INTERVAL), `DROP TABLE`,
`INSERT ... VALUES` (multi-row), `COPY FROM/TO` (bulk), `SELECT` with expressions
(arithmetic, comparison, AND/OR/NOT, LIKE, IN, IS NULL, CASE), `WHERE`, `GROUP BY` +
aggregates (count, sum, min, max, avg, count_distinct) + `HAVING`, `ORDER BY`, `LIMIT`/
`OFFSET`, `INNER/LEFT JOIN`, `UPDATE`, `DELETE`, `BEGIN/COMMIT/ROLLBACK` (snapshot
isolation: readers pin manifests; writers serialize via the CAS commit), index DDL
(`CREATE INDEX ... USING hash|btree`) with point/range plans.

Known v1 limits, documented in README: no subqueries in FROM, no window functions, no
server-side functions beyond a builtin scalar set, no `generate_series`, single database
per deployment.

## Temporal (v1.2)

`DATE` (days, i32, PG epoch 2000-01-01), `TIME` (µs i64), `TIMESTAMP`
(µs since 2000-01-01, i64), `INTERVAL` (months i32 + days i32 + µs i64 —
PG's triple). Encoding: part segments carry TY codes 4-7; WAL rows carry
tags 5-8; the wire sends PG binary formats for extended-protocol results
(interval = [µs i64][days i32][months i32]). Arithmetic follows PG:
`add_months` with day clamping, `time ± interval` wraps mod day,
`ts − ts` justifies µs into days (truncating division like C), interval
± interval is component-wise, output pluralizes on `== 1` (so
`-1 mons -1 days`, verified against real PG 17). Ordering is nominal via
`interval_us` (30-day months, 24h days) so indexes and ORDER BY work;
`sum(interval)` is component-wise. All display decisions were taken from
a live PostgreSQL 17 side-by-side, not from memory.

## Snapshots & time travel (v2.0)

LSN-addressed immutable layers, Neon-pageserver-shaped:

* every transaction gets a global, **durable-unique** LSN (the watermark
  is recomputed from the WAL at recovery — LSNs are never reused across
  restarts); each segment is one txn, one LSN
* manifests carry a coverage watermark `lsn` = "contains every txn below
  this"; checkpoints and ALTERs bump it
* `CREATE SNAPSHOT` pins `(table_id, manifest_n)` per table + the last
  included LSN into `meta.json` (single CAS). A snapshot pins the table
  *incarnation*, so it reads through DROP + recreate
* `t AS OF ...` = load the pinned (or newest retained ≤ LSN+1) manifest +
  its parts, then replay segments in `[base_lsn, target]` for that table
  only — ephemeral, per query, never cached (the RSS contract)
* **snapshot-aware GC**: a WAL segment dies only when its LSN is below
  every live snapshot's oldest pinned base (`Some(0)` = a WAL-only pin =
  keep everything); pinned manifests and their parts are protected;
  DROP SNAPSHOT + CHECKPOINT reclaims
* the vectorized path is gated off for AS OF (reference executor only);
  txn-snapshot Dbs hold a *read-only* persist handle so time travel works
  inside BEGIN while `log_txn` stays a no-op there (a rolled-back txn
  must never leave a segment)

## Query features (v2.1)

- **Derived tables / CTEs**: `FROM (SELECT ...)` executes the subquery via the normal
  `execute` entry point and adopts its output as the relation (schema via recursive
  `describe`, never executed — so Describe on prepared statements works). CTEs are a
  thread-local stack of frames (one connection = one thread): materialize-once with
  memoization, PG declaration-order visibility (`in_flight` indices make self- and
  forward references `42P01`), innermost shadowing. All RefCell borrows are short so
  lookups re-enter freely. VecX stands down when a FROM name is defined by an active
  frame (it reads the catalog directly and would otherwise miss the shadowing).
- **INSERT ... SELECT**: the source query runs before the write lock; rows coerce per
  target column (`42601` on arity mismatch).
- **UPSERT**: the conflict arbiter is a btree index on the target column (single-column
  indexes; `42P10` when none matches; an untargeted ON CONFLICT requires exactly one
  index). DO UPDATE evaluates SET exprs with the existing row plus the proposed row
  under `excluded`; the batch tracks affected keys (`21000` on a second touch); inserts
  and updates land in one atomic WAL transaction with secondary-index maintenance.
- **Window functions**: window calls in SELECT/ORDER BY are rewritten to `__win_k`
  pseudo-columns; a window pass computes them over the WHERE-filtered rows (plain
  queries) or the group rows (aggregate queries — window specs may embed aggregates,
  split into the same AggSpec list so the group pass computes them). Partitioning by
  gkey, stable sort by the spec's ORDER BY, peer groups for rank/dense_rank/cume_dist
  and RANGE frames; ROWS frames as index ranges; cumulative frames get a running
  aggregate. Window queries run on the reference executor.

## SQL surface completion (v2.2)

- **RIGHT / FULL OUTER JOIN**: the hash-join machinery runs with sides
  swapped (RIGHT) or null-extends both directions (FULL); unmatched-row
  emission matches PG's row order for matched rows. `JOIN ... USING (c)`
  rewrites to the qualified equality at parse time — PG additionally
  merges the two output columns, puddle keeps both (a bare column name
  over a USING join stays ambiguous; documented).
- **VALUES**: table-less `VALUES` rows become a SelectStmt with a
  synthesized row-relation; usable standalone, as a derived table with
  aliases, and as an INSERT ... source.
- **WITH RECURSIVE**: anchor runs once; the recursive term iterates on
  the delta (UNION ALL) or the whole working table (UNION) until it adds
  no rows. A 54,000-iteration cap turns runaway recursion into a
  `54000` error instead of an OOM (PG has no cap — documented
  divergence). The self-reference in the anchor is `42P01`.
- **Named windows + FILTER**: `WINDOW w AS (...)` + `OVER w` with
  PG-legal PARTITION/ORDER/frame overrides (`42P10` on illegal
  re-specification, `42704` on unknown names); `FILTER (WHERE ...)` on
  plain aggregates (pre-aggregation row mask) and windowed aggregates
  (per-accumulation mask, computed before the window pass peeks rows).
- **Constraints**: `ColumnDef` carries pk/unique/default/check. CREATE
  TABLE builds one implicit btree per PK/UNIQUE column
  (`build_constraint_indexes`), flagged `internal` — invisible to DROP
  INDEX (`0A000`), visible in `pg_indexes` like PG's constraint indexes.
  Every DML staging path (INSERT VALUES, INSERT…SELECT, COPY IN, UPDATE
  plan) runs `check_row_constraints` before touching the table: NOT NULL
  (`23502`), CHECK via the scalar evaluator with the row bound as scope
  (`23514`, passes on TRUE or NULL), UNIQUE via index probe + a
  batch-local `pending` key set that catches intra-batch duplicates
  (`23505`). `fill_defaults` const-evaluates DEFAULTs for omitted
  columns (INSERT and COPY). UPDATE probes skip the row's own rowid.
  ON CONFLICT resolves its arbiter at analysis time (PG error
  precedence: `0A000`/`42703`/`42P10` before any execution) and requires
  a genuinely unique index — implicit constraint index or explicit
  `CREATE UNIQUE INDEX`; the arbiter column is exempt from the staging
  unique check because the clause itself resolves those conflicts.
  Persistence: constraint metadata rides the WAL as a new
  `Constraints` record after `Create` (older logs replay unchanged) and
  the manifest's `ManCol` (serde-defaulted fields; exprs round-trip as
  SQL text via the expr renderer and re-parse through
  `parse_one("SELECT <expr>")`); recovery rebuilds the implicit indexes
  after the explicit ones.

## Embedding & local IPC (v2.2)

- `libpuddle.so` exposes a C ABI (`puddle_open` with `mem:` and durable
  `file:` URIs, `puddle_exec`, result accessors incl. NULL cells and
  SQLSTATE-carrying errors, `puddle_result_free`, `puddle_close`).
  ctypes-verified (26 checks): the whole engine with zero sockets.
- `--bind unix:/path` listens on a unix domain socket; libpq connects
  via `host=/path` with no TCP in the middle.

## Executor (M4)

Two executors, one semantics:

- **Vectorized path** (`engine::select_vectorized` + `vecx.rs`): single-table scans borrow
  the columnar table directly (zero row materialization); WHERE/ORDER BY/GROUP BY keys/
  aggregate arguments evaluate as column batches (`veval`) with a selection-mask row id
  list; equi-join chains (INNER/LEFT) run as hash joins (build right, probe left, LEFT
  null-fill; NULL keys never match, per SQL); INNER-only non-equi ON conjuncts filter
  post-join; CASE branches stay lazy per branch-subset so error semantics match the
  reference exactly. Aggregates accumulate per group over pre-evaluated argument columns,
  then each group assembles its output row with the scalar evaluator.
- **Reference path** (`engine::select`): the M2/M3 scalar executor — the correctness
  oracle, always one `--vectorized off` away.

The vectorized path takes a query only after a shape check (single FROM item; joins all
INNER/LEFT with at least one equi-key conjunct; LEFT joins pure-equi). Anything else —
comma cross joins, non-equi LEFT joins, RIGHT/FULL — falls back. Both paths share the
scalar operators (`eval_bin`, `eval_like`, `compare`, `coerce`, `compute_agg_vals`), so
differential testing compares true peers. `tests/diff_test.py` runs 118 queries (results
*and* SQLSTATEs) against both executors on identical data and asserts byte-equality.

## Indexes (M5)

`Table.indexes: HashMap<String, TableIndex>` keeps each index next to its
table under the same write lock — transactional DML (which mutates private
table clones through the same executor arms) maintains them for free. Keys
are rowids (stable across UPDATE/DELETE), so compaction never renumbers.
Lookups resolve rowid → position via binary search over the (ascending)
rowid vector. Index definitions live in meta.json with the table's
incarnation id; recovery rebuilds structures in one pass and heals defs whose
table vanished or was recreated. The vectorized path consults indexes
(single-table WHERE conjuncts); the reference executor stays full-scan —
`tests/diff_test.py` runs every index shape on both and asserts byte-equality
(156/156).

Parts (M4 storage): payloads LZ4-block-compressed when ≥64 B and clearly smaller
(`enc |= 0x40`, payload = `[u32 raw_len][lz4 block]`); CHECKPOINT writes one immutable
part per 65,536 rows, each part header carrying per-column min/max zone stats and
null-aware encodings (raw/dict/RLE, all compressible).

## Wire protocol

PostgreSQL v3. Targets: `psql`, and the Python/Go/Rust/Node PG drivers. Simple query (`Q`)
for psql/`-c`; extended query (`P`/`B`/`D`/`E`/`S`) with unnamed+named prepared statements for
drivers; text format results first, binary result format where cheap; COPY both directions;
`RowDescription` uses real type OIDs (bool 16, int8 20, float8 701, text 25, timestamp 1114).
Auth: `trust`, `password` (cleartext over TLS), and SCRAM-SHA-256 (we already need SHA-256 +
HMAC for SigV4, so SCRAM is ~100 lines of reuse).

Known wire limitation: **binary-format Bind parameters are rejected with SQLSTATE 0A000**
(feature not supported). Text-format parameters and binary-format *results* (int8 BE etc.)
are both supported — drivers that matter (psycopg2, JDBC with `binaryTransfer=false`,
rust-postgres) negotiate this fine. Binary params remain on the deferred list.

## Auth + TLS (M6)

Server-side auth happens after startup-packet parsing, before any SQL.
`trust` (default) skips it. `password` = cleartext PasswordMessage round
(only sensible over TLS or localhost). `scram-sha-256` = SASL exchange:
AuthenticationSASL (mechanism list terminated by an EMPTY C string — no
count field, libpq is the oracle here) → SASLInitialResponse → Continue →
SASLResponse → Final + Ok. Crypto is hand-rolled over the existing sha2/hmac
deps (PBKDF2 included); base64 hand-rolled too. Verifiers are PG-compatible
(`SCRAM-SHA-256$4096:salt$stored:server`), so pg verifiers work as-is.
Channel binding (SCRAM-SHA-256-PLUS) is not advertised; `y,,` clients are
accepted per RFC 5802.

TLS: SSLRequest gets 'S' + a rustls handshake (ring provider, installed
process-wide explicitly), 'N' when TLS is off. Same-port dual-mode (cleartext
+ TLS); `--tls-required` also rejects clients that never send SSLRequest.
One code path: wire.rs helpers are generic over Read+Write and the session
runs on a `Conn` enum (Plain | Tls). For S3, https comes from ureq/rustls
unchanged.

## Memory policy (the "lightweight = RAM" contract)

- Idle RSS target: < 10 MB. Loaded working set: bounded by the chunk cache budget
  (default 64 MB, `--cache-mb`), enforced by byte-accounted LRU with pin counts.
- Scans process in bounded batches; sorts/aggregations spill to the bucket (temporary
  objects under a `tmp/` prefix, GC'd) rather than grow unbounded.
- Tests assert RSS ceilings (`/proc/<pid>/status` sampling) — a memory regression fails CI.
- Connection cost budget: ~32 KB each beyond buffers; `--maxclients` cap.

## Process model

Thread-per-connection (rustdis experiment 7 winner) + three daemons: WAL writer (group
commit + local fsync + uploads), snapshotter (tuple mover / L0→L1 compaction), and a
maintenance thread (GC of unreferenced objects, orphan sweep). Shared state behind
`RwLock`s where hot; the catalog is an in-RAM structure rebuilt from manifests at boot.

## Dependencies (small, audited, RAM-cheap)

`ureq` (blocking HTTP + rustls TLS), `serde_json` (manifests), `lz4_flex` (chunk
compression), `sha2` + `hmac` (SigV4, SCRAM, part checksums). Everything else — protocol,
SQL, executor, storage formats, buffer pool, S3 client semantics, CRC32C — is hand-built,
because that's the point of the project.

## Milestones

- **M1** — ✅ wire protocol live: psql connects, `SELECT 1+2`, simple query loop, real errors.
- **M2** — ✅ full reference SQL engine + extended query protocol + snapshot/OCC transactions.
- **M3** — ✅ durable layer: hand-rolled SigV4 S3 client (verified against the AWS doc
  vector + an independent python server that re-derives signatures), memory/file/S3 object
  stores with conditional writes, global WAL stream, columnar parts (raw/dict/RLE + zone
  stats), versioned manifests with CAS commit, CHECKPOINT, idempotent crash recovery,
  relaxed/committed ack levels. Verified with kill -9 crash loops and corruption injection.
- **M4** — ✅ vectorized executor: columnar in-RAM tables (`Vec<Vec<Value>>` + rowids),
  batch expression evaluation with selection masks (`src/vecx.rs`), hash joins for
  INNER/LEFT equi-join chains (non-equi conjuncts on INNER joins run as post-join
  filters; everything else falls back), LZ4 block compression on part payloads
  (`enc |= 0x40`, `[u32 raw_len][lz4]`), multi-part checkpoints (65,536 rows/part,
  per-part zone stats in every part header). The reference executor stays as the
  differential-testing oracle (`--vectorized off`, `tests/diff_test.py`, 118/118
  queries byte-identical incl. error codes). 200K-row bench: joins 50–55×, scans
  4–8×, group-by 4.7× vs the M3 baseline; idle RSS 1.3 MB unchanged.
  Deferred (honest): typed SoA arrays + validity bitmaps, paged scans with zone-stat
  pruning, RIGHT/FULL joins, paranoid ack.
- **M5** — ✅ secondary indexes: `CREATE INDEX ... USING btree|hash (col)` /
  `DROP INDEX` (PG error codes 42P07/42704/42P01/42703, auto-named indexes,
  relation-namespace sharing). BTree serves `=`, `<`, `<=`, `>`, `>=`,
  `BETWEEN`, `IN`; hash serves `=` / `IN`. The planner runs indexes as a
  **prefilter** — the full WHERE still evaluates over the candidates, so an
  index can only change speed, never results (type-mismatched constants
  decline the index so error semantics stay identical). NULLs are never
  indexed; keys are a total-order wrapper (sign-flip float bits, NaN last,
  like PG). DML maintains indexes incrementally (INSERT/UPDATE/DELETE);
  index *definitions* persist in meta.json (with table incarnation ids, so
  drop+recreate heals), structures rebuild from table data at recovery.
  200K-row bench: point 8.6→0.08 ms (107×), range 12.8→0.15 ms (85×),
  IN-list 54→0.07 ms (771×); idle RSS 1.3 MB unchanged; verified on file and
  S3 stores (kill -9 → rebuild → exact lookups after mutations).
- **M6** — ✅ SCRAM-SHA-256 auth + TLS. `--auth trust|password|scram-sha-256`
  with `--auth-file` (JSON: password and/or PG-format SCRAM verifiers;
  `--scram-secret PASS` prints a verifier for the file). Full RFC 5802
  server exchange (hand-rolled over sha2+hmac: PBKDF2, proofs, server
  signature — vectors cross-checked against python hashlib), gs2 `n,,` and
  `y,,` (libpq over TLS), unknown users run against a dummy verifier so
  failures are indistinguishable. TLS via rustls (already in the tree for
  ureq): `--tls` = ephemeral self-signed cert (rcgen), `--tls-cert/--tls-key`
  = provided PEM, `--tls-required` rejects cleartext (both SSLRequest-'N'
  and the no-request path). Auth failures are 28P01 with the username.
  Verified by `tests/auth_test.py` (17 checks incl. real libpq SCRAM
  handshakes, TLS+SCRAM together, wrong/unknown passwords) — idle RSS with
  TLS configured: 1.35 MB.
- **M7** ✅ — the brutal test suite (see "M7 — the brutal test suite" below).
- **M8** ✅ — bench tool + README + docs + polish (see below).
- **M8 detail** — `tests/bench.py` is now self-contained (own server, load,
  executor battery vs the recorded M3 baseline, index battery, 8-thread
  point-select QPS/latency probe, kill -9 restart check; JSON appended to
  `bench-results.json`). `README.md` is the user-facing entry (features,
  quick start, fresh 1.0.0 numbers, memory contract, durability model,
  honest limitations). v1.0.0.
- **M9** ✅ — v1.1 "works with your tools": catalog + COPY + ALTER.
- **v1.2.0 temporal** ✅ — native DATE/TIME/TIMESTAMP/INTERVAL with PG
  oids, binary wire formats, arithmetic/clamping/display conventions
  verified against live PG 17; 74 regression checks + 20 differential
  queries added; 132/132 + 176/176 green.
- **v2.0.0 snapshots** ✅ — CREATE/DROP SNAPSHOT + AS OF SNAPSHOT/LSN on
  LSN-addressed immutable layers with snapshot-aware GC; durable LSN
  watermark; 39 regression checks, 9 differential queries, a durability
  round, and 12 hostile directed probes added; also fixed the latent
  v1.2 part-header bug (part.rs ty_of mapped temporal codes to text —
  any CHECKPOINT of a temporal table produced unreadable parts).
  171/171 + 185/185 + full battery green.
  - **Catalog** (`src/catalog.rs`): `pg_catalog` + `information_schema`
    synthesized from live table metadata on every query — no stored system
    tables, zero footprint. pg_class (incl. relhasindex/relkind/…),
    pg_attribute (typed, attnotnull), pg_index, pg_namespace, pg_database,
    pg_type, pg_roles, and well-formed empty stubs (pg_constraint,
    pg_policy, pg_publication*, pg_statistic_ext*, pg_collation,
    pg_inherits, pg_auth_members) so psql's LEFT-JOIN probes stay
    well-formed. Oids are fnv-hashed `&0x7fffffff | 1<<31`. `format_type`,
    `pg_get_userbyid`, `pg_table_is_visible`, `current_database` etc. are
    answered in eval_func. Parser support this required: schema-qualified
    function calls (`pg_catalog.x(...)`), `E'…'` escape strings,
    `OPERATOR(pg_catalog.~)`, `::pg_catalog.type` casts (incl. array-type
    suffix `int2[]`), `ARRAY(SELECT …)` subquery arguments, `[i]` subscripts
    (parsed, clean 0A000 if evaluated), `CASE`, `~`/`!~` regex match.
  - **Scalar subqueries** (uncorrelated, v1.1 scope): parsed into
    `Expr::Subquery`, evaluated *eagerly* for gating clauses (WHERE/HAVING/
    ORDER/LIMIT) and *deferred* for SELECT items until rows are known to
    exist — that deferment is what keeps psql's correlated probes over
    empty catalog relations correct instead of erroring. >1 row → 21000.
    A truly correlated reference fails resolution with a clean 42703.
  - **UNION [ALL]** with compound ORDER BY / LIMIT / OFFSET; comma
    cross-joins and `CROSS JOIN` (already present); `generate_series(a,b)`
    as a set-returning function in FROM (bare alias renames the column).
  - **COPY** (`src/copy.rs` + wire flows in main.rs + `Engine::copy_in/
    copy_out`): text + CSV (RFC 4180 state machine: quoted fields, doubled
    quotes, quoted-empty vs unquoted-empty/NULL, CRLF), DELIMITER / NULL /
    QUOTE / HEADER [true|false|match], column lists with per-column coerce
    (22P02/23502/22P04-style errors), `COPY (query) TO STDOUT`. Wire level:
    CopyInResponse → CopyData* → CopyDone/Fail; one WAL batch per COPY, so
    kill -9 mid-COPY is all-or-nothing (tested). Simple protocol only;
    extended-protocol COPY and COPY inside an explicit txn are clean 0A000.
    `\copy` (client-side psql) and psycopg2 `copy_expert` both verified.
  - **ALTER TABLE ADD COLUMN** (autocommit): eager `persist::alter_add` —
    one WAL AlterAdd record + manifest n+1 with the *same* parts (no part
    rewrite; part loading NULL-pads short rows → schema evolution). In an
    explicit txn it becomes a create-rewrite in the commit diff (atomic).
    42701 duplicate, IF NOT EXISTS / IF EXISTS no-ops. WAL record type 6.
  - `tests/psql_test.py` (suite #14, 53 checks): real psql binary — `\l`
    `\dt` `\d` render, server- and client-side COPY round-trips (text/CSV/
    quoting/NULL/column-subset), error codes, ALTER + kill -9 restart
    exact, psycopg2 copy_expert, kill -9 mid-COPY all-or-nothing, special
    registers, generate_series, UNION. Full 14-suite battery green.
  - v1.1 numbers: 12,356 lines, 7.6 MB binary, idle RSS 1.8 MB.
- **v1.1.2 startup hardening**: the listener is bound before any
  storage is opened, so a port conflict fails fast without touching (or
  initializing) a datadir another live server may own.
- **v1.1.1 lint policy — "no stubs, enforced by the compiler"**: a
  `[lints.clippy]` table in Cargo.toml sets `todo` / `unimplemented` /
  `dbg_macro` / `unreachable` to **forbid** (cannot be re-allowed locally);
  `cargo clippy --release --all-targets -- -D warnings` is gate #0 and is
  zero-warning clean, `cargo test --release` runs 31 unit tests (store
  contract incl. CAS + range reads on Mem/File backends, part codec
  roundtrips raw/dict/RLE/LZ4 + corruption detection, WAL roundtrip, IKey
  total order, SCRAM vectors, CRC32C vectors, AWS SigV4 doc vector).
  Cleanup that got us there: 77 clippy warnings fixed (43 auto + manual),
  the two `unreachable!()` panics replaced (infer_ty restructured; part
  encoder raw-as-final-arm), `OPERATOR()` pattern fixed for real (the "."
  inside `OPERATOR(schema.op)` previously landed in the operator slot and
  only worked because the real operator overwrote it), dead v0.x leftovers
  deleted (write_part, notice/row_description/data_row superseded by the
  _fmt/_raw variants, ack_level, flat_cols, BATCH, QIdent, MemStore's
  production path — MemStore is now explicitly `#[cfg(test)]`-scoped test
  tooling together with the trait's get_range), two justified local allows
  (ureq::Error size, SigV4's 9-arg test vector shape). Toolchain moved to
  ~/.cargo / ~/.rustup so sandbox resets stop wiping it.

## M7 — the brutal test suite

Sixteen suites, all green on v2.2.0 (psql is not installed in this
environment; psycopg2/libpq is the wire oracle):

| suite | what it proves |
|---|---|
| `feature_test.py` (124, v2.2) | full v2.2 surface: joins (all kinds + USING), VALUES, WITH RECURSIVE, named windows, FILTER, COPY, transactions, params, types, ON CONFLICT, CREATE UNIQUE INDEX, constraints — plus a `--phase2` rerun after restart + CHECKPOINT proving constraints survive both recovery paths |
| `embed_test.py` (26, v2.2) | the C ABI: in-process engine over ctypes, zero TCP |
| `cargo test` (31) | crypto vectors, SCRAM exchange, sigv4 vs AWS docs |
| `regression_test.py` (171) | full SQL surface semantics |
| `ext_test.py` (8) | extended protocol: named statements, portals, params |
| `diff_test.py` (156) | vectorized executor == reference executor, incl. errors |
| `durability_test.py` (8 rounds) | kill -9 at every stage; acked = durable; rollback stays rolled back |
| `auth_test.py` (17) | SCRAM-SHA-256 + TLS via real libpq |
| `fuzz_test.py` | seeded differential fuzzer: random schema/data on vec-ON vs ref-OFF servers; wild (type-chaos) + typed expression generators; WHERE/GROUP BY/HAVING/JOIN/LIMIT shapes; interleaved DML; params. Policy: both-ok ⇒ byte-identical rows (NaN-aware); both-error ⇒ pass (error *detail* depends on evaluation order — same property as PG parallel plans); one-sided error ⇒ divergence. 7,500+ queries across seeds |
| `hostile_fuzz.py` | 7-layer hostile fuzzer: L0 directed kill-probes (expression nesting to 64K, length-field lies incl. 0/1/3/5/MAX/MAX+1/0xFFFFFFFF, startup lies, 1 MB pipelined queries, unterminated literals/comments, invalid UTF-8, $1..$4096 params, abandoned COPY + stray copy/describe/flush, SSLRequest/GssencRequest/CancelRequest abuse, second startup mid-session, 24-conn memory-amplification probe) · L1 startup/framing mutations · L2 message-body mutations · L3 extended-protocol corruption (param-length lies, absurd format codes, dropped pipeline stages) · L4 SCRAM handshake corruption · L5 SQL grammar generation + mutation · L6 recorded-session replay mutation. Oracle per case: process alive + trust/scram-aware liveness + every response byte frames as valid PG v3 + RSS ceiling (220 MB) + unguessably-named canary table byte-exact. Violations are ddmin-minimized and saved as artifacts. Found 2 server defects (stack overflow at ~512-deep nesting; 64 MB preallocation per length-lie) |
| `crash_loop_test.py` | 20 rounds of SIGKILL mid-concurrent-writes (6 writers, singles + 3-row txns). Every acked insert durable; per-writer k-space a contiguous prefix (holes = lost or torn txns); no duplicates; static table checksum unchanged |
| `corrupt_test.py` (15) | bit-flip/truncate/garbage/delete in meta.json, manifests, parts, WAL. Loud refusal or exact consistent prefix — never silent wrong data, never a half-applied transaction |
| `soak_test.py` | 12 threads × 30 s contended transfers (32-account hot set) + inserts/deletes + background CHECKPOINT: sum(bal) invariant on every concurrent read, 65k+ OCC 40001 retries retried to completion, exact final accounting, restart-exact |
| `rss_test.py` | idle <10 MB (plain + TLS), 200-conn churn growth <2 MB, 100 live conns <40 MB, 200K rows + 2 indexes <64 MB |
| `wire_test.py` | hostile sockets: garbage, truncation, length lies, SSLRequest abuse, binary Query bodies, slowloris-lite, 100 rapid garbage conns — server survives and keeps serving |
| `s3_test.py` | full S3 mode against an in-memory test server that independently verifies SigV4 on every request; kill -9 + restart exact; second process + dual-writer collision refused loudly |

Bugs the suite found (all fixed, all would have shipped without it):

1. **CASE WHEN conditions evaluated over already-resolved rows** in the
   vectorized executor — a later WHEN's static type error surfaced even when
   no row reached it (reference executor is per-row lazy). Fixed: WHENs
   evaluate only over pending rows.
2. **Durability hole under mixed concurrency**: autocommit DML never bumped
   table versions, so an explicit transaction's OCC validation passed with a
   stale snapshot and its wholesale swap silently swallowed concurrently
   acked rows — `diff_recs` even emitted WAL *deletes* for them, making the
   loss durable. Fixed: every effective INSERT/UPDATE/DELETE bumps the table
   version; stale-snapshot commits now fail 40001 and retry.
3. **Torn WAL segment replayed its complete-frame prefix** — a bit-flip in a
   frame length masqueraded as truncation and exposed HALF a transaction
   (1 of 50 rows). Fixed: one segment = one (unacked) transaction ⇒ a torn
   segment is discarded whole; 0-byte segments are torn, not valid-empty.
4. **Silent data loss on catalog damage**: a deleted meta.json booted an
   empty database; a deleted manifest silently vanished a table with parts
   on disk. Both now refuse loudly (fresh start only on a truly empty store;
   registered-table-with-parts-but-no-manifest ⇒ refuse).
5. **Manifest CAS compared the new manifest's key against the previous
   manifest's etag** — every second CHECKPOINT failed with a storage error.
   Fixed: manifests are monotonic create-once objects (`put_if_absent` is the
   CAS); superseded manifests are GC'd (they used to leak per checkpoint).
6. **`.tmp` in-flight writes from a killed process were parsed as WAL
   segments** ("torn tail in non-latest segment" ⇒ refusal to boot). Fixed:
   strict `wal/<16d>-<32hex>.wal` key filter at all three listing sites;
   stale `.tmp`s are cleaned at recovery.
7. **Dual-writer WAL seq collision was silently deduped at replay** (two
   processes, overlapping seq numbers ⇒ one writer's transactions dropped).
   Fixed: duplicate seqs refuse loudly — single-writer-per-prefix is the
   contract, and violations must never be silent.
8. **Stack exhaustion from deep expression nesting** (hostile fuzzer, L0):
   `SELECT ((((…1…))))` with 512 nested parens crashed the process — the
   recursive-descent parser had no depth limit and connection threads run
   1 MB stacks, so ~500 nested atoms were a remote kill. Fixed: a single
   depth guard on `primary_atom` (every recursive descent re-enters there;
   operator chains are iterative) caps nesting at 128 levels with a clean
   `42601` — parenthesized/subquery/CASE/function/cast nesting are all
   covered by the one guard.
9. **Memory amplification via length-field lies** (hostile fuzzer, L0):
   a 9-byte message header claiming a 64 MB body made the server
   preallocate the full 64 MB before reading a single payload byte —
   24 idle connections ≈ 1.5 GB RSS for ~200 bytes of attacker input.
   Fixed: `read_exact_n` allocates incrementally (64 KiB chunks, capped
   in-flight buffer); the same 24-connection probe now costs +2.5 MB.
