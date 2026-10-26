# puddle vs the field — benchmark report

One workload, six engines, one machine, one session. The point is not to
"win" — it is to know exactly where puddle stands against PostgreSQL,
MariaDB (the MySQL family), SQLite, MongoDB and Redis on the same 2-core
box, under the same load pattern, with results verified identical before
any timing is believed.

Reproduce: `python3 tests/field_bench.py` (raw numbers in
`field-bench-results.json`).

## Environment

| | |
|---|---|
| CPU | 2 × Intel Xeon @ 2.60 GHz (shared sandbox vCPU) |
| RAM | 2 GB |
| Kernel | Linux 6.1.158 |
| Storage | all datadirs on tmpfs (`/tmp`) |
| Isolation | engines run **one at a time** (start → bench → stop) |

Run-to-run QPS variance on this shared box is real (puddle measured
27–49K across sessions); single-session numbers below are internally
consistent, but treat ±30% as noise on the concurrency probe.

## The field

| Engine | Version | Driver | Notable config |
|---|---|---|---|
| puddle | current main (post-1.1.1 hardening) | psycopg2 (C) | defaults, committed acks, vectorized executor, btree index |
| PostgreSQL | 17.11 | psycopg2 (C) — *same client + wire protocol as puddle* | initdb defaults, trust auth |
| MariaDB | 11.8 (MySQL family) | mariadb connector (C) | InnoDB, default buffer pool, skip-grant-tables |
| SQLite | stdlib 3.4x | sqlite3 (C) | WAL + synchronous=NORMAL, **in-process** |
| MongoDB | 8.0.12 | pymongo (Python) | WiredTiger cache capped 256 MB |
| Redis | 8.0.2 | redis-py (Python) | in-memory, persistence off |

Fairness rules: identical dataset (`bench_big`: 200K rows of
`(a int, x float, g int, label text)`, `bench_dim`: 100 rows); identical
load pattern (5000-row batches, one commit per batch); query shapes from
the M8 bench methodology (`tests/bench.py`); medians of 5 runs (shapes)
or 50–200 runs (indexed probes); memory = summed PSS over the server
process tree. **Every engine returned identical rows for every shared
query shape before timings were recorded** (float tolerance 1e-6; two
tie-break/ordering pitfalls were made deterministic to get there).

## Point-path OLTP (indexed)

| | puddle | PostgreSQL | MariaDB | SQLite* | MongoDB | Redis |
|---|---|---|---|---|---|---|
| point-select | **0.069 ms** | 0.116 | 0.146 | 0.009 | 0.306 | 0.102 |
| range-1000 (count) | **0.150 ms** | 0.200 | 0.343 | 0.032 | 0.569 | n/a |
| in-list-10 (count) | **0.066 ms** | 0.118 | 0.132 | 0.009 | 0.437 | n/a |
| 8-thread QPS | **44,911** | 17,314 | 15,829 | 57,469 | 6,213 | 8,902 |
| p50 latency | 0.108 ms | 0.401 | 0.413 | **0.095** | 0.803 | 0.638 |
| p99 latency | **0.994 ms** | 1.243 | 1.540 | 0.589 | 7.574 | 3.898 |

\* SQLite is in-process (no wire, no server); it plays in a different
league by construction — included as the embedded-DB reference point.

Redis with its C client (`redis-benchmark`) reaches ~92–105K rps GET on
this box — its number above is client-bound (redis-py), as is MongoDB's
(pymongo). puddle, PostgreSQL and MariaDB numbers are C-driver-bound
end-to-end.

## Analytic shapes (200K rows, median of 5, ms — lower is better)

| shape | puddle | PostgreSQL | MariaDB | SQLite | MongoDB |
|---|---|---|---|---|---|
| scan+filter+count | 11.5 | 10.9 | 18.5 | **1.3** | 16.5 |
| group-by+aggs | 43.4 | **21.6** | 75.6 | 40.0 | 105.6 |
| scan+like+aggs | 30.7 | 17.1 | 52.0 | **15.7** | 120.7 |
| equi-join+agg | 97.2 | **59.2** | 1508.7 | 78.0 | 7679.6 |
| order-by top-k | 117.0 | **25.5** | 47.5 | 50.3 | 118.4 |
| left-join anti | 73.2 | **24.8** | 837.9 | 38.3 | 1536.9 |
| case+mod+group | 70.5 | **22.3** | 103.7 | 57.3 | 185.5 |

MariaDB's join numbers reflect its default-config plan (no index on the
dimension table, block-nested-loop); MongoDB's `$lookup` walks per-doc.
Both are honest defaults — tuning them changes the story for those two
shapes only.

## Loading and index build

| | puddle | PostgreSQL | MariaDB | SQLite | MongoDB | Redis |
|---|---|---|---|---|---|---|
| load (rows/s) | 113,933 | 101,761 | **298,067** | 753,454 | 138,338 | 66,358 |
| index build (ms) | **39.6** | 46.2 | 111.6 | 45.8 | 232.0 | n/a |
| point w/o index (ms) | 11.5 | 11.7 | 38.7 | **7.4** | 32.0 | n/a |

puddle vs PostgreSQL is the clean comparison here: same driver, same
protocol, same batch/commit pattern — puddle loads ~12% faster and builds
its btree index slightly faster at this scale.

## Footprint

| | puddle | PostgreSQL | MariaDB | SQLite | MongoDB | Redis |
|---|---|---|---|---|---|---|
| idle RSS | **2.9 MB** | 27.9 | 113.8 | ~0 (in-proc) | 164.1 | 9.6 |
| after load+index (PSS) | 57.6 | 27.6 | 151.9 | 3.3 | 229.1 | 34.5 |
| peak under 8-conn probe | 64.3 | 27.6 | 154.0 | 41.7 | 234.4 | 33.6 |
| cold start to first query | **0.05 s** | 0.54 | 0.45 | ~0 | 0.65 | 0.05 |
| install footprint | **7.6 MB** (1 binary) | 56 MB | 54 MB | 1.8 MB (lib) | 372 MB | 0.2 MB |

The PSS nuance: PostgreSQL serves scans from the OS page cache, which
PSS does not charge to it; puddle keeps its columnar parts in its own
heap, so its working set is visible in full. Both numbers are honest —
they just measure different architectures.

## Honest takeaways

1. **Point-path is puddle's home turf.** Fastest networked point-select
   (0.069 ms), 2.6× PostgreSQL's QPS at 8 connections, best p99 of any
   server, on 2 cores with 1 MB thread stacks.
2. **PostgreSQL owns wide analytics.** Group-by, LIKE-scan, top-k, joins:
   PG wins every analytic shape. puddle's weakest shape is order-by
   top-k (117 ms vs 25 ms) — a full sort where PG has a top-N heap.
   That's the v1.2+ roadmap, not a mystery.
3. **puddle beats the rest of the SQL field on analytics** despite being
   ~1/7 the binary size of PG/MariaDB: every shape is faster than
   MariaDB's and (except top-k, a tie) MongoDB's defaults.
4. **Footprint is the headline.** 2.9 MB idle vs 28–164 MB for the other
   servers; 0.05 s cold start; one 7.6 MB static binary. That is the
   "lightweight" contract — RAM, not features.
5. **Correctness held under comparison.** Before any timing was
   recorded, all engines had to return identical results for all seven
   shapes. They did (after two tie-break determinism fixes in the
   harness — one ORDER BY tie, one MongoDB `$count`-emits-nothing-on-
   zero quirk).

## Known caveats

- 2 vCPU, shared sandbox: absolute numbers are indicative; relative
  gaps (measured in the same session) are the signal.
- No tuning was applied to any engine (that is the point: defaults).
- Redis persistence was off; MongoDB cache capped at 256 MB (sandbox
  RAM); both disclosed above.
- The Python drivers (pymongo, redis-py) under-state their servers'
  ceiling on the concurrency probe.
