# puddle — Research Digest

**The papers that design puddle.** Every architectural decision below is traceable to a
published result. Order is roughly the order the decisions get made.

---

## 1. Hellerstein, Stonebraker, Hamilton — *Architecture of a Database System* (Foundations and Trends in DB Systems, 2007)

The map of how a real DBMS decomposes: process/parallelism model, memory/storage hierarchy,
lock manager vs. buffer manager vs. WAL, and the wire protocol layer. The famous observation
that "a DBMS is a set of cooperating subsystems, each a research topic in itself."

**puddle takes:** the layer boundaries — wire protocol / SQL front-end / executor / access
methods / buffer pool / WAL — with clean, individually testable interfaces. Also its process-
model discussion (process pool vs thread pool vs thread-per-conn): we already measured this
in the rustdis experiments (E7: thread-per-connection with small stacks won on throughput
*and* fairness), so puddle inherits that result and adds a background writer/snapshotter
thread, exactly the "worker + utility threads" shape the paper describes.

## 2. Verbitski et al. — *Amazon Aurora: Design Considerations for High Throughput Cloud-Native Relational Databases* (SIGMOD 2017)

Moved the storage engine *underneath* the database and made it understand the WAL. Compute
ships only redo records; the storage tier materializes pages asynchronously, 6 replicas,
4/6 quorum. The headline: "the log is the database" — I/O traffic to storage drops by ~7.7x
because pages are never written, only log records.

**puddle takes:** the inversion of truth. In puddle, **WAL segment objects in the bucket are
the source of truth; columnar snapshots are derived, materialized state** that can be deleted
and rebuilt from WAL + a base part at any time. This is what makes the safety story clean
(snapshots are never load-bearing for durability) and what makes the background snapshotter
free to run at whatever pace S3 economics prefer.

## 3. Dageville et al. — *The Snowflake Elastic Data Warehouse* (SIGMOD 2016)

Compute nodes with local disks as pure caches; all durable data in immutable, compressed,
**micro-partitions** (50–350 MB) on S3; a metadata layer holding per-partition min/max stats
used for pruning; multi-versioned table state for snapshot isolation; virtual warehouses that
can be resized or dropped because they are stateless.

**puddle takes:** the whole lakehouse shape. A puddle table is a **manifest** (versioned JSON
object listing immutable columnar parts + their per-column stats + a WAL truncate point).
Readers pin a manifest version → snapshot isolation with zero locking. Parts are immutable and
carry zone stats; scans prune by min/max before touching bytes. Compute holds only a bounded
local cache and can restart/scale without ceremony.

## 4. Neon — *Pageserver: storage for serverless Postgres* (Neon engineering docs; GetPage@LSN design)

Neon splits Postgres into ephemeral compute and a storage layer connected by WAL. The
pageserver answers `GetPage@LSN`: it finds the latest image layer for a page and replays delta
WAL records on top to reconstruct the page at that exact LSN. Layer files are **immutable,
never modified in place**; incoming WAL accumulates in memory, flushes to L0 delta files, and
compacts into L1 layers with garbage collection. Branches are just pointers to an LSN —
copy-on-write without copying ([Neon docs: pageserver storage](https://github.com/neondatabase/neon/blob/main/docs/pageserver-storage.md), [GetPage@LSN](https://neon.com/blog/get-page-at-lsn)).

**puddle takes:** LSN-addressed reads and the immutable-layer discipline (L0 deltas → L1
compaction → GC). Every puddle WAL record carries an LSN; a table state at LSN L = snapshot
part set + WAL records with LSN ≤ L. Branch/clone is a future feature that comes almost free
with this design.

## 5. Stonebraker et al. — *C-Store: A Column-oriented DBMS* (VLDB 2005)

Read-optimized column store + a small writable store (WS), with a **tuple mover** migrating
rows from WS into the read store (RS) in bulk. Plus: sort keys per projection, heavy
compression from columnar layout, and coverage indices.

**puddle takes:** the dual-store architecture, which is exactly what "does everything"
requires: the **row-oriented WAL is the WS** (fast INSERT/UPDATE/DELETE appends), the
**columnar snapshot parts are the RS** (fast scans), and the **snapshotter is the tuple
mover** (batches WAL into new columnar parts, LSM-style).

## 6. Zukowski et al. — *MonetDB/X100: Hyper-Pipelining Query Execution* (CIDR 2005)

Tuple-at-a-time interpretation wastes the CPU (interpretation overhead per value, poor
instruction/data cache behavior); column-at-a-time materializes too much. The fix is
**vectorized execution**: process ~1–2 K values per operator call in tight loops over flat
arrays. X100 showed order-of-magnitude gains over MonetDB's tuple-at-a-time MIL interpreter
on the TPC-H suite.

**puddle takes:** vectors, not rows. The executor's operators consume/produce batches
(vectors) of ~2048 values. Primitives (`cmp_lt_i64`, `add_f64`, hash, …) are plain loops
over `&[i64]`/`&[f64]` slices — the exact shape that lets the compiler keep everything in
registers/L1. (We also evaluated HyPer-style data-centric codegen (Kemper & Neumann, ICDE
2011) — the principled alternative — but vectorized interpretation is debuggable, dependency-
free, and gets most of the win; revisit later as an optimization.)

## 7. Raasveldt & Mühleisen — *DuckDB: an Embedded Analytical Database* (SIGMOD 2020)

Proof that a tiny team can build a serious columnar engine: vectorized execution, row groups
of ~122 K rows as the unit of storage/compression, dictionary + RLE + bit-packing per column
segment, out-of-core operators, MVCC in append-only structures.

**puddle takes:** the row-group granularity (we use 65,536 rows per part initially, tunable)
and the "compression is per column segment, chosen by cheap sampling" approach. Also the
cultural point: small, readable, complete — puddle's explicit goal.

## 8. O'Neil et al. — *The Log-Structured Merge-Tree* (Acta Informatica 1996)

Cascading immutable sorted runs amortize random writes into sequential ones; reads merge
across runs; compaction bounds run count. The write/read/space-amplification triangle that
every LSM system lives inside.

**puddle takes:** the bucket-level view of the same idea. L0 = WAL segments (append-only),
L1 = columnar parts written by the snapshotter, L2 = compacted parts. S3 makes the
amplification math brutal — every byte written is a PUT — so we bias hard toward *few, large,
immutable objects* and compaction only when the WAL/S3-GET economics say so (amplification
budgets are config, and the experiments phase measures the curves).

## 9. Melnik et al. — *Dremel: Interactive Analysis of Web-Scale Datasets* (VLDB 2010) + the Parquet format it shaped

Column chunks with per-chunk encodings (dictionary, RLE, bit-packing), nested data via
repetition/definition levels, record shredding/assembly, aggressive late materialization.

**puddle takes:** the column-chunk file layout — per-column chunks, each independently
encoded (dictionary + RLE + raw fallback), each independently checksummed, each readable via
a single S3 **range GET** (this is why per-chunk boundaries matter). We deliberately drop
Dremel's nested record model: puddle v1 is flat relational, so no repetition/definition
levels. This is the one place we knowingly diverge from Parquet instead of implementing it.

## 10. Stonebraker — *The Design of POSTGRES* (SIGMOD 1987)

No-overwrite storage: every update creates a new version; vacuum reclaims; readers never
block writers and vice versa. Time-travel queries fell out of the design for free.

**puddle takes:** immutability all the way down. Columnar parts and WAL records are never
modified; UPDATE/DELETE write new versions (and delete bits) into the WAL/snapshot lineage;
readers pin a manifest and always see a consistent table state. "Vacuum" = snapshot
compaction + GC of parts not referenced by any retained manifest version.

## 11. Gray & Graefe — *The Five-Minute Rule Ten Years Later* (SIGMOD 1999/2007)

Cache economics: keep a page cached when (access rate × miss cost) justifies the memory.
Buffers should be sized by workload value, not by maximalism.

**puddle takes:** the **bounded LRU column-chunk cache** as a first-class subsystem with a
byte budget (default modest — the "lightweight in RAM" contract), pin/unpin semantics for
scans in flight, and observability (hit rates) so the budget can be tuned with data. The
rustdis RAM discipline carries over: idle RSS should be single-digit MB, and tests *assert*
RSS ceilings.

## 12. Amazon S3 — operational properties (the "papers" are the announcements + docs)

- **Strong read-after-write consistency for all S3 operations since Dec 1, 2020** — no more
  read-your-writes gymnastics ([S3 consistency](https://hidekazu-konishi.com/entry/how_amazon_s3_achieves_consistency_and_durability.html)).
- **Conditional writes, Aug 2024**: `PutObject` with `If-None-Match: *` — create-if-absent,
  atomic, 412 on race ([AWS announcement](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes/)).
- **ETag compare-and-swap, Nov 2024**: `If-Match: <etag>` — write-only-if-current
  ([AWS announcement](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes/)).
- Request/byte economics: per-request pricing and 10s-of-ms first-byte latency ⇒ large
  sequential GETs, range GETs for column pruning, avoid small objects.

**puddle takes:** the **commit protocol**. Table state changes = CAS loop on the manifest
object: read manifest (get ETag) → write derived objects (WAL segments/parts, unconditional —
they're immutable, content-addressed by UUID) → `PutObject(manifest, If-Match: etag)` → on
412, re-read and retry. No lock service, no coordinator, no consensus client — S3 *is* the
linearization point. This is the single most enabling external fact for puddle's existence.

---

## Decision table

| # | Decision | Driven by | puddle choice |
|---|---|---|---|
| 1 | What is truth? | Aurora (#2) | WAL objects in the bucket; snapshots are derived |
| 2 | Table state | Snowflake (#3) | Versioned manifest of immutable parts + stats |
| 3 | Write path | C-Store (#5), LSM (#8) | Row WAL (WS) → columnar parts (RS) → compaction |
| 4 | Read path | X100 (#6), DuckDB (#7) | Vectorized scans over 64K-row parts, zone pruning |
| 5 | Part format | Dremel/Parquet (#9) | Per-column chunks, dict+RLE, one range-GET per chunk |
| 6 | Concurrency | POSTGRES (#10), Snowflake (#3) | Manifest-pinned snapshots, no-overwrite lineage |
| 7 | Commit atomicity | S3 conditional writes (#12) | ETag CAS on the manifest; S3 is the lock |
| 8 | Memory policy | Five-Minute Rule (#11) | Bounded LRU chunk cache; RSS ceilings in tests |
| 9 | Process model | Architecture of a DBS (#1) + rustdis E7 | Thread-per-connection + writer/snapshotter threads |
| 10 | Read addressing | Neon (#4) | LSN-stamped records; state@LSN = parts + WAL replay |

Next: `ARCHITECTURE.md` turns this table into concrete formats, protocols, and code layout.
