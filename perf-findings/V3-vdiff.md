# V3-vdiff: how long VDiff takes, and what it costs, before switching traffic

Investigator V3-vdiff, BASE 30000, base commit aa9ccf9. Patch: `findings/V3-vdiff.patch` (uncommitted in worktree
`agent-a1a22c48515c3b1fb`; it includes the V6 VDiff hunks (#7 compare fast path, metrics fix) and my harness under
`perf-findings/V3-scripts/`). Negative prototype: `findings/V3-p4-waitforpos-backoff-negative.diff`.

## Summary (ranked by user-visible impact)

| # | Change | Measured end-to-end effect | Size | Risk |
|---|---|---|---|---|
| 1 | **Bug: restarting a target vttablet while a VDiff table diff initializes orphans the workflow's topo lock for 24 h** (`NamedLockTTL`) and leaves the stream stopped at the VDiff snapshot position. Fix: restart the streams through the local VReplication engine (and, if it is closed, write the stream state directly) instead of a tabletmanager RPC to itself (`table_differ.go`). | Reproduced twice on base: after a normal (SIGTERM) restart, the lock stays with a 86400 s lease. Every later VDiff of the workflow hangs retrying, and **`SwitchTraffic` (even `--dry-run`) fails after 45 s with `failed to lock the dst/wf workflow`**. The stream stayed `Stopped at position ...`, and `Workflow start` does not clear `stop_pos`. Patched: the lock is released 3 ms into shutdown and the streams are `Running` after the restart. `TestRestartTargetVReplicationStreams` fails on main. | S | low |
| 2 | **VDiff init is slow per table and serialized across target shards** under a keyspace-wide lock. P4's WaitForPos fix (100 ms poll plus position flush) cuts it; I measured that end to end for VDiff. | 30 small tables (1000 rows each), 2 target shards, 50 unrelated writes/s on the source: **64.8 / 64.5 s -> 10.0 / 11.7 s** (P4 fix alone). The same with my patch: 64.1 / 64.0 s -> 9.8 / 9.7 s. Per table and shard, `syncing_target_streams` goes from 1.0 s to 0.1 s. Each shard waits for the other shards' inits, so the cost is (shards x per-table init) per table. | S (P4) | low-medium (P4) |
| 3 | **Row pipeline: read the shard streams directly in the diff goroutine** instead of `engine.MergeSort` plus `primitiveExecutor`, which passed every row through 2 channels and 2 extra goroutines. Also **take the rows out of the pooled gRPC response instead of `CloneVT`** (`row_iterator.go`, `table_differ.go`). | Target (VDiff) vttablet CPU **3.54 -> 2.72 s per 1M rows compared (-23%)** alone. Combined with #4: see the final A/B below. | S-M | low |
| 4 | **V6 #7 measured end to end: byte-equality fast path before `NullsafeCompare`** (`table_differ.go`). | Target vttablet CPU **3.54 -> 2.60 s per 1M rows (-27%)** alone. The `wide` table (JSON) per-shard diff time 2.87 -> 1.86 s, because the JSON was parsed on both sides for every row. | S | low |
| 5 | **`vtctldclient VDiff create --wait` only checked completion every `--wait-update-interval` (1 min by default).** It now checks every 1 s, backing off to 10 s, and prints progress on the interval as before. | **60.09 / 60.07 s -> 7.12 / 7.18 s** for a 5-6 s VDiff. `TestWaitForVDiff`. | S | low |
| 6 | **V6 metrics bug confirmed end to end and fixed** (V6's hunk): `VDiffRowsCompared` read 13,250,030 for a 500,030-row table (base). Patched: 500,030. | observability | S | low |

**Final A/B (#3 + #4 + #1 + #6, base vs patched, 3 rounds alternating, load 3.4-6.1):** MoveTables of 1.5M rows
(sbtest1 1M, wide 250k TEXT+JSON, coll 250k with a utf8mb4_general_ci PK), unsharded to 2 shards.
- **VDiff wall time 7.21 / 6.98 / 6.57 s -> 5.05 / 4.81 / 4.72 s (-30%).**
- **Target vttablet 3.82 -> 2.03 CPU-s per 1M rows compared (-47%).**
- **Whole cluster 14.1 -> 10.1 CPU-s per 1M rows (-28%).**
- Source vttablet, source mysqld and target mysqld are unchanged.

## Setup

- Cluster: P4/V1's multi-keyspace harness (`V3-scripts/cluster.sh`), `src` unsharded (tablet 100), `dst` -80/80- (101, 102),
  512 MB buffer pool, `innodb_flush_log_at_trx_commit=2`, `sync_binlog=0`. The same 4 vCPUs run all 7 processes.
- Dataset (`load.sh`), about 580 MB on the source:
  - `sbtest1`: 1M rows;
  - `wide`: 250k rows of about 1 KB, with a 660-byte TEXT and a nested JSON document;
  - `coll`: 250k rows. Its PK is `VARCHAR(64) utf8mb4_general_ci`, with mixed-case, accented values. It also has a
    `utf8mb4_general_ci` group column and a `utf8mb4_unicode_ci` note column, and uses the `xxhash` vindex.
- Workflow `wf` = MoveTables of the 3 tables. Copy: 18.1 s, 82.7k rows/s (not the subject here; see V1).
- `vd.sh` runs `VDiff create` and polls `_vt.vdiff.state` on both target primaries every 0.2 s. It records the
  per-process CPU from `/proc`, the MySQL counters, `VDiffPhaseTimings` and `VDiffRowsCompared`, and optionally pprof.
- `ab.sh` restarts only the target vttablets for each arm (outside the lock) and runs `vd.sh` under
  `flock -o /home/vt/perf/bench.lock`.
- Default VDiff options. The target shards have only primaries, so the VDiff picks the target primary for its own
  target stream, and the source primary for the source stream.

## Baseline

### Wall time and CPU (base binaries; first runs, load 3.1-3.8)

| run | wall | rows compared/s | src vttablet | src mysqld | tgt vttablet (each) | tgt mysqld (each) |
|---|---|---|---|---|---|---|
| base1 | 7.50 s | 200k | 3.07 s/1M rows | 2.37 | 4.11 / 4.01 | 0.66 / 0.63 |
| prof1 | 6.47 s | 232k | 2.66 | 1.69 | 3.63 / 3.71 | 0.57 / 0.57 |

- Per target shard, the table diffs run sequentially. Per-table phases (ms, tablet 101):

  | table | init (lock, stop, pick, snapshots, restart) | diffing_table | rows on this shard |
  |---|---|---|---|
  | sbtest1 | 114-199 | 2412-2882 | 500k |
  | wide | 102-181 | 2793-3082 | 125k |
  | coll | 156-188 | 630-671 | 125k |

- **The VDiff tablet (target primary) is the most expensive process: 4 CPU-s per 1M rows compared, 8 us per row on its
  shard.** The whole run used about 3 of the 4 vCPUs, so on this box the wall time follows total CPU.
- MySQL-side work per 1M rows compared (`VERBOSE=1`):
  - source mysqld: `Handler_read_next` 2.0M, `Innodb_rows_read` 2.0M, 630 MB sent. **Every target shard's stream reads
    every source row**, and the `in_keyrange` filter drops the other shard's half in the source vttablet (the known
    "reshard reads N times" issue, P4 follow-up).
  - each target mysqld: `Handler_read_next` 0.5M (its own half), 158 MB sent.
  - 81 `UPDATE`s: progress every 10k rows plus state changes. 240 `SELECT`s: WaitForPos polls and state reads.
  - 2 `LOCK TABLES` per table snapshot.
  - Negligible: progress writes, `copy_state`-like bookkeeping, `ANALYZE` (off by default).

### Where the VDiff tablet spends its CPU (prof1, 6 s, 4.48 s of samples)

| cumulative | what |
|---|---|
| 1.14 s (25%) | the diff loop (`tableDiffer.diff`), of which **`compare` 0.88 s**: `NullsafeCompare` per column. **0.53 s of it is `json.Parser` parsing the JSON column on both sides** (`valueToEval` -> `compareJSON`). |
| 0.66 s (15%) | receiving both gRPC streams (`streamOneShard`): unmarshal, plus **`CloneVT` of every response (0.20 s)** |
| 0.48 s + scheduler | **`engine.MergeSort` for one shard per side.** Each row went through `shardStreamer.result` -> `runOneStream` -> `handle.row` (cap 10) -> `MergeSort` -> a 1-row `sqltypes.Result` -> `pe.resultch` (cap 1) -> `diff`. `runtime.selectgo` alone was 0.56 s (12.5%), plus `schedule`/`park_m`/`findRunnable`. |
| 0.47 s (10%) | the rowstreamer serving the target side to itself over gRPC (VDiff streams its own shard through `tabletconn`) |
| ~0.6 s | GC (`gcBgMarkWorker`), from the per-row allocations above |

Source vttablet: `rowStreamer.streamQuery`, with `shouldFilter` -> `getKeyspaceID` -> `vindexes.Map` -> `Hash` (DES) at 26%.
Both target shards' streams hash every source row (F17 covers the filter overhead).

## 1. Orphaned workflow lock and stopped stream after a target restart during VDiff init (bug)

**How I hit it.** My A/B harness restarts the target vttablets between arms. After one restart, every VDiff hung in
`initializing`: it retried `Locking workflow dst/wf for VDiff ... failed ... deadline exceeded` with backoff for 15+
minutes. `etcdctl` showed a `dst/wf` named-lock key from 14:16:10 with a **24 h lease** (`topo.NamedLockTTL`).
`VDiff delete all` blocked for about 7 minutes. `MoveTables SwitchTraffic --dry-run --tablet-types rdonly` failed after
45 s: `failed to lock the dst/wf workflow: deadline exceeded`.

**Cause** (vttablet log of the restarted tablet):
1. `tableDiffer.initialize` holds the workflow's named lock from stopping the streams until it has restarted them.
   It defers `restartTargetVReplicationStreams`, and before it (in LIFO order) the unlock.
2. The restart used `ct.tmc.VReplicationExec(ctx, thisTablet, ...)`, **a gRPC call to the tablet itself**, with a new
   `context.Background()` + `BackgroundOperationTimeout` (60 s).
3. On SIGTERM, the gRPC server stops first ("Initiated graceful stop of gRPC server"). The self-RPC then retries
   `connection refused` on `localhost:<grpc port>`.
4. The OnTermSync hooks time out after 10 s and the process exits before the deferred unlock runs.
5. Result: the lock is orphaned for 24 h, and the stream keeps the `stop_pos` that the VDiff set in
   `syncTargetStreams`. After the restart it stops there ("Stop position ... already reached"). `Workflow start` did
   not clear it (I had to clear `stop_pos` by hand, `fixwf2.sh`). The same happens on any crash inside the init window.
6. The init window is short when the source is idle (about 100-200 ms per table and shard). With writes on the source it
   is about 1 s per table and shard, times the number of shards, because of the lock queue (#2).

**Also:** the restart loop declared `_, err := ...` inside the loop, so it always returned nil and restart failures
were never logged.

**Fix** (`restartTargetVReplicationStreams`):
- Use the local engine, the same way `stopTargetVReplicationStreams` already does:
  `ct.vde.vre.ExecWithDBA(ReplaceTableQualifiers(query))`.
- If that fails because the engine is closed (tablet shutting down or demoted), write `state='Running', stop_pos=''`
  through the VDiff's own DB connection. The streams then resume normally when the engine opens.
- Fix the shadowed `err`.

**Verified end to end** (`locktest.sh`: VDiff of 30 tables under writes, SIGTERM restart of tablet 101 during init):

| binary | lock after restart | stream 101/wf2 after restart |
|---|---|---|
| base | key `.../dst/wf2/locks/...142364` left with a 24 h lease (86355 s remaining) | `Stopped` (stop position reached) |
| patched | none; "Unlocking named dst/wf2" 3 ms after "Firing OnTermSync hooks" | `Running` |

**Test:** `TestRestartTargetVReplicationStreams`, for an open and a closed engine; the tabletmanager RPC fails as it does
during shutdown. On main (the same test adapted to main's signature, run through a `go test -overlay`), it fails:
`expected "Running", actual "Stopped"`.

Not changed: `syncTargetStreams` also uses a self-RPC to set `stop_pos`. It runs under the VDiff context, which the
engine cancels on shutdown, so it does not hang.

## 2. Per-table VDiff init: WaitForPos, and serialization across shards

**Test:** workflow `wf2` = 30 tables of 1000 rows (`small.sh`), and `writes.sh` inserting 50 rows/s into `src.other`, a
table that is not in any workflow (any real source has some writes). Two rounds per arm.

| arm | wall | `syncing_target_streams` per shard (30 tables) | `initializing` per shard | `diffing_table` per shard |
|---|---|---|---|---|
| base | **64.8 / 64.5 s** | 29.2-30.2 s | 62-63 s | 0.12 s |
| P4 WaitForPos fix only (`bin-P4`) | **10.0 / 11.7 s** | 2.9-3.0 s | 8.3-8.4 s | 0.10 s |
| V3 (this patch) | 64.1 / 64.0 s | (as base) | (as base) | (as base) |
| V3 + P4 | 9.8 / 9.7 s | 2.96 s | 8.2-8.4 s | 0.08 s |

- On base, every table's `syncing_target_streams` takes 1.0 s: `Engine.WaitForPos` polls every 1 s. The target
  stream's saved position lags the source snapshot GTID, because the stream does not save positions after filtered-out
  transactions (P4 #2).
- **This time is spent while holding the keyspace-wide `dst/wf` lock**, so the other shard waits: `initializing`
  (62 s) is twice `syncing_target_streams` (30 s).
- With S target shards, each table costs about **S x (per-shard init)** of wall time, however small the table:
  - base: S x ~1.1 s;
  - with P4: S x ~0.14 s.
- 99.8% of this VDiff's wall time is init.
- P4's fix (already proposed for SwitchTraffic) makes this VDiff 6.5x faster. That is its biggest measured effect outside
  the traffic switch.
- `TestWorkflowLockRetriesWithMultipleShards` (64 shards) shows that the serialization is by design.

**What remains (per table and shard, with P4):** about 140 ms under the lock:
- the stream restart and catch-up in `syncTargetStreams` takes about 100 ms. That is the time for the restarted
  controller to reconnect to the source, stream and save the position. It is not poll granularity.
- tablet picking about 12 ms, source snapshot 13 ms, target snapshot 10 ms, stop/restart 3-5 ms.

**Negative:** a 10 ms -> 100 ms backoff poll in `WaitForPos` on top of P4 (`V3-p4-waitforpos-backoff-negative.diff`):
10.3 / 10.3 s -> 11.2 / 12.2 s. That is no gain (noise or slightly worse), consistent with the 100 ms being stream-restart
latency.

**Options, not implemented:**
- (a) Pick the tablets before taking the lock (they don't need it).
- (b) Take one consistent snapshot for a group of small tables: `startSnapshotAllTables` exists. That cuts
  T x S inits to S, at the cost of a longer-lived read view.
- (c) A shared/exclusive workflow lock: VDiff shards shared, SwitchTraffic/Complete exclusive. Topo has no shared named
  lock today.

## 3. Row pipeline without per-row channel hops, and without CloneVT

**Before:** `diff()` pulled rows from `primitiveExecutor`, which ran `engine.MergeSort` over `shardStreamer`s.
- `MergeSort` runs one goroutine per input (`runOneStream`) that splits every batch into single rows on `handle.row`.
  It merges them and calls back with a 1-row `sqltypes.Result` (2 allocations).
- `primitiveExecutor` then forwards that on `pe.resultch` (cap 1) to the diff goroutine.
- That is 2 channel operations and at least 2 goroutine handoffs per row per side, even for one shard (always the case
  for the target, and for MoveTables from an unsharded source).

**Now** (`row_iterator.go`):
- `diff()` reads the `shardStreamer` batches itself.
- One shard: iterate the batch.
- Several shards: `mergeRowIterator` uses the same `evalengine.Merger` with the same `OrderByParams` as `MergeSort`
  (`pkOrderBy`, factored out of `newMergeSorter`), pulling the next row from the batch of the shard that won.
- Comparison panics are converted with `evalengine.PanicHandler`, as `MergeSort` does.
- Workflows with aggregates (Materialize `group by`) keep the `OrderedAggregate(MergeSort)` path unchanged.
- Existing tests that inject primitives still work: an unset `sourceStreamers`/`targetStreamers` falls back to them.

**CloneVT:**
- `gRPCQueryClient.VStreamRows` reuses one pooled response and `ResetVT`s it after each callback. So `streamOneShard`
  deep-copied every response.
- `takeVStreamRowsResponse` moves `Fields` and `Rows` out (and nils them in the pooled response). The next
  `UnmarshalVT` then allocates new rows instead of overwriting them.
- That is one copy of every value instead of two.
- `TestTakeVStreamRowsResponse` reproduces the pool reuse (Unmarshal, take, ResetVT, Unmarshal) and checks that the taken
  rows are intact.

Alone (arm `nofast`: merge + steal, no compare fast path): target vttablet 3.54 -> 2.72 s per 1M rows (-23%).

## 4. Compare fast path (V6 #7), measured

`if sv.Type() == tv.Type() && bytes.Equal(sv.Raw(), tv.Raw()) { continue }` before `NullsafeCompare`, for the PK and the
non-PK comparisons. Identical bytes of the same type are equal under every collation. NULL has type NULL on both sides.
Differences still take the old path.

Alone (`fastonly`): target vttablet 3.54 -> 2.60 s per 1M rows (-27%). Per-shard `diffing_table`:

| table | base | fast path | all (#3 + #4) |
|---|---|---|---|
| sbtest1 (500k rows/shard) | 2.40 s | 2.28 s | 1.90-1.94 s |
| wide, JSON (125k) | 2.87 s | 1.86 s | 1.86-2.11 s |
| coll, general_ci PK (125k) | 0.68 s | 0.54 s | 0.46-0.47 s |

(ab1 and ab4 means.) No mismatches reported in any run; the `coll` table's case-insensitive PK sorted identically.

## A/B tables

CPU in s per 1M rows compared. "tgt tablet" is the mean of the two target vttablets. "total" is src tablet + src
mysqld + 2 x (tgt tablet + tgt mysqld).

**ab1: components** (3 rounds; loads 2.4-21.7 because another investigator was restarting clusters; the wall times of
this batch are unreliable, the CPU per row is not):

| arm | wall (s) | tgt tablet | src tablet | src mysqld | tgt mysqld | total |
|---|---|---|---|---|---|---|
| base | 7.24 / 6.19 / 6.18 | **3.54** | 2.91 | 1.76 | 0.52 | 12.78 |
| fast path only | 6.06 / 4.78 / 4.91 | **2.60** | 2.84 | 1.80 | 0.52 | 10.88 |
| merge + steal only | 10.00 / 5.35 / 4.74 | **2.72** | 2.86 | 1.66 | 0.52 | 11.00 |
| all | 5.24 / 4.36 / 6.23 | **1.93** | 2.69 | 1.79 | 0.52 | 9.38 |

**ab4: final** (base vs final binary, 3 rounds alternating, load 3.4-6.1):

| arm | wall (s) | tgt tablet | src tablet | src mysqld | tgt mysqld | total |
|---|---|---|---|---|---|---|
| base | 7.21 / 6.98 / 6.57 | 3.82 | 3.18 | 1.87 | 0.71 | 14.12 |
| patched | **5.05 / 4.81 / 4.72** | **2.03** | 2.93 | 1.76 | 0.68 | **10.10** |

**ab2 / ab3: small tables with writes:** see #2.

**`--wait`** (`waitab.sh`, same VDiff): base vtctldclient 60.09 / 60.07 s, patched 7.12 / 7.18 s.

## Profile after the patch (target vttablet, 5 s, 2.20 s of samples)

| cumulative | what |
|---|---|
| 0.71 s (32%) | gRPC receive + `VStreamRowsResponse.UnmarshalVT` (`Row.UnmarshalVT` 0.32 s) for both streams |
| 0.39 s (18%) | the rowstreamer serving its own target side |
| 0.28 s | GC |
| **0.14 s (6%)** | the diff loop, including `compare` (0.06 s) |

- At this point **the source side sets the pace**. Each target shard's stream reads and hashes all 1M `sbtest1` rows to
  keep 500k: 1.9 s per table, about 526k rows/s read per stream.
- Levers there: F17 (vindex filter fast path, round 1), and one shared scan per source for N targets (P4 follow-up).

## Negative results and non-issues

- **WaitForPos backoff poll** (10 ms -> 100 ms) on top of P4: no gain (see #2).
- **In-process target stream** (skip gRPC when the target stream is this tablet): about 27% of the patched VDiff
  tablet's CPU is the self-gRPC path (marshal, transport, unmarshal).
  - The rowstreamer reuses its response and row objects, so a local path needs one copy.
  - In production the default `--tablet-types in_order:rdonly,replica,primary` usually streams the target side from a
    replica, not from the VDiff primary.
  - So not prototyped; follow-up.
- **Fixed sleeps:**
  - `--max-diff-duration` restarts sleep a flat 30 s (`time.Sleep`, not interruptible by stop or cancel). That is small
    next to the useful durations (hours).
  - The errored-VDiff retry ticker is 30 s.
  - Neither affects a normal VDiff.
- **Progress writes:** one `UPDATE` with the JSON report every 10k rows: 81 statements per 1M rows. Negligible.
- **Snapshots:** `LOCK TABLES ... READ` plus a consistent snapshot per table and side: milliseconds.
- **`--update-table-stats`:** `ANALYZE TABLE` per table on the target; off by default.
- **Table concurrency:** there is none. Tables run sequentially per shard, and the shards run in parallel.
  - On this box the run is CPU-bound (about 3 of 4 cores), so parallel tables would not help here.
  - On real hosts the per-shard pipeline is latency-bound. It is now about 260k rows/s per shard for sbtest1, bound by
    the source stream.
- **`VDiffRowsComparedTotal`** (V6 bug part 2, re-adding earlier attempts' rows after resume or `--max-diff-duration`):
  covered by V6's unit test, not reproduced end to end. The `VDiffRowsCompared` gauge was reproduced (#6).

## Follow-ups

1. **Land P4's WaitForPos fix.** For VDiff it is 6.5x on many-table workflows with any source write traffic. Then attack
   the per-table x per-shard init under the keyspace lock:
   - group small tables into one snapshot (option b);
   - pick tablets outside the lock;
   - a shared lock mode for VDiff shards.
2. **Source side:**
   - F17's vindex-filter fast path, measured under VDiff;
   - a shared scan when N target shards diff against one source (every row is read and hashed N times today; 2x
     `Handler_read_next` here).
3. **In-process target stream** when the target stream is this tablet (tablet types including primary with no
   replicas), to save about 25% of the VDiff tablet's CPU.
4. **`VDiff stop/delete` blocks while a controller waits for the workflow lock.** It took about 7 minutes here, until I
   deleted the orphaned key: the controller's `LockName` wait did not end on `ct.cancel`. Worth a look.
5. **Legacy `vtctl VDiff --wait`** (`go/vt/vtctl/vdiff2.go`) has the same 1-minute quantization. I did not change it,
   because vtctl is legacy.
6. **Workflow start** does not clear a leftover `stop_pos`. It is only relevant for streams left behind by old binaries
   (#1).

## Tests

- New:
  - `TestShardsRowIterator` (single shard with empty batches, merge in `utf8mb4` ci collation order, empty, errors,
    cancel);
  - `TestDiffShardStreamers` (diff over 2 source shards and 1 target: match, mismatch, extra rows on both sides);
  - `TestTakeVStreamRowsResponse`;
  - `TestRestartTargetVReplicationStreams` (open and closed engine);
  - `TestWaitForVDiff`;
  - V6's `TestUpdateTableProgressRowCounts`.
- **Checked to fail on main** (via `go test -overlay` with the base production files and the tests adapted to main's
  signatures): `TestRestartTargetVReplicationStreams` ("Running" vs "Stopped") and `TestUpdateTableProgressRowCounts`
  (20000 vs 30000).
- `TestWaitForVDiff` and the iterator/steal tests cover new functions, so they do not compile on main.
- Passing:
  - full `go/vt/vttablet/tabletmanager/vdiff` (MySQL-backed, run as `vt`): 177 tests, and again with `-race`: no races;
  - `go/cmd/vtctldclient/command/vreplication/vdiff`;
  - `go/vt/vtgate/engine`;
  - `go vet`, and `scripts/fmt` on all changed Go files.
- Compatibility:
  - no protocol or flag changes;
  - the VDiff results and `_vt` state are unchanged;
  - the `--wait` output is the same summary, now printed within about 1-10 s of completion.

## Environment notes

- Every measurement ran under `flock -o /home/vt/perf/bench.lock`.
- Load averages are in each table: 2.4-21.7 during ab1, when other investigators were restarting clusters, and 3.4-6.1
  for the final A/B.
- The CPU per row was stable within about ±10% through that; the wall times vary by about ±15%.
- The cluster is down. `/home/vt/perf/c30000`, `/home/vt/bin-V3*`, `/home/vt/bin-P4` and the test data dirs are removed.
