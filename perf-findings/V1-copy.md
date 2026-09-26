# V1-copy: the VReplication copy phase (MoveTables, Reshard, Online DDL)

Investigator V1-copy, BASE 30000, base commit aa9ccf9. Patch: `findings/V1-copy.patch` (uncommitted in worktree
`agent-ae7d4f5153ea00ba3`). Patched binaries: `/home/vt/bin-V1`. Harness and drivers: `perf-findings/V1-scripts/` in the
patch (copied from `/home/vt/perf/V1/`). The negative pipelined-copy prototype is kept separately in
`findings/V1-pipelined-copy-negative.patch`.

## Summary (ranked by user-visible impact)

| # | Change | Measured end-to-end effect | Size | Risk |
|---|---|---|---|---|
| 1 | **Online DDL: send JSON values as text in the copy phase** (`replicator_plan.go`, `table_plan_builder.go`), always on | Online DDL of a 250k-row table with a JSON column (+ #2): **copy 15.1 / 15.0 / 15.0 s -> 11.5 / 9.1 / 10.5 s**, mysqld CPU **15.2 -> 9.6-11.9 s (-30..-37%)**. Result is byte-identical to today's (verified), because Online DDL already converts the value to text on the target. | S | low |
| 2 | **Online DDL: defer non-unique secondary keys** until after the copy, like MoveTables (`onlineddl/vrepl.go`, `vreplicator.go`), behind experimental bit 16 | Online DDL of sbtest1 (1M rows, 1 secondary key): **copy 13.8 / 12.2 / 11.6 s -> 10.6 / 10.6 / 10.9 s**, mysqld CPU -9%. Bigger on tables whose secondary indexes do not fit the buffer pool (not reproducible at this scale). | S | low-medium |
| 3 | **MoveTables/Reshard: opt-in JSON-as-text copy** (experimental bit 8, per workflow via `--config-overrides vreplication-experimental-flags=15`) | MoveTables of the JSON table to 2 shards: **7.67 / 7.23 / 6.94 s -> 5.84 / 5.34 / 5.29 s (-24%)**, target mysqld **23.4 -> 16.7 s per 1M rows (-29%)**, target vttablet -12%. Changes JSON number typing (see below), so opt-in. | S | medium (semantics) |
| 4 | **Bug fix: extra copy connections lacked the stream's session settings** (`vreplicator.newClientConnection`): no `time_zone='+00:00'`, no `set names binary`, no net timeouts. Used by `--vreplication-parallel-insert-workers > 1`. | Correctness: TIMESTAMP values shifted by the server's time zone and non-utf8 string bytes re-interpreted when parallel insert workers are used. New test fails on main. | S | low |
| 5 | Workaround, no code: **copy tables concurrently by splitting a MoveTables into one workflow per table (or table group)** | 3 tables, 1.75M rows, 2 shards: **17.40 / 17.45 s -> 12.16 / 12.52 s (-29%)**, CPU per row about the same (+5% target mysqld). There is no flag for concurrent table copy inside one workflow. | - | operational (N switch-traffics, N binlog streams) |

Negative results: pipelined single-writer copy, larger INSERT statements beyond the packet, `innodb_ddl_buffer_size`, durable
commit settings vs packet size, shorter copy-phase durations. Details below. Already known (P4) and re-confirmed: 1 s per table
and per copy cycle, 1 MB `vstream-packet-size`, Online DDL review tick.

## Setup

- Cluster (`V1-scripts/cluster.sh`, P4's multi-keyspace harness): `src` unsharded (tablet 100), `dst` 2 shards (-80, 80-;
  tablets 101, 102), durability none, `innodb_flush_log_at_trx_commit=2`, `sync_binlog=0`, 512 MB buffer pool.
- Dataset (`load.sh`), ~600 MB on the source:
  - `sbtest1`: 1M rows (sysbench schema, `k` secondary key),
  - `sbtest2`: 500k rows,
  - `wide`: 250k rows of ~1 KB: `BIGINT` PK, `INT`, `DATETIME(6)`, `VARCHAR`, `DECIMAL`, a 660-byte `TEXT`, a small nested
    `JSON` document (ints, doubles, strings, bool, array, object), 3 secondary keys (one composite, one prefix).
- MoveTables (`mt.sh`): `MoveTables create --tables sbtest1,sbtest2,wide` src -> dst, timed until both streams are `Running`
  with an empty `copy_state`. CPU per process from `/proc`, per 1M rows copied (all shards). MySQL counters from
  `SHOW GLOBAL STATUS` and `SHOW BINARY LOGS` before/after.
- Online DDL (`oddl.sh`): `ALTER TABLE <t> ENGINE=InnoDB` with `--ddl-strategy vitess`. Copy duration = until the stream is
  `Running` with an empty `copy_state` (finer than the migration timestamps).
- Every measurement ran under `flock -o /home/vt/perf/bench.lock`. The machine was shared: **load average 3-17 during the
  runs, mostly 6-14**. Wall times vary by about ±10%; CPU per row is more stable. A/B arms alternate.

## Baseline

### MoveTables, 3 tables, 1.75M rows, unsharded -> 2 shards (base binaries)

| run | duration | rows/s | src vttablet | src mysqld | tgt vttablet (each) | tgt mysqld (each) |
|---|---|---|---|---|---|---|
| base-prof | 18.47 s | 94.8k | 2.94 s/1M rows | 1.93 | 1.06 / 0.98 | 7.34 / 7.25 |
| base-perf | 17.44 s | 100.4k | 3.01 | 1.75 | 1.05 / 1.07 | 7.36 / 7.38 |
| single-r1 | 17.40 s | 100.6k | 2.89 | 1.93 | 1.09 / 1.02 | 7.19 / 7.19 |
| single-r2 | 17.45 s | 100.3k | 2.98 | 1.78 | 1.06 / 1.11 | 7.50 / 7.41 |

- Total **~21 CPU-s per 1M rows copied**. The **target mysqlds use 69%** of it, the source vttablet 14%, the source mysqld 9%, the
  target vttablets 10%.
- Per table (separate runs):
  - `sbtest1`, 1M rows: 6.4 s, 156k rows/s. Target mysqld **4.56 s per 1M rows = 9.1 us per inserted row**.
  - `wide`, 250k rows: 6.8 s, 37k rows/s. Target mysqld **21.7-22.2 s per 1M rows = 43 us per inserted row**.
- Timeline of shard -80 (vttablet log), 18.5 s total:

  | step | sbtest1 (500k rows on this shard) | sbtest2 (250k) | wide (125k) |
  |---|---|---|---|
  | copy rows | 4.69 s | 3.16 s | 6.12 s |
  | post-copy `ALTER ... ADD KEY` (deferred keys) | 0.58 s | 0.37 s | 1.14 s |
  | wait before the next table (P4 #1) | 1.0 s | 1.0 s | - |

  Row copy is 76% of the wall time, deferred key builds 11%, the per-table waits 11%.
- `VReplicationTableCopyTimings` (time inside the insert+commit tasks) is 97% of the row-copy wall time: **the stream is bound
  by its single target connection**. The source is ahead, buffered by gRPC flow control.

### MySQL counters per 1M rows copied

| | source mysqld | each target mysqld (receives half of the rows) |
|---|---|---|
| Handler_read_next | 2,000,205 (each of the 2 streams scans every row) | 7,945 |
| Bytes_sent / Bytes_received | 607 MB sent | 162 MB received |
| Com_insert / Com_commit | - | 978 / 486: one row INSERT + one `copy_state` INSERT per commit, **~1030 rows per batch** |
| binlog bytes | - | 148 MB |
| Innodb_os_log_written (redo) | - | 185 MB |
| Innodb_data_written | 30 MB | 251 MB |
| Com_alter_table | - | 3 (deferred key builds) |

Captured statements (general log): per batch `begin` / `insert into wide(...) values (...), ...` (8-9 ms for ~270 wide
rows) / `insert into _vt.copy_state` (0.2 ms) / `commit` (0.2 ms), then ~1 ms until the next `begin`. One INSERT is ~250 KB
(`vstream-packet-size` and `relay-log-max-size` are both 250 KB).

### Where the target mysqld spends its time (perf, 14 s of the copy)

- One thread, the vreplication connection, is 76% of the mysqld samples. 26% of all samples are in the kernel:
  `write()` of binlog and redo into the page cache, and context switches.
- Top user-space symbols:
  - `MYSQLparse` 6.5%, `THD::cleanup_after_query` 4.4%, and `Item::itemize`, `PT_insert_values_list::contextualize`,
    `setup_fields`, `fill_record`, `Item::save_in_field`: the cost of a 250 KB textual multi-row INSERT,
    about 25% in total;
  - `validate_string` 1.6% (binary-charset literals into utf8mb4 columns);
  - `row_mysql_store_col_in_innobase_format`, `log_buffer_write`, `malloc`/`free`;
  - JSON: `json_value`, `get_json_object_member_name`, `Item_func::fix_func_arg` (see #1/#3).

### Replaying the vcopier's statements directly on a target mysqld (`sqlab.sh`, 2 rounds, us of mysqld CPU per row)

| workload | mysqld CPU/row | vs vitess form |
|---|---|---|
| sbtest, 1000 rows per INSERT (vitess form) | 7.70 / 8.25 | - |
| sbtest, 4000 rows per INSERT | 7.40 / 7.35 | -5% |
| sbtest, 250 rows per INSERT | 9.45 / 9.60 | +20% |
| sbtest, `sql_log_bin=0` | 6.30 / 6.40 | **binlog = ~18%** (not an option in production) |
| sbtest, with the `k` key present (not deferred) | 10.30 / 11.45 | **+35%** |
| wide, JSON as `JSON_OBJECT(...)` (vitess form) | 38.3 / 36.3 | - |
| **wide, JSON as `_utf8mb4'<json text>'`** | **24.8 / 24.8** | **-33%** |
| wide, `sql_log_bin=0` | 34.8 / 32.2 | binlog = ~9% |
| wide, with its 3 keys present | 47.2 / 48.7 | **+27%** |

Redo is ~1.2x and binlog ~0.9x the row size, in every variant.

### Online DDL baseline (base binaries)

| table | copy | rows/s | mysqld CPU | vttablet CPU | total (review ticks, P4 #3) |
|---|---|---|---|---|---|
| sbtest1 (1M) | 10.7-13.8 s | 73-93k | 11.2-12.2 s | 2.4-2.6 s | 23.4-23.5 s |
| wide (250k) | 12.6-15.1 s | 17-20k | 14.3-15.3 s | 2.4-3.2 s | 22.8-23.8 s |

Online DDL keeps all secondary keys on the shadow table during the copy (`defer_secondary_keys=false`), unlike MoveTables.

## 1. Online DDL: JSON values are built as documents only to be printed back to text

**Cause.** The copy phase reads JSON columns as MySQL's JSON text. `appendFromRow` turns every value into a
`JSON_OBJECT(_utf8mb4'k', v, ...)` / `JSON_ARRAY(...)` expression, so the target builds the document from SQL literals. For
Online DDL, the filter for a JSON column is `convert(j using utf8mb4)` (`onlineddl/vrepl.go`), so the target statement is:

```
insert into _vt_vrp_...(id,j,t) values (1,convert(JSON_OBJECT(_utf8mb4'dec', 1.50, ...) using utf8mb4),'numbers'), ...
```

The target parses the literals, builds a JSON document, prints it back to text, and parses that text into the JSON column.
The result is exactly what inserting the source text would give.

**Fix.** At plan time, the table plan builder records the fields whose target expression is `convert(<field> using utf8mb4)`
(`TablePlan.TextConvertedFields`). For those, `appendFromRow` writes the source JSON text as `_utf8mb4'...'`:
`convert(_utf8mb4'{"dec": 1.50, ...}' using utf8mb4)`. There is no flag, because the stored bytes do not change:

- `jsontest.sh` builds a table with 12 tricky rows and compares the Online DDL result with an `INSERT ... SELECT` copy. The rows
  cover decimals, doubles, uint64 max, int64 min, escapes (`\"`, `\\`, `\%`, `\_`, newline, tab, `\u0001`), non-ASCII and
  emoji, arrays, JSON null, SQL null, scalars, DATE/DATETIME/TIME, opaque binary, deep nesting, a single quote, and 1e300.
- The diffs are **identical on base and patched**. Both are pre-existing: decimal `1.50` becomes double `1.5`, and
  temporal/opaque values become strings, because of the text round trip.

`TestCopyJSONAsTextWhenConvertedToText` builds the plan from a `convert(j1 using utf8mb4)` rule and checks the INSERT. It fails
on main.

**Effect.** It removes the `JSON_OBJECT` construction and one JSON print/parse per value on the target, and the JSON scan in
the vttablet. On the target mysqld it also removes the memory blowup that #19878 and #19916 fought for large documents.

## 2. Online DDL: deferred secondary keys

**Change.**
- Online DDL workflows are created with `defer_secondary_keys` when vttablet runs with experimental bit 16
  (`VReplicationExperimentalFlagOnlineDDLDeferSecondaryKeys`).
- `supportsDeferredSecondaryKeys` includes OnlineDDL.
- For Online DDL, `stashSecondaryKeys` keeps UNIQUE keys. The migration may use one to identify rows, and a migration that adds
  a unique key must still fail as soon as the copied rows violate it.
- It also keeps FULLTEXT keys (as today) and keys needed by foreign keys (as today).

The post-copy `ALTER TABLE ... ADD KEY` runs before the stream leaves the copy phase, so the migration only becomes ready once
the keys exist.

Tests:
- `TestDeferSecondaryKeys`: the OnlineDDL case now expects the key to be stashed. It fails on main, where OnlineDDL returns
  "not supported".
- New case: OnlineDDL keeps the unique key and stashes the other one.
- New MoveTables case: pins that MoveTables stashes unique keys too.
- `TestGenerateInsertStatementDeferSecondaryKeys` covers the onlineddl side.

Alone (bit 16 vs none, 2 rounds, `aboddl.sh`):

| | copy | mysqld CPU |
|---|---|---|
| sbtest1 base | 12.7 / 12.7 s | 12.3 / 12.8 s |
| sbtest1 deferred | 11.2 / 12.6 s | 10.5 / 11.6 s |
| wide base | 15.9 / 15.9 s | 16.0 / 16.0 s |
| wide deferred | 14.8 / 14.9 s | 15.9 / 15.3 s |

At this scale every index fits the 512 MB buffer pool, so incremental maintenance is cheap: +35% / +27% insert CPU in the
replay above, while the sorted build afterwards takes 0.5-1.1 s. Incremental maintenance of a secondary index much larger than
the buffer pool is random I/O per row, which is where deferral matters most (the reason MoveTables defaults to it). **This
setup cannot show that effect, so the numbers here are a lower bound.** Kept behind a flag for now. Suggested path: default on
for Online DDL in the next major.

## Final A/B, Online DDL (base binaries vs patched with bit 16; #1 + #2), 3 rounds alternating, `abfinal_oddl.sh`

| | base copy | patched copy | base mysqld CPU | patched mysqld CPU | base total | patched total |
|---|---|---|---|---|---|---|
| sbtest1 (1M) | 13.75 / 12.15 / 11.57 s | **10.59 / 10.61 / 10.88 s** | 11.65 / 12.15 / 11.47 s | 10.47 / 10.60 / 10.87 s | 23.5 / 23.4 / 23.3 s | **14.7 / 14.7 / 14.5 s** |
| wide (250k) | 15.15 / 15.03 / 15.04 s | **11.48 / 9.07 / 10.52 s** | 15.23 / 15.22 / 15.28 s | **9.64 / 10.27 / 11.89 s** | 22.8 / 23.7 / 23.8 s | 23.8 / 13.7 / 14.6 s |

Load average: 5-17.

- Copy: sbtest1 **-15%**, wide **-30%**. mysqld CPU: sbtest1 **-9%**, wide **-30%**.
- The "total" column is quantized by the executor's 10 s / 20 s review ticks (P4 #3). When the copy finishes before the 10 s
  tick fires, the migration completes ~9 s earlier. With P4's fix the totals would follow the copy times.

## 3. MoveTables / Reshard: JSON as text is faster but changes JSON number types

`VReplicationExperimentalFlagCopyJSONAsText` (bit 8; per workflow with
`--config-overrides vreplication-experimental-flags=15`) sends every JSON value as text.

Final A/B, MoveTables of `wide` (250k rows) to 2 shards, 3 rounds alternating (`abflags.sh`, load 8-17):

| | duration | src vttablet | tgt vttablet (avg) | tgt mysqld (avg) |
|---|---|---|---|---|
| flags 7 (today) | 7.67 / 7.23 / 6.94 s | 7.4-7.8 s/1M | 3.45 | **23.2** |
| flags 15 (JSON as text) | **5.84 / 5.34 / 5.29 s** | 7.2-7.4 | 3.03 | **16.7 (-29%)** |

On the 3-table set (earlier batch, old bit numbering), target mysqld went from 7.45 to 6.4-6.9 s per 1M rows.

**Why it is opt-in.** Without a text conversion on the target, the two forms store different JSON *types*:

- **Today (JSON_OBJECT):** every fractional number becomes a SQL DECIMAL literal. Verified on this cluster:
  `JSON_TYPE(attrs->'$.nested.x')` is `DOUBLE` on the source and **`DECIMAL` on the target after MoveTables**. The text is
  unchanged (`318.0`), so VDiff does not notice.
- **Text:** the target parses the text like a reload of `mysqldump`/`mydumper` output: doubles stay `DOUBLE`, but a
  source `DECIMAL` `1.50` becomes `DOUBLE` `1.5`, a text change that VDiff would report.
- Temporal and opaque JSON values become strings in both forms.

For JSON written by applications (parsed from text, so doubles), the text form is more faithful. For JSON built in SQL from
DECIMAL columns, today's form is more faithful. Whether to make it the default is a product decision. The running phase
(binlog -> JSON_OBJECT) keeps today's behavior in either case. With `--config-overrides`, a VDiff after the text copy
reported no mismatch on this dataset (750k rows, JSON doubles only).

## 4. Bug: parallel-insert-worker connections miss the stream's session settings

`vreplicator.newClientConnection` creates the extra connections that `--vreplication-parallel-insert-workers > 1` uses for the
copy. It set `sql_mode` and the foreign key settings, but not what `setDBClientSettings` sets on the stream's own connection:

- `time_zone='+00:00'`: TIMESTAMP values are encoded for UTC, so a target whose time zone is not UTC stores shifted values;
- `set names binary`: string literals are the raw column bytes, so non-utf8mb4 columns get their bytes re-interpreted as the
  default client charset;
- `net_read_timeout` / `net_write_timeout`.

Fix: apply `setDBClientSettings` in `newClientConnection`. `TestNewClientConnectionSessionSettings` sets the server time zone
to +05:00 and checks the worker connection's `time_zone` and charsets; it fails on main. I did not reproduce data corruption
end to end, because BRIEF3 says not to enable parallel insert workers; the settings difference is certain.

## 5. Concurrent table copy (workaround measured, no code)

A stream copies one table at a time. Tables cannot be copied concurrently within a stream, because the stream has a single
position: every table's rows must come from a snapshot at that position (`fastForward`). A second table's snapshot at a later
GTID would make the catchup vplayer re-apply events that the snapshot already contains.

Upper bound measured with one MoveTables workflow per table, all started together (`mtmulti.sh`, 2 rounds alternating):

| | duration | src vttablet | tgt vttablet | tgt mysqld |
|---|---|---|---|---|
| 1 workflow, 3 tables | 17.40 / 17.45 s | 2.89 / 2.98 s/1M | 1.02-1.11 | 7.19-7.50 |
| 3 workflows, 1 table each | **12.16 / 12.52 s (-29%)** | 2.68 / 2.73 | 0.99-1.04 | 7.59-7.98 |

This box has only 4 vCPUs, shared with another cluster. Target hosts with more cores would scale further, since each stream
is bound by one target connection.

Design options for a real implementation:
- **(a) N streams per target shard.** The workflow server creates one `_vt.vreplication` row per table group (the same as the
  workaround, inside one workflow). SwitchTraffic, journals and VDiff would need checking for more than one stream per source
  shard. The running phase then has N binlog streams on the source.
- **(b) One snapshot for several tables.** Lock several tables, open K snapshot connections with
  `START TRANSACTION WITH CONSISTENT SNAPSHOT`, read the GTID once, then stream the tables concurrently on K connections and
  insert them concurrently. This needs a new multi-table row stream RPC. It is closest to what `VStreamTables` (atomic copy)
  already does sequentially.

## Negative and neutral results

- **Pipelined single-writer copy** (`V1-pipelined-copy-negative.patch`). The VStreamRows callback builds the next INSERT while a
  dedicated connection writes the previous batch, still one commit at a time and in order.
  - MoveTables, 3 rounds: 18.3 / 18.9 / 19.4 s vs 18.2 / 18.4 / 19.4 s.
  - Online DDL: no better than without it.
  - Target vttablet CPU +15% (+75% before the buffer reuse fix).
  - The target insert thread is the bottleneck. The vttablet work that pipelining overlaps is ~10% of a batch, and on a
    loaded box there is no idle CPU to overlap it with.
- **Larger INSERT statements.** `relay-log-max-size=1MB` on top of a 1 MB packet (INSERTs of 1 MB instead of 4 x 250 KB):
  16.9 / 18.2 s vs 17.2 / 17.9 s, target mysqld the same. The replay shows -5% for 4x larger statements, not visible end to end.
- **1 MB `vstream-packet-size`** (P4 #5, re-measured): target vttablet -27%, source vttablet -5..-10%, target mysqld -2%,
  duration -3..-6% (noise level).
- **Durable commits** on the targets (`sync_binlog=1`, `innodb_flush_log_at_trx_commit=1`): 250 KB vs 1 MB packets gave
  11.1 / 11.6 s vs 10.2 / 11.3 s. fsync is cheap on this VM, so the per-batch commit cost does not show here. It would on
  slower storage.
- **`innodb_ddl_buffer_size=64M`** for the deferred key build: `ADD KEY` on wide 0.59 / 0.69 s vs 0.57 / 0.57 s, sbtest1
  0.46 / 0.46 vs 0.50 / 0.53 s. No gain at this size.
- **Short `vreplication-copy-phase-duration`** (2 s vs 1 h, sbtest1): 8.3 / 8.4 s vs 6.6 / 5.8 s. Every extra copy cycle
  costs exactly 1.0 s of `catchup`: the same 1 s ticker as P4 #1, which P4's patch also fixes for these cycles. The
  `MAX_EXECUTION_TIME` hint on the source select did not cause errors.
- **Binlog on the target** is ~18% (sbtest) / ~9% (wide) of the target mysqld's insert CPU. It is required, and there is no
  cheaper row image for inserts.
- **Things that are not issues:**
  - The source scan is a `force index (PRIMARY)` range scan: 1.75-1.93 s of source mysqld per 1M rows for 2 streams.
  - Snapshot and LOCK TABLES per table: milliseconds.
  - `copy_state`: one insert per batch plus a periodic GC delete, ~0.2 ms per batch.
  - `unique_checks` / `foreign_key_checks`: FK checks are already off, and `unique_checks` only helps secondary unique
    indexes, which MoveTables defers.
  - `innodb_autoinc_lock_mode`: explicit ids.
  - `bulk_insert_buffer_size`: MyISAM only.
  - LOAD DATA: needs `local_infile` and client support; not viable.

## Follow-ups

1. **Concurrent table copy** (5a or 5b). It is the only lever that scales one workflow past one target connection (~100k small
   rows/s or ~20k 1 KB rows/s per stream here).
2. **Reshard to N shards reads each row N times** (P4 follow-up; 2x `Handler_read_next` here). A shared scan per source.
3. **JSON in the running phase.** The vplayer also builds `JSON_OBJECT` from binlog JSON (same DECIMAL typing). A typed text or
   `CAST(... AS JSON)` path with an explicit decision on number types would give one consistent behavior across the copy and
   running phases.
4. **Overlap the deferred `ADD KEY` of table N with the copy of table N+1** (11% of the wall time here). Requires keeping the
   table's `copy_state` until the async ALTER finishes and waiting for all ALTERs before leaving the copy phase.
5. **Validate deferred keys for Online DDL on a table larger than the buffer pool** before making bit 16 the default.
6. **Reproduce the parallel-worker time zone bug end to end**, and the latin1 one, on a non-UTC target.

## Tests

- New, each fails on main (checked by reverting only the fix and running the compiled test binary):
  `TestCopyJSONAsTextWhenConvertedToText`, `TestCopyJSONAsTextExperimentalFlag`, `TestNewClientConnectionSessionSettings`,
  `TestDeferSecondaryKeys` (updated OnlineDDL cases). `TestGenerateInsertStatementDeferSecondaryKeys` does not compile on main
  (new flag constant).
- Passing: full `go/vt/vttablet/tabletmanager/vreplication` (MySQL-backed, run as `vt`), `go/vt/vttablet/onlineddl`,
  `go/vt/vttablet/common`, `go vet`, `scripts/fmt` on all changed Go files.
- End to end, both checked by `jsontest.sh`, `jsontest_mt.sh` and VDiff:
  - the Online DDL JSON output is identical to base;
  - MoveTables with JSON as text: VDiff clean on 750k rows.

## Environment notes

- The shared box ran another investigator's CPU-heavy workload: load average 3-17, mostly 6-14. Durations are noisy (±10%),
  CPU per row is more stable. Every copy measurement ran inside the bench lock.
- Disk:
  - fixed the harness binlog purge: `PURGE BEFORE NOW()` missed the just-rotated file, so it now purges up to the current
    file;
  - removed the replay SQL files;
  - kept the cluster directory under ~3 GB.
- The cluster is down and the data directory, test directories and `/home/vt/bin-V1` are removed.
