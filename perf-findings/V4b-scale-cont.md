# V4b-scale-cont: SwitchTraffic outage, many tables, many workflows (continuation of V4-scale)

Investigator V4b-scale-cont, BASE 40000, base commit aa9ccf9. Patch: `findings/V4b-scale-cont.patch`. It is the full worktree diff:
V4-scale's patch (P4's changes, V6 #5/#6, F17 hunks, V4's changes) plus the changes listed in "Code changes" below.
Harness and drivers: `findings/V4b-scripts/` (copied from `/home/vt/perf/V4b/`). Raw logs: `findings/V4b-raw/`.

Cluster (one etcd, vtctld, vtgate): `src:0` (2 sysbench tables x 200k rows), `dst2:-80,80-`, `many:0` (500 or 2000 small tables of
100 rows: `id BIGINT PK, k INT, c VARCHAR(64), ts DATETIME, KEY(k)`), `mdst:0`. One primary per shard, durability none, 256 MB buffer
pool. The other investigator (V3, VDiff) ran on BASE 30000 at the same time; load average 1.3-10 during measurements (recorded
per run in the raw logs). Every measurement ran under `flock -o /home/vt/perf/bench.lock`; clusters were never (re)started inside it.

Binaries ("arms"):

| arm | what |
|---|---|
| base | `/home/vt/bin` (aa9ccf9) |
| ref | aa9ccf9 + `P4-vreplication.patch` only (rebuilt from a clean tree; identical sizes to V4's `bin-V4ref`) |
| new | V4-scale's full patch: ref + V6 #6 (no sleep after the last LOCK TABLES cycle, `ReloadSchema: false`) + no second `allowTargetWrites` + V6 #12 rule index + per-table `copyTable` plan + V4's vstreamer changes |
| new2 | new + WaitForPos fast re-poll (this work) |
| new3 | new2 + per-table primary key query in `GetTableForPos` (this work) |
| new4 | new3 + one schema reload after dropping the workflow's tables (this work) |
| new5 | new4 + `GetSchema` reads only the requested tables from `information_schema.tables` (this work) |
| new6 | new5 + `--vstream-coalesce-empty-transactions-window` (this work; off by default) |

## Summary (ranked by user-visible effect)

| # | Change | Measured effect | Size | Risk |
|---|---|---|---|---|
| 1 | SwitchTraffic write outage: V6 #6 + no 2nd `allowTargetWrites` (V4) + **WaitForPos first re-poll after 5 ms, doubling to 100 ms** (`engine.go`) | Client-visible write gap (400 writes/s prober, 6 runs/arm): **base 0.28-2.30 s (bimodal), P4 0.29-0.40 s, new 0.16-0.27 s, new2 0.16-0.18 s**. ReverseTraffic 0.29-0.32 -> 0.17 s. | S | low |
| 2 | MoveTables of 2000 small tables: V4 plan-builder fix + **per-table PK query under `se.mu`** (`schema/engine.go`) + **`GetSchema` filtered by table name** (`mysqlctl/schema.go`) | Copy **172-182 s -> 109-112 s (-38%)**. Source mysqld CPU 26-28 -> 10 ms/table, target vttablet 26-27 -> 12 ms/table, target mysqld 48 -> 41 ms/table. 500 tables: within noise on duration (26-29 s), source mysqld 12 -> 8 ms/table. | S | low |
| 3 | **Workflow cancel/complete reloads the schema once per tablet instead of once per dropped table** (`traffic_switcher.go`) | Cancel of a 2000-table MoveTables: **fails after 46-56 s** ("keyspace does not exist: node doesn't exist: lease": the keyspace lock expired; tables dropped but the workflow left behind) -> **succeeds in 10-12 s**. | S | low |
| 4 | Flag: `MoveTables create --defer-secondary-keys=false` for schemas of many small tables | 500 tables 26-27 s -> **13.4 s**; 2000 tables (new5) 110 s -> **64 s**. Target mysqld 35 -> 16 ms/table (500). The per-table post-copy `ALTER TABLE ... ADD KEY` costs more than building a key while inserting 100 rows. | flag | none (per workflow) |
| 5 | Prototype `--vstream-coalesce-empty-transactions-window=20ms` (with `--vstream-coalesce-empty-transactions`) | 50 small workflows on one source, 200 trx/s: **target vttablet 2.06 -> 1.02 ms per source trx (-51%)**, source vttablet 2.58 -> 2.14 ms (-17%); all processes -27%. SwitchTraffic gap: see section 1.5. | S | medium (experimental; consumers see positions up to 20 ms later) |
| 6 | Finding (not fixed): `_vt.vreplication` heartbeat and position updates log the full row, including `source` (all filter rules) | One idle 2000-table workflow writes **198 KB/s = 700 MB/h of target binlog**; 180 KB of target binlog per replicated 349-byte source transaction. Small workflows: 1.07 KB/s (3.7 MB/h) per idle stream. `--vreplication-heartbeat-update-interval=10` -> 20 KB/s. | flag / M-L for a fix | see section 3 |

## 1. SwitchTraffic write outage

Workflow `wf_dst2`: MoveTables `sbtest1,sbtest2` src -> dst2 (2 streams). `switch.sh`: P4's prober (4 threads x 100 UPDATEs/s through
vtgate) runs 10 s; after 4 s, `MoveTables switchtraffic` (default: all tablet types) and later `reversetraffic`. Metric: longest gap
between successful writes. `steps.py` splits the switch into steps from the vtctld log. `abswitch.sh`: 3 rounds; in each round every
arm restarts vttablets+vtctld (mysqld, vtgate kept; readiness = every Running stream recorded a heartbeat after the restart), then
runs 2 switch+reverse pairs. 6 switches and 6 reverses per arm, 48 runs, no client errors in any run.

### 1.1 Results

| arm | SwitchTraffic gap, runs (ms) | median | ReverseTraffic gap median (range) | command |
|---|---|---|---|---|
| base | 2282, 290, 313, 2304, 2280, 279 | 1296 | 301 (291-322) | 0.48-2.66 s |
| ref (P4) | 400, 399, 394, 288, 286, 380 | 387 | 290 (283-312) | 0.50-0.79 s |
| new | 161, 272, 260, 169, 181, 269 | 220 | 176 (168-197) | 0.37-0.51 s |
| **new2** | 173, 171, 173, 180, 182, 160 | **173** | **171 (166-173)** | 0.37-0.41 s |

Step durations (median, ms; from vtctld log timestamps):

| arm/action | gap starts after "Stopping source writes" | stop writes | LOCK TABLES phase | catch-up | reverse streams | journal | 2nd allowTargetWrites | routing rules | gap ends after routing saved |
|---|---|---|---|---|---|---|---|---|---|
| base switch | 18 | 19 | 216 | 1013 (10 or 2015) | 24 | 3 | 14 | 6 | 7 |
| ref switch | 17 | 20 | 217 | 110 (11 or 112) | 26 | 3 | 13 | 6 | 9 |
| new switch | 18 | 20 | 108 | 62 (10 or 114) | 24 | 3 | 0 | 7 | 10 |
| new2 switch | 15 | 18 | 107 | 15 (10-18) | 25 | 3 | 0 | 7 | 10 |
| new2 reverse | 25 | 23 | 107 | 13 | 24 | 4 | 0 | 8 | 8 |

Observations:
- **Writes are held, not failed.** The source tablet rejects writes with "enforce denied tables" as soon as its RefreshState applies
  the deny list (~15-25 ms into "Stopping source writes"). vtgate retries such a query after waiting for a newer SrvVSchema
  (`plan_execute.go`, `waitForNewerVSchema`), so clients see one slow write, not an error. The gap ends 7-11 ms after vtctld saved the
  routing rules (RebuildSrvVSchema -> vtgate watch -> replan -> execute on the target).
- **Catch-up is bimodal on base and ref.** If the last source transaction before the deny list was relevant to a stream, the stream
  saved its position and the first WaitForPos poll succeeds (10-16 ms). If it was filtered out (50% per shard here), the position is
  unsaved: base waits for the vplayer idle timeout + its 1 s poll (2.0 s); P4 asks for a flush and polls again after 100 ms (110 ms).
  3 of 6 base switches and 4 of 6 ref switches hit the slow case. new2 re-polls after 5, 10, 20, 40, 80, then every 100 ms, so the
  flushed position is seen within 5-10 ms: 10-18 ms in 12 of 12 runs.
- **LOCK TABLES phase**: ref = LOCK (with schema reload, ~12 ms) + 100 ms + LOCK + 100 ms; new = LOCK + 100 ms + LOCK (~3 ms each).
  V6 #6 removed 110 ms.
- The second `allowTargetWrites` (13-19 ms: shard record updates and RefreshState on all source and target tablets) is gone in new.
- The two changes together (new vs ref) and WaitForPos (new2 vs new) are both clearly above run-to-run variance (range 160-182 vs
  286-400).

### 1.2 What remains (new2, ~170 ms) and what could be cut

| step | ms | what could be done |
|---|---|---|
| wait between the two LOCK TABLES cycles | 100 | **Largest item, 60% of the remaining outage.** The pause lets writes that passed the deny-list check before the first lock reach mysqld. A tablet-side barrier would make it deterministic and ~0 ms: after RefreshState installs new query rules, wait until every query that was checked under the previous rules has finished (a rules generation plus an in-flight counter per generation in the query executor), then a single LOCK TABLES cycle is enough. Size M, touches the query path. |
| 2 x LOCK TABLES RPC | 6-8 | - |
| create reverse streams | 22-28 | Serial per stream: delete a stale reverse workflow + VDiff delete RPC + insert + a separate update of cell/tablet_types/options. Delete before stopping writes and fold the update into the insert: ~10-15 ms. |
| WaitForPos + stop streams + PrimaryPosition | 10-18 | new2 already. |
| gap before "Stopping streams" (source refresh) | 15-25 | Part of the switch before the outage starts; the source tablet applies the rule mid-step. |
| journal | 3-5 | - |
| routing rules + RebuildSrvVSchema | 6-9 | - |
| vtgate: SrvVSchema watch, replan, execute | 7-11 | - |

With the barrier and the reverse-stream change, the outage would be ~50-60 ms.

### 1.3 Test for the WaitForPos change
`TestWaitForPosPollsSoonAfterFirstCheck` sets the maximum poll interval to 10 minutes and expects three polls within a 30 s context: it
fails with a fixed ticker (P4 and main). `TestWaitForPosCancel` now pins the first interval (it expects exactly one poll before the
engine closes).

### 1.4 Coalescing window vs SwitchTraffic
See the end of this report (section 5).

## 2. Many tables: MoveTables --all-tables many -> mdst (one stream), tables of 100 rows

`mtmany.sh`: time from `MoveTables create` until copy_state is empty, state Running and pos set; CPU per process; source/target
`Questions` and `Connections`. `abmany.sh` alternates arms (restart outside the lock); the workflow is cancelled outside the lock.

### 2.1 Results

500 tables (2 rounds, load 2-6):

| arm | create | copy (incl. create) | src vttablet | src mysqld | tgt vttablet | tgt mysqld (ms/table) |
|---|---|---|---|---|---|---|
| ref | 5.7, 5.0 s | 28.8, 27.6 s | 5.9, 5.6 | 12.3, 11.6 | 12.1, 11.7 | 36.1, 35.7 |
| new | 5.4, 6.2 s | 26.9, 28.4 s | 5.8, 5.4 | 11.7, 11.2 | 9.7, 9.1 | 35.6, 35.3 |
| new3 | 5.1, 5.6 s | 27.2, 25.6 s | 6.0, 5.4 | **8.7, 7.9** | 10.3, 9.2 | 38.0, 34.4 |
| new3 + `--defer-secondary-keys=false` | 5.2, 5.2 s | **13.4, 13.4 s** | 5.1, 4.9 | 7.5, 7.6 | 6.0, 5.8 | **15.8, 15.9** |

2000 tables (load 2-6):

| arm | copy (incl. create ~21-28 s) | src vttablet | src mysqld | tgt vttablet | tgt mysqld (ms/table) | cancel |
|---|---|---|---|---|---|---|
| ref | 181.8, 172.5 s | 7.0, 7.0 | 27.7, 25.7 | 26.9, 25.6 | 49.4, 47.7 | **failed** (46 s, lock lease lost) |
| new | 163.8 s | 7.2 | 28.1 | 14.5 | 50.4 | failed (47 s) |
| new3 | 130.7, 125.6 s | 5.8, 5.4 | 10.7, 9.7 | 13.7, 12.9 | 48.8, 47.0 | failed (56, 46 s) |
| new4 | 134.5, 129.7 s | 5.5, 5.8 | 9.9, 10.5 | 13.0, 13.3 | 48.6, 48.1 | **10.3, 11.4 s** |
| **new5** | **111.9, 109.1 s** | 5.7, 5.9 | 10.3, 10.5 | 12.3, 12.3 | **40.7, 40.8** | 11.8, 11.5 s |
| new5 + `--defer-secondary-keys=false` | **64.1 s** | 5.5 | 9.9 | 8.8 | 19.9 | - |

- The plan-builder fixes (V6 #12 index + per-table plan in copyTable: `new` vs `ref`) cut target vttablet CPU per table in half at
  2000 tables (26 -> 14 ms) and ~20% at 500 tables; duration -8% at 2000 tables (one run), noise at 500.
- **The per-table PK query (new3 vs new) is the largest duration win at 2000 tables (164 -> 128 s):** the source runs it under
  `schema.Engine.mu`, on the critical path of every table's row streamer.
- **GetSchema filtered by name (new5 vs new4): -17% duration, target mysqld -16%.**
- `create` (vtctld + CREATE TABLE on the target) is ~10-11 ms per table and did not change.

### 2.2 Per-table work (general log of source and target during a 500-table copy, new3)

Source mysqld, per table copied: **6 new connections** and ~36 statements. The row streamer: a snapshot connection (lock tables, start
transaction with consistent snapshot, gtid_executed, unlock), a streaming connection (session settings + select), `getExtColInfos`
(a new connection + `information_schema.columns`), `GetTableForPos` (under `se.mu`: `information_schema.COLUMNS` names, `select ...
where 1 != 1`, and the primary key query), the fast-forward vstreamer (binlog connection: SHOW BINARY LOGS, SHOW REPLICA STATUS,
hostname/port, history length).

- **V6 #13 quantified.** `GetTableForPos` ran `BaseShowPrimary`, which reads the primary keys of **every table of the schema**, to
  refresh one table: **6-12 ms at 500 tables, 20-28 ms at 2000 tables** vs 0.8 ms for the per-table query (measured with
  `SHOW PROFILES` on the source mysqld). It runs under `se.mu`, so it serializes every stream start, schema reload and `GetSchema`
  caller of the tablet. Fixed: `populateTablePrimaryKeys` with `mysql.ShowPrimaryForTable`. Source mysqld per table at 2000 tables:
  26-28 -> 10 ms.
- The remaining per-stream-start cost (~3 new connections, 4 information_schema queries) is small next to the target's work. Using the
  schema engine's pool in `getExtColInfos`, and caching `extColInfo` per table (V6 #13 second part) are follow-ups.

Target mysqld, per table: ~72 statements (41 selects), 2 new vt_filtered connections and 1 vt_allprivs connection, and:
- the post-copy `ALTER TABLE ... ADD KEY k (k)` (deferred secondary key): **13 ms wall** plus a schema reload on a 100-row table;
- **two `GetSchema` calls for one table** (`getTableSecondaryKeys`: when stashing keys and in the post-copy action), each of which read
  `information_schema.tables` for the **whole schema** and filtered in Go: 10-17 ms at 2000 tables vs 0.9 ms filtered. Fixed in
  `collectBasicTableData` (adds `AND table_name IN (...)` when all requested names are plain names).
- 6 reads of the `_vt.vreplication` row, 4 `count(distinct table_name)` on copy_state, 2 `vreplication_log` inserts and 2 selects.

### 2.3 Cancel / Complete with thousands of tables

`removeTargetTables` (cancel) and `removeSourceTables` (complete) drop the tables one by one with `ExecuteFetchAsDba{ReloadSchema:
true}`: a full schema reload on the tablet after every DROP, O(T^2). At 2000 tables the cancel took 46-56 s, the keyspace lock lease
expired and vtctld returned `mdst keyspace does not exist: node doesn't exist: lease`. The tables were dropped, but the workflow's
streams were left behind (the next `create` failed with "workflow wfm already exists") until cancel was run again.

Fix: `ReloadSchema: false` per statement and one `tmc.ReloadSchema` per tablet after its tables (reload errors are only logged, as
before). Cancel: 10.3-11.8 s, succeeds. `TestWorkflowDelete` now asserts one schema reload per target tablet (fails on main: 3).
The legacy `wrangler` copy has the same loop; not changed.

### 2.4 Deferred secondary keys

`--defer-secondary-keys` (MoveTables default true) drops secondary keys before the copy and re-adds each with an `ALTER TABLE` after
the table is copied. For small tables that costs more than it saves: 500 tables 26-27 -> 13.4 s, 2000 tables 110 -> 64 s. A size
threshold (defer only for tables above N rows/bytes, using the source's `information_schema.tables` estimates that vtctld already
reads at create time) would get both behaviors. Not implemented; the flag is per workflow today.

## 3. Many workflows per tablet: 20 and 50 concurrent small workflows

`mkwf.sh`: W MoveTables workflows many -> mdst, 2 tables each (100 rows). `idle.sh` 60 s with no traffic; `manyload.sh`: 200
single-row UPDATE trx/s on the source mysqld over the moved tables (each trx is relevant to 1 of the W streams). Arm new5/new6.

### 3.1 Creation
W=20: 9.1 s (0.45 s per workflow, created serially); 30 more: 13.6 s. All running 0.5 s after the last create.

### 3.2 Idle cost (60 s)

| | W=0 | W=20 | W=50 | per stream |
|---|---|---|---|---|
| target mysqld | 0.005 cores | 0.021 | 0.029 | ~0.5 millicore |
| target vttablet | 0.011 | 0.018 | 0.025 | ~0.3 millicore |
| source vttablet / mysqld | 0.009 / 0.006 | 0.011 / 0.005 | 0.014 / 0.006 | ~0.1 millicore |
| target `_vt.vreplication` updates | 0 | 22.0/s | 55.0/s | 1.1/s (one per 900 ms vstreamer heartbeat) |
| **target binlog growth** | 0 | 21.4 KB/s (73 MB/h) | 53.4 KB/s (183 MB/h) | **1.07 KB/s, 3.7 MB/h** |
| source mysqld threads | 8 | 48 | 78 | 1-2 (binlog dump) |
| MySQL sockets: source / target vttablet | 12 / 33 | 72 / 73 | 132 / 141 | 2.4 / 2.2 |
| goroutines: source / target vttablet | 112 / 110 | 229 / 410 | 409 / 884 | 6 / 15 |
| vttablet RSS source / target | 120 / 96 MB | 104 / 97 MB | 110 / 115 MB | < 0.5 MB |

Idle CPU is negligible (about 1 millicore per stream). **What dominates at rest is the heartbeat write**: each stream updates
`time_updated`/`time_heartbeat` of its `_vt.vreplication` row after every source heartbeat. With `binlog_row_image=FULL` each update
logs the before and after images of the whole row, including `source` (the filter rules, one per table) and `pos`:
- 2-table workflow: ~1 KB per update, 3.7 MB/h per stream.
- **2000-table workflow (source column 102 KB): 198 KB/s, 700 MB/h of target binlog while idle**, replicated to every target replica.
  `--vreplication-heartbeat-update-interval=10`: 20 KB/s. (The idle lag shown by `workflow show` and used by SwitchTraffic's
  max-lag check is `now - time_updated`, so the interval must stay well below `--max-replication-lag-allowed`, 30 s by default.)
- The same full-row image is logged by every position update in the running phase: **180 KB of target binlog per replicated 349-byte
  source transaction** for the 2000-table workflow at 200 trx/s (1.2 KB for a 2-table workflow). Target mysqld 1.3 ms per trx.

Fix options (not implemented): `SET binlog_row_image` for the heartbeat statement needs SESSION_VARIABLES_ADMIN, which vt_filtered
does not have, and `binlog_row_image` cannot be set with a `SET_VAR` hint (tested: warning 3637). The structural fix is to keep the
frequently updated columns (pos, time_updated, time_heartbeat, transaction_timestamp, rows_copied) out of the row that holds `source`
(a narrow sidecar table, or `source` moved out), or to serve time_updated from the tablet's in-memory heartbeat (it already records it
in `vr.stats`) and write it rarely. Both need a staged, N-1-compatible rollout.

### 3.3 Under load: every stream pays for every source transaction

200 trx/s, 30 s (us per source trx):

| | W=1 | W=50 (2 runs) | per extra stream | W=50 + coalescing (no window) | **W=50 + 20 ms window (2 runs)** |
|---|---|---|---|---|---|
| source vttablet | 409 | 2555-2639 | 45 | 2584 | **2141, 2144** |
| source mysqld (incl. the sysbench statements) | 665 | 1985-2043 | 27 | 2023 | 2048, 2061 |
| target vttablet | 518 | 2062-2096 | 32 | 2032 | **1021, 1026** |
| target mysqld | 787 | 864-904 | 2 | 900 | 868, 878 |

- Every stream has its own binlog dump connection and parses and sends every transaction, even when it is empty for its filter. At
  200 trx/s, 50 streams cost ~1.3 cores for what one stream does in 0.2. Source profile (W=50): gRPC loopy-writer write syscalls
  20%, binlog reads 13%, `parseEvent` 15%, scheduling ~20%: per-event fixed costs, one write per stream per transaction.
- `--vstream-coalesce-empty-transactions` (V4) does nothing here: it only holds an empty transaction while more binlog events are
  queued, and at 200 trx/s each transaction arrives alone.
- **Prototype: `--vstream-coalesce-empty-transactions-window`.** A caught-up stream holds an empty transaction for up to the window
  (a later one replaces it; any send before the timer, including a heartbeat, carries it first). With 20 ms: target vttablet -51%,
  source vttablet -17%; source mysqld unchanged (the dump threads still send every event). `TestVStreamCoalescesEmptyTransactionsWithinWindow`
  (MySQL-backed, passes). Cost: a consumer sees an empty-transaction position up to the window later (section 5).
- The structural fix for the source side is a shared binlog reader per source tablet (one dump connection and one parse, fanned out
  to the vstreamers by filter), as V4 proposed for the running phase.

## 4. Empty-transaction coalescing A/B, 2 more rounds (running-phase drain, 2 streams src -> dst2)

`abdrain.sh` / `drain.sh` from V4 (dst2 only): stop the workflow, 30k oltp_write_only or 60k oltp_update_non_index trx on the source,
start, time until both targets reach the source position. Binaries new6; arms by flag. us per source trx:

| case | arm | round 1 src vttablet / tgt vttablets (sum) | round 2 |
|---|---|---|---|
| write_only | none | 85.0 / 91.0 | 87.3 / 96.0 |
| write_only | coalesce | 84.3 / 92.7 | 88.3 / 101.6 |
| write_only | coalesce + 20 ms window | 85.7 / 90.7 | 80.7 / 83.3 |
| update_non_index | none | 39.3 / 32.1 | 36.3 / 29.6 |
| update_non_index | coalesce | 36.8 / 28.5 | 36.0 / 29.0 |
| update_non_index | coalesce + 20 ms window | 35.2 / 26.7 | 38.0 / 30.0 |

- write_only with 2 streams: no difference beyond noise (each write_only transaction touches rows of both shards, so almost none is
  empty for a stream). V4's single +25% run is not reproduced; coalesce round 2 was +6% on the target, within the spread.
- update_non_index with 2 streams: -3 to -8% on both sides, at the noise level. V4 measured -17-20% on the target with 4-6 streams,
  where most transactions are empty for a stream.

## 5. Coalescing window vs SwitchTraffic

new6 binaries (all changes), 2 rounds x 3 switch+reverse pairs per arm, alternating, load 1-3.5:

| arm | SwitchTraffic gap (ms) | median | catch-up | ReverseTraffic gap median |
|---|---|---|---|---|
| no window | 160, 169, 171, 173, 162, 159 | 165 | 10-14 ms | 166 |
| coalescing + 20 ms window | 193, 169, 180, 161, 160, 160 | 165 | 14-19 ms | 173 |

No measurable penalty: the empty transaction held by the window is flushed during the ~107 ms LOCK TABLES phase, before WaitForPos
starts. A waiter that starts within the window after the last source transaction (e.g. an Online DDL cut-over on a busy source) can
wait up to the window longer. These runs also confirm the new2 result with all changes: 159-173 ms.

## Code changes (this work, on top of V4-scale's patch)

1. `vreplication/engine.go`: `WaitForPos` re-polls after `waitForPosMinPollInterval` (5 ms), doubling up to `waitForPosPollInterval`
   (100 ms). Tests: `TestWaitForPosPollsSoonAfterFirstCheck` (new), `TestWaitForPosCancel` (pins the first interval).
2. `go/mysql/schema.go`, `tabletserver/schema/engine.go`: `ShowPrimaryForTable` and `populateTablePrimaryKeys`, used by
   `GetTableForPos` for the one table it refreshes. `TestGetTableForPos*` "cache initialized, table found" now register only the
   per-table query (they fail on main).
3. `mysqlctl/schema.go`: `collectBasicTableData` adds `AND table_name IN (...)` when the request names tables (no regexps).
   `TestGetSchemaReadsOnlyRequestedTables` (fails on main).
4. `vtctl/workflow/traffic_switcher.go`: `removeSourceTables` and `removeTargetTables` (tables and shards) use `ReloadSchema: false`
   per statement and `reloadSchemaAfterTableRemoval` once per tablet. `framework_test.go` counts reloads; `TestWorkflowDelete` asserts
   one per target tablet (fails on main).
5. `vttablet/common/flags.go`, `vstreamer/vstreamer.go`: `--vstream-coalesce-empty-transactions-window` (default 0 = V4's behavior).
   Flag docs in `go/flags/endtoend/vttablet.txt` and `vtcombo.txt`. `TestVStreamCoalescesEmptyTransactionsWithinWindow`.

Compatibility: all are local to one binary; no protocol or schema change. The window flag is experimental and off by default (see V4's
note on moving coalescing to a `VStreamOptions` field set by the consumer).

## Tests

- Full `go/vt/vttablet/tabletserver/vstreamer` suite (MySQL 8.0.46, as vt, from the package dir): PASS, 193 tests/subtests.
- Full `go/vt/vttablet/tabletmanager/vreplication` suite: 454 passed, 1 failed (`TestWaitForPosCancel`, broken by change 1; fixed by
  pinning the first poll interval; `-run 'TestWaitForPos|TestEngine'` then PASS). The full suite was not re-run after that fix.
- `go test` of `go/vt/vtctl/workflow`, `go/vt/vttablet/tabletserver/schema`, `go/vt/mysqlctl`, `go/mysql`, `go/vt/discovery`,
  `go/vt/binlog`, `go/vt/vttablet/onlineddl`: pass. `go/vt/mysqlctl/s3backupstorage` `TestClientInitializationEmptyBucket` fails in
  this sandbox (AWS CA bundle / proxy environment), unrelated.
- `go vet` and `scripts/fmt` on all changed files.

## Negative / neutral results

- `--vstream-coalesce-empty-transactions` without a window: no effect at 200 trx/s with 50 streams, none in 2-stream drains.
- Plan-builder fixes alone: no measurable duration change at 500 tables (the target mysqld dominates); -8% at 2000 tables.
- Low-rate per-transaction CPU (409-800 us/trx on the source vttablet at 200 trx/s) is syscall and wakeup overhead per event, not
  table count: profiles show futex/scheduling and one read + one write syscall per event.
- Idle CPU per stream is ~1 millicore; memory per stream < 0.5 MB. Not worth optimizing.

## Follow-ups

1. Tablet-side deny-list barrier to replace the 100 ms LOCK TABLES pause (-100 ms of the remaining ~170 ms outage).
2. Narrow heartbeat/position row (or in-memory `time_updated`) to stop logging the `source` blob on every heartbeat and position
   update (700 MB/h idle for a 2000-table workflow).
3. `--defer-secondary-keys`: size threshold per table.
4. Shared binlog reader per source tablet for many streams (source mysqld/vttablet cost per stream per transaction).
5. V6 #13 remainder: pooled connection + cached `extColInfo` in the vstreamer; per-table copy_state/vreplication reads on the target
   (6 reads of the vreplication row per table).
6. The legacy `wrangler` drop loops and `dropTargetVReplicationStreams`/`optimizeCopyStateTable` paths have the same per-table patterns.
