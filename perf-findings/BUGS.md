# Bugs found during the performance investigation

## Priority scale

- **P0:** silent wrong data. Fix now.
- **P1:** wrong query results, user-visible errors, or avoidable unavailability.
- **P2:** data races, latent crashes, degraded behaviour, misleading observability.
- **P3:** unreachable today, tests only, or cosmetic.

## Fix status

- **patch ready:** a fix plus a test that fails on main exists in the named patch.
- **fixed in patch:** fixed as a side effect of a performance patch, with no dedicated regression test.
- **open:** confirmed, not fixed.
- **unverified:** observed once; needs a repro.

## Ranked list (highest to lowest)

**Ranking criteria:**
- Severity: silent data corruption > wrong query results > outages or stuck operations > user-visible errors > races and degradation > observability > cosmetic.
- Reach: default code paths rank above opt-in flags and rare setups.
- Confidence: unverified reports rank lower.

The tables below give details for each bug.

**Validation levels (as of this list):**
- **R:** reproduced on a real cluster against unpatched code.
- **T:** a unit or integration test fails on `main`, but the bug hasn't been reproduced end to end.
- **P:** partly validated; some of the claim is proven, the rest only analysed.
- **C:** confirmed by code reading only.
- **U:** observed once and never reproduced.

The validation pass (VAL-A…D, reports `VAL-*.md`) is complete: every entry except the docs-only #32 is now R or T, or refuted. **IDs are stable** (the reports and patches cite them); the Rank column is the order to fix in. IDs 36–41 were found during validation.

**Upstream tracking (searched 2026-09-30, vitessio/vitess only; details in `ISSUES-A…C.md`):** only #11, #12, #15 and #30 have an exact issue or PR; #12 is fixed, #15's issue was closed with a mitigation only. Nothing merged after the base commit fixes any of these. GitHub rate-limited the searches (2–5 queries per bug), so "none" means none found.

| Rank | ID | Bug | Pri | Validated | Why this position | Status  Upstream |
|---|---|---|---|---|---|---|---|
| 1 | 40 | Online DDL `ALTER TABLE t RENAME COLUMN a TO b` (vitess strategy) completes and leaves `b` NULL in every row | P0 | R | **Silent data loss on a standard DDL statement.** `schemadiff.OnlineDDLAlterTableAnalysis` only maps `CHANGE COLUMN` renames (`onlineddl.go:278`), so `RENAME COLUMN` is treated as drop + add. `CHANGE COLUMN a b ...` keeps the data (VAL-A) | open; no upstream issue found | none |
| 2 | 2 | Copy phase turns JSON doubles into DECIMAL (MoveTables/Reshard) | P0 | R | Silent, on the **default** path, and invisible to VDiff; any JSON with floating-point numbers is affected | open (needs a consistent type rule) | related: [#19880](https://github.com/vitessio/vitess/issues/19880) (open; JSON type loss in the binlog path, not copy); fix [PR #20120](https://github.com/vitessio/vitess/pull/20120) closed unmerged |
| 3 | 4 | Parallel-insert-worker connections skip session setup (TZ, `set names binary`, timeouts) | P0 | R | Silent corruption with an opt-in flag: TIMESTAMPs shifted on a non-UTC target, and latin1 (non-utf8mb4) non-ASCII data corrupted **even on a UTC target** (904/905 rows, VAL-C). VDiff catches it; V1's fix gives 0 diffs | patch ready | none |
| 4 | 5 | PAD SPACE ignored in general_ci/latin1/unicode_ci/`_bin` Collate and Hash | P1 | R | Silent wrong query results whenever vtgate evaluates: GROUP BY, COUNT(DISTINCT), UNION, ORDER BY merge, HAVING, planner-chosen hash joins (1 row instead of 6). `utf8mb4_general_ci` is very common. The weight_string paths are wrong too (VAL-B) | open (behaviour change; see VAL-B for the fix plan) | none |
| 5 | 36 | `COUNT(*) FROM (SELECT DISTINCT v FROM t)` on a sharded table is scattered unchanged and the shard counts summed, so it over-counts (5 vs 3) | P1 | R | Silent wrong results for a common query shape, any collation (VAL-B) | open | none (closest: [#13578](https://github.com/vitessio/vitess/issues/13578), a different DISTINCT case, fixed 2024) |
| 6 | 37 | vtgate types `CONCAT(...,v,...)` / `IFNULL(v,'x')` over a general_ci column as `utf8mb4_0900_bin`, so it groups case-sensitively | P1 | R | Wrong GROUP BY results (1,2,2 vs MySQL 2,3), silently (VAL-B) | open | none |
| 7 | 7 | Semi-sync PRS has a ~1 s write outage (DemotePrimary waits for the ACK receiver) | P1 | R | Hits **every** planned reparent in semi-sync deployments, which is the common production setup | patch ready | none ([PR #18714](https://github.com/vitessio/vitess/pull/18714) added the `force` flag our fix uses) |
| 8 | 6 | Tablet restart during VDiff setup orphans the workflow lock for 24 h | P1 | R | A routine restart blocks VDiff and **SwitchTraffic** for a day and leaves the stream stopped; reproduced on base | patch ready | none |
| 9 | 1 | VReplication with an explicit column list that includes a target-generated column panics in the copy phase | P1 | R | VAL-A: no wrong data is possible (every skip panics before SQL runs), but the migration **wedges silently** (0 rows, no error; CANCEL/`Workflow stop`/`GetWorkflows` time out on the engine lock), and with parallel insert workers vttablet **crash-loops**. Trigger: `CHANGE COLUMN g h ..., ADD COLUMN g ... AS (...)`, or a Materialize expression naming a target-generated column | open | none |
| 10 | 9 | VTOrc polling off-by-one (every 6 s instead of 5) | P1 | R | Adds ~1 s to every unplanned failover's detection, fleet-wide | patch ready | none |
| 11 | 8 | VStream `minimize_skew` stalls with 3+ shards, then fails after 10 minutes | P1 | R | Deterministic (6/6 on 4 shards) for every CDC user of that option | patch ready | none |
| 12 | 10 | Cancelling a MoveTables with thousands of tables leaves a half-cancelled workflow | P1 | R | Inconsistent state that needs manual cleanup, but only for very large table counts | patch ready | related: [#20340](https://github.com/vitessio/vitess/issues/20340) (open; same lease expiry, on SwitchTraffic) |
| 13 | 11 | vtgate accepts a quoted-int `SET innodb_lock_wait_timeout`, then every query in the session fails | P1 | R (observed) | The session is poisoned, but it needs specific client input; an easy fix | open | **exact:** [#21150](https://github.com/vitessio/vitess/issues/21150) (open); fix draft [PR #21151](https://github.com/vitessio/vitess/pull/21151) |
| 14 | 13 | VTOrc tablet RPCs wait 15 s on a dead tablet (tmclient uses WaitForReady), so VTOrc holds the shard lock ~43 s after every failover | P2 | R | VAL-D: detection loses only ≤1 s, but after ERS the post-recovery refresh (~13 s) and StaleTopoPrimary recovery (~30 s) hold the shard lock; a PRS 1 s after the failover failed after 26.7 s. P1 if opt-in quorum ERS is used (it refreshes the dead primary first: +15 s to ERS, from code) | open | partial: [#12578](https://github.com/vitessio/vitess/issues/12578); its fix [PR #12870](https://github.com/vitessio/vitess/pull/12870) only covers the pre-recovery refresh |
| 15 | 14 | `VDiff stop/delete` hangs while another VDiff on the tablet waits for its workflow lock (engine-wide `snapshotMu` taken with a plain `Lock()`) | P2 | T | VAL-D: the lock wait itself cancels fine; the blocker is `snapshotMu`, held across the lock retry loop, while stop/delete call `controller.Stop()` under the engine mutex. With an orphaned lock (#6) it never clears. Affects other workflows too | open | none (cause came in with [PR #18998](https://github.com/vitessio/vitess/pull/18998)) |
| 16 | 41 | Graceful vttablet SIGTERM gives ~10 s of "connection refused" on a single-tablet target | P2 | R | VAL-D: gRPC graceful stop waits for vtgate health streams until `--onterm-timeout` (10 s); kill -9 gives 0.3 s. With a second replica clients see no errors | open | none |
| 17 | 15 | Heartbeats rewrite the whole `_vt.vreplication` row (rules included) into the binlog | P2 | R | ~700 MB/h of binlog for a large idle workflow can fill disks and slow replicas | open (flag mitigates) | **exact:** [#6805](https://github.com/vitessio/vitess/issues/6805), closed 2024 with only the interval-flag mitigation ([PR #7659](https://github.com/vitessio/vitess/pull/7659)) |
| 18 | 28 | `counters.String` emits invalid JSON for control characters | P2 | R | Any client can break `/debug/vars` (vtgate and vttablet) for every JSON consumer until restart, e.g. with the user name `bob\a` or a table named `t\x01x` (VAL-B). Monitoring only | open | related: [#12872](https://github.com/vitessio/vitess/issues/12872) (open; same `%q` problem in querylog) |
| 19 | 38 | With schema tracking off, cross-shard UNION merges different values (`'a '` and `'b'`) and hash joins return extra weight_string columns and a near cross-product | P2 | R | Wrong results, but only with schema tracking off (VAL-B) | open | none |
| 20 | 18 | Plan-cache sketch: `reset` corrupts counters and `indexOf` uses half of them | P2 | T | Worse plan-cache admission (up to −1.8 pp hit ratio); a miss costs +28% vtgate CPU | patch ready | none |
| 21 | 19 | Streaming `FetchNext` returns uncapped slices | P2 | T | Latent column overwrite (no caller appends, VAL-C checked all paths). The default-on consolidator over-counts memory ~(ncols+1)/2 (6.1x for 20 columns), so late identical queries join for ~255 rows instead of ~1530 | open (1-line fix; both tests pass with it) | none |
| 22 | 23 | mysqld shutdown waits 2 s for Vitess's own idle connection | P2 | R | +2–3 s on every backup, restore and tablet shutdown | patch ready | none |
| 23 | 17 | `processExactKeyRange` sorts the shared cached shard list in place | P2 | T | A data race on the srvtopo cache, only with unsorted partitions | fixed in patch | none |
| 24 | 16 | `Throttler.checkScope` mutates a shared global result | P2 | T | A data race (`-race` confirms). The effect is cosmetic: wrong `app_name`/summary in check responses, even after the throttler is disabled; no throttling decision or metric changes (VAL-C). P3 defensible | open | none |
| 25 | 20 | `QueryPlanCacheSize` shows ~4% of the real size | P2 | T | Operators can't see a full plan cache | patch ready | partial: [#16592](https://github.com/vitessio/vitess/issues/16592) (closed; a different plan-cache metric) |
| 26 | 21 | VDiff progress metrics over-report (50x) and double-count after a resume | P2 | R | Misleading progress | patch ready | related: [#20877](https://github.com/vitessio/vitess/issues/20877) (open; recount on resume, not the gauges) |
| 27 | 25 | `VDiff --wait` checks for completion only once a minute (vtctldclient and legacy vtctl) | P3 | R | A slow CLI return, no correctness impact | patch ready (vtctldclient) | none |
| 28 | 24 | Relay-log stall-flag race | P3 | T | A spurious stall error (Send checks the flag before re-checking for room); needs the 5-min timer to fire within µs of a Fetch; the stream just retries | open | related: [PR #20925](https://github.com/vitessio/vitess/pull/20925) (merged, in base) reworked the stall timer; our patch needs a rebase |
| 29 | 3 | `select *` rules drop ConvertCharset / ConvertIntToEnum | P3 | T | Unreachable through supported operations: only Online DDL sets conversion rules, and it always uses an explicit column list (VAL-A). Needs a hand-written `_vt.vreplication` row. Side note: the copy phase never applies int→enum even with an explicit list; Online DDL avoids it with `CONCAT(col)` | open | none |
| 30 | 22 | A nil `ConvertCharset` entry panics vreplication | P3 | T | Unreachable: no rule parser can produce a nil entry (VAL-A) | fixed in patch | none |
| 31 | 26 | `Collation_binary.Hash` panics when `numCodepoints > len` | P3 | T | No production caller today: every caller passes 0 (VAL-B). Could drop to P4 | open | none |
| 32 | 27 | `charset.Convert` panics on a destination buffer smaller than 4 bytes | P3 | T | No current caller | fixed in patch | none |
| 33 | 29 | `Timings.Reset` race | P3 | T | Only tests call it | fixed in patch | none |
| 34 | 39 | `metro.Metro128.Sum128()` finalizes in place, so a second call returns a different hash | P3 | T | Test pitfall; found by VAL-B | open | none |
| 35 | 30 | vtexplain test flake (background `wait_timeout` query) | P3 | R | Flaky CI | open | **exact:** fix [PR #21059](https://github.com/vitessio/vitess/pull/21059) (open) |
| 36 | 31 | servenv cgroup tests fail in containers | P3 | R (env?) | Test robustness | open | none; open [PR #21269](https://github.com/vitessio/vitess/pull/21269) edits the same test file |
| 37 | 32 | Wrong "not thread safe" doc comment on the throttler client | P3 | C | Docs only | fixed in patch | none |
| 38 | 12 | vtgate took ~40 s to route to a restarted tablet | closed | refuted | CANNOT REPRODUCE (VAL-D): 31 restarts (primary/replica, kill -9/SIGTERM, 0–52 s down), worst 6.3 s. The reconnect backoff is capped at 10 s since #19967; the original run likely predates it | closed | **exact, fixed:** [#19894](https://github.com/vitessio/vitess/issues/19894) by [PR #19967](https://github.com/vitessio/vitess/pull/19967) (merged before base) |
| – | 33–35 | Upstream: Go `utf8.RuneCount` allocation; missing `VZEROUPPER` in the simd experiment; grpc-go BDP pings per round trip | – | – | Report to Go / grpc-go | – | – |

## P0: silent wrong data

| Bug | Where | Impact | Status | Source |
|---|---|---|---|---|
| Online DDL `RENAME COLUMN a TO b` drops the column's data: `b` is NULL in every row after the migration completes | `go/vt/schemadiff/onlineddl.go` `OnlineDDLAlterTableAnalysis`: only `*sqlparser.ChangeColumn` fills `ColumnRenameMap`, `*sqlparser.RenameColumn` is ignored, so the filter omits the column | Silent data loss for a standard MySQL 8 statement with the default vitess strategy | open; `repro_rename_column.sh` in `VAL-A.patch` reproduces it on a cluster | VAL-A |
| VReplication `select *` rules drop `ConvertCharset` / `ConvertIntToEnum` (VAL-A: unreachable through supported operations, moved to P3) | `vreplication/replicator_plan.go` (table plan built for `select *`) | MoveTables / Online DDL with charset or int→enum conversion rules copy unconverted values without any error | open; a test in `replicator_plan_test` can show it | F16, F30 #17a |
| An explicit column list that selects a column generated on the target breaks `appendFromRow` | `replicator_plan.go` `appendFromRow` skip loop (no bounds check) | Panics with index out of range. VAL-A: shifted values can't actually be written, since the copy row has exactly as many values as slots; the panic wedges the copy (default) or kills vttablet (parallel insert workers). Moved to P1 | open; `TestExplicitColumnListWithTargetGeneratedColumnName` and a MySQL-backed test fail on main | F30 #17b, VAL-A |
| VReplication copy phase silently turns JSON doubles into DECIMAL (MoveTables/Reshard); VDiff doesn't detect it | copy phase JSON handling (`replicator_plan.go` / `table_plan_builder.go`) | Target JSON documents differ in number types from the source (`JSON_TYPE` DOUBLE → DECIMAL) | open; V1 adds an opt-in "JSON as text" mode that keeps doubles (but turns decimals into doubles); a consistent rule is still needed | V1 #3 |
| `--vreplication-parallel-insert-workers` connections skip the session setup: no UTC time zone, no `set names binary`, no network timeouts | vcopier parallel insert worker connections | On a non-UTC target, TIMESTAMP values are shifted; non-utf8mb4 columns (e.g. latin1) with non-ASCII data are corrupted on **any** target, UTC included (opt-in flag) | patch ready (unit test fails on main; reproduced end to end by VAL-C: 904/905 rows differ, 0 with the fix) | V1 #4, VAL-C |

## P1: wrong results, user-visible errors, avoidable outages

| Bug | Where | Impact | Status | Source |
|---|---|---|---|---|
| PAD SPACE not honoured: `'a'` vs `'a '` compare unequal and hash differently | `colldata` `Collate`/`Hash` for general_ci, latin1 ci, legacy unicode_ci, utf8mb4_bin (0900 collations are NO PAD, so not affected) | Wrong results whenever vtgate evaluates: cross-shard filters, GROUP BY/DISTINCT/UNION, hash joins, aggregation. `utf8mb4_general_ci` is very common in 5.7-era schemas. | open; the fix is a behaviour change (trim a trailing all-space tail in `Collate`, trim trailing spaces in `Hash`). **VAL-B:** leaving `WeightString` alone only fixes plans where vtgate knows the column collation. MySQL's `WEIGHT_STRING()` keeps trailing spaces, so weight_string-based plans (no schema tracking, views, untyped expressions) need PAD-aware weight comparison too. Coordinate with F14. `TestPadSpaceCollateAndHash` fails on main | F30 #16, F14, VAL-B |
| vtgate accepts `SET SESSION innodb_lock_wait_timeout = '26'` (a quoted int that MySQL rejects), then every later query in the session fails with errno 1232 | vtgate SET handling / settings pool | The session is poisoned; clients see errors on unrelated queries | open; validate at SET time | P2 |
| Restarting a target vttablet during VDiff setup orphans the workflow lock in topo with a 24 h lease | `vdiff/table_differ.go`: the stream restart is a gRPC call from the tablet to itself after its gRPC server stopped | For up to 24 h, later VDiffs hang and SwitchTraffic fails ("failed to lock the ... workflow"); the stream is left stopped at the VDiff snapshot position and `Workflow start` doesn't clear it. Reproduced on base with a plain SIGTERM. | patch ready (`TestRestartTargetVReplicationStreams` fails on main) | V3 #1 |
| `VDiff stop/delete` hangs while another VDiff on the same tablet waits for its workflow lock | `vdiff` engine: `snapshotMu` is taken with a plain `Lock()` before the lock retry loop; stop/delete call `controller.Stop()` while holding the engine mutex | Operators can't stop or delete any VDiff on that tablet (other workflows included) until the first one gets its lock; never, with an orphaned lock (#6) | open; `TestVDiffStopWhileWaitingForWorkflowLock` fails on main (both same- and other-workflow cases) | V3 follow-up, VAL-D |
| Cancelling a MoveTables with many tables (2000) reloads the schema after every dropped table; the keyspace lock expires after 46–56 s and cancel returns an error with the tables already dropped but the workflow's streams still present | `go/vt/vtctl/workflow` cancel/drop path (the legacy `wrangler` drop loops have the same per-table reload) | Inconsistent workflow state after a cancel of a large MoveTables | patch ready (reload once per tablet: 10–12 s, succeeds; test fails on main) | V4b #3 |
| VStream `minimize_skew` stalls with 3+ shards: `vs.lowestTS` is never set, so streams tied with or near the laggard also pause, and once the laggard overtakes a paused stream the skew never closes | `vtgate/vstream_manager.go` `computeSkew` (~:601) | The VStream stops until the 10-minute skew timeout, then fails (6/6 runs on 4 shards; not seen with 2 shards) | patch ready (two new tests fail without the fix) | V6, V5 #2 |
| VTOrc polling off-by-one: three 1-second-resolution checks | `vtorc/inst/instance_dao.go`, `vtorc.go` | Polls every poll+1 s (6 s by default), which delays dead-primary detection. All three checks must change together. | patch ready (two tests fail on main) | P5 #2 |
| Semi-sync PRS causes a ~1 s write outage: `DemotePrimary` disables primary-side semi-sync, which waits for MySQL's ACK receiver 1 s poll | `rpc_replication.go` | Every planned reparent with semi-sync has ~1 s with no writable primary | patch ready (`TestDemotePrimaryPrimarySideSemiSync` fails on main) | P5 #1 |
| VTOrc and vtctld tablet RPCs wait the full 15 s `RemoteOperationTimeout` on a dead tablet, because tmclient dials with WaitForReady (`FailFast(false)`) | `grpctmclient`, VTOrc recoveries | After every unplanned failover VTOrc holds the shard lock ~43 of the next 45 s (post-recovery refresh, then StaleTopoPrimary recovery); PRS issued meanwhile fails. Quorum ERS (opt-in) is delayed 15 s | open; two `grpctmclient` tests take 15 s instead of <2 s on main; end-to-end numbers in VAL-D | P5 follow-up, VAL-D |
| Graceful vttablet shutdown refuses connections for ~10 s on a single-tablet target | vttablet gRPC graceful stop waits for vtgate's health streams until `--onterm-timeout` | ~10 s of client errors per graceful restart when there is no other tablet to route to (0.3 s with kill -9) | open; repro in VAL-D | VAL-D |

## P2: races, latent crashes, degraded behaviour, observability

| Bug | Where | Impact | Status | Source |
|---|---|---|---|---|
| Count-min sketch `reset` corrupts neighbouring counters, and `indexOf` uses only half the counters (both fixed upstream in theine-go in Oct 2024) | `go/cache/theine/sketch.go` | Plan-cache admission is worse: hit ratio up to −1.8 pp | patch ready (tests fail on main); both fixes must ship together | F01 |
| `Throttler.checkScope` mutates the shared global `okMetricCheckResult` on the exempted-app path | `tabletserver/throttle` | Data race; corrupts the global result's AppName, visible in `CheckThrottler` / `/throttler/check` responses; no decision or metric impact | open; `TestCheckExemptedAppDoesNotMutateSharedResult` fails on main | F27, VAL-C |
| `processExactKeyRange` sorts the shared cached shard list in place | `go/vt/key/destination.go` | Data race on the srvtopo cache | fixed in patch (sorts a copy, only when unsorted); the new test fails on main | F04, F30 #14 |
| Streaming `FetchNext` returns sub-slices with capacity to the end of the packet | `go/mysql/encoding.go` `readLenEncStringAsBytes` | An append to one value can overwrite the next column (no caller does this today). The stream consolidator's `CachedSize` over-counts by ~ncols/2, so it gives up consolidating too early. | open; the fix is `data[pos:pos+s:pos+s]`, but it changes the accounting; `TestFetchNextValuesHaveCappedCapacity` and `TestStreamConsolidatorAccountsStreamedRowsExactly` fail on main | F11, F30 #15, VAL-C |
| `QueryPlanCacheSize` counts only the admission window (~4% of the real size) | `go/cache/theine/store.go` | Operators can't tell when the plan cache is full, and a miss costs +28% vtgate CPU | patch ready | P6 #6 |
| A `ConvertCharset` entry mapped to nil panics | `replicator_plan.go` | vreplication crashes on a malformed rule | fixed in patch (nil now means no conversion) | F16 |
| Each VReplication heartbeat / position update rewrites the whole `_vt.vreplication` row, including all filter rules, into the binlog | `_vt.vreplication` schema / heartbeat updates | An idle 2000-table workflow writes ~700 MB/h of target binlog; 180 KB of binlog per replicated 349-byte transaction | open (`--vreplication-heartbeat-update-interval=10` cuts it 10x); the proper fix is to keep frequently updated columns out of the row that holds the rules | V4b #6 |
| mysqld shutdown waits 2 s for Vitess's own idle dba-pool connection | `mysqlctl/mysqld.go` | Every backup, restore and tablet shutdown is 2–3 s slower | patch ready (`TestClosePooledConnections`) | P5 #3 |
| The VDiff `VDiffRowsCompared` gauge re-adds the cumulative count every 10k rows (a 1M-row diff reads 50.5M), and `VDiffRowsComparedTotal` double-counts earlier attempts after a resume | `vdiff/table_differ.go:875`, `:588` | Misleading VDiff progress metrics | patch ready (`TestUpdateTableProgressRowCounts`; passes with the fix, not yet run on main) | V6 |

## Found during validation (needs triage)

| Bug | Where | Impact | Status | Source |
|---|---|---|---|---|
| `SELECT COUNT(*) FROM (SELECT DISTINCT v FROM t) x`, where `v` is not a vindex, is sent to every shard unchanged and vtgate sums the counts (`Aggregate sum_count_star` over a scatter Route) | vtgate planner (derived table + DISTINCT + aggregation) | Over-counts for any collation (5 vs MySQL's 3 on the NO PAD control) | open | VAL-B S1 |
| `CONCAT('[',v,']')` and `IFNULL(v,'x')` over a `utf8mb4_general_ci` column are typed `utf8mb4_0900_bin` | vtgate/evalengine collation derivation | Case-sensitive grouping: `GROUP BY ifnull(v,'x')` gives 1,2,2 vs MySQL's 2,3 | open | VAL-B S2 |
| With `--schema-change-signal=false`, a cross-shard UNION merges `'a '` and `'b'` into one row, and hash joins return extra binary weight_string columns and a near cross-product | vtgate weight_string handling without column types | Wrong results without schema tracking | open | VAL-B S3 |
| `metro.Metro128.Sum128()` finalizes in place, so calling it twice gives different hashes | `go/hack` / metro hash | Test pitfall, no known production misuse | open | VAL-B S4 |

## P3: latent, tests only, or cosmetic

| Bug | Where | Impact | Status | Source |
|---|---|---|---|---|
| `Timings.Reset` swaps the map under a read lock | `go/stats/timings.go` | Race; only tests call it | fixed in patch (the `-race` test fails before the fix) | F30 #13 |
| `Collation_binary.Hash` panics when `numCodepoints > len(src)` | `colldata` | No production caller passes a nonzero value today | open; `TestCollationBinaryHashNumCodepointsLongerThanInput` fails on main | F06, VAL-B |
| `charset.Convert` panics when a non-nil `dst` has capacity < 4 | `charset/convert.go` | No current caller does this | fixed in patch (`TestConvertSmallDestination` panics on main) | F13 |
| `counters.String` uses `%q`, which is not valid JSON for control characters | `go/stats/counters.go` | `/debug/vars` becomes invalid JSON; any client can trigger it through its user name or a table name (reproduced on a cluster). Candidate for P2 | open; `TestCountersStringIsValidJSONForControlCharacters` fails on main | F30 #2, VAL-B |
| The throttler client doc comment says "not thread safe", which is wrong | `throttle/client.go` | Misleading docs | fixed in patch | F27 |
| Relay-log stall flag race: a stall timer that fires just as `Fetch` drains can report "relay log I/O stalled" | `vreplication/relaylog.go` | A spurious stall error (needs exact timing at the 5-min deadline) | open; `TestRelayLogSendNoStallAfterFetchDrains` fails on main (18/50 with a 20 ms deadline) | V6, VAL-C |
| `vtctldclient VDiff create --wait` and legacy `vtctl VDiff --wait` only check for completion every `--wait-update-interval` (default 1 min) | vtctldclient vdiff | A 5 s VDiff takes 60 s to return | patch ready for vtctldclient (`TestWaitForVDiff`); legacy vtctl open | V3 #5 |
| vtexplain test flakes on a background `select @@global.wait_timeout` (2/20 on base) | vtexplain tests | Flaky CI | open | P7 |
| servenv cgroup tests fail in containers without cgroup metrics | `go/vt/servenv` | Test robustness | open | P3, P7 |

## Closed after validation

| Bug | Why closed | Source |
|---|---|---|
| vtgate took ~40 s to route to a restarted tablet | Not reproduced in 31 restarts (worst 6.3 s). The health-check reconnect backoff is capped at 10 s since #19967; the original P4 run most likely used an older build | P4 follow-up, VAL-D |

## Related upstream issues (not Vitess bugs)

- **Go 1.27 `utf8.RuneCount`:** scans ASCII one byte at a time and heap-copies non-ASCII input over 32 bytes via `RuneCountInString(string(p[n:]))`. Verified in the stdlib source (F29).
- **Go compiler (simd experiment):** didn't emit `VZEROUPPER` after 256-bit `archsimd` code, which caused AVX-SSE transition penalties (initial SIMD experiment).
- **grpc-go:** BDP pings on every round trip cost ~250 µs/query of CPU on this VM (H1). It would be better to start BDP measurement only for bulk transfers.
