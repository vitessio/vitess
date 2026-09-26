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

| # | Bug | Pri | Why this position | Status |
|---|---|---|---|---|
| 1 | VReplication with an explicit column list that includes a target-generated column writes shifted values (or panics) | P0 | The worst corruption: wrong values in wrong columns. Plausible whenever an Online DDL/Materialize target has generated columns. | open |
| 2 | Copy phase turns JSON doubles into DECIMAL (MoveTables/Reshard) | P0 | Silent, on the **default** path, and invisible to VDiff; any JSON with floating-point numbers is affected | open (needs a consistent type rule) |
| 3 | `select *` rules drop ConvertCharset / ConvertIntToEnum | P0 | Silent unconverted data, but only for workflows that combine `select *` with conversion rules | open |
| 4 | Parallel-insert-worker connections skip session setup (TZ, `set names binary`, timeouts) | P0 | Silent TIMESTAMP shifts and misread bytes, but only with an opt-in flag on a non-UTC target; not reproduced end to end | patch ready |
| 5 | PAD SPACE ignored in general_ci/latin1/unicode_ci/`_bin` Collate and Hash | P1 | Silent wrong query results whenever vtgate evaluates (cross-shard filters, GROUP BY, DISTINCT, joins); `utf8mb4_general_ci` is very common | open (behaviour change) |
| 6 | Tablet restart during VDiff setup orphans the workflow lock for 24 h | P1 | A routine restart blocks VDiff and **SwitchTraffic** for a day and leaves the stream stopped; reproduced on base | patch ready |
| 7 | Semi-sync PRS has a ~1 s write outage (DemotePrimary waits for the ACK receiver) | P1 | Hits **every** planned reparent in semi-sync deployments, which is the common production setup | patch ready |
| 8 | VStream `minimize_skew` stalls with 3+ shards, then fails after 10 minutes | P1 | Deterministic (6/6 on 4 shards) for every CDC user of that option | patch ready |
| 9 | VTOrc polling off-by-one (every 6 s instead of 5) | P1 | Adds ~1 s to every unplanned failover's detection, fleet-wide | patch ready |
| 10 | Cancelling a MoveTables with thousands of tables leaves a half-cancelled workflow | P1 | Inconsistent state that needs manual cleanup, but only for very large table counts | patch ready |
| 11 | vtgate accepts a quoted-int `SET innodb_lock_wait_timeout`, then every query in the session fails | P1 | The session is poisoned, but it needs specific client input; an easy fix | open |
| 12 | vtgate took ~40 s to route to a restarted tablet | P1? | Big if real (40 s of unavailability after restarts) | unverified; **reproduce first** |
| 13 | VTOrc `FullStatus` hangs ~15 s on a refused connection | P1? | Slows recovery when a tablet process is down | unverified |
| 14 | `VDiff stop/delete` blocked ~7 minutes waiting for the workflow lock | P1? | Operators can't stop a stuck VDiff | unverified |
| 15 | Heartbeats rewrite the whole `_vt.vreplication` row (rules included) into the binlog | P2 | ~700 MB/h of binlog for a large idle workflow can fill disks and slow replicas | open (flag mitigates) |
| 16 | `Throttler.checkScope` mutates a shared global result | P2 | A data race that corrupts shared state on every exempted check | open |
| 17 | `processExactKeyRange` sorts the shared cached shard list in place | P2 | A data race on the srvtopo cache, only with unsorted partitions | fixed in patch |
| 18 | Plan-cache sketch: `reset` corrupts counters and `indexOf` uses half of them | P2 | Worse plan-cache admission (up to −1.8 pp hit ratio); a miss costs +28% vtgate CPU | patch ready |
| 19 | Streaming `FetchNext` returns uncapped slices | P2 | Latent column overwrite (no caller today); the consolidator over-counts memory and gives up early | open (1-line fix) |
| 20 | `QueryPlanCacheSize` shows ~4% of the real size | P2 | Operators can't see a full plan cache | patch ready |
| 21 | VDiff progress metrics over-report (50x) and double-count after a resume | P2 | Misleading progress | patch ready |
| 22 | A nil `ConvertCharset` entry panics vreplication | P2 | A crash, but only on a malformed rule | fixed in patch |
| 23 | mysqld shutdown waits 2 s for Vitess's own idle connection | P2 | +2–3 s on every backup, restore and tablet shutdown | patch ready |
| 24 | Relay-log stall-flag race | P3 | A spurious stall error needing exact timing at the 5-min deadline | open |
| 25 | `VDiff --wait` checks for completion only once a minute (vtctldclient and legacy vtctl) | P3 | A slow CLI return, no correctness impact | patch ready (vtctldclient) |
| 26 | `Collation_binary.Hash` panics when `numCodepoints > len` | P3 | No production caller today | open |
| 27 | `charset.Convert` panics on a destination buffer smaller than 4 bytes | P3 | No current caller | fixed in patch |
| 28 | `counters.String` emits invalid JSON for control characters | P3 | Only odd label values in `/debug/vars` | open |
| 29 | `Timings.Reset` race | P3 | Only tests call it | fixed in patch |
| 30 | vtexplain test flake (background `wait_timeout` query) | P3 | Flaky CI | open |
| 31 | servenv cgroup tests fail in containers | P3 | Test robustness | open |
| 32 | Wrong "not thread safe" doc comment on the throttler client | P3 | Docs only | fixed in patch |
| 33–35 | Upstream: Go `utf8.RuneCount` allocation; missing `VZEROUPPER` in the simd experiment; grpc-go BDP pings per round trip | – | Report to Go / grpc-go | – |

## P0: silent wrong data

| Bug | Where | Impact | Status | Source |
|---|---|---|---|---|
| VReplication `select *` rules drop `ConvertCharset` / `ConvertIntToEnum` | `vreplication/replicator_plan.go` (table plan built for `select *`) | MoveTables / Online DDL with charset or int→enum conversion rules copy unconverted values without any error | open; a test in `replicator_plan_test` can show it | F16, F30 #17a |
| An explicit column list that selects a column generated on the target breaks `appendFromRow` | `replicator_plan.go` `appendFromRow` skip loop (no bounds check) | Panics with index out of range; with extra PK columns it **binds shifted values**, so wrong data is written | open; easy unit test | F30 #17b |
| VReplication copy phase silently turns JSON doubles into DECIMAL (MoveTables/Reshard); VDiff doesn't detect it | copy phase JSON handling (`replicator_plan.go` / `table_plan_builder.go`) | Target JSON documents differ in number types from the source (`JSON_TYPE` DOUBLE → DECIMAL) | open; V1 adds an opt-in "JSON as text" mode that keeps doubles (but turns decimals into doubles); a consistent rule is still needed | V1 #3 |
| `--vreplication-parallel-insert-workers` connections skip the session setup: no UTC time zone, no `set names binary`, no network timeouts | vcopier parallel insert worker connections | On a non-UTC target, TIMESTAMP values are shifted and non-utf8 bytes misread (opt-in flag) | patch ready (unit test fails on main; not reproduced end to end) | V1 #4 |

## P1: wrong results, user-visible errors, avoidable outages

| Bug | Where | Impact | Status | Source |
|---|---|---|---|---|
| PAD SPACE not honoured: `'a'` vs `'a '` compare unequal and hash differently | `colldata` `Collate`/`Hash` for general_ci, latin1 ci, legacy unicode_ci, utf8mb4_bin (0900 collations are NO PAD, so not affected) | Wrong results whenever vtgate evaluates: cross-shard filters, GROUP BY/DISTINCT/UNION, hash joins, aggregation. `utf8mb4_general_ci` is very common in 5.7-era schemas. | open; the fix is a behaviour change (trim a trailing all-space tail in `Collate`, trim trailing spaces in `Hash`, leave `WeightString` alone); coordinate with F14 | F30 #16, F14 |
| vtgate accepts `SET SESSION innodb_lock_wait_timeout = '26'` (a quoted int that MySQL rejects), then every later query in the session fails with errno 1232 | vtgate SET handling / settings pool | The session is poisoned; clients see errors on unrelated queries | open; validate at SET time | P2 |
| Restarting a target vttablet during VDiff setup orphans the workflow lock in topo with a 24 h lease | `vdiff/table_differ.go`: the stream restart is a gRPC call from the tablet to itself after its gRPC server stopped | For up to 24 h, later VDiffs hang and SwitchTraffic fails ("failed to lock the ... workflow"); the stream is left stopped at the VDiff snapshot position and `Workflow start` doesn't clear it. Reproduced on base with a plain SIGTERM. | patch ready (`TestRestartTargetVReplicationStreams` fails on main) | V3 #1 |
| `VDiff stop/delete` blocked ~7 minutes while a controller waited for the workflow lock | vdiff controller / workflow lock | Operators can't stop a stuck VDiff promptly | unverified (observed once) | V3 follow-up |
| Cancelling a MoveTables with many tables (2000) reloads the schema after every dropped table; the keyspace lock expires after 46–56 s and cancel returns an error with the tables already dropped but the workflow's streams still present | `go/vt/vtctl/workflow` cancel/drop path (the legacy `wrangler` drop loops have the same per-table reload) | Inconsistent workflow state after a cancel of a large MoveTables | patch ready (reload once per tablet: 10–12 s, succeeds; test fails on main) | V4b #3 |
| VStream `minimize_skew` stalls with 3+ shards: `vs.lowestTS` is never set, so streams tied with or near the laggard also pause, and once the laggard overtakes a paused stream the skew never closes | `vtgate/vstream_manager.go` `computeSkew` (~:601) | The VStream stops until the 10-minute skew timeout, then fails (6/6 runs on 4 shards; not seen with 2 shards) | patch ready (two new tests fail without the fix) | V6, V5 #2 |
| VTOrc polling off-by-one: three 1-second-resolution checks | `vtorc/inst/instance_dao.go`, `vtorc.go` | Polls every poll+1 s (6 s by default), which delays dead-primary detection. All three checks must change together. | patch ready (two tests fail on main) | P5 #2 |
| Semi-sync PRS causes a ~1 s write outage: `DemotePrimary` disables primary-side semi-sync, which waits for MySQL's ACK receiver 1 s poll | `rpc_replication.go` | Every planned reparent with semi-sync has ~1 s with no writable primary | patch ready (`TestDemotePrimaryPrimarySideSemiSync` fails on main) | P5 #1 |
| vtgate took ~40 s to route to a restarted tablet | vtgate healthcheck / tablet discovery | Long unavailability after tablet restarts, if it reproduces | unverified | P4 follow-up |
| VTOrc `FullStatus` hangs ~15 s on a refused connection | VTOrc tablet RPCs | Slower recovery when a tablet process is down | unverified (observed) | P5 follow-up |

## P2: races, latent crashes, degraded behaviour, observability

| Bug | Where | Impact | Status | Source |
|---|---|---|---|---|
| Count-min sketch `reset` corrupts neighbouring counters, and `indexOf` uses only half the counters (both fixed upstream in theine-go in Oct 2024) | `go/cache/theine/sketch.go` | Plan-cache admission is worse: hit ratio up to −1.8 pp | patch ready (tests fail on main); both fixes must ship together | F01 |
| `Throttler.checkScope` mutates the shared global `okMetricCheckResult` on the exempted-app path | `tabletserver/throttle` | Data race; corrupts the global result's AppName | open | F27 |
| `processExactKeyRange` sorts the shared cached shard list in place | `go/vt/key/destination.go` | Data race on the srvtopo cache | fixed in patch (sorts a copy, only when unsorted); the new test fails on main | F04, F30 #14 |
| Streaming `FetchNext` returns sub-slices with capacity to the end of the packet | `go/mysql/encoding.go` `readLenEncStringAsBytes` | An append to one value can overwrite the next column (no caller does this today). The stream consolidator's `CachedSize` over-counts by ~ncols/2, so it gives up consolidating too early. | open; the fix is `data[pos:pos+s:pos+s]`, but it changes the accounting | F11, F30 #15 |
| `QueryPlanCacheSize` counts only the admission window (~4% of the real size) | `go/cache/theine/store.go` | Operators can't tell when the plan cache is full, and a miss costs +28% vtgate CPU | patch ready | P6 #6 |
| A `ConvertCharset` entry mapped to nil panics | `replicator_plan.go` | vreplication crashes on a malformed rule | fixed in patch (nil now means no conversion) | F16 |
| Each VReplication heartbeat / position update rewrites the whole `_vt.vreplication` row, including all filter rules, into the binlog | `_vt.vreplication` schema / heartbeat updates | An idle 2000-table workflow writes ~700 MB/h of target binlog; 180 KB of binlog per replicated 349-byte transaction | open (`--vreplication-heartbeat-update-interval=10` cuts it 10x); the proper fix is to keep frequently updated columns out of the row that holds the rules | V4b #6 |
| mysqld shutdown waits 2 s for Vitess's own idle dba-pool connection | `mysqlctl/mysqld.go` | Every backup, restore and tablet shutdown is 2–3 s slower | patch ready (`TestClosePooledConnections`) | P5 #3 |
| The VDiff `VDiffRowsCompared` gauge re-adds the cumulative count every 10k rows (a 1M-row diff reads 50.5M), and `VDiffRowsComparedTotal` double-counts earlier attempts after a resume | `vdiff/table_differ.go:875`, `:588` | Misleading VDiff progress metrics | patch ready (`TestUpdateTableProgressRowCounts`; passes with the fix, not yet run on main) | V6 |

## P3: latent, tests only, or cosmetic

| Bug | Where | Impact | Status | Source |
|---|---|---|---|---|
| `Timings.Reset` swaps the map under a read lock | `go/stats/timings.go` | Race; only tests call it | fixed in patch (the `-race` test fails before the fix) | F30 #13 |
| `Collation_binary.Hash` panics when `numCodepoints > len(src)` | `colldata` | No production caller passes a nonzero value today | open | F06 |
| `charset.Convert` panics when a non-nil `dst` has capacity < 4 | `charset/convert.go` | No current caller does this | fixed in patch (`TestConvertSmallDestination` panics on main) | F13 |
| `counters.String` uses `%q`, which is not valid JSON for control characters | `go/stats/counters.go` | `/debug/vars` can emit invalid JSON for odd label values | open | F30 #2 |
| The throttler client doc comment says "not thread safe", which is wrong | `throttle/client.go` | Misleading docs | fixed in patch | F27 |
| Relay-log stall flag race: a stall timer that fires just as `Fetch` drains can report "relay log I/O stalled" | `vreplication/relaylog.go` | A spurious stall error (needs exact timing at the 5-min deadline) | open | V6 |
| `vtctldclient VDiff create --wait` and legacy `vtctl VDiff --wait` only check for completion every `--wait-update-interval` (default 1 min) | vtctldclient vdiff | A 5 s VDiff takes 60 s to return | patch ready for vtctldclient (`TestWaitForVDiff`); legacy vtctl open | V3 #5 |
| vtexplain test flakes on a background `select @@global.wait_timeout` (2/20 on base) | vtexplain tests | Flaky CI | open | P7 |
| servenv cgroup tests fail in containers without cgroup metrics | `go/vt/servenv` | Test robustness | open | P3, P7 |

## Related upstream issues (not Vitess bugs)

- **Go 1.27 `utf8.RuneCount`:** scans ASCII one byte at a time and heap-copies non-ASCII input over 32 bytes via `RuneCountInString(string(p[n:]))`. Verified in the stdlib source (F29).
- **Go compiler (simd experiment):** didn't emit `VZEROUPPER` after 256-bit `archsimd` code, which caused AVX-SSE transition penalties (initial SIMD experiment).
- **grpc-go:** BDP pings on every round trip cost ~250 µs/query of CPU on this VM (H1). It would be better to start BDP measurement only for bulk transfers.
