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
| vtgate VStream skew check reads `vs.lowestTS`, which is never set | `vtgate/vstream_manager.go:601` | With minimize-skew on, every stream except the slowest always pauses (the tolerance check is dead code) | open | V6 |
| Relay-log stall flag race: a stall timer that fires just as `Fetch` drains can report "relay log I/O stalled" | `vreplication/relaylog.go` | A spurious stall error (needs exact timing at the 5-min deadline) | open | V6 |
| vtexplain test flakes on a background `select @@global.wait_timeout` (2/20 on base) | vtexplain tests | Flaky CI | open | P7 |
| servenv cgroup tests fail in containers without cgroup metrics | `go/vt/servenv` | Test robustness | open | P3, P7 |

## Related upstream issues (not Vitess bugs)

- **Go 1.27 `utf8.RuneCount`:** scans ASCII one byte at a time and heap-copies non-ASCII input over 32 bytes via `RuneCountInString(string(p[n:]))`. Verified in the stdlib source (F29).
- **Go compiler (simd experiment):** didn't emit `VZEROUPPER` after 256-bit `archsimd` code, which caused AVX-SSE transition penalties (initial SIMD experiment).
- **grpc-go:** BDP pings on every round trip cost ~250 µs/query of CPU on this VM (H1). It would be better to start BDP measurement only for bulk transfers.
