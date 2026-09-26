# V4-scale: VReplication fan-out and scale (PARTIAL: stopped before completion)

Investigator V4-scale, BASE 40000, base aa9ccf9. Patch `findings/V4-scale.patch` is the full worktree diff, not only my changes:
P4's Go changes (the reference, arm "ref", binaries `/home/vt/bin-V4ref`), V6 #6 and V6 #5 (relaylog), F17's planbuilder/rowstreamer
hunks, and my own changes. Harness: `/home/vt/perf/V4/` (cluster.sh adds `restart-vtctld`, `BUFPOOL`, `REDO`; drivers mtn.sh, drain.sh,
abdrain.sh, abcopy.sh, abstead.sh, steady.sh, switch.sh, mkmany.sh, mtmany.sh, mkwf.sh, idle.sh, manyload.sh + many.lua, abstats.py).
Cluster: src:0 (1M rows = 2 sysbench tables x 500k), dst2 (-80,80-), dst4 (4 shards), 256 MB buffer pool, durability none.
Load average 3-21 during runs (V3 plus my own runs). The cluster is down and its data dir removed.

## What was measured

### 1. Reshard-style fan-out copy (MoveTables unsharded -> N shards; the source-side path is the same as Reshard)

| N | runs | wall | src vttablet CPU / 1M src rows | src mysqld | targets (sum) | src Handler_read_next | src Bytes_sent |
|---|---|---|---|---|---|---|---|
| 2 | 3 | 6.0-6.8 s | 2.41-2.54 s | 0.89-0.93 s | 11.1-11.9 s | 2.0M | 396 MB |
| 4 | 4 | 5.4-7.4 s | 3.75-4.14 s | 1.62-1.76 s | 11.4-13.4 s | 4.0M | 791 MB |

- The source work is exactly N-fold: every stream scans and sends every row to the source vttablet, which filters by keyrange.
  Linear fit: **each extra stream costs 0.72 CPU-s (vttablet) + 0.40 CPU-s (mysqld) per 1M source rows and ~198 MB** of
  mysqld->vttablet traffic; the non-scaling part (sending the 1/N kept rows) is ~1.06 s vttablet + 0.12 s mysqld.
  Extrapolated: 1 -> 16 shards on 1B rows = ~4.7 extra source CPU-hours and 16x the source read bandwidth.
- Wall time did not grow from N=2 to N=4 here because the 4-vCPU box is saturated by the target mysqlds (65-70% of all CPU);
  on real hardware with targets on other hosts, the source becomes the limit at high N.
- Source vttablet profile at N=4: `Plan.shouldFilter` 33% (hash vindex `vindexes.Map` + `crypto/des` 11% + allocs), GC 17%,
  throttler check per row 7% (F27), `parseRow` 5%.
- F17 (generic Map fast path) was merged into the tree for an A/B, but the A/B was not run.

**Shared-scan design (evaluated, not implemented).** One scan per (table, lastpk) serving all N streams would remove (N-1)/N of
the source scan and filter cost. Obstacles: the N target streams are independent and not synchronized (each finishes a table,
fast-forwards and requests the next table at a different time), so a group needs a rendezvous window or late joiners that
backfill their missed prefix; a slow target stalls the group (acceptable: copy is target-bound); restarts and
`--vreplication-copy-phase-duration` break groups. It is worth it only for high fan-out (1 -> 16+). A cheaper partial step is to cut
the per-row filter cost (F17, F27, and not materializing non-key columns of filtered rows).

### 2. Running-phase fan-out (drain of a 30k/60k transaction backlog, CPU per source trx), 2 rounds, alternating arms

`ref` = base+P4; `v4` = ref + lazy event-GTID string + keyrange pre-filter (V6 #3); `v4c` = v4 + `--vstream-coalesce-empty-transactions` (V6 #4).
v4c has only 1 round (run cut short). us per source trx:

| case | streams | ref src vttablet | v4 | v4c | ref tgt vttablets (sum) | v4 | v4c |
|---|---|---|---|---|---|---|---|
| oltp_write_only | 2 | 96.7 | 90.8 (-6%) | 120.7 (1 run, outlier?) | 99.2 | 104.9 | 140 |
| oltp_write_only | 4 | 162.3 | 144.0 (-11%) | 144.7 | 144.0 | 135.6 | 133.6 |
| oltp_write_only | 6 | 244.3 | 220.5 (-10%) | 220.0 | 268.4 | 236.5 | 248.7 |
| update_non_index (1 row/trx) | 2 | 42.5 | 39.5 (-7%) | 40.3 | 38.1 | 36.5 | 36.5 |
| update_non_index | 4 | 76.5 | 70.0 (-8%) | 68.7 | 70.3 | 67.8 | **56.2 (-20%)** |
| update_non_index | 6 | 113.1 | 111.2 | 104.0 | 130.1 | 125.5 | **108.3 (-17%)** |

- Baseline scaling: the source vttablet pays ~29 us per stream per write_only trx and ~15 us per stream per single-row trx;
  source mysqld (binlog dump threads) ~5-10 us per stream per trx. Every stream reads and parses the whole binlog.
- Profile (ref, update_non_index, N=4): `parseEvent` 40%, of which `Mysql56GTID.String` (fmt.Sprintf, once per binlog event,
  even for events that produce nothing) 19% and `EncodePosition` 9%; `processRowEvent` only 10%; `selectgo`/scheduling 12%, gRPC send ~14%.
- **v4 (lazy GTID string, pre-filter) cuts source vttablet CPU 6-11%.** Most of it is the GTID string; the keyrange pre-filter
  saves little on 4-column sysbench rows (decoding is only 4% there), more on wide rows (not measured).
- **v4c (coalescing) cuts target vttablet CPU 17-20% when most transactions are empty for a stream** (single-row trx, N=4/6);
  the source saving is small, because the source still reads and parses every event. The one v4c write_only N=2 run (+25%) is
  unexplained and needs repeats before trusting either direction.

### Not measured (run stopped)
Steady-state fixed-rate fan-out, copy A/B with F17, target profiles, SwitchTraffic timeline and A/B, the many-tables
(500-3000) MoveTables, and the many-concurrent-workflows idle/load cost. Scripts for all of them are in `/home/vt/perf/V4/`.

## Code changes (my part of the patch)

1. `vstreamer.go`: compute the event GTID string once per GTID, and only when the event produces vevents (was fmt.Sprintf per
   binlog event per stream).
2. `vstreamer.go` (V6 #3): `streamerPlan.filterColumns` + `prefilterRow`/`getFilterValues` decode only the filter columns
   (vindex columns), skipping the others with `CellLength`, and drop rows whose both images fail before the full decode. It falls back
   to the full path for partial images, ENUM/SET/JSON filter columns, or when the filters read more than half the columns.
3. `vstreamer.go`, `engine.go`, `common/flags.go` (V6 #4): `--vstream-coalesce-empty-transactions` (default off, experimental).
   While more binlog events are queued (`len(throttledEvents) > 0`), an empty [BEGIN, GTID, COMMIT] transaction is held; a later
   empty one replaces it (its GTID set subsumes it); it is prepended to the next send, or flushed alone as soon as the queue is empty.
   The last empty trx before a non-empty one is always delivered first, so a consumer's stop position is never overshot into data
   (vplayer checks stopPos at COMMIT). Stat `VStreamerEmptyTransactionsCoalesced`. Flag docs added to `go/flags/endtoend`.
   For upstream, use a `VStreamOptions` proto field set by the vplayer instead of a source flag: unknown `ConfigOverrides` keys are
   rejected by older sources, so an override is not N-1 safe.
   Test `TestVStreamCoalescesEmptyTransactions` (MySQL-backed, passes as vt; it cannot compile on main).
4. `table_plan_builder.go`, `vcopier.go` (V6 #12): `tableRuleMatcher` (exact rules in a map, regexps compiled once) replaces
   per-table `MatchTable` in `buildReplicatorPlan`; `copyTable` builds the plan only for the table being copied (it built all T
   tables' plans for each table: O(T^2)). `TestTableRuleMatcher`. **Not measured end to end.**
5. `server.go` (SwitchTraffic): for table migrations, skip the second `allowTargetWrites` (it repeats the source and target
   denied-tables updates and refreshes every source and target tablet while writes are blocked; `stopSourceWrites` already removed
   the target denials). With V6 #6 (no trailing LOCK TABLES sleep, `ReloadSchema: false`). `go test ./go/vt/vtctl/workflow` passes.
   **Not measured end to end.**
6. `relaylog.go`: V6 #5 merged onto P4's version (timers armed only when waiting). Not separately measured.

Tests run: vstreamer filter tests (TestSelectFilter, TestFilteredMultipleWhere, TestFilteredVarBinary, TestFilteredInt,
TestREKeyRange, TestInKeyRangeMultiColumn, TestMinimalMode) and the new coalesce test pass; `go test ./go/vt/vtctl/workflow` passes;
the full vstreamer and vreplication suites were NOT run.

## Follow-ups
- Finish the A/Bs above (more rounds of v4c; copy with F17; SwitchTraffic timeline with P4's prober; 1000/3000-table MoveTables with
  the plan-builder fix; 20-50 workflows idle cost: connections, time_updated writes, binlog growth).
- A shared binlog reader per source tablet (one dump connection and one parse for all vstreamers) is the running-phase analogue of
  the shared copy scan: it would remove the per-stream ~15-29 us/trx source cost that coalescing cannot.
- Observed: `buildReplicatorPlan` for the vplayer during copy still loops over all copied tables per table (O(T^2) total); caching
  per-table plans across copy cycles is the next step if the 1000-table run shows it.
