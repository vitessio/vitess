# Brief for the VReplication deep dive (round 3)

Read `perf-findings/BRIEF2.md` first; it covers the environment, harness, build/run instructions, the A/B method, the rules and the deliverable format. This file adds VReplication-specific context.

## Goal

Find VReplication performance problems that users actually feel and that we have NOT found yet:
- how long MoveTables / Reshard / Materialize / Online DDL / VDiff / LookupVindex take;
- replication lag under write load;
- source and target CPU and MySQL load;
- VStream CDC throughput;
- scale: many streams or many tables per tablet.

## Already known (don't re-report; you may build on these or measure them end to end)

**Round 1** (`perf-findings/SUMMARY.md`, `F*.md`):
- F05: binlog cell formatting via fmt.
- F16: applier per-column work, bind-var allocation, bulk-insert builder.
- F17: vstreamer vindex filter via generic `Map`.
- F26: GTID set String and AddGTID copies.
- F27: throttler client mutex per row.
- F13: charset conversion rune by rune.
- F18: JSON escaping.
- F03: SQL escaping.
- F20: BIT encoding.
- F11: ReadQueryResult allocations.

**Round 2** (`perf-findings/P4-vreplication.md`):
- Copy phase waits 1 s before each table.
- WaitForPos polls at 1 s; positions aren't saved after filtered-out transactions.
- Online DDL waits for the 1-minute review tick.
- Tablet picker sleeps a flat 30 s.
- `vstream-packet-size=1MB` is better.
- The vstreamer event-queue change (inconclusive).
- `--vreplication-parallel-insert-workers=4` doesn't help on 4 vCPUs.
- Running-phase apply drains ~6.5–8k trx/s.

P4's open follow-ups:
- The LOCK TABLES cycles with 100 ms sleeps are ~60% of the remaining 0.4 s switch outage.
- Target mysqld dominates copy CPU at ~4.5 µs/row.
- Reshard to N shards reads every source row N times.
- vtgate took ~40 s to route to a restarted tablet.
- VStream CDC through vtgate was not measured.

**Bugs** (`perf-findings/BUGS.md`): `select *` drops conversions; generated-column panic. Those are known correctness bugs.

**Related open upstream work:**
- vitessio/vitess#19535 "VReplication: Implement Experimental Parallel Applier" (mattlord).
- #20959 "fix(vstreamer): bound rowstreamer per-stream row buffer retention".

## Harness

P4's multi-keyspace VReplication harness is in `/home/vt/perf/P4-scripts/`. Read `cluster.sh` first: `KEYSPACES="src:0 dst:-80,80-"`, `load`, `restart-tablets`, `purge-binlogs`. The other scripts cover MoveTables (`mt.sh`), Online DDL (`oddl.sh`), catch-up (`catchup.sh`), switch-traffic (`switch.sh`), resharding (`rsab.sh`), and a prober (`prober.go.txt`). Copy the directory to `/home/vt/perf/<ID>/` and adapt it there.

- **Don't enable `--vreplication-parallel-insert-workers`.**
- **Use the vt user for clusters.** MySQL-backed Go tests run as vt too: compile the test binary as root with `go test -c`, then run it as vt with `VT_MYSQL_ROOT=/usr`.

## Coordination and resources

- **Benchmark lock:** 2–3 investigators run at once. Wrap EVERY measurement in `flock -o /home/vt/perf/bench.lock <cmd>`. The `-o` flag is mandatory. Never start or restart a cluster inside the lock, because the daemons inherit the lock fd and deadlock everyone. Keep each locked section under ~3 minutes, and record `uptime` with every result.
- **Disk:** ~17 GB free, shared by everyone.
  - Keep datasets to a few hundred MB.
  - Purge binlogs between runs.
  - Delete your data dirs and patched binaries when you finish.
  - Run `down` on your cluster before your final reply.
- **Ports:** use only your assigned BASE range.
