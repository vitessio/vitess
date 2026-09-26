# V5-vstream: VStream API (CDC) through vtgate, end to end

Investigator V5-vstream, BASE 30000, base commit aa9ccf9, worktree `agent-a976cbcec7168e1a1` (changes uncommitted).
Patch: `findings/V5-vstream.patch` (full worktree diff, including the regenerated protos and the harness under
`perf-findings/V5-scripts/`). Final binaries: `/home/vt/bin-V5` (vtgate + vttablet; other binaries are symlinks to `/home/vt/bin`).

## Summary (ranked by user-visible impact)

| # | Change | Measured end-to-end effect | Size | Risk |
|---|---|---|---|---|
| 1 | **Skip the per-table catchup of the VStream copy phase when the previous table's copy snapshot is < 10 s old** (`vstreamer/copy.go`, `uvstreamer.go`; builds on V6 #1, same idea as P4 #1) | VStream copy of **40 tables x 2k rows: 39.5 s -> 0.53 s (75x)**. 1.03M rows in 5 tables, 2 shards: **6.28 s -> 1.75 s**; 4 shards: 5.83 s -> 1.85 s. V6's event-driven check alone: 39.5 -> 35.8 s (saves only the 0.1 s between the heartbeat and the tick on an idle source). | S | low |
| 2 | **Bug fix: `minimize_skew` stalls the whole VStream (until the 10-minute skew timeout, then fails) with >= 3 shards** (`vtgate/vstream_manager.go` `computeSkew`) | 4 shards, 200k-trx backlog with `minimize_skew`: **base and V6 arms stalled in 6/6 runs** (93k-194k of 200k trx delivered in 180 s, then nothing); patched: **6/6 complete in 4.0-4.4 s** (same as without the option). | S | low |
| 3 | **Opt-in `VStreamFlags.batch_copy_rows` -> `VStreamOptions.batch_copy_rows`: one ROW event per copied batch instead of one per row** (V6 #11) | Copy of 1.03M rows (on top of #1): **1.75 s -> 1.10 s**; vtgate **1.87 -> 0.97 us/row**, vttablets 1.66 -> 1.16, client 1.33 -> 0.71, wire bytes -19%, 1.03M events -> 3k. | M (proto) | low (opt-in; old tablets ignore it) |
| 4 | **Running phase: fewer messages and less per-event work** (all together: V6 #2/#10, V4 GTID string, P4 event queue, vtgate per-batch bookkeeping, vtgate response coalescing (opt-in `VStreamFlags.coalesce_transactions`), tablet transaction coalescing (`VStreamOptions.coalesce_transactions`, set by vtgate), codec buffer pool #5) | Drain of 200k `oltp_write_only` trx, 2 shards: **vtgate 34.1 -> 21.3 us/trx (-37%), vttablets 53.2 -> 36.5 (-31%), client 20.7 -> 13.8 (-33%)**, drain 7.4 s -> 5.0 s (27k -> 40k trx/s). Single-row trx: vtgate -39%, vttablets -30%, drain -38%. 100-row trx: vtgate **4.26 -> 1.81 us/row (-58%)**, vttablets -27%, drain -43%. 4 shards: vtgate 31.9 -> 21.9 us/trx, vttablets 53.8 -> 41.1, wall 6.5 -> 4.7 s. | M | low-medium (see per-part notes) |
| 5 | **gRPC codec buffer pool without zeroing and with power-of-two tiers** (`servenv/grpc_codec.go`) | gRPC's default pool has no tier between 32KB and 1MB and zeroes the whole buffer on every Get: **every message of 32KB-1MB zeroes 1MB** on each side (`memclrNoHeapPointers` 8-10% of vtgate). On top of the rest: vtgate -11% (write_only) / -16% (100-row trx), vttablets -8% / -5%, copy -3-5%. Applies to all Vitess gRPC traffic (VReplication copy packets are 250KB). | S | low |

Steady-state CDC latency (400 trx/s through vtgate, 1 or 4 consumers) is ~2-3 ms p50 and 7-12 ms p99 from commit to the
client, and is not changed by any of this: at moderate rates the per-transaction cost of CDC is small next to serving the writes.
The wins are in catch-up throughput (after a consumer restart or a lag spike), CPU per event, and initial snapshots.

## Setup

- Cluster: `perf-findings/V5-scripts/cluster.sh` (the base harness plus `restart` (vtgate + vttablets, mysqld kept) and
  `purge-binlogs`), keyspace `sbtest` with 2 shards (-80, 80-) and later 4 shards; one primary per shard, durability none.
  4 sysbench tables x 250k rows (1M rows) plus a probe table.
- Client: `V5-scripts/vsclient.go.txt`, a Go VStream client on `vtgateconn` (grpc). Modes: `pos` (record the current VGTID), `drain`
  (stream from a recorded VGTID to another one; exact stop at the COMMIT that reaches the stop position), `copy` (empty GTID, until
  the final COPY_COMPLETED), `live` (from `current`, with a probe row inserted through vtgate every 50 ms; the commit -> receive
  latency of each probe row is measured with ns timestamps). It counts responses, events by type, row changes, payload bytes
  (`SizeVT` of the events) and its own CPU.
- **Running phase = backlog drain.** A backlog is generated directly on each shard's mysqld (sysbench `oltp_write_only` (4 row
  events/trx), `oltp_update_non_index` (1 row/trx), and a Lua script doing `UPDATE ... WHERE id BETWEEN x AND x+199` (~100 rows/trx)),
  with its start and stop VGTIDs recorded. Every arm drains the same binlog range. The drain rate is the maximum sustainable CDC rate
  (a consumer catching up after a restart); CPU per trx/row from `cluster.sh cpu`. A drain saturates the 4 vCPUs (vtgate +
  2 vttablets + client), so CPU/trx is the robust metric and wall time is secondary.
- A/B: for each round, each arm's binaries are started (vtgate + vttablets restarted, mysqld kept), then each measurement runs under
  `flock -o /home/vt/perf/bench.lock`. 3 rounds for the main tables (2 for options, live and 4 shards). Values are median [min-max].
  Load average 0.2-9.7 during the runs (another investigator was active).
- Arms: `base` = /home/vt/bin; `v6` = V6's VStream hunks (#1 event-driven catchup check, #2 shell clone, #9 rule match cache, #10
  SizeVT only with chunking) + V4's lazy event GTID string; `p4q` = v6 + P4's buffered binlog event queues and lazy heartbeat timer
  (`binlog_connection.go`, `vstreamer.go`); `v5a` = p4q + vtgate changes (per-batch bookkeeping, buffered event channel, response
  coalescing, skew fix); `v5b` = v5a + tablet transaction coalescing + copy catchup skip + batch copy option; `v5c` = v5b + codec
  pool; `fin` = final tree (v5c with coalescing behind the client flag and a skipped-catchup counter).
  Note: in v5a-v5c the vtgate response coalescing was always on; in `fin` it needs `VStreamFlags.coalesce_transactions`
  (client `-coalesce`), which is what makes it safe (see below).

## Baseline (base binaries, 2 shards)

| workload | result | vtgate | vttablets (sum) | client |
|---|---|---|---|---|
| drain 200k `oltp_write_only` trx (708k rows, 1.31M events, 368 MB) | 7.4 s: 27k trx/s, 96k rows/s, 180k events/s, 50 MB/s | 34 us/trx (9.6 us/row) | 53 us/trx | 21 us/trx |
| drain 178k single-row updates | 3.6 s: 49k trx/s | 19.9 us/trx | 29.0 us/trx | 12.5 us/trx |
| drain 6k x 100-row updates (619k rows) | 2.6 s: 240k rows/s | 4.26 us/row | 5.38 us/row | 2.34 us/row |
| copy 1.03M rows, 5 tables | 6.3 s: 164k rows/s, 1 event per row | 2.78 us/row | 1.99 us/row | 1.62 us/row |
| copy 40 tables x 2k rows | 39.5 s (~1 s per table) | | | |
| steady 400 trx/s through vtgate, 1 consumer | probe latency p50 2.0 ms, p99 7.4 ms, max 17-27 ms | | | |

Every vtgate response carries exactly one transaction (200k responses for 200k trx), and so does every tablet -> vtgate message.

**vtgate profile (drain):** gRPC receive + unmarshal of tablet messages 19%; the loopy writer and write syscalls to the client 15%
(one write per transaction); `maybeUpdateTableName` 8.5%, of which `CloneVT` 7% (V6 #2: deep copy of every row); `sendEvents` 8%;
`vstreamsLag.Set` (labeled gauge, per event) 2.8%; `SizeVT` 1.9% (V6 #10); `fmt.Sprintf` of error strings per callback 1.4%; GC ~11%.
Streaming with `exclude_keyspace_from_table_name` (no clone) costs 26.3 instead of 34.1 us/trx: **the clone was 23% of vtgate's CDC CPU.**

**vttablet profile (drain):** `parseEvents` 44% (`parseEvent` 27%, `processRowEvent` 10%); `selectgo` 17% (two unbuffered channel hops
per binlog event, P4's finding); the loopy writer 10%; `Mysql56GTID.String` 3.9% (V4); GC ~11%.

**Copy timeline (vttablet log):** each table after the first starts with a catchup that waits 0.9 s for the first heartbeat on an
idle source (the V6 #1 fix saves only the remaining 0.1 s); copying a 125k-row table per shard takes ~0.6 s. So 60% of the 1M-row
copy and 97% of the 40-table copy is waiting.

## Results

### Running phase, 2 shards (3 rounds each; us per trx, or per row for the 100-row case)

`oltp_write_only` (200k trx):

| arm | wall s | vtgate | vttablets | client | responses |
|---|---|---|---|---|---|
| base | 7.42 [7.34-7.48] | 34.05 [33.65-34.83] | 53.21 [52.51-54.38] | 20.70 | 208k |
| v6 | 7.48 [7.11-8.50] | 30.01 [29.90-31.48] | 49.01 [48.71-50.76] | 20.05 | 219k |
| p4q | 5.94 [5.47-7.77] | 26.39 [26.09-28.71] | 39.14 [38.88-41.77] | 17.44 | 203k |
| v5a | 5.96 [5.54-6.04] | 24.31 [23.87-27.14] | 39.29 [38.32-42.75] | 14.53 | 25k |
| v5b | 5.77 [5.54-6.51] | 23.57 [23.13-24.20] | 39.27 [38.74-39.36] | 15.03 | 21k |
| v5b (2nd batch) | 4.80 [4.77-4.89] | 24.09 [23.79-24.29] | 39.73 [39.13-40.49] | 15.00 | 22k |
| v5c | 4.99 [4.42-5.14] | 21.34 [21.11-23.51] | 36.51 [36.48-39.49] | 13.84 | 22k |

Single-row updates (178k trx):

| arm | wall s | vtgate | vttablets | client | responses |
|---|---|---|---|---|---|
| base | 3.64 [3.46-3.70] | 19.92 [19.91-21.25] | 28.96 [28.66-30.38] | 12.51 | 178k |
| v6 | 3.41 | 18.14 | 26.00 | 12.02 | 178k |
| p4q | 3.40 | 16.73 | 22.80 | 12.18 | 178k |
| v5a | 2.81 | 14.39 | 22.85 | 8.91 | 17k |
| v5b | 2.36 [2.36-2.62] | 12.27 [12.06-12.74] | 20.56 [19.80-21.56] | 8.46 | 14k |
| v5c (2nd batch; v5b 11.57/19.49 there) | 2.24 | 12.24 | 20.22 | 8.31 | 15k |

100-row updates (619k rows), us per row:

| arm | wall s | vtgate | vttablets | client | responses |
|---|---|---|---|---|---|
| base | 2.57 [2.55-2.69] | 4.26 [4.09-4.42] | 5.38 [5.15-5.59] | 2.34 | 6.1k |
| v6 | 2.37 | 3.54 | 5.33 | 2.31 | 6.1k |
| p4q | 2.53 | 3.46 | 5.10 | 2.36 | 6.1k |
| v5a | 2.08 | 2.89 | 5.02 | 2.05 | 2.7k |
| v5b | 1.82 [1.81-1.96] | 2.23 [2.23-2.28] | 4.17 [4.17-4.41] | 1.73 | 1.2k |
| v5c (2nd batch; v5b 2.16/4.13 there) | 1.47 [1.33-1.59] | 1.81 [1.76-2.05] | 3.91 [3.68-4.30] | 1.58 | 1.3k |

Attribution (from adjacent arms):
- **V6 #2/#10 + V4 GTID string** (v6 vs base): vtgate -12% / -9% / -17%; vttablets -8% / -10% / -1%.
- **P4 event queue** (p4q vs v6): vttablets -20% (write_only), -12% (single row); vtgate also -12% on write_only (larger batches per
  callback). It is P4's change; this confirms it end to end for CDC.
- **vtgate per-batch bookkeeping + response coalescing** (v5a vs p4q): responses 203k -> 25k; vtgate -8% / -14% / -16%; client -17% / -27% / -13%.
- **tablet transaction coalescing** (v5b vs v5a): vttablets 0% / -10% / -17%, vtgate -3% / -15% / -23% (fewer, larger messages).
- **codec pool** (v5c vs v5b, same batch): vtgate -11% / 0% / -16%; vttablets -8% / 0% / -5%.

### Copy phase (3 rounds; 1.03M rows in 5 tables, 2 shards; us per row)

| arm | wall s | vtgate | vttablets | client | events | bytes/row |
|---|---|---|---|---|---|---|
| base | 6.28 [6.01-6.68] | 2.78 | 1.99 | 1.62 | 1.03M | 255 |
| v6 | 5.62 [5.42-5.68] | 2.30 | 1.93 | 1.57 | 1.03M | 255 |
| v5b | 1.75 [1.63-1.87] | 1.87 | 1.66 | 1.33 | 1.03M | 255 |
| v5b + `batch_copy_rows` | 1.10 [1.02-1.65] | 0.97 | 1.16 | 0.71 | 3.0k | 206 |
| v5c + `batch_copy_rows` | 1.01 [0.96-1.03] | 0.93 | 1.12 | 0.72 | 3.0k | 206 |

4 shards (2 rounds): base 5.83 s (vtgate 2.82, vttablets 2.17 us/row) -> fin 1.85 s (1.95, 1.98) -> fin + batch 1.08 s (0.91, 1.29).

Many small tables (40 tables x 1000 rows per shard, 2 shards, 2 rounds): **base 39.5 s, v6 35.75 s, v5b 0.53 s**.

### Options (2 rounds, base vs v5c, write_only backlog unless noted; us per trx)

| case | base: wall / vtgate / vttablets | v5c: wall / vtgate / vttablets |
|---|---|---|
| filtered (1 of 4 tables) | 4.38 s / 20.5 / 38.9 | 3.62 s / 15.0 / 30.4 |
| 4 consumers on one vtgate (per trx, all consumers) | 25.4 s / 97.4 / 173.1 | 21.1 s / 72.4 / 130.9 |
| `minimize_skew` (2 shards) | 6.42 s / 33.5 / 51.2 | 5.18 s / 25.4 / 41.7 |
| `exclude_keyspace_from_table_name` | 6.30 s / 26.3 / 48.0 | 4.75 s / 20.7 / 38.5 |
| `transaction_chunk_size=64KB`, 100-row trx (per row) | 2.49 s / 4.39 / 5.63 | 1.54 s / 2.01 / 4.13 |

- **Several consumers:** each consumer gets its own tablet streams, so the source cost is linear: ~43 us/trx of vttablet CPU per
  consumer (write_only). A shared binlog reader per tablet (V4's follow-up) would be the next step for many CDC consumers.
- **Filtered consumers** still pay for every transaction: for a consumer of 1 table in 4, 3/4 of the transactions reach it as empty
  BEGIN/VGTID/COMMIT. V4's empty-transaction coalescing (as a `VStreamOptions` field) would cut that.
- **Chunking** costs 3% in base; with the patch, 11% (SizeVT is back and the lock path is taken). Not investigated further.
- **Not measured:** `heartbeat_interval`, `stream_keyspace_heartbeats` and `include_reshard_journal_events` (no reshard). By code
  reading their per-event cost is negligible (a ticker reset per send; one extra internal table in the filter).

### 4 shards (3 rounds; 200k trx backlog)

| case | base | v6 | v5c |
|---|---|---|---|
| drain: wall / vtgate / vttablets (us/trx) | 5.88 s / 30.5 / 50.8 | 5.29 s / 26.9 / 46.9 | 4.23 s / 20.7 / 38.7 |
| drain with `minimize_skew` | **stalled 3/3** (180 s timeout, 94k-194k trx) | **stalled 3/3** (77k-132k trx) | 4.06 s / 20.2 / 37.4 |

Final tree (`fin`, 2 rounds, a busier machine): without `-coalesce` vtgate 32.3 -> 24.7, vttablets 54.2 -> 42.6; with `-coalesce`
vtgate 31.9 -> 21.9, vttablets 53.8 -> 41.1, client 19.5 -> 14.4, wall 6.46 -> 4.67 s.

### Steady state (400 trx/s `oltp_write_only` through vtgate, 30 s, 2 rounds)

| case | arm | sysbench avg / p95 ms | probe p50 / p99 / max ms | vtgate / vttablets us per sysbench trx |
|---|---|---|---|---|
| 1 consumer | base | 9.6 / 17.7 | 2.0 / 7.4 / 22 | 2067 / 2388 |
| 1 consumer | v5c | 9.4 / 16.9 | 1.9 / 8.0 / 29 | 2064 / 2329 |
| 4 consumers | base | 15.4 / 33.7 | 2.7 / 11.9 / 63 | 2046 / 2536 |
| 4 consumers | v5c | 14.6 / 29.5 | 2.7 / 10.6 / 124 | 2056 / 2456 |

No latency difference beyond noise; the vtgate CPU is dominated by serving the writes (~2 ms per sysbench trx, which is 1.8 binlog
transactions). vttablets -2.5% (1 consumer) and -3% (4 consumers). The max probe latencies are single outliers on a loaded machine.

## The changes

### vtgate (`go/vt/vtgate/vstream_manager.go`)
- **V6 #2 shell clone and #10 SizeVT only with chunking** (V6's hunks, unchanged).
- **Per batch instead of per event:** the liveness timer reset and the lag gauge (labeled, mutex + label join) are done once per
  callback (equivalent: the last value wins); the two error strings are built only on errors (they were `fmt.Sprintf`'d per callback).
- **Buffered event channel (16 batches) and response coalescing:** shard goroutines no longer wait for the client send while holding
  `vs.mu`; `sendEvents` appends the batches that are already queued (up to ~256KB, estimated from row value lengths) to one response.
  It never waits, so it adds no latency. Because it changes how many transactions a response carries (tests and possibly clients
  assume one), it is **opt-in via `VStreamFlags.coalesce_transactions`** (new field 14; old vtgates ignore it). Recommendation: make
  it the default in a later release.
- **Tablet options:** vtgate sets `VStreamOptions.coalesce_transactions` (it regroups events per transaction anyway) and passes
  `batch_copy_rows` from the client flags.
- **Skew fix** (bug below).

### vttablet (`go/vt/vttablet/tabletserver/vstreamer/`)
- **V6 #1** (event-driven catchup check), **V6 #9** (rule match cache), **V4 lazy event GTID string**, **P4 buffered event queues**
  (`binlog/binlog_connection.go`, `vstreamer.go`), as in their patches.
- **Transaction coalescing (`VStreamOptions.coalesce_transactions`, field 6):** in the replicate phase only (catchup and fastforward must
  stop exactly at a position), a complete transaction is held instead of sent while more binlog events are already queued; it goes out
  with the next send, or alone as soon as the queue is empty; bounded by the packet size and 1000 events. Stat
  `VStreamerTransactionsCoalesced`. The vplayer does not set the option yet (possible follow-up; V4 measured the related
  empty-transaction case).
- **Catchup skip:** `uvstreamer.lastCopySnapshot` is set before each `StreamRows`; `catchupAndCopy` skips the catchup when it is younger
  than `CatchupSkipMaxSnapshotAge` (10 s): the fastforward of the next copy replays the same events the catchup would have (only the
  duplicate FIELD event of the extra vstreamer disappears). 10 s bounds how long the next snapshot's fastforward can take while the
  row stream is open. Stat `VStreamerCopyCatchupsSkipped`.
- **`batch_copy_rows` (field 7):** `sendEventsForRows` sends one ROW event with all the batch's row changes.

### gRPC codec (`go/vt/servenv/grpc_codec.go`)
`defaultBufferPool` is now a Vitess pool with sync.Pool tiers for every power of two from 256B to 16MB and no zeroing: `Marshal` fills
exactly `[:size]`, and `MaterializeToBuffer` copies over the whole requested length, so zeroing is unnecessary. gRPC's
`mem.DefaultBufferPool()` has tiers 256B, 4KB, 16KB, 32KB, 1MB and clears `b[:cap(b)]` on every Get.

### Protos
`binlogdata.VStreamOptions.coalesce_transactions = 6`, `batch_copy_rows = 7`; `vtgate.VStreamFlags.batch_copy_rows = 13`,
`coalesce_transactions = 14`. Regenerated with protoc 21.3 and the module's plugins (all protos in one run, as `make proto` does);
only the 4 Go files changed. `vtadmin` web proto types were not regenerated.

N-1/N+1: all new behavior is behind new proto fields that older components ignore (an old tablet sends per-row copy events and one
transaction per message; an old vtgate never coalesces). The catchup skip and the skew fix change behavior without a flag; both keep
the API semantics (same events in the same order, except fewer duplicate FIELD events in the copy phase).

## Bugs

1. **`minimize_skew` stalls with >= 3 shards (fixed, P1 for users of the flag).** `mustPause` compares against `vs.lowestTS`, which is
   never set (V6 found the dead check), so when a skew is detected every non-laggard stream pauses, including streams within `MaxSkew`
   of the laggard or tied with it. Paused streams only wake when the skew is fixed, but the skew is computed over all streams including
   the paused ones: once the laggard passes a paused stream, the paused stream is the slowest and can never catch up. The VStream then
   delivers nothing until `maxSkewTimeoutSeconds` (10 min) and fails. Observed in 6/6 runs with 4 shards (vtgate log:
   `laggard is sbtest/40-80, map[-40:...469 40-80:...456 80-c0:...457 c0-:...458]`: 80-c0 and c0- paused 1-2 s from the laggard).
   Fix: set `lowestTS` to the minimum, and when the laggard is no longer the slowest stream, wake the paused streams and make the slowest
   one the laggard. Tests `TestComputeSkewDoesNotPauseStreamsCloseToTheLaggard` and `TestComputeSkewLaggardOvertakesPausedStream` fail
   without the fix.
2. **gRPC codec buffer pool zeroes 1MB for every 32KB-1MB message** (performance bug, fixed as #5).
3. Pre-existing test issue: `TestVStreamCopyCompleteFlow` fails with `-count>1` (global event list); not changed.

## Negative and small results
- **V6 #1 (event-driven catchup check) alone:** 39.5 -> 35.8 s for 40 tables. On an idle source the first event is the heartbeat after
  0.9 s, so it only saves the last 0.1 s; the snapshot-age skip is what removes the wait.
- **Tablet transaction coalescing on write_only:** no vttablet CPU change (-10% / -17% only on small and 100-row transactions).
- **Steady state at 400 trx/s:** no latency change; CDC CPU is a few % of the query path.
- **`minimize_skew` with 2 shards:** no stall (a tie or overtake needs a third stream, or a >2 s jump of the laggard).
- **Chunking:** slightly more expensive relative to no chunking with the patch (+11% vtgate per row), still 54% below base.

## Tests
- vtgate: `go test ./go/vt/vtgate -run 'TestVStream|TestCoalesceQueuedEvents|TestComputeSkew'` passes (new: `TestCoalesceQueuedEvents`,
  the two skew tests; the skew tests reset the global delay counter that `TestVStreamSkew` expects at 0).
- vstreamer (MySQL-backed, run as vt): full suite passes (97 tests), including the new `TestVStreamCoalescesTransactions`,
  `TestVStreamCopyBatchCopyRows` and `TestVStreamCopySkipsCatchupAfterRecentSnapshot` (the first and last fail with the feature
  disabled), and `TestVStreamCopyCompleteFlow` with updated expectations (the catchups before t2 and t3 are skipped; the same row events
  arrive in the same order via fastforward, minus two duplicate t1 FIELD events).
- servenv: `TestCodecBufferPool`, `TestCodecRoundTrip` pass (the cgroup tests fail in this sandbox, unrelated).
- binlog: passes. The vreplication suite was not run (the vplayer does not set the new options; P4's queue change was tested by P4).

## Follow-ups
1. Make `coalesce_transactions` the vtgate default after a release with the flag; let the vplayer set the tablet option too.
2. Empty-transaction coalescing for filtered CDC consumers (V4 #4 as a `VStreamOptions` field set by vtgate).
3. The vttablet still spends ~15% in `selectgo` at high rates: hand binlog events between goroutines in slices, not one by one.
4. A shared binlog reader per tablet for several consumers (linear source cost per consumer measured above).
5. Measure the codec pool on VReplication copy (250KB packets on both hops) and on streaming query results.
6. `stream_keyspace_heartbeats`, `heartbeat_interval` and journal events under a real reshard were not measured.

## Files
- Patch: `findings/V5-vstream.patch`. Harness and drivers: `perf-findings/V5-scripts/` (in the patch): `cluster.sh`/`c.sh` (harness),
  `gen.sh`/`upd100.lua` (backlogs), `drain.sh`, `copy.sh`, `live.sh`, `prof.sh`, `ab*.sh` (A/B drivers), `abstats.py`,
  `livestats.py`, `cpudiff.py`, `mkmany.sh`, `runtest.sh` (MySQL-backed tests as vt), `vsclient.go.txt` (client source; build it
  inside the module).
