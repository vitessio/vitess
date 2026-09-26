# V6-static: static review of VReplication hot paths not covered by earlier rounds

Investigator V6-static. Base commit aa9ccf9. Worktree `agent-a56f910b68ee17fd3` (changes uncommitted).
Prototype patch: `findings/V6-static.patch` (10 files, +169/-43). Microbenchmark sources (not in the patch; they were
scratch packages inside the module, since removed): `findings/V6-bench/v6bench_test.go.txt` (generic) and
`findings/V6-bench/vrepl_bench_test.go.txt` (package-internal, run against a copy of the `vreplication` package).

Method: code review, plus short microbenchmarks (`-benchtime` 200-300 ms or a fixed `Nx`, `-count` 2-4, `nice`). I ran no
clusters. MySQL-backed package tests (vstreamer, vreplication, vdiff) could not be run: running a test binary as `vt`
was refused by the sandbox. The machine load average was 4.9-10.6 during the benchmarks, so absolute ns are noisy. Ratios
and allocation counts are reliable.

Everything in "Already known" (BRIEF3, SUMMARY, P4) is excluded, except where a finding builds on it; I say so there.

## Ranked findings

| # | Finding | Where | Who feels it | Measured micro | Expected end-to-end (estimate) | Size / risk | Prototype |
|---|---|---|---|---|---|---|---|
| 1 | **VStream API copy (uvstreamer) waits ≥1 s per table**: the same issue as P4 #1, on the vtgate CDC path, which P4 did not fix | `vstreamer/copy.go:65-111` | VStream clients doing an initial copy (Debezium-style CDC, `VStream` with `TablesToCopy`/copy mode) | n/a (a fixed 1 s ticker) | ≥(T-1) s saved for T tables per shard (e.g. 1000 tables ≈ 17 min) | S / low | yes (event-driven check); a snapshot-age skip is a follow-up |
| 2 | **vtgate deep-clones every ROW/FIELD event** just to prefix the table name (default `ExcludeKeyspaceFromTableName=false`) | `vtgate/vstream_manager.go:1026-1036` | all vtgate VStream (CDC) users | 100-row event: **32.9 µs, 404 allocs → 0.35 µs, 3 allocs**. The clone costs ~3x the proto marshal of the same event | vtgate CDC CPU per row -30-60% (the clone is the largest per-row cost left in vtgate) | S / low | yes |
| 3 | **Reshard/sharded MoveTables: every stream fully decodes every column of both images before `in_keyrange` rejects the row** | `vstreamer.go:1210-1245`, `getValues` `:1330` | Reshard to N shards (N streams per source): (N-1)/N of all decode work is thrown away | 8-col row: decode all 1.5-2.3 µs, 17 allocs vs key column + `CellLength` skips 75-100 ns, 1 alloc | source vttablet running-phase CPU -30-50% for N≥4 (before F05; about half of that with F05) | M / medium | no |
| 4 | **Every filtered-out (empty) source transaction is still one gRPC message per stream**: `[BEGIN, GTID, COMMIT]`, then a full `DecodePosition` and relay-log round on the target | `vstreamer.go:309-330` (COMMIT flush), `:593-605` | MoveTables of a subset of tables from a busy keyspace; Reshard (each trx is empty for N-1 streams); idle forward streams whose `time_updated` heartbeats appear as empty trx on reverse streams | not measured; per empty trx: `EncodePosition` (F26) + 3 VEvents + a gRPC Send/Recv + relay Send/Fetch (2.5 µs, see #5) + `DecodePosition` | estimate 10-25 µs CPU per filtered trx per stream, split source/target | M / medium (VStream API semantics; gate it behind an option) | no |
| 5 | **relayLog starts a goroutine and a timer on every `Send` and every `Fetch`**, even when it never waits | `vreplication/relaylog.go:87,113,141-207` | every running vplayer (per source packet) | Send+Fetch without waiting: **2.3-2.9 µs, 13 allocs → 57-92 ns, 1 alloc** | target vttablet ~3-6% CPU/trx (P4: 44 µs/trx). May be P4's unexplained "+5-9% target CPU" once packets got smaller | S / low | yes (`TestRelayLogSendStallDeferredWhileThrottled` passes) |
| 6 | **SwitchTraffic LOCK TABLES cycles**: the 100 ms sleep also runs after the **last** cycle, and each `LOCK TABLES` RPC sets `ReloadSchema: true`, so each cycle does a full schema reload on every source primary, all while writes are blocked | `vtctl/workflow/server.go:3310-3326`, `traffic_switcher.go:1472-1504` | every MoveTables SwitchTraffic | n/a | **-100 ms fixed** from the ~0.4 s outage (25%), plus 2 schema reloads (tens to hundreds of ms on schemas with thousands of tables) | S / low | yes (workflow tests pass) |
| 7 | **VDiff row comparison uses `evalengine.NullsafeCompare` for every column, even for byte-identical values** | `vdiff/table_differ.go:774-799` | every VDiff | 6-col matching row: **470-610 ns, 6 allocs → 28 ns, 0 allocs** | VDiff tablet CPU -10-20% (the merge sort and row decoding remain) | S / low | yes |
| 8 | **`workflow show` / `GetWorkflows`: a serial topo `GetShard` per (workflow, shard)**, re-reading a record already fetched in parallel | `vtctl/workflow/workflows.go:337-355` | vtctld `GetWorkflows` / `workflow list/show`, VTAdmin | n/a | W workflows x S shards serial topo reads → S parallel reads (e.g. 50x32 = 1600 x ~1 ms ≈ 1.6 s → ~ms) | S / low | yes (workflow tests pass) |
| 9 | **vstreamer re-evaluates the filter rules for every TableMap event of a non-matching table**. It caches no plan for such tables, and `ruleMatches` calls `regexp.MatchString`, which compiles the regexp each time | `vstreamer.go:788`, `planbuilder.go:438-456` | VStream with regexp rules (`/customer.*`); MoveTables with many explicit rules (R string compares per event); busy keyspaces | regexp: **2.9 µs, 21 allocs → 7 ns** (map) | -3 µs per row event of every unmatched table, per stream | S / low | yes (memoized per table name) |
| 10 | **vtgate computes `event.SizeVT()` for every event even when transaction chunking is off** (the default) | `vstream_manager.go:822` | vtgate CDC | 100-row event: 1.3 µs (≈13% of its marshal) | vtgate CDC CPU -5-10% | S / low | yes |
| 11 | **uvstreamer copy sends one VEvent (+RowEvent+RowChange, keyspace/shard strings twice) per row** | `vstreamer/copy.go:131-179` | VStream API copy phase (also multiplies every per-event vtgate cost: #2, #10, lag gauge, timer reset) | 1000 rows marshal+unmarshal: **1.22 ms, 16 allocs/row, 177 B/row → 0.47 ms, 5 allocs/row, 127 B/row** | VStream copy CPU -50% on the tablet-vtgate hop, -28% wire bytes | M / medium (clients see fewer, larger ROW events; behind an option) | no |
| 12 | **buildReplicatorPlan is O(tables x rules) and runs 2-3 times per table copied** (copyTable, fastForward, catchup vplayer), and it re-parses every table's filter each time | `table_plan_builder.go:137-181`, `vcopier.go:227,388`, `vplayer.go:195` | MoveTables/Reshard/Materialize of thousands of tables | 1000 tables: 4.5-6.8 ms/call. 3000 tables: 18-23 ms/call | copy of 3000 tables: ~3000 x 3 x 20 ms ≈ 3 min of target CPU spent building plans | S-M / low | no |
| 13 | **vstreamer plan build: 1 new MySQL connection + 3 queries per table per stream start**, 2 of them under the global `schema.Engine.mu` | `vstreamer.go:1017-1091` (`GetTableForPos` → `fetchColumns` + `populatePrimaryKeys` under `se.mu`; `getExtColInfos` → `cp.Connect` + `information_schema.columns`) | stream (re)starts: every per-table fastForward/catchup vstream in the copy phase, tablet restarts, VStream `/.*` over many tables | n/a | per table ~1-5 ms (more on MySQL 8 with large info schema), serialized across all streams of the tablet | M / medium | no |

Smaller items and the correctness bugs are at the end.

---

## 1. uvstreamer catchup waits for a 1 s tick per table (VStream API copy)

```go
// vstreamer/copy.go:65
func (uvs *uvstreamer) catchup(ctx context.Context) error {
	uvs.setReplicationLagSeconds(math.MaxInt64)
	...
	tkr := time.NewTicker(uvs.config.CatchupRetryTime)   // 1 s (uvstreamer.go:110)
	seconds := int64(uvs.config.MaxReplicationLag / time.Second) // 1ns -> 0
	for {
		sbm := uvs.getReplicationLagSeconds()
		if sbm <= seconds { ... return nil }
		select { case err := <-errch: ... case <-ctx.Done(): ... case <-tkr.C: }
	}
```

- `copy()` → `catchupAndCopy()` calls `catchup` before each table except the first, because `uvs.pos` is set by the
  previous table's field event.
- The first check always fails: the lag was just set to MaxInt64. The next check is at the 1 s tick.
- So a VStream in copy mode over T tables costs at least T-1 seconds per shard, no matter how small the tables are. This is P4 #1
  (`vcopier.catchup`) on the other copy implementation: the one used by vtgate `VStream` (CDC), not by VReplication
  workflows. P4 fixed only vcopier.
- `TestVStreamCopyCompleteFlow` (3 tables) spends ~2 s of its 5 s budget here.

**Prototype.**
- `setReplicationLagSeconds` does a non-blocking signal on a 1-slot `lagUpdated` channel.
- `catchup` also selects on that channel and re-checks the lag as soon as the catchup stream has delivered events.
- On a source with any write traffic, catchup now ends within milliseconds of the first batch whose lag is < 1 s. On an idle
  source it ends at the first heartbeat (900 ms), so it saves only ~0.1 s there.
- A nil channel (tests that build `uvstreamer{}` by hand) disables the signal.

**Follow-up for idle sources** (same idea as P4): record the time just before `StreamRows` of the previous table. If the
snapshot is younger than `MaxReplicationLag + 1s`, skip catchup: the `fastForward` in `copyTable` replays the same events. I
did not prototype it, because `TestVStreamCopyCompleteFlow` counts catchup events exactly and I could not run it.

**Risk.** Low. The exit condition is unchanged; it is only evaluated earlier. Events move from catchup to fastForward, but
both pass through `send2` in order.

**Status.** Compiles and vets. Not run: vstreamer tests need MySQL.

## 2. vtgate deep-clones each ROW/FIELD event

```go
// vtgate/vstream_manager.go:1026
func maybeUpdateTableName(event *binlogdatapb.VEvent, keyspace string, excludeKeyspaceFromTableName bool, ...) *binlogdatapb.VEvent {
	if excludeKeyspaceFromTableName { return event }
	ev := event.CloneVT()              // copies every RowChange, every Row, every value
	tableName := tableNameExtractor(ev)
	*tableName = keyspace + "." + *tableName
```

- This runs for every ROW and FIELD event of every VStream through vtgate. `ExcludeKeyspaceFromTableName` defaults to false.
- The received events are not pooled: `grpctabletconn.VStream` hands `r.Events` straight to the callback. Nothing else modifies
  the row changes.
- Microbenchmark (8-column rows, 3 runs):

| rows/event | CloneVT | shell clone | MarshalVT of the same event (reference) |
|---|---|---|---|
| 1 | 931-1129 ns, 8 allocs | 438-527 ns, 3 allocs | 172-206 ns |
| 10 | 3.5 µs, 44 allocs | 0.31-0.33 µs, 3 allocs | 1.1 µs |
| 100 | **31-33 µs, 404 allocs, 36 KB** | **0.33-0.41 µs, 3 allocs** | 9.6-12.7 µs |

So vtgate spends ~3x more CPU cloning a ROW event than it later spends serializing it to the client.

**Prototype.** Clone only the shells:
- Detach `RowEvent.RowChanges` (or `FieldEvent.Fields`), `CloneVT`, re-attach the same slice to both events, then rename.
- The original event is unchanged after the call.
- This still requires that no other goroutine reads the same event object during the call. That holds for gRPC-received
  events and for vtcombo's local vstreamer, which creates fresh events.
- `go test ./go/vt/vtgate -run TestVStream -race` passes.

**Risk.** Low. The alternative is to rename in place, with no clone at all, but tests that re-send the same event objects
would then see a double prefix.

## 3. Reshard/sharded filters decode whole rows that are then dropped

- `processRowEvent` calls `getValues` for the before image and for the after image. Each call runs `mysqlbinlog.CellValue` on
  every column, including ENUM/SET mapping, before `plan.shouldFilter` evaluates `in_keyrange`.
- In a reshard to N shards, each of the N streams on the source does this for every row, and keeps only 1/N of them.
  Per-image micro (8 columns: bigint, int, 2 varchar, datetime2, timestamp2, decimal, bigint):

| | ns/op | allocs |
|---|---|---|
| decode all columns (`CellValue`) | 1547-2289 | 17 |
| decode the vindex column, skip the rest with `CellLength` | 73-102 | 1 |

**Fix sketch.** When the plan's filters only reference a few columns (VindexMatch on the vindex columns, or
column comparisons):
1. Walk the image once with `CellLength` to get the column offsets. It needs the same null/data-bitmap handling as
   `getValues`.
2. Decode only the filter columns and evaluate the filter.
3. Decode the remaining columns only if the image passes.

For updates, both images are needed only if one of them passes. This composes with F05 (cheaper decode) and F17 (cheaper
hash): after them, the relative saving is smaller but still several times larger per rejected row.

**Risk.** Medium. `getValues` has subtle partial-image (NOBLOB), JSON partial-update and ENUM/SET logic that must stay in the
full-decode path.

## 4. Empty (filtered) transactions: one gRPC message per source transaction per stream

`parseEvent` always emits BEGIN for the GTID event and GTID+COMMIT for the XID, even if every row in between was filtered.
`bufferAndTransmit` flushes on COMMIT (`vstreamer.go:309-330`), so each source transaction becomes one
`VStreamResponse{BEGIN, GTID(full GTID set string), COMMIT}` Send. The target then:
- does a relay-log Send and Fetch,
- runs `binlogplayer.DecodePosition` on the full GTID set (`vplayer.go:716`),
- only keeps the transaction as `unsavedEvent`.

Who pays:
- MoveTables of a few tables from a busy keyspace: almost every trx is empty.
- Reshard to N: each trx is empty for N-1 streams.
- Every idle forward stream writes `time_updated` once per second (`--vreplication-heartbeat-update-interval=1`). Each write
  is an empty trx for every reverse stream and CDC stream reading that tablet.

**Fix sketch** (not prototyped):
- In `bufferAndTransmit`, hold an empty transaction (`[BEGIN, GTID, COMMIT]` with nothing else buffered) instead of sending it.
- Replace it with the next empty trx's GTID. The GTID is a full set, so the latest one subsumes the earlier ones.
- Drop it when a non-empty trx is sent.
- Flush it when the input queue is empty (needs P4's buffered event channel: `len(ch)==0`), on the heartbeat timer, or when
  the packet would be sent anyway.
- The stop position still works, because `AtLeast` works on sets.
- For VStream API clients this changes the event stream, so enable it through a `VStreamOptions` flag that the vplayer sets.
  That is N±1 safe: an old source ignores the flag.
- Expected gain (estimate): 10-25 µs CPU per filtered trx per stream, over both processes, plus far fewer target wake-ups.
  Worth an A/B on P4's harness with MoveTables of 1 table out of 4.

## 5. relayLog goroutine+timer per Send and per Fetch

```go
// relaylog.go:87 (Send) / :113 (Fetch)
cancelTimer := rl.startSendTimer()   // time.NewTimer + chan + goroutine, before checking whether we must wait
defer cancelTimer()
for rl.curSize > rl.maxSize || len(rl.items) >= rl.maxItems { rl.canAccept.Wait() ... }
```

- `Send` runs once per packet received from the source, i.e. per transaction at low to moderate rates. Almost every time,
  the timer is started only to be cancelled.
- `Fetch` does the same whenever items are already present, which is the case under load.

**Prototype.**
- Arm the send timer only inside the wait branch.
- Arm the fetch timer only when there are no items.
- Use `time.AfterFunc` for the fetch timer, so a goroutine runs only if it fires. The stall deferral while throttled is
  unchanged.

Micro (Send+Fetch pair, no waiting, 20000 iterations x 4): **2258-2913 ns, 13 allocs → 57-92 ns, 1 alloc**.

- `TestRelayLogSendStallDeferredWhileThrottled` passes. I ran it on a MySQL-free copy of the package.
- This is a plausible cause of P4's "+5-9% target vttablet CPU" with smoother, smaller packets: each packet costs two
  goroutines and two timers here.

**Risk.** Low. The deadlines are unchanged: the send deadline used to start at `Send` entry, and there was no waiting before
the loop.

## 6. SwitchTraffic: LOCK TABLES cycles (P4 follow-up 1)

This is the P4 follow-up. `switchWrites` (`vtctl/workflow/server.go:3310-3326`):

```go
for cnt := 1; cnt <= lockTablesCycles; cnt++ {        // 2
	if err := ts.executeLockTablesOnSource(ctx); err != nil { ... }
	time.Sleep(lockTablesCycleDelay)                   // 100 ms, also after the 2nd (last) cycle
}
```

and `executeLockTablesOnSource` (`traffic_switcher.go:1472-1504`) issues
`ExecuteFetchAsDba{Query: "LOCK TABLES ... READ", ReloadSchema: true}` on every source primary.

- **The trailing sleep.** The comment says the pause exists to catch writes that raced past the deny-list check before the
  *next* lock. After the last cycle there is no next lock, so that sleep is 100 ms of pure write outage.
- **The schema reloads.** `ReloadSchema: true` makes the tablet run `se.Reload()` (full `ReloadAt` with an empty position, under
  `se.mu`) before the RPC returns. So each cycle includes a complete schema reload on every source primary, while writes are
  blocked. `LOCK TABLES` changes no schema.
- The wrangler copy (`go/vt/wrangler/traffic_switcher.go:596-606`) has the same code. I did not change it (legacy vtctl).

**Prototype.**
- Sleep only between cycles.
- Use `ReloadSchema: false` for the lock statement.
- `go test ./go/vt/vtctl/workflow` passes. The fake TMC does not model the reload flag, and I added no timing test (it would be
  flaky).

**Expected.**
- -100 ms of the remaining ~0.4 s write outage P4 measured: ~25%, deterministic.
- -2 schema reloads per source shard. Negligible on P4's 4-table schema, but a reload runs `information_schema` queries, which
  take tens to hundreds of ms on schemas with thousands of tables.

Further options (not done):
- One cycle plus a `SELECT` barrier instead of two cycles.
- Merging the two serial `VReplicationExec` calls per uid in `createReverseVReplication` (insert, then update of cells and
  options) into one. That is a few ms, also inside the outage.

## 7. VDiff: byte-equality fast path in `compare`

`tableDiffer.compare` calls `evalengine.NullsafeCompare` for every compared column of every row: it parses both sides (for
example text integers into numbers), with type and collation dispatch.
- In a VDiff almost all columns of almost all rows are identical.
- Values of the same `Type` with identical bytes compare equal under every collation and numeric type, and NULL == NULL.

**Prototype.** `if sv.Type()==tv.Type() && bytes.Equal(sv.Raw(), tv.Raw()) { continue }` before the generic compare.

Micro (bigint, 2 varchar utf8mb4_0900_ai_ci, datetime, decimal, NULL): **470-610 ns, 6 allocs → 28 ns, 0 allocs per row**.

**Risk.** Low. Different bytes, or different types (e.g. INT32 vs INT64 after a type change), still take the old path.

The PK merge sort still uses the generic path via `engine.MergeSort` (see F08). A similar fast path in its comparator
would help too.

## 8. GetWorkflows: serial topo read per (workflow, shard)

- `scanWorkflow` does `wf.ts.GetShard(ctx, keyspace, tablet.Shard)` the first time it sees each workflow on each shard
  (`workflows.go:337-355`).
- It runs inside the serial `buildWorkflows` loop, while `fetchWorkflowsByShard` → `forAllShards` has already read the same
  shard records, in parallel.

**Prototype.**
- `fetchWorkflowsByShard` stores the `ShardInfo`s it read in the fetcher (the fetcher is per call).
- `scanWorkflow` uses them, and falls back to `GetShard` if one is missing.
- `go test ./go/vt/vtctl/workflow` passes.

**Expected.** `workflow list/show` and VTAdmin on keyspaces with many workflows and shards drop from W x S serial topo round
trips to none (S parallel reads that already happen). Same data: the records are from the same call.

Also per stream, but small: `json.Unmarshal` of the workflow options and `strings.Split` of cells/tags are redone for every
stream of the same workflow (`workflows.go:376`).

## 9. Unmatched tables: rules re-evaluated per TableMap event

In `parseEvent`, a TableMap for a table outside the filter returns before any plan is cached (`vstreamer.go:788`). So the next
TableMap for that table id (one per row event per transaction) parses the TableMap again and calls `ruleMatches`:
- For a regexp rule, `regexp.MatchString` compiles the pattern every time: 2.7-3.2 µs and 21 allocs, vs 31-34 ns
  precompiled and 6-8 ns from a map.
- MoveTables filters have one explicit rule per table, so for R tables it's R string compares per event.

**Prototype.**
- Memoize the result per table name in the vstreamer. The filter never changes during a stream.
- I did not cache a nil plan per table id, to keep the existing handling of reused table ids.
- `ruleMatches` compiling per call is also used by `tableMatches` for statement events (rare).

**Related, not changed.**
- `buildPlan` (`planbuilder.go:466-484`), `uvstreamer.matchTable` (`uvstreamer.go:211`) and vreplication `MatchTable` also call
  `regexp.MatchString`. They run per plan build, not per event.
- `TableMap` parsing of cached tables still allocates the TableMap, name strings, metadata and collation ids per event (~6
  allocs), only to compare the name. A byte-level name check before the full parse would remove that.

## 10. vtgate: `SizeVT()` per event when chunking is off

- `accumulatedSize += event.SizeVT()` (`vstream_manager.go:822`) walks the whole event, all row values included, for every
  event.
- Its only consumer is the transaction-chunking decision, and `transactionChunkSizeBytes` is 0 by default.

**Prototype.** Compute it only when chunking is enabled.

Other per-event work in the same loop:
- `vs.streamLivenessTimer.Reset(...)` on one timer shared by all shard goroutines.
- `vs.vsm.vstreamsLag.Set(labelValues, lag)`: a labeled gauge with a mutex, per event.
- `fmt.Sprintf` of `aligningStreamsErr` per callback.

Moving these to once per callback batch would be equivalent (the last event wins), but I did not do it.

## 11. uvstreamer copy: one VEvent per row

`sendEventsForRows` (`copy.go:131-179`) turns each copied row into its own VEvent, RowEvent and RowChange, repeating keyspace
and shard twice per row. It is documented ("send one RowEvent per row").

Marshal+unmarshal of a 1000-row packet:

| | time | allocs/row | wire B/row |
|---|---|---|---|
| one event per row | 1.22-1.35 ms | 16 | 177 |
| one ROW event with all rows | 0.45-0.49 ms | 5 | 127 |

In vtgate each ROW event also goes through #2, #10, the lag gauge and the liveness timer reset. Batching should be opt-in:
clients see different event granularity. A `VStreamFlags`/`VStreamOptions` flag passed from vtgate is N±1-compatible.

## 12. buildReplicatorPlan cost, paid per table

`buildReplicatorPlan` loops over every table in `colInfoMap`:
- For each table, `MatchTable` scans all rules. MoveTables has one rule per table, so the work is O(T²) string compares.
- A regexp rule is recompiled for every table.
- It also parses and plans each matched table's filter SQL (`buildTablePlan`).

It is called by `copyTable`, by the fastForward vplayer's `play`, by the catchup vplayer's `play` (when not skipped) and by
`initTablesForCopy`, so 2-3 times per table in the copy phase.

| tables | movetables (T exact rules) | reshard (`/.*`) |
|---|---|---|
| 100 | 0.26-0.32 ms | 0.57 ms |
| 1000 | 4.5-4.8 ms | 4.7-6.8 ms |
| 3000 | 21-23 ms | 18-22 ms |

- 3000 tables: ~3 x 3000 x 20 ms ≈ 3 min of target CPU.
- 1000 tables: ~15 s. That is noticeable but small next to P4's ~0.1 s fixed cost per table.

**Fix.**
- Index the exact-match rules in a map and precompile the regexps once per vreplicator.
- Cache the per-table partial plans across calls. Only the `copyState` lastpk differs between calls.

**Related.** `buildColInfoMap` (`vreplicator.go:374-468`) issues one `information_schema.columns` query per table in the
database, on every vreplicator start: stream start and every retry after an error. A single query for the schema, ordered
by table and ordinal position, would cut thousands of round trips per restart per stream.

## 13. vstreamer plan build per table: new connection plus queries, partly under `se.mu`

For each table a stream sees for the first time (`buildTableColumns`, `vstreamer.go:1017-1091`):
- `se.GetTableForPos`. With the historian disabled (the default: `--track-schema-versions=false`), it runs `fetchColumns` and
  `populatePrimaryKeys` against MySQL **while holding `se.mu`** (`schema/engine.go:911-940`).
- `getFields` → `getExtColInfos` opens a **new MySQL connection** (`cp.Connect`) and queries `information_schema.columns`.

Each new vstreamer does this for every matched table it encounters:
- every per-table fastForward and catchup stream of the copy phase,
- every stream restart after a tablet restart or reparent,
- every VStream API client start.

All streams of the tablet serialize on `se.mu` (so do schema reloads and `GetSchema` callers).

**Fix.**
- Cache the `extColInfo` and the refreshed `MinimalTable` per table in the vstreamer `Engine`, invalidated by the schema
  engine's change notifications (or keyed by `se`'s last-change timestamp).
- At minimum, use the connection pool instead of `cp.Connect`.

Not prototyped; the gain needs a cluster to measure (stream restart latency with many tables).

---

## Smaller items (low impact, noted for completeness)

- `vstreamer.parseEvent` computes `vs.eventGTID.String()` for every event that reaches the end of `parseEvent`, even when
  `vevents` is empty (e.g. row events fully rejected by the filter) (`vstreamer.go:888-891`). Compute it once per GTID event
  and only when needed.
- vtgate `sendEventsLocked` clones the whole VGTID (all shards' GTID sets and TablePKs) for every GTID and LASTPK event
  (`vstream_manager.go:1091,1120`): O(shards) per transaction. The client receives the full VGTID anyway (API), so only the
  clone is avoidable (copy-on-write).
- `uvstreamer.send2` runs `DecodePosition` on every GTID event of catchup/fastForward (`uvstreamer.go:346-349`). On the target,
  `vplayer.applyEvent(GTID)` decodes every transaction's GTID set (known from F26; lazily decoding only the last GTID of a
  relay-log batch would remove it).
- Each controller dials its own gRPC connection to the source tablet on every (re)start (`external_connector.go:182-186`).
  With hundreds of streams that means hundreds of HTTP/2 connections and TLS handshakes.
- `copyNext` builds the copy_state query as `id in (select max(id) from _vt.copy_state group by vrepl_id, table_name)` without
  a `vrepl_id` filter in the subquery (`vcopier.go:297`). It scans every workflow's copy_state rows, once per table per
  stream.
- `ExecuteFetchAsDba` for LOCK TABLES also runs `ReplaceTableQualifiersMultiQuery` (a full parse) on the tablet. That is
  negligible.

## Correctness bugs found

1. **VDiff `VDiffRowsCompared` gauge is wildly over-reported (P2, observability).**
   - `updateTableProgress` does `TableDiffRowCounts.Add(table, dr.ProcessedRows)` every 10,000 rows
     (`table_differ.go:875`), but `dr.ProcessedRows` is already cumulative. After 1M rows the gauge reads
     10k·(1+2+…+100) = 50.5M.
   - `VDiffRowsComparedTotal` (`globalStats.RowsDiffedCount.Add(dr.ProcessedRows)`, `:588`) re-adds the rows of earlier
     attempts, which are loaded from the persisted report, after every resume or `--max-diff-duration` restart.
   - **Fixed in the patch:** add the delta, and count only this attempt's rows. `TestUpdateTableProgressRowCounts` would fail on
     main (it would read 10000, 30000, 60000). It compiles and vets, but I could not run it: the vdiff package's TestMain needs
     MySQL.
2. **vtgate `mustPause` compares against `vs.lowestTS`, which is never assigned** (`vstream_manager.go:601`, field at `:148`,
   P3).
   - The condition `timestamps[streamID] - 0 <= MaxSkew` is never true.
   - So with `MinimizeSkew`, every non-laggard stream always pauses once skew is detected. That may well be the intent, but
     the check is dead code and suggests an intended tolerance that doesn't exist.
3. **relayLog `timedout` is shared by the Send and Fetch timers, and a fired-but-cancelled timer can still set it**
   (`relaylog.go`, P3, pre-existing).
   - A Send stall timer that fires just as Fetch drains the log sets `timedout=true` after Fetch reset it. The Send then
     reports `relay log I/O stalled` although progress was made.
   - This needs the 5-minute deadline to expire at that exact moment, so it is very unlikely. The patch does not change it.
4. **Journal/ROW with `IsInternalTable`**: nothing found.
5. I found no new data-correctness bug in the vplayer or vcopier beyond the known BUGS.md items.

## Patch contents (`V6-static.patch`)

| file | change | tests |
|---|---|---|
| `vtctl/workflow/server.go`, `traffic_switcher.go` | #6: no sleep after the last LOCK TABLES cycle; `ReloadSchema: false` | `go test ./go/vt/vtctl/workflow` passes |
| `vtctl/workflow/workflows.go` | #8: reuse the ShardInfos | same |
| `vtgate/vstream_manager.go` | #2 shell clone, #10 SizeVT only with chunking | `go test ./go/vt/vtgate -run TestVStream -race` passes |
| `vreplication/relaylog.go` | #5 lazy timers, AfterFunc for fetch | `TestRelayLogSendStallDeferredWhileThrottled` passes (run on a MySQL-free copy) |
| `vstreamer/copy.go`, `uvstreamer.go` | #1 event-driven catchup check | build and vet only (needs MySQL) |
| `vstreamer/vstreamer.go` | #9 memoized ruleMatches | build and vet only (needs MySQL) |
| `vdiff/table_differ.go`, `_test.go` | #7 compare fast path; bug 1 fix + test | build and vet only (needs MySQL) |

`scripts/fmt` and `go vet` are clean on all changed packages.

**Not verified:** the vstreamer, vreplication and vdiff package tests. They need MySQL, and running a test binary as `vt` was
refused in this sandbox. Someone with a cluster should run them before trusting #1, #7 and #9.

## Addendum (orchestrator): MySQL-backed test run of the V6 patch

The binaries were compiled as root from the V6 worktree and run as `vt`, with VTROOT=/home/vt (holding `bin/mysqlctl` and a copy of `config/`), VT_MYSQL_ROOT=/usr and MySQL 8.0.46:
- `go/vt/vttablet/tabletmanager/vdiff`, full suite: PASS (165 tests, including the new `TestUpdateTableProgressRowCounts`).
- `go/vt/vttablet/tabletserver/vstreamer`, full suite: PASS (189 tests).
- `go/vt/vttablet/tabletmanager/vreplication`, `-run 'TestRelayLog|TestPlayer'`: PASS (289 tests).

Not yet checked: whether `TestUpdateTableProgressRowCounts` fails on main.
