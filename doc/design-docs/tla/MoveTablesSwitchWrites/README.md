# TLA+ model of MoveTables SwitchTraffic (writes)

`MoveTablesSwitchWrites.tla` models switching PRIMARY traffic for a
`MoveTables` workflow, with and without vtgate buffering. It covers the
traffic switcher itself and the vtgate and vttablet code that keeps client
writes flowing while it runs.

| Model component | Code it follows |
| --- | --- |
| `Begin` … `Freeze`, `Fail`, `Cancel` | `Server.switchWrites` (`go/vt/vtctl/workflow/server.go`), `stopSourceWrites`, `switchDeniedTables`, `executeLockTablesOnSource`, `gatherSourcePositions`, `waitForCatchup`, `createJournals`, `allowTargetWrites`, `changeWriteRoute`, `cancelMigration` (`traffic_switcher.go`) |
| `srcTabDeny`, `tgtTabDeny`, `Send`, `AcquireMDL`, `Commit` | the `enforce denied tables` query rule (`tm_state.go` `applyDenyList`), checked in `QueryExecutor.checkPermissions` *before* `execAutocommit` takes a tx pool connection (`query_executor.go`) |
| `StreamApply` | the forward VReplication stream |
| `Start`, `Capture`, `Load`, `GwDone`, `VSchemaWait*` | `Executor.newExecute` (`plan_execute.go`): 3 tries, `waitForNewerVSchema`, deferred `ClearPlans`; `fetchOrCreatePlan` (`newVCursor` captures the VSchema, `plans.GetOrLoad` reads the epoch later) |
| `GwBuffer`, `GwCheck` | `TabletGateway.withRetry` (`tabletgateway.go`): buffer once, a `FAILED_PRECONDITION` marks the only primary invalid |
| `Buffer*`, `MinTimeElapsed` | `shardBuffer` (`go/vt/vtgate/buffer/shard_buffer.go`) |
| `KewSeesVSchema`, `HealthCheck`, `Ensure` | `KeyspaceEventWatcher` (`go/vt/discovery/keyspace_events.go`): `getMoveTablesStatus`, `onHealthCheck`, `MarkShardNotServing`, `ensureConsistentLocked` |
| `VtgateSeesVSchema` | the VSchemaManager's SrvVSchema watch, `Executor.SaveVSchema` → `ClearPlans` |

## Scope and abstractions

- One source shard and one target shard. One vtgate. `Reqs` concurrent,
  non-transactional, single-statement writes of the same query shape, so they
  share a plan cache entry.
- `SwitchTraffic` in reverse (`ReverseTraffic`) runs the same code with the
  keyspaces swapped, so the model covers it too.
- Not modeled: reads and `switchReads`, mirror rules, sequences, stream
  migration, the reverse stream, multi-tenant and shard-by-shard migrations,
  tablet reparents during the switch, and buffer size limits. A multi-statement
  transaction behaves like a write that holds its MDL longer, so `LOCK TABLES`
  waits for it; it is not modeled separately.
- `LOCK TABLES … READ` is an atomic step. It is enabled only when no write
  holds the table's MDL; the connection is closed right away, which releases
  the lock.
- Time is not modeled. Every timeout can fire whenever its timer is running:
  `--buffer-window`, `--buffer-max-failover-duration`, the `waitForNewerVSchema`
  timeout, and `--buffer-min-time-between-failovers`. `Timeouts = FALSE`
  disables the first three, which models a client and a buffer that wait
  forever.
- `SlowWrites = FALSE` encodes the timing assumption behind the two
  `LOCK TABLES` cycles: a write that passed the deny check takes its MDL
  before the second cycle. `SlowWrites = TRUE` drops it.

## Properties

| Name | Kind | Meaning |
| --- | --- | --- |
| `NoLostWrites` | safety | after the journal is written, every row committed on the source is on the target |
| `NoSplitBrain` | safety | once a client wrote to the target, the source commits nothing the target lacks |
| `NoEarlyTargetWrites` | safety | the target takes client writes only after the routing rules point at it |
| `CancelKeepsSource` | safety | a cancelled switch leaves every client write on the source |
| `NoDuplicates` | safety | a retried write is applied at most once |
| `NeverDeniedAtTarget` | safety | vtgate never routes a write to a target that denies it |
| `RequestsTerminate` | liveness | every request eventually returns to the client |
| `SwitchTerminates` | liveness | the switch ends, either `done` or `cancelled` |
| `WritesSucceedIfSwitchSucceeds` | liveness | with a patient client (`Timeouts = FALSE`), if the switch succeeds every write eventually succeeds |
| `BufferEndsOnlyAfterSwitch` | diagnostic | buffering is never ended by a keyspace event before the routing rules switch |
| `NoFailureAfterSwitch` | diagnostic | a write that arrives after vtgate already routes to the target never fails |

`FixedPlanCache`, `DrainOnDeny` and `KewFix` switch on candidate fixes for the
problems the model finds. They are described under Results.

## Running

Requires Java 11 or newer and `tla2tools.jar` (put it next to `run.sh` or set `$TLA2TOOLS`)
([releases](https://github.com/tlaplus/tlaplus/releases)).

```sh
./run.sh Safety_Buffer   # or any other <name> for which <name>.cfg exists
python3 trace.py out/Safety_Buffer.trace.json   # compact view of a counterexample
```

## Results

Checked with TLC 2026.10. "2 writes" and "3 writes" are the size of `Reqs`.
A row marked *pending* was still running when this was written.

### Current code

| Config | Buffering | What is checked | Result |
| --- | --- | --- | --- |
| `Safety_NoBuffer` (3 writes, cancel, unrelated rebuild) | off | all safety properties | *pending* |
| `Safety_Buffer` (3 writes, cancel) | on | all safety properties | *pending* |
| `Liveness_NoBuffer` (1 write, cancel, unrelated rebuild) | off | safety, `RequestsTerminate`, `SwitchTerminates` | *pending* |
| `Liveness_Buffer` (1 write, cancel, unrelated rebuild) | on | safety, `RequestsTerminate`, `SwitchTerminates` | *pending* |
| `SlowWrites_NoBuffer` / `SlowWrites_Buffer` | off / on | safety without the `LOCK TABLES` timing assumption | **violated**: `NoLostWrites` (finding 1) |
| `Force_Buffer` | on | safety with `--force` and a failed refresh | **violated**: `NoLostWrites` (as `--force` warns) |
| `Avail_NoBuffer` / `Avail_Buffer` | off / on | `WritesSucceedIfSwitchSucceeds` | **violated** (finding 2) |
| `Diag_FailAfterSwitch_Buffer` | on | `NoFailureAfterSwitch` | **violated** (finding 2) |
| `Diag_EarlyRelease` | on | `BufferEndsOnlyAfterSwitch` | **violated** (finding 3) |
| `Cancel_NoTimeouts_NoBuffer` / `_Buffer` | off / on | `RequestsTerminate` with a patient client and a cancelled switch | **violated** (finding 4) |

When the `LOCK TABLES` timing assumption holds and `--force` is not used, the
current code keeps every write it acknowledges, both with and without
buffering. It never applies a write twice, never takes client writes on both
sides, and a cancelled switch leaves every write on the source. Results for
the larger configurations are pending (see the table). The problems found
are a lost-write window that depends on timing (1) and availability problems
(2–4) in vtgate.

### Findings

**1. A write admitted just before the deny rule loads can commit after the
cutover position.** (`SlowWrites_*`)

`checkPermissions` evaluates the deny rule before `execAutocommit` takes a
connection from the tx pool. While it waits there, by default for up to
`--queryserver-config-txpool-timeout` (1s), the write holds no MDL.
`executeLockTablesOnSource` runs two `LOCK TABLES … READ` cycles 100ms apart.
Those cycles only wait for writes that already hold the MDL. A write still in
the pool queue reaches MySQL after the second cycle. It is then committed
after `gatherSourcePositions`, the stream is stopped at that position, and
the write is acknowledged to the client but never reaches the target.

```
 3 SrcDenyTopo            srcTopoDeny=True
11 Send                   rs[r1:passed]          <- r1 passed the deny check, waits for a pool connection
12 SrcRefresh             srcTabDeny=True        <- the deny rule is loaded on the source primary
14 Lock1 / 15 Lock2                              <- r1 holds no MDL, so LOCK TABLES does not wait for it
16 GatherPositions        srcPos=0
17 Catchup                streamOn=False
19 AcquireMDL             rs[r1:mdl]
20 Commit                 srcLog=<<r1>>  rs[r1:acked]   <- acknowledged, never copied: NoLostWrites violated
```

`DrainOnDeny` models a fix: after loading the new rules, the `RefreshState`
that loads the deny rule returns only once the writes admitted under the
previous rules have finished. With it, all safety properties hold even with
`SlowWrites = TRUE` (`Fix_SlowWrites_*`), and the `LOCK TABLES` cycles are no
longer needed for correctness. A smaller mitigation is to check the rule
again after the pool connection is acquired. That narrows the window but does
not remove it.

**2. The plan cache can keep a source-routed plan after the switch.**
(`Avail_*`, `Diag_FailAfterSwitch_Buffer`)

`fetchOrCreatePlan` captures the VSchema in `newVCursor`, then reads the cache
epoch in `plans.GetOrLoad`. If `SaveVSchema` runs between the two, a plan
built for the old routing is cached under the new epoch. Every later write of
that shape goes to the source and is denied. It then waits for a newer VSchema
that never comes, and fails after three tries. Only the deferred `ClearPlans`
of that failure clears the plan. This affects writes that arrive *after*
vtgate already routes to the target, with or without buffering:

```
15 ChangeRouting          routeHist=<<src,tgt>>
17 Capture                (r1 captures VSchema v1)
18 VtgateSeesVSchema      epoch=2  vtVer=2
19 Start                  startedAfter[r2]=TRUE  <- vtgate already routes to the target
21 Load                   cache={ep:2,src}       <- r1 caches a v1 plan under epoch 2
22 Load                   (r2 uses the cached plan: source)
   … denied, buffered, the VSchema waits time out three times …
48 GwDone                 rs[r2:failed]
```

`FixedPlanCache` models reading the VSchema and the epoch as one snapshot.
With it, `NoFailureAfterSwitch` holds (2.5M states), and with a patient client
`WritesSucceedIfSwitchSucceeds` holds with and without buffering.

**3. A periodic health check ends MoveTables buffering early.**
(`Diag_EarlyRelease`)

Buffering starts on the first `disallowed due to rule` error and calls
`MarkShardNotServing`. That marks the source shard not serving, although its
primary is serving. During `switchWrites` nothing rebuilds the SrvVSchema
until `changeRouting`, so the KeyspaceEventWatcher's MoveTables state is
still `None`. The next health stream message from the source primary (every
`--health-check-interval`, 20s by default) flips the shard back to serving.
`ensureConsistentLocked` then broadcasts, and the buffer stops with
`FailoverEndDetected` while writes are still denied:

```
11 GwBuffer               bufState=buffering  kewServing=False   <- MarkShardNotServing
12 HealthCheck            kewCons=True  kewEvent=True            <- source primary is serving
13 BufferHandleEvent      bufState=draining  releasedEarly=True  <- before the routing switch
```

Writes are not lost: a released write is denied again and falls back to
`newExecute`'s `waitForNewerVSchema`. But buffering ends early. Then
`--buffer-min-time-between-failovers` (1m) keeps the buffer from starting
again for the rest of the switch, and the buffer metrics report the wrong
reason. `KewFix` models a fix: for a MoveTables deny error, re-evaluate the
MoveTables state from topo instead of marking the primary not serving. With
it, `BufferEndsOnlyAfterSwitch` holds (`Fix_EarlyRelease`, 1.45M states).

**4. After a cancelled switch, denied writes wait out the VSchema timeout.**
(`Cancel_NoTimeouts_*`)

`cancelMigration` restores the deny rules but does not rebuild the
SrvVSchema. A write that was denied during the attempt waits in
`waitForNewerVSchema` for a VSchema that never comes. It waits the full
timeout, 30s without buffering or 10s with it, although the source is already
writable again. `KewFix` also makes `Cancel` rebuild the SrvVSchema.

### With all three fixes

| Config | Buffering | What is checked | Result |
| --- | --- | --- | --- |
| `Fix_SlowWrites_NoBuffer` | off | safety with `SlowWrites`, cancel, unrelated rebuild | holds (2.8M states) |
| `Fix_SlowWrites_Buffer` | on | safety with `SlowWrites` and cancel | *pending* |
| `Fix_EarlyRelease` | on | `BufferEndsOnlyAfterSwitch` | holds (1.45M states) |
| `Avail_NoBuffer_FixedCache` / `Avail_Buffer_FixedCache` | off / on | `WritesSucceedIfSwitchSucceeds` | holds |
| `Diag_FailAfterSwitch_Buffer_FixedCache` | on | `NoFailureAfterSwitch` | holds (2.5M states) |
| `Fix_All_Liveness_NoBuffer` / `_Buffer` | off / on | safety and liveness, all fixes | *pending* |
| `Fix_All_NoTimeouts_NoBuffer` / `_Buffer` | off / on | `RequestsTerminate` and `WritesSucceedIfSwitchSucceeds`, patient client, cancel | *pending* |

### Buffering on versus off

The model makes one point clear: for non-transactional statements, the
`newExecute` retry loop already holds writes during a switch. A write denied
by the source waits for a newer VSchema and is planned again. With buffering
off, that wait is `vschemaWaitTimeout` (30s) per retry. With buffering on, it
is `--buffer-max-failover-duration / 2` (10s by default), plus the time the
write spent in the shard buffer. Turning buffering on therefore shortens the
`newExecute` wait. Statements inside a transaction are never retried or
buffered: they fail and the client must retry the transaction.
