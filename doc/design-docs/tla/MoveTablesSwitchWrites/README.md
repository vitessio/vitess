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

Further constants:

| Constant | Meaning |
| --- | --- |
| `StartupEvent` | the model starts with a "keyspace consistent" event still queued, like the one the keyspace event watcher sends when it first sees a primary |
| `Diagnostics` | record the history variables the diagnostic properties read |
| `FixedPlanCache`, `DrainOnDeny`, `KewFix`, `EventGenerations` | candidate fixes for the problems the model finds, described under Results |

## State space reductions

Four reductions make the larger configurations tractable without changing what
the model can do:

- A per-request field is reset to its initial value as soon as nothing reads
  it again before it is next written.
- withRetry's choice of the next step after a buffer check reads only the
  request's own state, so it is folded into the step before it.
- The history variables used only by `BufferEndsOnlyAfterSwitch` and
  `NoFailureAfterSwitch` are written only when `Diagnostics = TRUE`.
- Requests are interchangeable, so the invariant-only configurations declare
  `SYMMETRY Perms`. TLC does not support symmetry for liveness checking, so
  configurations that check temporal properties do not use it.

`refinement/RefinementCheck.tla` checks the first three mechanically. It maps
every state of the unreduced model to a state of the reduced one and has TLC
check that every behavior of the unreduced model, fairness included, is a
behavior of the reduced model. Every property that holds for the reduced model
and does not read a reset field therefore holds for the unreduced one.
`refinement/check.sh <name>` takes the unreduced model from git history and
runs the check for `refinement/<name>.cfg`. `OptSafety` (every step maps to a
step or a stutter) is checked with 2 writes for the current code with and
without buffering, slow writes with `--force`, and all three fixes. `OptSpec`
(the same plus fairness) costs more and is checked with 1 write for the
current code with and without buffering and for all three fixes, and with
2 writes when timeouts are disabled. A deliberately broken reduction is
reported as a violation.

## Running

Requires Java 11 or newer and `tla2tools.jar` (put it next to `run.sh` or set `$TLA2TOOLS`)
([releases](https://github.com/tlaplus/tlaplus/releases)).

```sh
./run.sh Safety_Buffer   # or any other <name> for which <name>.cfg exists
python3 trace.py out/Safety_Buffer.trace.json   # compact view of a counterexample
```

## Results

Checked with TLC 2026.10 on the reduced model. "1 write", "2 writes" and
"3 writes" are the size of `Reqs`. The counterexample traces below are
abridged excerpts; step numbers depend on the exploration order.

### Current code

| Config | Buffering | What is checked | Result |
| --- | --- | --- | --- |
| `Safety_NoBuffer` (3 writes, cancel, unrelated rebuild) | off | all safety properties | holds (9.2M states) |
| `Safety_Buffer` (3 writes, cancel) | on | all safety properties | holds (12.3M states) |
| `Liveness_NoBuffer` (1 write, cancel, unrelated rebuild) | off | safety, `RequestsTerminate`, `SwitchTerminates` | holds |
| `Liveness_Buffer` (1 write, cancel, unrelated rebuild) | on | safety, `RequestsTerminate`, `SwitchTerminates` | holds |
| `SlowWrites_NoBuffer` / `SlowWrites_Buffer` | off / on | safety without the `LOCK TABLES` timing assumption | **violated**: `NoLostWrites` (finding 1) |
| `Force_Buffer` | on | safety with `--force` and a failed refresh | **violated**: `NoLostWrites` (as `--force` warns) |
| `Avail_NoBuffer` / `Avail_Buffer` | off / on | `WritesSucceedIfSwitchSucceeds` | **violated** (finding 2) |
| `Diag_FailAfterSwitch_Buffer` | on | `NoFailureAfterSwitch` | **violated** (finding 2) |
| `Diag_EarlyRelease` | on | `BufferEndsOnlyAfterSwitch` | **violated** (finding 3) |
| `Cancel_NoTimeouts_NoBuffer` / `_Buffer` | off / on | `RequestsTerminate` with a patient client and a cancelled switch | **violated** (finding 4) |
| `Diag_StaleEvent` | on | `BufferEndsOnlyAfterSwitch` with the finding 3 fix and a queued event | **violated** (finding 5) |

When the `LOCK TABLES` timing assumption holds and `--force` is not used, the
current code keeps every write it acknowledges, with and without buffering:
it never loses or duplicates a write, never takes client writes on both
sides, and a cancelled switch leaves every write on the source (checked with
3 writes). Every request returns and every switch ends (checked with 1 write).
The problems found are a lost-write window that
depends on timing (1) and availability problems in vtgate (2–5).

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

The code implements `DrainOnDeny` in vttablet. Each write (DML, `LOAD DATA`,
`nextval`) registers with the query engine before `checkPermissions` and
unregisters when it finishes. After `applyDenyList` loads the deny rule, it
calls `WaitForAdmittedWrites`, which waits for the registered writes to the
denied tables, for up to 30s. If they don't finish in time, `RefreshState`
fails and the switch is canceled, as for any other refresh failure. A write
whose plan was cached before the rule was loaded, but that registers after
the wait looked, sees the plan cache epoch that loading the rule advanced, and
checks the current rules instead of the ones cached with its plan. The
`LOCK TABLES` cycles stay: older tablets don't wait.

The drain only covers writes the deny rule stops. A deny rule matches a
statement by the tables it names, so it never stops a write to a moved table
made by a stored function, procedure, trigger or event, before or after the
switch. The model has no such writes.

**2. The plan cache can keep a source-routed plan after the switch.**
(`Avail_*`, `Diag_FailAfterSwitch_Buffer`)

`fetchOrCreatePlan` captured the VSchema in `newVCursor`, then read the cache
epoch in `plans.GetOrLoad`. If `SaveVSchema` ran between the two, a plan
built for the old routing was cached under the new epoch. Every later write of
that shape went to the source and was denied. It then waited for a newer
VSchema that never came, and failed after three tries. Only the deferred
`ClearPlans` of that failure cleared the plan. This affected writes that
arrive *after* vtgate already routes to the target, with or without
buffering:

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
With it, `NoFailureAfterSwitch` holds, and with a patient client
`WritesSucceedIfSwitchSucceeds` holds with and without buffering.

**3. A periodic health check ended MoveTables buffering early.**
(`Diag_EarlyRelease`)

Buffering started on the first `disallowed due to rule` error and called
`MarkShardNotServing`. That marked the source shard not serving, although its
primary is serving. During `switchWrites` nothing rebuilds the SrvVSchema
until `changeRouting`, so the KeyspaceEventWatcher's MoveTables state was
still `None`. The next health stream message from the source primary (every
`--health-check-interval`, 20s by default) flipped the shard back to serving.
`ensureConsistentLocked` then broadcast an event, and the buffer stopped with
`FailoverEndDetected` while writes were still denied:

```
11 GwBuffer               bufState=buffering  kewServing=False   <- MarkShardNotServing
12 HealthCheck            kewCons=True  kewEvent=True            <- source primary is serving
13 BufferHandleEvent      bufState=draining  releasedEarly=True  <- before the routing switch
```

Writes were not lost: a released write is denied again and falls back to
`newExecute`'s `waitForNewerVSchema`. But buffering ended early, then
`--buffer-min-time-between-failovers` (1m) kept the buffer from starting
again for the rest of the switch, and the buffer metrics reported the wrong
reason. `KewFix` models the fix: for a MoveTables deny error, re-evaluate the
MoveTables state from topo instead of marking the primary not serving. With
it, `BufferEndsOnlyAfterSwitch` holds (`Fix_EarlyRelease`).

**4. After a cancelled switch, denied writes waited out the VSchema
timeout.** (`Cancel_NoTimeouts_*`)

`cancelMigration` restored the deny rules but did not rebuild the
SrvVSchema. A write that was denied during the attempt waited in
`waitForNewerVSchema` for a VSchema that never came. It waited the full
timeout, 30s without buffering or 10s with it, although the source was
already writable again. `KewFix` also makes `Cancel` rebuild the SrvVSchema.

**5. A queued keyspace event could end a buffering that had just started.**
(`Diag_StaleEvent`)

The shard buffer ended its buffering on any keyspace event. An event that
was still queued when buffering started, such as the one the watcher sends
when it first sees a primary, ended the new buffering although it said
nothing about it. This affects reparent buffering as well. The model only
shows it with `StartupEvent`, because it otherwise starts with no event
queued:

```
10 GwBuffer               bufState=buffering  kewMT=switching   <- buffering starts, KewFix applied
11 BufferHandleEvent      bufState=draining  releasedEarly=True <- the queued startup event ends it
```

`EventGenerations` models the fix: starting a buffering advances a
generation, every event carries the generation it was sent at, and the buffer
ignores events older than its buffering. With it, `BufferEndsOnlyAfterSwitch`
holds (`Fix_StaleEvent`).

### With the fixes

| Config | Buffering | What is checked | Result |
| --- | --- | --- | --- |
| `Fix_SlowWrites_NoBuffer` | off | safety with `SlowWrites`, cancel, unrelated rebuild | holds |
| `Fix_SlowWrites_Buffer` | on | safety with `SlowWrites` and cancel | holds |
| `Avail_NoBuffer_FixedCache` / `Avail_Buffer_FixedCache` | off / on | `WritesSucceedIfSwitchSucceeds` | holds |
| `Diag_FailAfterSwitch_Buffer_FixedCache` | on | `NoFailureAfterSwitch` | holds |
| `Fix_EarlyRelease` | on | `BufferEndsOnlyAfterSwitch` | holds |
| `Fix_StaleEvent` | on | `BufferEndsOnlyAfterSwitch` with a queued event | holds |
| `Fix_All_Liveness_NoBuffer` / `_Buffer` | off / on | safety, `RequestsTerminate`, `SwitchTerminates`; all fixes, queued event | holds |
| `Fix_All_NoTimeouts_NoBuffer` / `_Buffer` | off / on | safety, `RequestsTerminate`, `WritesSucceedIfSwitchSucceeds`; patient client, cancel, all fixes, queued event | holds |

All eight refinement configurations pass on this model.

### Fixes in the code

| Model | Code |
| --- | --- |
| `FixedPlanCache` | `fetchOrCreatePlan` reads the VSchema and the plan cache epoch together (`vschemaForPlanning`) |
| `KewFix` | a MoveTables deny error calls `KeyspaceEventWatcher.MarkMoveTablesDenied` instead of `MarkShardNotServing`; `cancelMigration` rebuilds the SrvVSchema |
| `EventGenerations` | `KeyspaceEvent.Generation`; the shard buffer ignores events older than its buffering |
| `DrainOnDeny` | `applyDenyList` calls `WaitForAdmittedWrites` after loading the deny rule; writes register before `checkPermissions` and check the current rules if their plan is older than the last rule load |

Two more changes in the code are not modeled. The executor delivers each
SrvVSchema to the keyspace event watcher only after it installed the VSchema
built from it (`SrvVSchemaInstalled`), so that writes released at the end of a
switch are planned with the new routing rules right away, and
`waitForNewerVSchema` returns without waiting when the executor already has a
newer VSchema. In the model the watcher and the executor see SrvVSchema
versions independently, which covers every ordering, including the one the
code now enforces.

`KewFix` abstracts `MarkMoveTablesDenied` in two ways. The model re-evaluates
the MoveTables state when buffering starts; the code marks the switch as in
progress and re-evaluates it in the background, which only adds an interval
in which nothing can end the buffering. And the code also re-checks
consistency on every SrvVSchema update without a MoveTables state, which can
only end a buffering once the denied tables are lifted, after a cancelled
switch.

### Buffering on versus off

For non-transactional statements, the `newExecute` retry loop already holds
writes during a switch. A write denied by the source waits for a newer
VSchema and is planned again. With buffering off, that wait is
`vschemaWaitTimeout` (30s) per retry. With buffering on, it is
`--buffer-max-failover-duration / 2` (10s by default), plus the time the write
spent in the shard buffer. Turning buffering on therefore shortens the
`newExecute` wait. Statements inside a transaction are never retried or
buffered: they fail and the client must retry the transaction.
