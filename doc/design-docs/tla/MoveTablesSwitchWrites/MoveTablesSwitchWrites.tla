------------------------- MODULE MoveTablesSwitchWrites -------------------------
(***************************************************************************)
(* A model of MoveTables SwitchTraffic for PRIMARY (write) traffic, as     *)
(* implemented by Server.switchWrites in go/vt/vtctl/workflow/server.go,   *)
(* together with the vtgate machinery that keeps client writes flowing     *)
(* while the switch happens:                                               *)
(*                                                                         *)
(*   - the tablet deny rule ("enforce denied tables", QRFailRetry) on the  *)
(*     source and target primaries, checked in checkPermissions BEFORE a  *)
(*     pooled connection is taken and the MySQL metadata lock (MDL) is     *)
(*     acquired (query_executor.go);                                       *)
(*   - the forward VReplication stream copying the source binlog;          *)
(*   - vtgate's Executor.newExecute retry loop (MaxBufferingRetries = 3,   *)
(*     waitForNewerVSchema, ClearPlans on final failure), including the   *)
(*     epoch based plan cache (newVCursor captures the VSchema, the epoch  *)
(*     is read later in GetOrLoad);                                        *)
(*   - TabletGateway.withRetry (buffer once, mark the primary invalid on  *)
(*     a FAILED_PRECONDITION error);                                       *)
(*   - the per-shard buffer (shard_buffer.go) and the KeyspaceEventWatcher *)
(*     (keyspace_events.go) that ends buffering.                           *)
(*                                                                         *)
(* One source shard, one target shard, one vtgate, |Reqs| concurrent       *)
(* non-transactional single-statement writes of the same query shape (so  *)
(* they share a plan cache entry).                                         *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Reqs,          \* client write requests (each is also the written row)
    Buffering,     \* vtgate --enable-buffer
    Timeouts,      \* buffer window / max failover duration / vschema wait can expire
    SlowWrites,    \* a write that passed the deny check may reach MySQL arbitrarily late
    Force,         \* SwitchTraffic --force: a failed tablet refresh is ignored
    AllowCancel,   \* a pre-journal step may fail, triggering cancelMigration
    MaxExtraRebuilds, \* unrelated RebuildSrvVSchema calls that may happen meanwhile
    FixedPlanCache,   \* proposed fix: epoch read atomically with the VSchema
    DrainOnDeny,      \* proposed fix: applying the deny rule waits for writes admitted before it
    KewFix,           \* proposed fix: a MoveTables deny error does not mark the primary
                      \* not serving; cancelMigration rebuilds SrvVSchema
    Diagnostics       \* record the history variables used only by the diagnostic properties

MaxTries == 3   \* MaxBufferingRetries in plan_execute.go

(***************************************************************************)
(* State space reductions that do not change behavior:                    *)
(*                                                                         *)
(*  - A per-request field is reset to its initial value as soon as no     *)
(*    action or property reads it again before it is next written, so     *)
(*    states that differ only in such stale values are not told apart.   *)
(*  - withRetry's choice of the next step after a buffer check ("give up  *)
(*    if the only primary was marked invalid, otherwise send") reads only *)
(*    the request's own state, so it is folded into the step before it.   *)
(*  - The history variables used only by diagnostic properties are not  *)
(*    written unless Diagnostics is TRUE.                                 *)
(*  - Requests are interchangeable, so safety-only configurations declare *)
(*    SYMMETRY Perms.                                                     *)
(*                                                                         *)
(* RefinementCheck.tla checks with TLC that every behavior of the model   *)
(* without these reductions is a behavior of this one.                    *)
(***************************************************************************)
Perms == Permutations(Reqs)

VARIABLES
    \* ---- switchWrites (vtctld) ----
    pc,            \* workflow program counter
    sw,            \* progress of the concurrent halves of stopSourceWrites
    srcPos,        \* source position gathered after writes were stopped
    \* ---- topo ----
    srcTopoDeny, tgtTopoDeny,  \* DeniedTables in the shard records
    routeHist,     \* routeHist[v] = keyspace the routing rules point at in SrvVSchema version v
    extra,         \* number of unrelated SrvVSchema rebuilds so far
    \* ---- tablets / MySQL ----
    srcTabDeny, tgtTabDeny,    \* deny rule as loaded on the primaries (after RefreshState)
    srcLog,        \* rows committed on the source, in binlog order
    tgtData,       \* rows present on the target
    tgtClient,     \* rows written to the target by clients
    replPos, streamOn,         \* forward VReplication stream
    \* ---- vtgate: VSchemaManager / plan cache ----
    vtVer, epoch, cache,
    \* ---- vtgate: KeyspaceEventWatcher for the source keyspace ----
    kewVer, kewCons, kewMT, kewServing, kewEvent,
    \* ---- vtgate: shard buffer for the source shard ----
    bufState, bufQ, drainQ, lastEndRecent,
    \* ---- per request ----
    rs, try, lastVer, capVer, dest, bufOnce, invalid, gwErr,
    \* ---- history variables, only used by properties ----
    releasedEarly, deniedAtTgt, failedAfterSwitch, startedAfter

wfVars  == <<pc, sw, srcPos>>
topoVars == <<srcTopoDeny, tgtTopoDeny, routeHist, extra>>
dbVars  == <<srcTabDeny, tgtTabDeny, srcLog, tgtData, tgtClient, replPos, streamOn>>
vsVars  == <<vtVer, epoch, cache>>
kewVars == <<kewVer, kewCons, kewMT, kewServing, kewEvent>>
bufVars == <<bufState, bufQ, drainQ, lastEndRecent>>
reqVars == <<rs, try, lastVer, capVer, dest, bufOnce, invalid, gwErr>>
PerAttempt == <<dest, bufOnce, invalid, gwErr>>   \* written when a plan is loaded
histVars == <<releasedEarly, deniedAtTgt, failedAfterSwitch, startedAfter>>
vars == <<wfVars, topoVars, dbVars, vsVars, kewVars, bufVars, reqVars, histVars>>

TopoVer   == Len(routeHist)
TopoRoute == routeHist[TopoVer]
Range(s)  == {s[i] : i \in 1..Len(s)}

PostJournal == {"journal", "allowTgt", "route", "freeze", "done"}
Switched    == pc \in {"freeze", "done"}       \* routing rules point at the target

------------------------------------------------------------------------------
Init ==
    /\ pc = "init"
    /\ sw = [srcTopo |-> FALSE, srcApplied |-> FALSE, srcRef |-> FALSE, tgtTopo |-> FALSE, tgtRef |-> FALSE]
    /\ srcPos = 0
    /\ srcTopoDeny = FALSE /\ tgtTopoDeny = TRUE   \* MoveTables denies the tables on the target at Create
    /\ routeHist = <<"src">>
    /\ extra = 0
    /\ srcTabDeny = FALSE /\ tgtTabDeny = TRUE
    /\ srcLog = <<>> /\ tgtData = {} /\ tgtClient = {}
    /\ replPos = 0 /\ streamOn = TRUE
    /\ vtVer = 1 /\ epoch = 1 /\ cache = [ep |-> 0, route |-> "src"]
    /\ kewVer = 1 /\ kewCons = TRUE /\ kewMT = "none" /\ kewServing = TRUE /\ kewEvent = FALSE
    /\ bufState = "idle" /\ bufQ = {} /\ drainQ = {} /\ lastEndRecent = FALSE
    /\ rs = [r \in Reqs |-> "idle"]
    /\ try = [r \in Reqs |-> 0]
    /\ lastVer = [r \in Reqs |-> 0]
    /\ capVer = [r \in Reqs |-> 0]
    /\ dest = [r \in Reqs |-> "src"]
    /\ bufOnce = [r \in Reqs |-> FALSE]
    /\ invalid = [r \in Reqs |-> FALSE]
    /\ gwErr = [r \in Reqs |-> FALSE]
    /\ releasedEarly = FALSE /\ deniedAtTgt = FALSE /\ failedAfterSwitch = FALSE
    /\ startedAfter = [r \in Reqs |-> FALSE]

------------------------------------------------------------------------------
(***************************************************************************)
(* switchWrites                                                            *)
(***************************************************************************)
MDLHolders(ks) == {r \in Reqs : rs[r] = "mdl" /\ dest[r] = ks}
PassedNoMDL(ks) == {r \in Reqs : rs[r] = "passed" /\ dest[r] = ks}

Begin ==
    /\ pc = "init"
    /\ pc' = "stopWrites"
    /\ UNCHANGED <<sw, srcPos, topoVars, dbVars, vsVars, kewVars, bufVars, reqVars, histVars>>

\* stopSourceWrites -> switchDeniedTables(backward=false): two goroutines in an
\* errgroup; each updates shard records, then RefreshTabletsByShard.
SrcDenyTopo ==
    /\ pc = "stopWrites" /\ ~sw.srcTopo
    /\ srcTopoDeny' = TRUE
    /\ sw' = [sw EXCEPT !.srcTopo = TRUE]
    /\ UNCHANGED <<pc, srcPos, tgtTopoDeny, routeHist, extra, dbVars, vsVars, kewVars, bufVars, reqVars, histVars>>

SrcRefresh ==
    /\ pc = "stopWrites" /\ sw.srcTopo /\ ~sw.srcApplied
    /\ \/ srcTabDeny' = TRUE
       \/ Force /\ UNCHANGED srcTabDeny   \* refresh failed, ignored because of --force
    \* With DrainOnDeny the RefreshState RPC only returns once the writes that
    \* were admitted under the previous rules have finished (SrcRefreshDrained).
    /\ sw' = [sw EXCEPT !.srcApplied = TRUE, !.srcRef = ~DrainOnDeny]
    /\ UNCHANGED <<pc, srcPos, topoVars, tgtTabDeny, srcLog, tgtData, tgtClient, replPos, streamOn,
                   vsVars, kewVars, bufVars, reqVars, histVars>>

SrcRefreshDrained ==
    /\ DrainOnDeny
    /\ pc = "stopWrites" /\ sw.srcApplied /\ ~sw.srcRef
    /\ {r \in Reqs : rs[r] \in {"passed", "mdl"} /\ dest[r] = "src"} = {}
    /\ sw' = [sw EXCEPT !.srcRef = TRUE]
    /\ UNCHANGED <<pc, srcPos, topoVars, dbVars, vsVars, kewVars, bufVars, reqVars, histVars>>

TgtUndenyTopo ==
    /\ pc = "stopWrites" /\ ~sw.tgtTopo
    /\ tgtTopoDeny' = FALSE
    /\ sw' = [sw EXCEPT !.tgtTopo = TRUE]
    /\ UNCHANGED <<pc, srcPos, srcTopoDeny, routeHist, extra, dbVars, vsVars, kewVars, bufVars, reqVars, histVars>>

TgtRefresh ==
    /\ pc = "stopWrites" /\ sw.tgtTopo /\ ~sw.tgtRef
    /\ tgtTabDeny' = FALSE
    /\ sw' = [sw EXCEPT !.tgtRef = TRUE]
    /\ UNCHANGED <<pc, srcPos, topoVars, srcTabDeny, srcLog, tgtData, tgtClient, replPos, streamOn,
                   vsVars, kewVars, bufVars, reqVars, histVars>>

StopWritesDone ==
    /\ pc = "stopWrites" /\ sw.srcRef /\ sw.tgtRef
    /\ pc' = "lock1"
    /\ UNCHANGED <<sw, srcPos, topoVars, dbVars, vsVars, kewVars, bufVars, reqVars, histVars>>

\* executeLockTablesOnSource: LOCK TABLES ... READ is granted only once no
\* writer holds an MDL on the tables, and released at once (connection closed).
\* The second cycle, 100ms later, is meant to catch writes that passed the
\* deny check before it was applied. Without SlowWrites we ASSUME it does:
\* every such write has taken its MDL by then.
Lock1 ==
    /\ pc = "lock1" /\ MDLHolders("src") = {}
    /\ pc' = "lock2"
    /\ UNCHANGED <<sw, srcPos, topoVars, dbVars, vsVars, kewVars, bufVars, reqVars, histVars>>

Lock2 ==
    /\ pc = "lock2" /\ MDLHolders("src") = {}
    /\ SlowWrites \/ PassedNoMDL("src") = {}
    /\ pc' = "gather"
    /\ UNCHANGED <<sw, srcPos, topoVars, dbVars, vsVars, kewVars, bufVars, reqVars, histVars>>

GatherPositions ==
    /\ pc = "gather"
    /\ srcPos' = Len(srcLog)
    /\ pc' = "catchup"
    /\ UNCHANGED <<sw, topoVars, dbVars, vsVars, kewVars, bufVars, reqVars, histVars>>

\* waitForCatchup: VReplicationWaitForPos, then stop the stream.
Catchup ==
    /\ pc = "catchup" /\ replPos >= srcPos
    /\ streamOn' = FALSE
    /\ pc' = "reverse"
    /\ UNCHANGED <<sw, srcPos, topoVars, srcTabDeny, tgtTabDeny, srcLog, tgtData, tgtClient, replPos,
                   vsVars, kewVars, bufVars, reqVars, histVars>>

\* migrateStreams / resetSequences / createReverseVReplication / init sequences
CreateReverse ==
    /\ pc = "reverse"
    /\ pc' = "journal"
    /\ UNCHANGED <<sw, srcPos, topoVars, dbVars, vsVars, kewVars, bufVars, reqVars, histVars>>

\* createJournals: point of no return.
Journal ==
    /\ pc = "journal"
    /\ pc' = "allowTgt"
    /\ UNCHANGED <<sw, srcPos, topoVars, dbVars, vsVars, kewVars, bufVars, reqVars, histVars>>

\* allowTargetWrites -> switchDeniedTables(false) again (idempotent here).
AllowTargetWrites ==
    /\ pc = "allowTgt"
    /\ tgtTopoDeny' = FALSE /\ tgtTabDeny' = FALSE
    /\ srcTopoDeny' = TRUE
    /\ srcTabDeny' \in IF Force THEN {srcTabDeny, TRUE} ELSE {TRUE}
    /\ pc' = "route"
    /\ UNCHANGED <<sw, srcPos, routeHist, extra, srcLog, tgtData, tgtClient, replPos, streamOn,
                   vsVars, kewVars, bufVars, reqVars, histVars>>

\* changeRouting -> changeWriteRoute: SaveRoutingRules + RebuildSrvVSchema.
ChangeRouting ==
    /\ pc = "route"
    /\ routeHist' = Append(routeHist, "tgt")
    /\ pc' = "freeze"
    /\ UNCHANGED <<sw, srcPos, srcTopoDeny, tgtTopoDeny, extra, dbVars, vsVars, kewVars, bufVars, reqVars, histVars>>

\* streamMigraterfinalize / startReverseVReplication / freezeTargetVReplication
Freeze ==
    /\ pc = "freeze"
    /\ pc' = "done"
    /\ UNCHANGED <<sw, srcPos, topoVars, dbVars, vsVars, kewVars, bufVars, reqVars, histVars>>

\* Any pre-journal step can fail (catch-up timeout, lost lock, ...).
Fail ==
    /\ AllowCancel
    /\ \/ pc \in {"lock1", "lock2", "gather", "catchup", "reverse"}
       \/ pc = "stopWrites" /\ sw.srcRef /\ sw.tgtRef
    /\ pc' = "cancel"
    /\ UNCHANGED <<sw, srcPos, topoVars, dbVars, vsVars, kewVars, bufVars, reqVars, histVars>>

\* cancelMigration: switchDeniedTables(backward=true), restart the stream,
\* delete the reverse workflow. Note: today there is no RebuildSrvVSchema.
Cancel ==
    /\ pc = "cancel"
    /\ srcTopoDeny' = FALSE /\ srcTabDeny' = FALSE
    /\ tgtTopoDeny' = TRUE  /\ tgtTabDeny' = TRUE
    /\ streamOn' = TRUE
    /\ routeHist' = IF KewFix THEN Append(routeHist, TopoRoute) ELSE routeHist
    /\ pc' = "cancelled"
    /\ UNCHANGED <<sw, srcPos, extra, srcLog, tgtData, tgtClient, replPos,
                   vsVars, kewVars, bufVars, reqVars, histVars>>

\* An unrelated RebuildSrvVSchema (e.g. another workflow, ApplyVSchema).
ExtraRebuild ==
    /\ extra < MaxExtraRebuilds
    /\ extra' = extra + 1
    /\ routeHist' = Append(routeHist, TopoRoute)
    /\ UNCHANGED <<wfVars, srcTopoDeny, tgtTopoDeny, dbVars, vsVars, kewVars, bufVars, reqVars, histVars>>

\* Forward VReplication applies the next source binlog event to the target.
StreamApply ==
    /\ streamOn /\ replPos < Len(srcLog)
    /\ tgtData' = tgtData \cup {srcLog[replPos + 1]}
    /\ replPos' = replPos + 1
    /\ UNCHANGED <<wfVars, topoVars, srcTabDeny, tgtTabDeny, srcLog, tgtClient, streamOn,
                   vsVars, kewVars, bufVars, reqVars, histVars>>

------------------------------------------------------------------------------
(***************************************************************************)
(* vtgate: VSchemaManager watching SrvVSchema                              *)
(***************************************************************************)
VtgateSeesVSchema ==
    /\ vtVer < TopoVer
    /\ vtVer' = TopoVer
    /\ epoch' = epoch + 1          \* SaveVSchema -> ClearPlans
    /\ UNCHANGED <<cache, wfVars, topoVars, dbVars, kewVars, bufVars, reqVars, histVars>>

(***************************************************************************)
(* vtgate: KeyspaceEventWatcher for the source keyspace                    *)
(***************************************************************************)
\* getMoveTablesStatus: reads the shard records from topo at callback time and
\* the routing rule of the SrvVSchema being delivered.
MTStatus(ver) ==
    IF ~srcTopoDeny THEN "none"
    ELSE IF routeHist[ver] = "tgt" THEN "switched" ELSE "switching"

\* ensureConsistentLocked, given the state after the caller's update.
\* Returns [cons, mt, event].
Ensure(cons, mt, serving) ==
    IF cons \/ mt = "switching" \/ ~serving
    THEN [cons |-> cons, mt |-> mt, ev |-> FALSE]
    ELSE [cons |-> TRUE, mt |-> "none", ev |-> TRUE]   \* broadcast; moveTablesState = nil

KewSeesVSchema ==
    /\ kewVer < TopoVer
    /\ LET mt == MTStatus(TopoVer)
           c0 == IF mt # "none" THEN FALSE ELSE kewCons
           e  == IF mt # "none" THEN Ensure(c0, mt, kewServing)
                 ELSE [cons |-> c0, mt |-> mt, ev |-> FALSE]
       IN /\ kewVer' = TopoVer
          /\ kewCons' = e.cons /\ kewMT' = e.mt
          /\ kewEvent' = (kewEvent \/ e.ev)
    /\ UNCHANGED <<kewServing, wfVars, topoVars, dbVars, vsVars, bufVars, reqVars, histVars>>

\* Periodic (and on-change) health stream update from the source primary,
\* which keeps reporting serving=true throughout: the deny rule does not
\* change serving state. KEW.onHealthCheck.
HealthCheck ==
    /\ LET c0 == IF ~kewServing THEN FALSE ELSE kewCons
           e  == Ensure(c0, kewMT, TRUE)
       IN /\ kewServing' = TRUE
          /\ kewCons' = e.cons /\ kewMT' = e.mt
          /\ kewEvent' = (kewEvent \/ e.ev)
    /\ UNCHANGED <<kewVer, wfVars, topoVars, dbVars, vsVars, bufVars, reqVars, histVars>>

(***************************************************************************)
(* vtgate: shard buffer for the source shard                               *)
(***************************************************************************)
\* withRetry, after the buffer check: pick a healthy tablet. The only primary
\* was marked invalid after a retryable (FAILED_PRECONDITION) error -> give up
\* with the last error ("gwDone"; the attempt's fields are dead from here on),
\* otherwise send the query. once is the value of bufOnce after the check.
GwCheck(r, once) ==
    IF invalid[r]
    THEN /\ rs' = [rs EXCEPT ![r] = "gwDone"]
         /\ dest' = [dest EXCEPT ![r] = "src"]
         /\ bufOnce' = [bufOnce EXCEPT ![r] = FALSE]
         /\ invalid' = [invalid EXCEPT ![r] = FALSE]
         /\ gwErr' = [gwErr EXCEPT ![r] = FALSE]
    ELSE /\ rs' = [rs EXCEPT ![r] = "send"]
         /\ bufOnce' = [bufOnce EXCEPT ![r] = once]
         /\ UNCHANGED <<dest, invalid, gwErr>>

NotYetSwitched == pc \notin {"freeze", "done", "cancelled"}

\* recordKeyspaceEvent: any event stops buffering (stopBufferingLocked).
BufferHandleEvent ==
    /\ kewEvent
    /\ kewEvent' = FALSE
    /\ IF bufState = "buffering"
       THEN /\ bufState' = "draining" /\ drainQ' = bufQ /\ bufQ' = {}
            /\ lastEndRecent' = TRUE
            /\ releasedEarly' = (releasedEarly \/ (Diagnostics /\ NotYetSwitched /\ bufQ # {}))
       ELSE UNCHANGED <<bufState, bufQ, drainQ, lastEndRecent, releasedEarly>>
    /\ UNCHANGED <<kewVer, kewCons, kewMT, kewServing, wfVars, topoVars, dbVars, vsVars, reqVars,
                   deniedAtTgt, failedAfterSwitch, startedAfter>>

BufferDrainOne ==
    /\ \E r \in drainQ :
        /\ drainQ' = drainQ \ {r}
        /\ GwCheck(r, TRUE)
    /\ UNCHANGED <<bufState, bufQ, lastEndRecent, try, lastVer, capVer,
                   wfVars, topoVars, dbVars, vsVars, kewVars, histVars>>

BufferDrainDone ==
    /\ bufState = "draining" /\ drainQ = {}
    /\ bufState' = "idle"
    /\ UNCHANGED <<bufQ, drainQ, lastEndRecent, wfVars, topoVars, dbVars, vsVars, kewVars, reqVars, histVars>>

\* --buffer-window expires for one entry (evictOldestEntry, nil error).
BufferWindowTimeout ==
    /\ Timeouts
    /\ \E r \in bufQ :
        /\ bufQ' = bufQ \ {r}
        /\ GwCheck(r, TRUE)
    /\ UNCHANGED <<bufState, drainQ, lastEndRecent, try, lastVer, capVer,
                   wfVars, topoVars, dbVars, vsVars, kewVars, histVars>>

\* --buffer-max-failover-duration expires.
BufferMaxDuration ==
    /\ Timeouts /\ bufState = "buffering"
    /\ bufState' = "draining" /\ drainQ' = bufQ /\ bufQ' = {}
    /\ lastEndRecent' = TRUE
    /\ UNCHANGED <<wfVars, topoVars, dbVars, vsVars, kewVars, reqVars, histVars>>

\* --buffer-min-time-between-failovers elapses.
MinTimeElapsed ==
    /\ lastEndRecent
    /\ lastEndRecent' = FALSE
    /\ UNCHANGED <<bufState, bufQ, drainQ, wfVars, topoVars, dbVars, vsVars, kewVars, reqVars, histVars>>

------------------------------------------------------------------------------
(***************************************************************************)
(* Client requests: Executor.newExecute + TabletGateway.withRetry          *)
(***************************************************************************)
UC_req(r) == UNCHANGED <<wfVars, topoVars, dbVars, vsVars, kewVars, bufVars, histVars>>

Start(r) ==
    /\ rs[r] = "idle"
    /\ rs' = [rs EXCEPT ![r] = "capture"]
    /\ lastVer' = [lastVer EXCEPT ![r] = vtVer]    \* lastVSchemaCreated
    /\ try' = [try EXCEPT ![r] = 0]
    /\ startedAfter' = [startedAfter EXCEPT ![r] = Diagnostics /\ routeHist[vtVer] = "tgt"]
    /\ UNCHANGED <<capVer, dest, bufOnce, invalid, gwErr>>
    /\ UNCHANGED <<wfVars, topoVars, dbVars, vsVars, kewVars, bufVars, releasedEarly, deniedAtTgt, failedAfterSwitch>>

\* fetchOrCreatePlan, part 1: newVCursor captures e.VSchema().
Capture(r) ==
    /\ rs[r] = "capture"
    /\ capVer' = [capVer EXCEPT ![r] = IF FixedPlanCache THEN 0 ELSE vtVer]
    /\ IF FixedPlanCache
       THEN \* plan lookup/build happens against an atomic (vschema, epoch) snapshot
            LET route == IF cache.ep = epoch THEN cache.route ELSE routeHist[vtVer]
            IN /\ dest' = [dest EXCEPT ![r] = route]
               /\ cache' = [ep |-> epoch, route |-> route]
               /\ rs' = [rs EXCEPT ![r] = "gwBuf"]
               /\ bufOnce' = [bufOnce EXCEPT ![r] = FALSE]
               /\ invalid' = [invalid EXCEPT ![r] = FALSE]
               /\ gwErr' = [gwErr EXCEPT ![r] = FALSE]
       ELSE /\ rs' = [rs EXCEPT ![r] = "load"]
            /\ UNCHANGED <<dest, cache, bufOnce, invalid, gwErr>>
    /\ UNCHANGED <<try, lastVer, wfVars, topoVars, dbVars, vtVer, epoch, kewVars, bufVars, histVars>>

\* fetchOrCreatePlan, part 2: plans.GetOrLoad(key, e.epoch.Load(), build with vcursor).
Load(r) ==
    /\ rs[r] = "load"
    /\ LET route == IF cache.ep = epoch THEN cache.route ELSE routeHist[capVer[r]]
       IN /\ dest' = [dest EXCEPT ![r] = route]
          /\ cache' = [ep |-> epoch, route |-> route]
    /\ rs' = [rs EXCEPT ![r] = "gwBuf"]
    /\ bufOnce' = [bufOnce EXCEPT ![r] = FALSE]
    /\ invalid' = [invalid EXCEPT ![r] = FALSE]
    /\ gwErr' = [gwErr EXCEPT ![r] = FALSE]
    /\ capVer' = [capVer EXCEPT ![r] = 0]
    /\ UNCHANGED <<try, lastVer, wfVars, topoVars, dbVars, vtVer, epoch, kewVars, bufVars, histVars>>

\* withRetry, top of loop: buffer.WaitForFailoverEnd(ctx, ks, shard, kev, err).
\* Only the source shard's buffer can be involved: the target is never denied
\* while routed to (checked by NeverDeniedAtTarget).
GwBuffer(r) ==
    /\ rs[r] = "gwBuf"
    /\ IF Buffering /\ ~bufOnce[r] /\ dest[r] = "src"
       THEN CASE bufState = "buffering" ->
                    /\ bufQ' = bufQ \cup {r}
                    /\ rs' = [rs EXCEPT ![r] = "buffered"]
                    /\ UNCHANGED <<bufState, kewCons, kewServing, kewMT, kewEvent, PerAttempt>>
              [] bufState = "idle" /\ gwErr[r] /\ ~lastEndRecent /\ ~KewFix ->
                    \* startBufferingLocked -> kev.MarkShardNotServing
                    /\ kewCons' = FALSE /\ kewServing' = FALSE
                    /\ bufState' = "buffering"
                    /\ bufQ' = bufQ \cup {r}
                    /\ rs' = [rs EXCEPT ![r] = "buffered"]
                    /\ UNCHANGED <<kewMT, kewEvent, PerAttempt>>
              [] bufState = "idle" /\ gwErr[r] /\ ~lastEndRecent /\ KewFix ->
                    \* Proposed: the primary is serving, MoveTables denied the
                    \* write. Re-evaluate the MoveTables state from topo instead
                    \* of marking the shard not serving.
                    LET mt == MTStatus(kewVer)
                        e  == Ensure(FALSE, mt, kewServing)
                    IN /\ kewCons' = e.cons /\ kewMT' = e.mt
                       /\ kewEvent' = (kewEvent \/ e.ev)
                       /\ UNCHANGED <<kewServing, PerAttempt>>
                       /\ bufState' = "buffering"
                       /\ bufQ' = bufQ \cup {r}
                       /\ rs' = [rs EXCEPT ![r] = "buffered"]
              [] OTHER ->
                    /\ GwCheck(r, bufOnce[r])
                    /\ UNCHANGED <<bufState, bufQ, kewCons, kewServing, kewMT, kewEvent>>
       ELSE /\ GwCheck(r, bufOnce[r])
            /\ UNCHANGED <<bufState, bufQ, kewCons, kewServing, kewMT, kewEvent>>
    /\ UNCHANGED <<drainQ, lastEndRecent, kewVer, try, lastVer, capVer,
                   wfVars, topoVars, dbVars, vsVars, histVars>>


\* vttablet checkPermissions: the deny rule is evaluated before a pooled
\* connection is taken (and so before the MDL is acquired).
Send(r) ==
    /\ rs[r] = "send"
    /\ LET denied == IF dest[r] = "src" THEN srcTabDeny ELSE tgtTabDeny
       IN IF denied
          THEN /\ gwErr' = [gwErr EXCEPT ![r] = TRUE]
               /\ invalid' = [invalid EXCEPT ![r] = TRUE]
               /\ rs' = [rs EXCEPT ![r] = "gwBuf"]
               /\ deniedAtTgt' = (deniedAtTgt \/ dest[r] = "tgt")
               /\ UNCHANGED bufOnce
          ELSE /\ rs' = [rs EXCEPT ![r] = "passed"]
               /\ bufOnce' = [bufOnce EXCEPT ![r] = FALSE]
               /\ invalid' = [invalid EXCEPT ![r] = FALSE]
               /\ gwErr' = [gwErr EXCEPT ![r] = FALSE]
               /\ UNCHANGED deniedAtTgt
    /\ UNCHANGED <<try, lastVer, capVer, dest, releasedEarly, failedAfterSwitch, startedAfter,
                   wfVars, topoVars, dbVars, vsVars, kewVars, bufVars>>

\* txPool.Begin (may wait up to the pool timeout), then MySQL takes the MDL.
AcquireMDL(r) ==
    /\ rs[r] = "passed"
    /\ rs' = [rs EXCEPT ![r] = "mdl"]
    /\ UNCHANGED <<try, lastVer, capVer, dest, bufOnce, invalid, gwErr>> /\ UC_req(r)

Commit(r) ==
    /\ rs[r] = "mdl"
    /\ IF dest[r] = "src"
       THEN /\ srcLog' = Append(srcLog, r)
            /\ UNCHANGED <<tgtData, tgtClient>>
       ELSE /\ tgtData' = tgtData \cup {r}
            /\ tgtClient' = tgtClient \cup {r}
            /\ UNCHANGED srcLog
    /\ rs' = [rs EXCEPT ![r] = "acked"]
    /\ try' = [try EXCEPT ![r] = 0]
    /\ lastVer' = [lastVer EXCEPT ![r] = 0]
    /\ dest' = [dest EXCEPT ![r] = "src"]
    /\ startedAfter' = [startedAfter EXCEPT ![r] = FALSE]
    /\ UNCHANGED <<capVer, bufOnce, invalid, gwErr,
                   srcTabDeny, tgtTabDeny, replPos, streamOn,
                   wfVars, topoVars, vsVars, kewVars, bufVars,
                   releasedEarly, deniedAtTgt, failedAfterSwitch>>

\* newExecute: the root cause contains "enforce denied tables" -> retry,
\* unless this was the last try: fail, and the deferred ClearPlans runs.
GwDone(r) ==
    /\ rs[r] = "gwDone"
    /\ IF try[r] + 1 < MaxTries
       THEN /\ try' = [try EXCEPT ![r] = try[r] + 1]
            /\ rs' = [rs EXCEPT ![r] = "vswait"]
            /\ UNCHANGED <<epoch, failedAfterSwitch>>
       ELSE /\ rs' = [rs EXCEPT ![r] = "failed"]
            /\ epoch' = epoch + 1
            /\ failedAfterSwitch' = (failedAfterSwitch \/ startedAfter[r])
            /\ try' = [try EXCEPT ![r] = 0]
    /\ lastVer' = IF rs'[r] = "failed" THEN [lastVer EXCEPT ![r] = 0] ELSE lastVer
    /\ startedAfter' = IF rs'[r] = "failed" THEN [startedAfter EXCEPT ![r] = FALSE] ELSE startedAfter
    /\ UNCHANGED <<capVer, dest, bufOnce, invalid, gwErr, vtVer, cache,
                   releasedEarly, deniedAtTgt, wfVars, topoVars, dbVars, kewVars, bufVars>>

\* waitForNewerVSchema(ctx, e, lastVSchemaCreated, timeout)
VSchemaWaitNewer(r) ==
    /\ rs[r] = "vswait" /\ vtVer > lastVer[r]
    /\ lastVer' = [lastVer EXCEPT ![r] = vtVer]
    /\ rs' = [rs EXCEPT ![r] = "capture"]
    /\ UNCHANGED <<try, capVer, dest, bufOnce, invalid, gwErr>> /\ UC_req(r)

VSchemaWaitTimeout(r) ==
    /\ Timeouts /\ rs[r] = "vswait"
    /\ rs' = [rs EXCEPT ![r] = "capture"]
    /\ UNCHANGED <<try, lastVer, capVer, dest, bufOnce, invalid, gwErr>> /\ UC_req(r)

------------------------------------------------------------------------------
Workflow ==
    \/ Begin \/ SrcDenyTopo \/ SrcRefresh \/ SrcRefreshDrained \/ TgtUndenyTopo \/ TgtRefresh \/ StopWritesDone
    \/ Lock1 \/ Lock2 \/ GatherPositions \/ Catchup \/ CreateReverse \/ Journal
    \/ AllowTargetWrites \/ ChangeRouting \/ Freeze \/ Cancel

Environment ==
    \/ StreamApply \/ VtgateSeesVSchema \/ KewSeesVSchema \/ HealthCheck
    \/ BufferHandleEvent \/ BufferDrainOne \/ BufferDrainDone \/ MinTimeElapsed

TimeoutActs ==
    \/ BufferWindowTimeout \/ BufferMaxDuration \/ \E r \in Reqs : VSchemaWaitTimeout(r)

ReqStep(r) ==
    \/ Capture(r) \/ Load(r) \/ GwBuffer(r) \/ Send(r)
    \/ AcquireMDL(r) \/ Commit(r) \/ GwDone(r) \/ VSchemaWaitNewer(r)

Next ==
    \/ Workflow \/ Fail \/ ExtraRebuild \/ Environment \/ TimeoutActs
    \/ \E r \in Reqs : Start(r) \/ ReqStep(r)

Fairness ==
    /\ WF_vars(Workflow)
    /\ WF_vars(StreamApply) /\ WF_vars(VtgateSeesVSchema) /\ WF_vars(KewSeesVSchema)
    /\ WF_vars(HealthCheck) /\ WF_vars(BufferHandleEvent) /\ WF_vars(BufferDrainOne)
    /\ WF_vars(BufferDrainDone) /\ WF_vars(MinTimeElapsed)
    /\ WF_vars(BufferWindowTimeout) /\ WF_vars(BufferMaxDuration)
    /\ \A r \in Reqs : WF_vars(ReqStep(r)) /\ WF_vars(VSchemaWaitTimeout(r))

Spec == Init /\ [][Next]_vars /\ Fairness

------------------------------------------------------------------------------
(***************************************************************************)
(* Properties                                                              *)
(***************************************************************************)
TypeOK ==
    /\ pc \in {"init", "stopWrites", "lock1", "lock2", "gather", "catchup", "reverse", "journal",
               "allowTgt", "route", "freeze", "done", "cancel", "cancelled"}
    /\ srcLog \in Seq(Reqs) /\ tgtData \subseteq Reqs /\ tgtClient \subseteq Reqs
    /\ bufState \in {"idle", "buffering", "draining"}
    /\ kewMT \in {"none", "switching", "switched"}
    /\ rs \in [Reqs -> {"idle", "capture", "load", "gwBuf", "buffered", "gwCheck", "send",
                        "passed", "mdl", "gwDone", "vswait", "acked", "failed"}]

\* S1. Once the journal exists (the point of no return), every row committed
\* on the source is on the target: no write acknowledged by the source is lost.
NoLostWrites == pc \in PostJournal => Range(srcLog) \subseteq tgtData

\* S2. Single writer: once any client write landed on the target, the source
\* never commits another client write that the target does not have.
NoSplitBrain == tgtClient # {} => Range(srcLog) \subseteq tgtData

\* S3. The target only takes client writes after the routing rules moved.
NoEarlyTargetWrites == tgtClient # {} => pc \in {"freeze", "done"}

\* S4. A cancelled switch leaves all client writes on the source.
CancelKeepsSource == pc = "cancelled" => tgtClient = {}

\* S5. A retried request is never applied twice.
NoDuplicates ==
    /\ \A i, j \in 1..Len(srcLog) : i # j => srcLog[i] # srcLog[j]
    /\ Range(srcLog) \cap tgtClient = {}

\* S6. vtgate never sends a write to a target primary that denies it.
NeverDeniedAtTarget == ~deniedAtTgt

Safety == TypeOK /\ NoLostWrites /\ NoSplitBrain /\ NoEarlyTargetWrites
          /\ CancelKeepsSource /\ NoDuplicates /\ NeverDeniedAtTarget

\* L1. Every request eventually returns to the client.
RequestsTerminate == \A r \in Reqs : rs[r] # "idle" ~> rs[r] \in {"acked", "failed"}

\* L2. The switch itself terminates.
SwitchTerminates == <>(pc \in {"done", "cancelled"})

\* L3. Availability: if the switch succeeds, every request eventually succeeds.
\* Meant for Timeouts = FALSE (an arbitrarily patient client).
WritesSucceedIfSwitchSucceeds ==
    (<>(pc = "done")) => \A r \in Reqs : rs[r] # "idle" ~> rs[r] = "acked"

\* Diagnostics (expected to be violated; used to obtain traces):
\* D1. Buffering only ends because the switch finished.
BufferEndsOnlyAfterSwitch == ~releasedEarly
\* D2. A request that arrives after vtgate already routes to the target never fails.
NoFailureAfterSwitch == ~failedAfterSwitch
==============================================================================
