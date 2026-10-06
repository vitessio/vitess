------------------------------- MODULE TwoPC -------------------------------
(***************************************************************************)
(* A model of Vitess atomic distributed transactions (transaction_mode =  *)
(* TWOPC), as implemented by                                               *)
(*                                                                         *)
(*   go/vt/vtgate/tx_conn.go                    coordinator and resolver   *)
(*   go/vt/vttablet/tabletserver/dt_executor.go tablet RPC handlers        *)
(*   go/vt/vttablet/tabletserver/tx_prep_pool.go  prepared pool            *)
(*   go/vt/vttablet/tabletserver/tx_engine.go   redo of prepared txns      *)
(*                                                                         *)
(* The model covers one distributed transaction (one DTID). The first     *)
(* shard session is the metadata manager (MM). It holds the transaction   *)
(* record and commits its own writes together with the commit decision    *)
(* (StartCommit). The other shard sessions are resource managers (RMs),   *)
(* which prepare by keeping their connection open in the prepared pool    *)
(* and writing a durable redo log.                                         *)
(*                                                                         *)
(* The coordinator is the VTGate that runs the commit. Resolvers are      *)
(* VTGates that act on the MM record when a tablet's transaction watcher  *)
(* reports it unresolved. The model does not track time, so a resolver    *)
(* can act while the coordinator is still running, which is what a slow  *)
(* coordinator looks like once the abandon age has elapsed.               *)
(*                                                                         *)
(* RPCs are asynchronous. A request becomes a handler on the target       *)
(* tablet that runs in one or more atomic steps; a step boundary is put   *)
(* where the Go code performs separate database transactions and another *)
(* RPC can interleave. The caller may time out and move on while the      *)
(* handler still runs, and a reply to a caller that has moved on is       *)
(* dropped.                                                                *)
(*                                                                         *)
(* Tablets can restart (a vttablet or MySQL restart, or a reparent with   *)
(* semi-sync). A restart loses open and prepared connections and the      *)
(* prepared pool; the redo log, the MM record and committed data survive. *)
(* On recovery, prepared transactions are redone from the redo log.       *)
(*                                                                         *)
(* Outside the model: isolation and conflicting writes from other         *)
(* transactions, the content of the redo statements, query rules from    *)
(* Online DDL and MoveTables, and operator actions such as concluding a  *)
(* transaction from VTAdmin.                                               *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    MM,             \* the metadata manager shard
    RMs,            \* the resource manager shards
    Resolvers,      \* VTGates that resolve the transaction from the MM record
    MaxCrashes,     \* bound on the number of tablet restarts
    MaxResolves,    \* bound on the number of resolution attempts
    FixRedoPending,   \* TRUE: a retryable redo failure reserves the DTID in the prepared pool
    FixDTIDLock,      \* TRUE: Prepare, CommitPrepared and RollbackPrepared on a DTID are serialized
    FixKeepLocks,     \* TRUE: RollbackPrepared keeps the prepared transaction if the redo log is not deleted
    NonRetryable,     \* TRUE: commits and redos may fail with non-retryable errors
    RetryableLockLoss \* TRUE: redos and commits may fail with retryable errors that release the row locks

ASSUME MM \notin RMs /\ RMs # {}
ASSUME MaxCrashes \in Nat /\ MaxResolves \in Nat
ASSUME \A b \in {FixRedoPending, FixDTIDLock, FixKeepLocks, NonRetryable, RetryableLockLoss} :
           b \in BOOLEAN

Coord   == "coord"
Tablets == RMs \cup {MM}
Callers == {Coord} \cup Resolvers

\* Reply values. "fail" is StartCommitState_Fail: the MM did not commit.
\* "err" is any other error, including StartCommitState_Unknown.
Results == {"skip", "wait", "ok", "err", "fail"}

VARIABLES
    mmTx,      \* the MM's own application transaction
    dt,        \* the MM transaction record (_vt.dt_state)
    conn,      \* each RM's connection that holds its writes
    committed, \* whether each RM's writes are durably committed
    redo,      \* each RM's redo log (_vt.redo_state)
    reserved,  \* each RM's TxPreparedPool.reserved entry for the DTID
    up,        \* whether each tablet is serving
    crashes,   \* number of tablet restarts so far
    resolves,  \* number of resolution attempts so far
    cs,        \* state of each caller (coordinator and resolvers)
    handlers   \* RPC handlers running on tablets

vars == <<mmTx, dt, conn, committed, redo, reserved, up, crashes, resolves, cs, handlers>>
data == <<mmTx, dt, conn, committed, redo, reserved>>
rmData == <<conn, committed, redo, reserved>>

\* mmTx:     "open" | "committed" | "aborted"
\* dt:       "none" (never created) | "prepare" | "commit" | "rollback" | "concluded" (deleted)
\* conn[r]:  "open"   - an unprepared transaction in the tx pool
\*           "pooled" - a prepared transaction in TxPreparedPool.conns
\*           "taken"  - removed from the pool by FetchForCommit, being committed
\*           "none"   - no connection holds the writes
\* redo[r]:  "none" | "prepared" | "failed"
\* reserved[r]: "none" | "committing" | "failed" | "redoPending"

TypeOK ==
    /\ mmTx \in {"open", "committed", "aborted"}
    /\ dt \in {"none", "prepare", "commit", "rollback", "concluded"}
    /\ conn \in [RMs -> {"open", "pooled", "taken", "none"}]
    /\ committed \in [RMs -> BOOLEAN]
    /\ redo \in [RMs -> {"none", "prepared", "failed"}]
    /\ reserved \in [RMs -> {"none", "committing", "failed", "redoPending"}]
    /\ up \in [Tablets -> BOOLEAN]
    /\ crashes \in 0..MaxCrashes
    /\ resolves \in 0..MaxResolves
    /\ \A c \in Callers : cs[c].res \in [Tablets -> Results]

Init ==
    /\ mmTx = "open"
    /\ dt = "none"
    /\ conn = [r \in RMs |-> "open"]
    /\ committed = [r \in RMs |-> FALSE]
    /\ redo = [r \in RMs |-> "none"]
    /\ reserved = [r \in RMs |-> "none"]
    /\ up = [t \in Tablets |-> TRUE]
    /\ crashes = 0
    /\ resolves = 0
    /\ cs = [c \in Callers |->
                [pc |-> IF c = Coord THEN "init" ELSE "idle",
                 res |-> [t \in Tablets |-> "skip"],
                 mmErr |-> FALSE]]
    /\ handlers = {}

-----------------------------------------------------------------------------
(* RPC plumbing *)

\* A handler is live while its caller still waits for its reply. Abandon(c)
\* marks the handlers of caller c stale, so that their replies are dropped.
Abandon(c) ==
    {IF h.caller = c THEN [h EXCEPT !.live = FALSE] ELSE h : h \in handlers}

NewHandlers(c, op, targets, txid, live) ==
    {[op |-> op, tgt |-> t, caller |-> c, live |-> live,
      step |-> 1, txid |-> txid, rok |-> TRUE] : t \in targets}

\* Caller c sends op to every tablet in targets and waits in pc. txid says
\* whether the request carries the original transaction id.
SendX(c, op, targets, txid, pc, mmErr) ==
    /\ handlers' = Abandon(c) \cup NewHandlers(c, op, targets, txid, TRUE)
    /\ cs' = [cs EXCEPT ![c] =
                [pc |-> pc, mmErr |-> mmErr,
                 res |-> [t \in Tablets |-> IF t \in targets THEN "wait" ELSE "skip"]]]

Send(c, op, targets, txid, pc) == SendX(c, op, targets, txid, pc, cs[c].mmErr)

\* Caller c sends op without waiting for the replies and moves to pc.
SendAndForget(c, op, targets, txid, pc) ==
    /\ handlers' = Abandon(c) \cup NewHandlers(c, op, targets, txid, FALSE)
    /\ cs' = [cs EXCEPT ![c] =
                [pc |-> pc, mmErr |-> FALSE, res |-> [t \in Tablets |-> "skip"]]]

Goto(c, pc) == cs' = [cs EXCEPT ![c].pc = pc]

\* The handler finishes. The reply reaches the caller only if it still waits for it.
Reply(h, r) ==
    /\ handlers' = handlers \ {h}
    /\ cs' = IF h.live /\ cs[h.caller].res[h.tgt] = "wait"
             THEN [cs EXCEPT ![h.caller].res[h.tgt] = r]
             ELSE cs

Advance(h, rok) ==
    /\ handlers' = (handlers \ {h}) \cup {[h EXCEPT !.step = h.step + 1, !.rok = rok]}
    /\ UNCHANGED cs

Waiting(c) == \E t \in Tablets : cs[c].res[t] = "wait"
Replied(c, T) == \A t \in T : cs[c].res[t] # "wait"
AllOk(c, T) == \A t \in T : cs[c].res[t] = "ok"

\* The caller stops waiting: its context expires or the connection breaks.
Timeout(c) ==
    /\ Waiting(c)
    /\ cs' = [cs EXCEPT ![c].res = [t \in Tablets |-> IF cs[c].res[t] = "wait" THEN "err" ELSE cs[c].res[t]]]
    /\ handlers' = Abandon(c)
    /\ UNCHANGED <<data, up, crashes, resolves>>

-----------------------------------------------------------------------------
(* Metadata manager handlers *)

\* CreateTransaction: insert the record in the PREPARE state.
HCreate(h) ==
    /\ h.op = "create"
    /\ \/ /\ up[MM] /\ dt = "none"
          /\ dt' = "prepare"
          /\ Reply(h, "ok")
          /\ UNCHANGED <<mmTx, rmData>>
       \/ /\ Reply(h, "err")
          /\ UNCHANGED data

\* SetRollback: roll back the MM's transaction if its id is given, then
\* transition PREPARE -> ROLLBACK. The transition fails in any other state.
HSetRollback(h) ==
    /\ h.op = "setRollback"
    /\ IF ~up[MM]
       THEN Reply(h, "err") /\ UNCHANGED data
       ELSE /\ mmTx' = IF h.txid /\ mmTx = "open" THEN "aborted" ELSE mmTx
            /\ IF dt = "prepare"
               THEN dt' = "rollback" /\ Reply(h, "ok")
               ELSE UNCHANGED dt /\ Reply(h, "err")
            /\ UNCHANGED rmData

\* StartCommit: transition PREPARE -> COMMIT inside the MM's transaction and
\* commit it. A failed commit returns StartCommitState_Unknown whether or not
\* MySQL applied it.
HStartCommit(h) ==
    /\ h.op = "startCommit"
    /\ \/ /\ ~up[MM]
          /\ Reply(h, "err") /\ UNCHANGED data
       \/ /\ up[MM] /\ mmTx # "open"
          /\ Reply(h, "fail") /\ UNCHANGED data
       \/ /\ up[MM] /\ mmTx = "open" /\ dt # "prepare"
          /\ mmTx' = "aborted"
          /\ Reply(h, "fail") /\ UNCHANGED <<dt, rmData>>
       \/ /\ up[MM] /\ mmTx = "open" /\ dt = "prepare"
          /\ \/ mmTx' = "committed" /\ dt' = "commit" /\ Reply(h, "ok")
             \/ mmTx' = "committed" /\ dt' = "commit" /\ Reply(h, "err")
             \/ mmTx' = "aborted" /\ UNCHANGED dt /\ Reply(h, "err")
          /\ UNCHANGED rmData

\* ConcludeTransaction: delete the record.
HConclude(h) ==
    /\ h.op = "conclude"
    /\ IF ~up[MM]
       THEN Reply(h, "err") /\ UNCHANGED data
       ELSE /\ dt' = IF dt = "none" THEN "none" ELSE "concluded"
            /\ Reply(h, "ok")
            /\ UNCHANGED <<mmTx, rmData>>

\* Rollback of the session's unprepared transaction, on any tablet.
HRollback(h) ==
    /\ h.op = "rollback"
    /\ IF ~up[h.tgt]
       THEN Reply(h, "err") /\ UNCHANGED data
       ELSE /\ IF h.tgt = MM
               THEN /\ mmTx' = IF mmTx = "open" THEN "aborted" ELSE mmTx
                    /\ UNCHANGED <<dt, rmData>>
               ELSE /\ conn' = [conn EXCEPT ![h.tgt] = IF @ = "open" THEN "none" ELSE @]
                    /\ UNCHANGED <<mmTx, dt, committed, redo, reserved>>
            /\ Reply(h, "ok")

-----------------------------------------------------------------------------
(* Resource manager handlers *)

\* The 2PC operations that take the DTID lock (dtidLocks) on the tablet. A
\* handler holds the lock from its first step until it replies, so another
\* operation can start only when no handler on the tablet is past step 1.
DTOps == {"prepare", "commitPrepared", "rollbackPrepared"}

Busy(r) == \E h \in handlers : h.tgt = r /\ h.op \in DTOps /\ h.step > 1

CanLock(h) == FixDTIDLock => ~Busy(h.tgt)

\* Prepare, in two steps:
\*  1. lock the transaction, put its connection in the prepared pool and
\*     check that the connection was not closed,
\*  2. save the redo log in a separate transaction.
HPrepare(h) ==
    LET r == h.tgt IN
    /\ h.op = "prepare"
    /\ \/ /\ h.step = 1
          \* A Prepare rejected by a query rule or a full pool rolls back the
          \* transaction; the model gets the same states from Kill followed by
          \* this step finding no open transaction.
          /\ \/ /\ ~up[r]
                /\ Reply(h, "err") /\ UNCHANGED data
             \/ /\ up[r] /\ CanLock(h) /\ conn[r] # "open"
                /\ Reply(h, "err") /\ UNCHANGED data
             \/ /\ up[r] /\ CanLock(h) /\ conn[r] = "open"
                /\ reserved[r] \in {"none", "redoPending"}
                /\ conn' = [conn EXCEPT ![r] = "pooled"]
                /\ reserved' = [reserved EXCEPT ![r] = "none"]
                /\ Advance(h, TRUE)
                /\ UNCHANGED <<mmTx, dt, committed, redo>>
       \/ /\ h.step = 2
          /\ \/ redo' = [redo EXCEPT ![r] = "prepared"] /\ Reply(h, "ok")
             \/ UNCHANGED redo /\ Reply(h, "err")
          /\ UNCHANGED <<mmTx, dt, conn, committed, reserved>>

\* CommitPrepared, in two steps:
\*  1. FetchForCommit: fail on a reservation, take a pooled connection, or
\*     return success when the DTID is unknown (assumed already committed),
\*  2. delete the redo log and commit in the prepared connection.
HCommitPrepared(h) ==
    LET r == h.tgt IN
    /\ h.op = "commitPrepared"
    /\ \/ /\ h.step = 1
          /\ \/ /\ ~up[r]
                /\ Reply(h, "err") /\ UNCHANGED data
             \/ /\ up[r] /\ CanLock(h) /\ reserved[r] # "none"
                /\ Reply(h, "err") /\ UNCHANGED data
             \/ /\ up[r] /\ CanLock(h) /\ reserved[r] = "none" /\ conn[r] = "pooled"
                /\ conn' = [conn EXCEPT ![r] = "taken"]
                /\ reserved' = [reserved EXCEPT ![r] = "committing"]
                /\ Advance(h, TRUE)
                /\ UNCHANGED <<mmTx, dt, committed, redo>>
             \/ /\ up[r] /\ CanLock(h) /\ reserved[r] = "none" /\ conn[r] # "pooled"
                /\ Reply(h, "ok") /\ UNCHANGED data
       \/ /\ h.step = 2
          /\ \/ \* committed
                /\ committed' = [committed EXCEPT ![r] = TRUE]
                /\ redo' = [redo EXCEPT ![r] = "none"]
                /\ conn' = [conn EXCEPT ![r] = "none"]
                /\ reserved' = [reserved EXCEPT ![r] = "none"]
                /\ Reply(h, "ok")
             \/ \* committed, but the commit returned an error
                /\ committed' = [committed EXCEPT ![r] = TRUE]
                /\ redo' = [redo EXCEPT ![r] = "none"]
                /\ conn' = [conn EXCEPT ![r] = "none"]
                /\ Reply(h, "err")
                /\ UNCHANGED reserved
             \/ \* retryable failure: rolled back, the reservation stays
                /\ RetryableLockLoss
                /\ conn' = [conn EXCEPT ![r] = "none"]
                /\ Reply(h, "err")
                /\ UNCHANGED <<committed, redo, reserved>>
             \/ \* non-retryable failure: marked failed in the pool and the redo log
                /\ NonRetryable
                /\ conn' = [conn EXCEPT ![r] = "none"]
                /\ reserved' = [reserved EXCEPT ![r] = "failed"]
                /\ redo' = [redo EXCEPT ![r] = "failed"]
                /\ Reply(h, "err")
                /\ UNCHANGED committed
          /\ UNCHANGED <<mmTx, dt>>

\* RollbackPrepared, in two steps:
\*  1. delete the redo log in a separate transaction,
\*  2. FetchForRollback, which drops a reservation or rolls back a pooled
\*     connection, then roll back the original transaction if its id was
\*     given. Without FixKeepLocks, FetchForRollback also runs when step 1
\*     failed; with it, the prepared pool is left as it is.
HRollbackPrepared(h) ==
    LET r == h.tgt IN
    /\ h.op = "rollbackPrepared"
    /\ \/ /\ h.step = 1
          /\ \/ /\ ~up[r]
                /\ Reply(h, "err") /\ UNCHANGED data
             \/ /\ up[r] /\ CanLock(h)
                /\ redo' = [redo EXCEPT ![r] = "none"]
                /\ Advance(h, TRUE)
                /\ UNCHANGED <<mmTx, dt, conn, committed, reserved>>
             \/ /\ up[r] /\ CanLock(h)
                /\ Advance(h, FALSE)
                /\ UNCHANGED data
       \/ /\ h.step = 2
          /\ LET fetch == h.rok \/ ~FixKeepLocks IN
             /\ reserved' = IF fetch THEN [reserved EXCEPT ![r] = "none"] ELSE reserved
             /\ conn' = [conn EXCEPT ![r] =
                           IF fetch /\ reserved[r] = "none" /\ @ = "pooled" THEN "none"
                           ELSE IF h.txid /\ @ = "open" THEN "none"
                           ELSE @]
          /\ Reply(h, IF h.rok THEN "ok" ELSE "err")
          /\ UNCHANGED <<mmTx, dt, committed, redo>>

Handle(h) ==
    /\ \/ HCreate(h) \/ HSetRollback(h) \/ HStartCommit(h) \/ HConclude(h)
       \/ HRollback(h) \/ HPrepare(h) \/ HCommitPrepared(h) \/ HRollbackPrepared(h)
    /\ UNCHANGED <<up, crashes, resolves>>

-----------------------------------------------------------------------------
(* Environment *)

\* A tablet restarts. Connections, the prepared pool and running handlers are lost.
Crash(t) ==
    /\ up[t] /\ crashes < MaxCrashes
    /\ up' = [up EXCEPT ![t] = FALSE]
    /\ crashes' = crashes + 1
    /\ handlers' = {h \in handlers : h.tgt # t}
    /\ IF t = MM
       THEN /\ mmTx' = IF mmTx = "open" THEN "aborted" ELSE mmTx
            /\ UNCHANGED <<dt, rmData>>
       ELSE /\ conn' = [conn EXCEPT ![t] = "none"]
            /\ reserved' = [reserved EXCEPT ![t] = "none"]
            /\ UNCHANGED <<mmTx, dt, committed, redo>>
    /\ UNCHANGED <<resolves, cs>>

\* The tablet serves again after redoing its prepared transactions
\* (TxEngine.prepareFromRedo).
Recover(t) ==
    /\ ~up[t]
    /\ up' = [up EXCEPT ![t] = TRUE]
    /\ IF t = MM \/ redo[t] = "none"
       THEN UNCHANGED data
       ELSE IF redo[t] = "failed"
       THEN /\ reserved' = [reserved EXCEPT ![t] = "failed"]
            /\ UNCHANGED <<mmTx, dt, conn, committed, redo>>
       ELSE /\ \/ \* redone
                  /\ conn' = [conn EXCEPT ![t] = "pooled"]
                  /\ UNCHANGED <<redo, reserved>>
               \/ \* retryable failure: the redo log stays prepared
                  /\ RetryableLockLoss
                  /\ reserved' = [reserved EXCEPT ![t] =
                                    IF FixRedoPending THEN "redoPending" ELSE "none"]
                  /\ UNCHANGED <<conn, redo>>
               \/ \* non-retryable failure
                  /\ NonRetryable
                  /\ reserved' = [reserved EXCEPT ![t] = "failed"]
                  /\ redo' = [redo EXCEPT ![t] = "failed"]
                  /\ UNCHANGED conn
            /\ UNCHANGED <<mmTx, dt, committed>>
    /\ UNCHANGED <<crashes, resolves, cs, handlers>>

\* The transaction killer rolls back an unprepared transaction.
Kill(t) ==
    /\ up[t]
    /\ IF t = MM
       THEN mmTx = "open" /\ mmTx' = "aborted" /\ UNCHANGED <<dt, rmData>>
       ELSE /\ conn[t] = "open"
            /\ conn' = [conn EXCEPT ![t] = "none"]
            /\ UNCHANGED <<mmTx, dt, committed, redo, reserved>>
    /\ UNCHANGED <<up, crashes, resolves, cs, handlers>>

-----------------------------------------------------------------------------
(* Coordinator: TxConn.commit2PC and TxConn.errActionAndLogWarn *)

CStep ==
    LET me == cs[Coord] IN
    \/ /\ me.pc = "init"
       /\ Send(Coord, "create", {MM}, FALSE, "wCreate")
    \/ /\ me.pc = "wCreate" /\ Replied(Coord, {MM})
       /\ IF me.res[MM] = "ok"
          THEN Send(Coord, "prepare", RMs, FALSE, "wPrepare")
          ELSE SendAndForget(Coord, "rollback", Tablets, TRUE, "done")
    \/ /\ me.pc = "wPrepare" /\ Replied(Coord, RMs)
       /\ IF AllOk(Coord, RMs)
          THEN \/ Send(Coord, "startCommit", {MM}, TRUE, "wStartCommit")
               \* the gRPC connection is closed: Fail without sending
               \/ SendX(Coord, "setRollback", {MM}, TRUE, "wRbMM", FALSE)
          ELSE SendX(Coord, "setRollback", {MM}, TRUE, "wRbMM", FALSE)
    \/ /\ me.pc = "wStartCommit" /\ Replied(Coord, {MM})
       /\ CASE me.res[MM] = "ok"   -> Send(Coord, "commitPrepared", RMs, FALSE, "wCommit")
            [] me.res[MM] = "fail" -> SendX(Coord, "setRollback", {MM}, TRUE, "wRbMM", FALSE)
            [] OTHER               -> Goto(Coord, "done") /\ UNCHANGED handlers
    \/ /\ me.pc = "wRbMM" /\ Replied(Coord, {MM})
       /\ SendX(Coord, "rollbackPrepared", RMs, TRUE, "wRbRMs", me.res[MM] # "ok")
    \/ /\ me.pc = "wRbRMs" /\ Replied(Coord, RMs)
       /\ IF ~me.mmErr /\ AllOk(Coord, RMs)
          THEN SendAndForget(Coord, "conclude", {MM}, FALSE, "done")
          ELSE Goto(Coord, "done") /\ UNCHANGED handlers
    \/ /\ me.pc = "wCommit" /\ Replied(Coord, RMs)
       /\ IF AllOk(Coord, RMs)
          THEN SendAndForget(Coord, "conclude", {MM}, FALSE, "done")
          ELSE Goto(Coord, "done") /\ UNCHANGED handlers

CCrash ==
    /\ cs[Coord].pc \notin {"done", "crashed"}
    /\ Goto(Coord, "crashed")
    /\ UNCHANGED handlers

Coordinator ==
    /\ CStep \/ CCrash
    /\ UNCHANGED <<data, up, crashes, resolves>>

-----------------------------------------------------------------------------
(* Resolver: TxConn.ResolveTransactions and TxConn.resolveTx *)

RStep(v) ==
    LET me == cs[v] IN
    \/ /\ me.pc = "idle" /\ resolves < MaxResolves
       /\ up[MM] /\ dt \in {"prepare", "rollback", "commit"}
       \* Timing assumption: a resolver acts on a record only after the abandon
       \* age (15 minutes by default), and the transaction killer rolls back
       \* unprepared transactions after the transaction timeout (30 seconds by
       \* default). So when a resolver acts, no RM still has an unprepared
       \* transaction that a late Prepare could prepare.
       /\ \A r \in RMs : conn[r] # "open"
       /\ resolves' = resolves + 1
       /\ CASE dt = "prepare"  -> Send(v, "setRollback", {MM}, TRUE, "wSetRb")
            [] dt = "rollback" -> Send(v, "rollbackPrepared", RMs, FALSE, "wRb")
            [] dt = "commit"   -> Send(v, "commitPrepared", RMs, FALSE, "wCm")
    \/ /\ me.pc = "wSetRb" /\ Replied(v, {MM})
       /\ IF me.res[MM] = "ok"
          THEN Send(v, "rollbackPrepared", RMs, FALSE, "wRb")
          ELSE Goto(v, "idle") /\ UNCHANGED handlers
       /\ UNCHANGED resolves
    \/ /\ me.pc \in {"wRb", "wCm"} /\ Replied(v, RMs)
       /\ IF AllOk(v, RMs)
          THEN SendAndForget(v, "conclude", {MM}, FALSE, "idle")
          ELSE Goto(v, "idle") /\ UNCHANGED handlers
       /\ UNCHANGED resolves

Resolver(v) ==
    /\ RStep(v)
    /\ UNCHANGED <<data, up, crashes>>

-----------------------------------------------------------------------------

Next ==
    \/ \E h \in handlers : Handle(h)
    \/ \E c \in Callers : Timeout(c)
    \/ \E t \in Tablets : Crash(t) \/ Recover(t) \/ Kill(t)
    \/ Coordinator
    \/ \E v \in Resolvers : Resolver(v)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

\* An RM commits only if the MM committed, that is, only after a commit decision.
CommitNeedsDecision ==
    \A r \in RMs : committed[r] => mmTx = "committed"

\* The decision in the record agrees with the MM's own transaction.
DecisionConsistent ==
    /\ dt = "commit" => mmTx = "committed"
    /\ mmTx = "committed" => dt \in {"commit", "concluded"}
    /\ dt = "rollback" => mmTx # "committed"

\* After a commit decision, every RM's writes are committed or still
\* recoverable from a prepared connection or the redo log.
CommitDurable ==
    mmTx = "committed" =>
        \A r \in RMs : committed[r] \/ conn[r] \in {"pooled", "taken"}
                       \/ redo[r] \in {"prepared", "failed"}

\* Once a committed transaction is concluded, nothing commits its
\* remaining RMs, so every RM must already be committed (or be in the
\* middle of its commit).
NoLostCommit ==
    (mmTx = "committed" /\ dt = "concluded") =>
        \A r \in RMs : committed[r] \/
            \E h \in handlers : h.op = "commitPrepared" /\ h.tgt = r /\ h.step = 2

Atomicity == CommitNeedsDecision /\ DecisionConsistent /\ CommitDurable /\ NoLostCommit

\* Once the record is concluded and the coordinator is gone, no RM is left
\* with a prepared transaction that nothing will resolve. Such a transaction
\* keeps its row locks until an operator concludes it.
NoOrphanPrepare ==
    (dt = "concluded" /\ cs[Coord].pc \in {"done", "crashed"}) =>
        \A r \in RMs : \/ (redo[r] = "none" /\ conn[r] \notin {"pooled", "taken"})
                       \/ \E h \in handlers : h.tgt = r

\* While an RM serves, a prepared redo log is backed by a prepared connection
\* that holds the transaction's row locks. When this fails, other
\* transactions can change the rows before the redo is applied.
LocksHeld ==
    \A r \in RMs : (up[r] /\ redo[r] = "prepared") => conn[r] \in {"pooled", "taken"}

=============================================================================
