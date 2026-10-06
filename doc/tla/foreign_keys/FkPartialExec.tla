---------------------------- MODULE FkPartialExec ----------------------------
(***************************************************************************)
(* What vtgate does when a foreign key plan fails part-way through,        *)
(* inside a transaction the client opened with BEGIN.                      *)
(*                                                                         *)
(* A foreign key plan (FkCascade, FkVerify) runs several queries. If a    *)
(* later one fails after an earlier one wrote rows, the statement must    *)
(* not leave those writes behind in the client's transaction. vtgate      *)
(* handles this with a per-statement state machine:                       *)
(*                                                                         *)
(*  Executor.execute: SafeSession.SetSavepointState(!mustCommit), i.e.    *)
(*    "savepoint needed" inside an explicit transaction.                   *)
(*  VCursorImpl.ExecuteMultiShard / StreamExecuteMulti:                    *)
(*    markSavepoint(rollbackOnError && shards > 1), then the query, then  *)
(*    setRollbackOnPartialExecIfRequired(some shard succeeded,             *)
(*                                       rollbackOnError).                 *)
(*  VCursorImpl.Execute (lookup vindexes): markSavepoint(rollbackOnError) *)
(*    on any number of shards, then setRollbackOnPartialExecIfRequired.   *)
(*  markSavepoint runs SAVEPOINT only if the state is still "needed".     *)
(*  SafeSession.SetRollbackCommand: "rollback to <savepoint>" if one was  *)
(*    set, otherwise a full transaction rollback.                          *)
(*  Executor.rollbackExecIfNeeded / rollbackPartialExec: on error, run    *)
(*    the stored command; if ROLLBACK TO fails, roll back the transaction. *)
(*                                                                         *)
(* DML routes pass rollbackOnError = true (engine/dml.go, insert.go);     *)
(* reads, including the FK Selection and Verify queries, pass false       *)
(* (engine/route.go). Each MySQL statement is atomic on its own shard.   *)
(*                                                                         *)
(* The model runs every plan of up to MaxSteps queries, with every        *)
(* outcome for each query, and checks that a failed statement leaves none *)
(* of its writes in a transaction that is still open.                     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    MaxSteps,  \* longest plan explored
    MaxShards  \* largest number of shards one query targets

ASSUME MaxSteps \in Nat /\ MaxShards \in Nat /\ MaxShards >= 1

VARIABLES
    spState,   \* "needed", "set", "rollbackSet" or "rolledBack"
    rbCmd,     \* "none", "toSavepoint" or "tx"
    applied,   \* this statement's writes that are in the transaction
    atSp,      \* this statement's writes that existed when SAVEPOINT ran
    nextId,    \* id for the next write
    steps,     \* queries run so far
    phase,     \* "exec", "error", or a final outcome (see Final)
    txOpen     \* FALSE once vtgate has rolled back the whole transaction

vars == <<spState, rbCmd, applied, atSp, nextId, steps, phase, txOpen>>

Final == {"ok", "errorReverted", "errorTxRolledBack", "errorNothingToRevert"}

Init ==
    /\ spState = "needed"
    /\ rbCmd = "none"
    /\ applied = {}
    /\ atSp = {}
    /\ nextId = 0
    /\ steps = 0
    /\ phase = "exec"
    /\ txOpen = TRUE

-----------------------------------------------------------------------------

\* markSavepoint: SAVEPOINT runs if requested and none has run in this
\* statement. ok = FALSE: the SAVEPOINT itself failed.
MarkSavepoint(want, ok) ==
    IF want /\ spState = "needed"
      THEN IF ok THEN [st |-> "set", sp |-> applied, err |-> FALSE]
                 ELSE [st |-> spState, sp |-> atSp, err |-> TRUE]
      ELSE [st |-> spState, sp |-> atSp, err |-> FALSE]

\* setRollbackOnPartialExecIfRequired, given the state after markSavepoint.
ArmRollback(st, anySuccess, rollbackOnError) ==
    IF anySuccess /\ rollbackOnError /\ st # "rollbackSet" /\ st # "rolledBack"
      THEN [st |-> "rollbackSet", cmd |-> IF st = "set" THEN "toSavepoint" ELSE "tx"]
      ELSE [st |-> st, cmd |-> rbCmd]

\* One query of the plan: a read or a write, on n shards, through
\* ExecuteMultiShard (multi = TRUE) or VCursorImpl.Execute (multi = FALSE),
\* of which k shards succeed.
Query(isWrite, n, k, multi, spOk) ==
    LET rollbackOnError == isWrite
        wantSp == IF multi THEN rollbackOnError /\ n > 1 ELSE rollbackOnError
        m == MarkSavepoint(wantSp, spOk)
    IN IF m.err
         THEN /\ phase' = "error"
              /\ UNCHANGED <<spState, rbCmd, applied, atSp, nextId>>
         ELSE LET W == IF isWrite THEN {nextId + j : j \in 0..(k - 1)} ELSE {}
                  a == ArmRollback(m.st, k > 0, rollbackOnError)
              IN /\ applied' = applied \cup W
                 /\ nextId' = nextId + Cardinality(W)
                 /\ atSp' = m.sp
                 /\ spState' = a.st
                 /\ rbCmd' = a.cmd
                 /\ phase' = IF k < n THEN "error" ELSE "exec"

Step ==
    /\ phase = "exec"
    /\ steps < MaxSteps
    /\ steps' = steps + 1
    /\ \E isWrite \in BOOLEAN, multi \in BOOLEAN, spOk \in BOOLEAN,
          n \in 1..MaxShards :
         \E k \in 0..n : Query(isWrite, n, k, multi, spOk)
    /\ UNCHANGED txOpen

\* An error raised by vtgate itself between queries: FkVerify finding a
\* row, a failed evalengine conversion in executeNonLiteralExprFkChild.
VtgateError ==
    /\ phase = "exec"
    /\ phase' = "error"
    /\ UNCHANGED <<spState, rbCmd, applied, atSp, nextId, steps, txOpen>>

Succeed ==
    /\ phase = "exec"
    /\ phase' = "ok"
    /\ UNCHANGED <<spState, rbCmd, applied, atSp, nextId, steps, txOpen>>

\* rollbackExecIfNeeded and rollbackPartialExec.
HandleError ==
    /\ phase = "error"
    /\ IF spState # "rollbackSet"
         THEN /\ phase' = "errorNothingToRevert"
              /\ UNCHANGED <<spState, applied, txOpen>>
         ELSE \E rollbackToOk \in BOOLEAN :
                IF rbCmd = "toSavepoint" /\ rollbackToOk
                  THEN /\ spState' = "rolledBack"
                       /\ applied' = atSp
                       /\ phase' = "errorReverted"
                       /\ UNCHANGED txOpen
                  ELSE /\ spState' = IF rbCmd = "toSavepoint" THEN "rolledBack" ELSE spState
                       /\ applied' = {}
                       /\ txOpen' = FALSE
                       /\ phase' = "errorTxRolledBack"
    /\ UNCHANGED <<rbCmd, atSp, nextId, steps>>

Next ==
    \/ Step \/ VtgateError \/ Succeed \/ HandleError
    \/ phase \in Final /\ UNCHANGED vars

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

\* A statement that fails leaves none of its writes in an open transaction.
NoPartialWrites ==
    (phase \in Final \ {"ok"} /\ txOpen) => applied = {}

\* The savepoint, if one was taken, preceded every write of the statement.
SavepointBeforeWrites == spState \in {"set", "rollbackSet"} /\ rbCmd = "toSavepoint" => atSp = {}

=============================================================================
