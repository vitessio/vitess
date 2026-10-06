---------------------------- MODULE FkCrossShard ----------------------------
(***************************************************************************)
(* Foreign keys whose parent and child rows live on different shards, and *)
(* how vtgate commits the transactions that maintain them.                *)
(*                                                                         *)
(* Schema (sharded keyspace, foreign_key_mode = managed):                  *)
(*   parent(k)  on shard B                                                 *)
(*   child(p)   on shard A, FOREIGN KEY (p) REFERENCES parent(k)           *)
(*                ON UPDATE CASCADE ON DELETE CASCADE                      *)
(*                                                                         *)
(* The constraint is not shard-scoped, so Vitess handles it in every      *)
(* direction (SemTable.RemoveNonRequiredForeignKeys keeps it). The plans  *)
(* follow the sharded_fk_allow cases in                                    *)
(* planbuilder/testdata/foreignkey_cases.json:                             *)
(*                                                                         *)
(*   CascadeDel  DELETE FROM parent WHERE k = 'k1'                         *)
(*     Selection:    SELECT k FROM parent WHERE k = 'k1' FOR UPDATE   (B)  *)
(*     CascadeChild: DELETE FROM child WHERE p IN ::fkc_vals          (A)  *)
(*     Parent:       DELETE FROM parent WHERE k = 'k1'                (B)  *)
(*                                                                         *)
(*   CascadeUpd  UPDATE parent SET k = 'k2' WHERE k = 'k1'                 *)
(*     Selection:    SELECT k FROM parent WHERE k = 'k1' FOR UPDATE   (B)  *)
(*     CascadeChild: UPDATE child SET p = 'k2' WHERE p IN ::fkc_vals  (A)  *)
(*     Parent:       UPDATE parent SET k = 'k2' WHERE k = 'k1'        (B)  *)
(*                                                                         *)
(*   UpdC        UPDATE child SET p = 'k1' WHERE id = 'c2'                 *)
(*     VerifyParent: a vtgate LEFT JOIN of                                 *)
(*                   SELECT ... FROM child WHERE ... FOR SHARE        (A)  *)
(*                   SELECT k FROM parent WHERE k = 'k1' FOR SHARE    (B)  *)
(*     PostVerify:   UPDATE child SET p = 'k1' WHERE id = 'c2'        (A)  *)
(*                                                                         *)
(* An INSERT into child with a cross-shard parent, and a cross-shard      *)
(* RESTRICT, are rejected with VT12002, so they are not modelled.         *)
(*                                                                         *)
(* vtgate commits a transaction that spans shards in one of two ways      *)
(* (go/vt/vtgate/tx_conn.go):                                              *)
(*   MULTI (the default): commitNormal commits the shards one at a time,  *)
(*     in the order they joined the transaction. If a commit fails, the   *)
(*     shards already committed stay committed (warning ERNonAtomicCommit) *)
(*     and the rest are rolled back.                                       *)
(*   TWOPC: the shards prepare, then commit atomically. The model treats  *)
(*     this as one all-or-nothing step.                                    *)
(*                                                                         *)
(* Abstractions: as in FkLocking, each statement is one atomic step and   *)
(* every transaction can roll back at any statement boundary. Only record *)
(* locks are modelled: every lock that protects a constraint here is on   *)
(* an existing row, so gap locks and the isolation level do not matter.   *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
    TxnMode,             \* "MULTI" or "TWOPC"
    CommitCanFail,       \* TRUE: a MULTI commit can fail after some shards
    Workload,            \* sequence of transaction kinds
    NULL, ABSENT         \* model values: SQL NULL, and "row does not exist"

TxnKinds == {"CascadeDel", "CascadeUpd", "UpdC"}

ASSUME TxnMode \in {"MULTI", "TWOPC"}
ASSUME CommitCanFail \in BOOLEAN
ASSUME \A i \in DOMAIN Workload : Workload[i] \in TxnKinds
ASSUME Cardinality({i \in DOMAIN Workload : Workload[i] = "UpdC"}) <= 1

OldKey == "k1"
NewKey == "k2"

PRows == {"p1"}          \* on shard B
CRows == {"c1", "c2"}    \* on shard A; c1 references k1, c2 starts NULL
Rows  == PRows \cup CRows
ShardOf(r) == IF r \in PRows THEN "B" ELSE "A"
TableOf(r) == IF r \in PRows THEN "P" ELSE "C"
RowsOf(tbl) == {r \in Rows : TableOf(r) = tbl}

Txns == DOMAIN Workload

VARIABLES
    db,       \* row values, including uncommitted writes
    cdb,      \* committed row values
    rlocks,   \* record locks: <<txn, row, "S" | "X">>
    written,  \* rows each transaction has written
    joined,   \* shards each transaction has joined, in order
    pc

vars == <<db, cdb, rlocks, written, joined, pc>>

InitialRows == [r \in Rows |->
                  CASE r = "p1" -> OldKey
                    [] r = "c1" -> OldKey
                    [] r = "c2" -> NULL]

-----------------------------------------------------------------------------
(* Locking helpers, as in FkLocking *)

Probe(tbl, v) == {r \in RowsOf(tbl) : db[r] = v \/ cdb[r] = v}
Found(tbl, v) == {r \in RowsOf(tbl) : db[r] = v}
Free(t, R, m) ==
    \A l \in rlocks : (l[1] # t /\ l[2] \in R) => (m = "S" /\ l[3] = "S")
RLocks(t, R, m) == {<<t, r, m>> : r \in R}
SetRows(R, v) == [r \in Rows |-> IF r \in R THEN v ELSE db[r]]

\* Shard s joins t's transaction the first time t runs a statement there.
Join(t, s) == joined' = [joined EXCEPT ![t] =
                           IF s \in {@[i] : i \in DOMAIN @} THEN @ ELSE Append(@, s)]

Fail(t) == /\ pc' = [pc EXCEPT ![t] = "abort"]
           /\ UNCHANGED <<db, written, joined>>

-----------------------------------------------------------------------------
(* Vitess cascades on parent (CascadeDel, CascadeUpd) *)

Selection(t) ==
    /\ pc[t] = "sel"
    /\ LET R == Probe("P", OldKey)
       IN /\ Free(t, R, "X")
          /\ rlocks' = rlocks \cup RLocks(t, R, "X")
          /\ Join(t, "B")
          /\ pc' = [pc EXCEPT ![t] = IF Found("P", OldKey) = {} THEN "commit" ELSE "child"]
    /\ UNCHANGED <<db, cdb, written>>

CascadeChild(t) ==
    /\ pc[t] = "child"
    /\ LET R == Probe("C", OldKey)
           F == Found("C", OldKey)
           v == IF Workload[t] = "CascadeDel" THEN ABSENT ELSE NewKey
       IN /\ Free(t, R, "X")
          /\ rlocks' = rlocks \cup RLocks(t, R, "X")
          /\ db' = SetRows(F, v)
          /\ written' = [written EXCEPT ![t] = @ \cup F]
          /\ Join(t, "A")
          /\ pc' = [pc EXCEPT ![t] = "parent"]
    /\ UNCHANGED cdb

ParentStmt(t) ==
    /\ pc[t] = "parent"
    /\ LET R == Probe("P", OldKey)
           F == Found("P", OldKey)
           v == IF Workload[t] = "CascadeDel" THEN ABSENT ELSE NewKey
       IN /\ Free(t, R, "X")
          /\ rlocks' = rlocks \cup RLocks(t, R, "X")
          /\ db' = SetRows(F, v)
          /\ written' = [written EXCEPT ![t] = @ \cup F]
          /\ Join(t, "B")
          /\ pc' = [pc EXCEPT ![t] = "commit"]
    /\ UNCHANGED cdb

-----------------------------------------------------------------------------
(* Vitess UPDATE child SET p = 'k1' WHERE id = 'c2' *)

\* VerifyParent: the LEFT JOIN runs its child side on A and its parent side
\* on B, both FOR SHARE. If no parent row matches, the update is rejected.
VerifyParent(t) ==
    /\ pc[t] = "verifyP"
    /\ LET RP == Probe("P", OldKey)
       IN /\ Free(t, {"c2"} \cup RP, "S")
          /\ rlocks' = rlocks \cup RLocks(t, {"c2"} \cup RP, "S")
          /\ joined' = [joined EXCEPT ![t] = <<"A", "B">>]
          /\ IF Found("P", OldKey) = {}
               THEN /\ pc' = [pc EXCEPT ![t] = "abort"]
                    /\ UNCHANGED <<db, written>>
               ELSE /\ pc' = [pc EXCEPT ![t] = "updC"]
                    /\ UNCHANGED <<db, written>>
    /\ UNCHANGED cdb

UpdateChild(t) ==
    /\ pc[t] = "updC"
    /\ Free(t, {"c2"}, "X")
    /\ rlocks' = rlocks \cup RLocks(t, {"c2"}, "X")
    /\ db' = [db EXCEPT !["c2"] = OldKey]
    /\ written' = [written EXCEPT ![t] = @ \cup {"c2"}]
    /\ pc' = [pc EXCEPT ![t] = "commit"]
    /\ UNCHANGED <<cdb, joined>>

-----------------------------------------------------------------------------
(* Commit and rollback *)

\* Commit or roll back t's work on shard s.
CommitOn(t, s) ==
    [r \in Rows |-> IF r \in written[t] /\ ShardOf(r) = s THEN db[r] ELSE cdb[r]]
RollbackOn(t, S) ==
    [r \in Rows |-> IF r \in written[t] /\ ShardOf(r) \in S THEN cdb[r] ELSE db[r]]
ReleaseOn(t, S) == {l \in rlocks : ~(l[1] = t /\ ShardOf(l[2]) \in S)}

Remaining(t) == {joined[t][i] : i \in DOMAIN joined[t]}

\* TWOPC: all shards commit or none do.
CommitAtomic(t) ==
    /\ TxnMode = "TWOPC"
    /\ pc[t] = "commit"
    /\ cdb' = [r \in Rows |-> IF r \in written[t] THEN db[r] ELSE cdb[r]]
    /\ rlocks' = ReleaseOn(t, Remaining(t))
    /\ written' = [written EXCEPT ![t] = {}]
    /\ joined' = [joined EXCEPT ![t] = <<>>]
    /\ pc' = [pc EXCEPT ![t] = "done"]
    /\ UNCHANGED db

\* MULTI: commit the next shard in join order.
CommitNextShard(t) ==
    /\ TxnMode = "MULTI"
    /\ pc[t] = "commit"
    /\ IF joined[t] = <<>>
         THEN /\ pc' = [pc EXCEPT ![t] = "done"]
              /\ written' = [written EXCEPT ![t] = {}]
              /\ UNCHANGED <<cdb, rlocks, joined>>
         ELSE LET s == Head(joined[t])
              IN /\ cdb' = CommitOn(t, s)
                 /\ rlocks' = ReleaseOn(t, {s})
                 /\ joined' = [joined EXCEPT ![t] = Tail(@)]
                 /\ UNCHANGED <<written, pc>>
    /\ UNCHANGED db

\* MULTI: a shard commit fails (or vtgate dies) after at least one shard
\* committed. The shards not yet committed are rolled back.
CommitFails(t) ==
    /\ TxnMode = "MULTI"
    /\ CommitCanFail
    /\ pc[t] = "commit"
    /\ joined[t] # <<>>
    /\ db' = RollbackOn(t, Remaining(t))
    /\ rlocks' = ReleaseOn(t, Remaining(t))
    /\ written' = [written EXCEPT ![t] = {}]
    /\ joined' = [joined EXCEPT ![t] = <<>>]
    /\ pc' = [pc EXCEPT ![t] = "failed"]
    /\ UNCHANGED cdb

Rollback(t) ==
    /\ db' = RollbackOn(t, {"A", "B"})
    /\ rlocks' = ReleaseOn(t, {"A", "B"})
    /\ written' = [written EXCEPT ![t] = {}]
    /\ joined' = [joined EXCEPT ![t] = <<>>]
    /\ pc' = [pc EXCEPT ![t] = "aborted"]
    /\ UNCHANGED cdb

Abort(t) == pc[t] = "abort" /\ Rollback(t)

\* Before commit starts, a transaction can roll back at any statement
\* boundary (lock wait timeout, NOWAIT, client rollback).
SpontaneousAbort(t) ==
    /\ pc[t] \in {"sel", "child", "parent", "verifyP", "updC"}
    /\ Rollback(t)

Terminated == \A t \in Txns : pc[t] \in {"done", "aborted", "failed"}

-----------------------------------------------------------------------------

FirstStep(kind) == IF kind = "UpdC" THEN "verifyP" ELSE "sel"

Init ==
    /\ db = InitialRows
    /\ cdb = InitialRows
    /\ rlocks = {}
    /\ written = [t \in Txns |-> {}]
    /\ joined = [t \in Txns |-> <<>>]
    /\ pc = [t \in Txns |-> FirstStep(Workload[t])]

Next ==
    \/ \E t \in Txns :
         \/ Selection(t) \/ CascadeChild(t) \/ ParentStmt(t)
         \/ VerifyParent(t) \/ UpdateChild(t)
         \/ CommitAtomic(t) \/ CommitNextShard(t) \/ CommitFails(t)
         \/ Abort(t) \/ SpontaneousAbort(t)
    \/ Terminated /\ UNCHANGED vars

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

LocksConsistent ==
    \A l1, l2 \in rlocks :
      (l1[1] # l2[1] /\ l1[2] = l2[2]) => (l1[3] = "S" /\ l2[3] = "S")

\* No committed child row references a parent key that is not committed.
\* Under MULTI this fails while a transaction is between its shard commits:
\* a reader that does not lock can see one shard's half of the change.
FkIntegrity ==
    \A c \in CRows :
      cdb[c] \notin {NULL, ABSENT} => \E q \in PRows : cdb[q] = cdb[c]

\* FkIntegrity whenever no transaction is part-way through its commit, so
\* a violation here persists.
FkIntegrityWhenSettled ==
    (\A t \in Txns : pc[t] # "commit") => FkIntegrity

=============================================================================
