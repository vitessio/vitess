----------------------------- MODULE FkLocking -----------------------------
(***************************************************************************)
(* A model of the row locking that protects foreign key integrity when    *)
(* Vitess manages foreign keys (keyspace foreign_key_mode = managed).     *)
(*                                                                         *)
(* Schema (one shard, so every constraint is shard-scoped):               *)
(*                                                                         *)
(*   GP(k)                                                                 *)
(*   P(k)  FOREIGN KEY (k) REFERENCES GP(k)                                *)
(*           ON UPDATE CASCADE ON DELETE CASCADE                           *)
(*   C(p)  FOREIGN KEY (p) REFERENCES P(k)                                 *)
(*           ON UPDATE RESTRICT ON DELETE RESTRICT                         *)
(*                                                                         *)
(* Vitess plans a DML on GP as an FkCascade primitive                      *)
(* (go/vt/vtgate/engine/fk_cascade.go). For                                *)
(*   UPDATE GP SET k = 2 WHERE k = 1                                       *)
(* the plan is (see the u_tbl7 cases in                                    *)
(* go/vt/vtgate/planbuilder/testdata/foreignkey_cases.json):               *)
(*                                                                         *)
(*   Selection:   SELECT k FROM GP WHERE k = 1 FOR UPDATE                  *)
(*   CascadeChild: FkVerify                                                *)
(*     VerifyChild: SELECT 1 FROM P parent, C child                        *)
(*                  WHERE parent.k = child.p AND parent.k IN ::fkc_vals    *)
(*                  AND child.p NOT IN (2) LIMIT 1 FOR SHARE               *)
(*     PostVerify:  UPDATE /*+ SET_VAR(foreign_key_checks=OFF) */ P        *)
(*                  SET k = 2 WHERE k IN ::fkc_vals                        *)
(*   Parent:      UPDATE GP SET k = 2 WHERE k = 1                          *)
(*                                                                         *)
(* Because the child UPDATE runs with foreign_key_checks=OFF, InnoDB does *)
(* not enforce C's RESTRICT constraint for it; only the VerifyChild query *)
(* and the locks it takes do. The DELETE plan differs: its child DELETE   *)
(* keeps foreign_key_checks on, so InnoDB enforces RESTRICT natively.     *)
(*                                                                         *)
(* Statements that Vitess leaves to MySQL (an INSERT or UPDATE of C,      *)
(* whose shard-scoped parent FK is removed by                              *)
(* SemTable.RemoveNonRequiredForeignKeys) run with foreign_key_checks on, *)
(* so InnoDB checks the parent row with a shared lock.                     *)
(*                                                                         *)
(* Abstractions:                                                           *)
(*  - Each SQL statement is one atomic step: it takes all of its locks at *)
(*    once or waits. Vitess's plans are interleaved at statement           *)
(*    boundaries, which is the level at which its locking choices act.    *)
(*    InnoDB deadlocks, NOWAIT failures and client rollbacks only abort a *)
(*    transaction, which SpontaneousAbort covers.                          *)
(*  - A gap lock is modelled per key value (a predicate lock on "value v  *)
(*    in table T's index"). Real InnoDB gaps cover ranges and block more  *)
(*    inserts, so a "no violation" result here carries over to InnoDB.    *)
(*  - Locking reads keep their locks on rows they examine, even under     *)
(*    READ COMMITTED (which may release them). More locking can only hide *)
(*    violations, so a violation found here does not depend on it.        *)
(*  - Gap locks: locking reads, UPDATE and DELETE take them only under    *)
(*    REPEATABLE READ. InnoDB foreign key checks take them under both     *)
(*    levels ("Gap locking is only used for foreign-key constraint        *)
(*    checking and duplicate-key checking" under READ COMMITTED).         *)
(*  - A failed statement aborts the whole transaction (autocommit).       *)
(***************************************************************************)
EXTENDS FiniteSets, Sequences, TLC

CONSTANTS
    Isolation,        \* "RR" (REPEATABLE READ) or "RC" (READ COMMITTED)
    VerifyChildLock,  \* lock clause of the VerifyChild query: "SHARE" (what
                      \* Vitess emits today) or "UPDATE"
    Workload          \* sequence of transaction kinds, one per transaction

TxnKinds == {"CascadeUpdGP", "CascadeDelGP", "InsC", "UpdC"}

ASSUME Isolation \in {"RR", "RC"}
ASSUME VerifyChildLock \in {"SHARE", "UPDATE"}
ASSUME \A i \in DOMAIN Workload : Workload[i] \in TxnKinds
\* InsC and UpdC each own one row of C.
ASSUME \A i, j \in DOMAIN Workload :
         (i # j /\ Workload[i] = Workload[j]) => Workload[i] \notin {"InsC", "UpdC"}

NULL   == "null"
ABSENT == "absent"   \* the row does not exist
OldKey == "k1"
NewKey == "k2"

GPRows == {"g1"}
PRows  == {"p1"}
CRows  == {"c1", "c2"}   \* c1 exists with p = NULL; c2 is the slot InsC fills
Rows   == GPRows \cup PRows \cup CRows

TableOf(r) == IF r \in GPRows THEN "GP" ELSE IF r \in PRows THEN "P" ELSE "C"
RowsOf(tbl) == {r \in Rows : TableOf(r) = tbl}

Txns == DOMAIN Workload

VARIABLES
    db,       \* current row values, including uncommitted writes
    cdb,      \* committed row values
    rlocks,   \* record locks: <<txn, row, "S" | "X">>
    glocks,   \* gap locks: <<txn, table, value>>
    written,  \* rows each transaction has written
    pc        \* next step of each transaction

vars == <<db, cdb, rlocks, glocks, written, pc>>

InitialRows == [r \in Rows |->
                  CASE r = "g1" -> OldKey
                    [] r = "p1" -> OldKey
                    [] r = "c1" -> NULL
                    [] r = "c2" -> ABSENT]

FirstStep(kind) ==
    CASE kind = "CascadeUpdGP" -> "sel"
      [] kind = "CascadeDelGP" -> "sel"
      [] kind = "InsC"         -> "ins"
      [] kind = "UpdC"         -> "upd"

-----------------------------------------------------------------------------
(* Locking helpers *)

\* Rows a locking read for value v in tbl must lock: every row whose current
\* or committed value is v. This covers rows that another transaction has
\* inserted, updated or deleted but not committed; InnoDB makes the reader
\* wait for those through the writer's record lock.
Probe(tbl, v) == {r \in RowsOf(tbl) : db[r] = v \/ cdb[r] = v}

\* Rows that hold value v once the reader has its locks.
Found(tbl, v) == {r \in RowsOf(tbl) : db[r] = v}

\* t can lock every row in R in mode m.
Free(t, R, m) ==
    \A l \in rlocks : (l[1] # t /\ l[2] \in R) => (m = "S" /\ l[3] = "S")

RLocks(t, R, m) == {<<t, r, m>> : r \in R}

\* Gap lock taken by a locking read, UPDATE or DELETE (REPEATABLE READ only).
ScanGap(t, tbl, v) == IF Isolation = "RR" THEN {<<t, tbl, v>>} ELSE {}

\* Gap lock taken by an InnoDB foreign key check (any isolation level).
FkGap(t, tbl, v) == {<<t, tbl, v>>}

\* Writing value v into tbl's index needs an insert intention lock, which
\* conflicts with gap locks that other transactions hold on v.
CanInsert(t, tbl, v) == \A g \in glocks : ~(g[1] # t /\ g[2] = tbl /\ g[3] = v)

SetRows(R, v) == [r \in Rows |-> IF r \in R THEN v ELSE db[r]]

Fail(t) == /\ pc' = [pc EXCEPT ![t] = "abort"]
           /\ UNCHANGED <<db, written>>

-----------------------------------------------------------------------------
(* Vitess FkCascade for UPDATE GP SET k = 2 WHERE k = 1, and               *)
(* DELETE FROM GP WHERE k = 1.                                             *)

\* Selection: SELECT k FROM GP WHERE k = 1 FOR UPDATE
Selection(t) ==
    /\ pc[t] = "sel"
    /\ LET R == Probe("GP", OldKey)
       IN /\ Free(t, R, "X")
          /\ rlocks' = rlocks \cup RLocks(t, R, "X")
          /\ glocks' = glocks \cup ScanGap(t, "GP", OldKey)
          /\ pc' = [pc EXCEPT ![t] =
                      IF Found("GP", OldKey) = {} THEN "commit"
                      ELSE IF Workload[t] = "CascadeUpdGP" THEN "verifyC"
                      ELSE "delP"]
    /\ UNCHANGED <<db, cdb, written>>

\* VerifyChild: SELECT 1 FROM P parent, C child WHERE parent.k = child.p
\*   AND parent.k IN (1) AND child.p NOT IN (2) LIMIT 1 FOR SHARE
VerifyChild(t) ==
    /\ pc[t] = "verifyC"
    /\ LET m  == IF VerifyChildLock = "SHARE" THEN "S" ELSE "X"
           RP == Probe("P", OldKey)
           RC == Probe("C", OldKey)
       IN /\ Free(t, RP \cup RC, m)
          /\ rlocks' = rlocks \cup RLocks(t, RP \cup RC, m)
          /\ glocks' = glocks \cup ScanGap(t, "P", OldKey) \cup ScanGap(t, "C", OldKey)
          /\ IF Found("P", OldKey) # {} /\ Found("C", OldKey) # {}
               THEN Fail(t)   \* "Cannot delete or update a parent row"
               ELSE /\ pc' = [pc EXCEPT ![t] = "updP"]
                    /\ UNCHANGED <<db, written>>
    /\ UNCHANGED cdb

\* PostVerify: UPDATE /*+ SET_VAR(foreign_key_checks=OFF) */ P
\*   SET k = 2 WHERE k IN (1)
\* InnoDB checks neither C's RESTRICT nor GP for this statement.
CascadeUpdateP(t) ==
    /\ pc[t] = "updP"
    /\ LET R == Probe("P", OldKey)
           F == Found("P", OldKey)
       IN /\ Free(t, R, "X")
          /\ F # {} => CanInsert(t, "P", NewKey)
          /\ rlocks' = rlocks \cup RLocks(t, R, "X")
          /\ glocks' = glocks \cup ScanGap(t, "P", OldKey)
          /\ db' = SetRows(F, NewKey)
          /\ written' = [written EXCEPT ![t] = @ \cup F]
          /\ pc' = [pc EXCEPT ![t] = "parent"]
    /\ UNCHANGED cdb

\* Cascade child of the DELETE plan: DELETE FROM P WHERE k IN (1)
\* foreign_key_checks stays on, so InnoDB enforces C's RESTRICT natively.
CascadeDeleteP(t) ==
    /\ pc[t] = "delP"
    /\ LET RP == Probe("P", OldKey)
           FP == Found("P", OldKey)
           RC == IF FP = {} THEN {} ELSE Probe("C", OldKey)
           FC == IF FP = {} THEN {} ELSE Found("C", OldKey)
       IN /\ Free(t, RP, "X")
          /\ Free(t, RC, "S")
          /\ rlocks' = rlocks \cup RLocks(t, RP, "X") \cup RLocks(t, RC, "S")
          /\ glocks' = glocks \cup ScanGap(t, "P", OldKey)
                              \cup (IF FP = {} THEN {} ELSE FkGap(t, "C", OldKey))
          /\ IF FC # {}
               THEN Fail(t)
               ELSE /\ db' = SetRows(FP, ABSENT)
                    /\ written' = [written EXCEPT ![t] = @ \cup FP]
                    /\ pc' = [pc EXCEPT ![t] = "parent"]
    /\ UNCHANGED cdb

\* Parent: UPDATE GP SET k = 2 WHERE k = 1, or DELETE FROM GP WHERE k = 1.
\* foreign_key_checks is on, so InnoDB applies GP's CASCADE to any P row
\* that still references the old key, and C's RESTRICT to those P rows.
\* After the cascade child step there should be none.
ParentStmt(t) ==
    /\ pc[t] = "parent"
    /\ LET newVal == IF Workload[t] = "CascadeUpdGP" THEN NewKey ELSE ABSENT
           RG == Probe("GP", OldKey)
           FG == Found("GP", OldKey)
           RP == Probe("P", OldKey)
           FP == Found("P", OldKey)
           RC == IF FP = {} THEN {} ELSE Probe("C", OldKey)
           FC == IF FP = {} THEN {} ELSE Found("C", OldKey)
       IN /\ Free(t, RG \cup RP, "X")
          /\ Free(t, RC, "S")
          /\ (newVal # ABSENT /\ FG # {}) => CanInsert(t, "GP", newVal)
          /\ (newVal # ABSENT /\ FP # {}) => CanInsert(t, "P", newVal)
          /\ rlocks' = rlocks \cup RLocks(t, RG \cup RP, "X") \cup RLocks(t, RC, "S")
          /\ glocks' = glocks \cup ScanGap(t, "GP", OldKey)
                              \cup (IF FP = {} THEN {} ELSE FkGap(t, "C", OldKey))
          /\ IF FC # {}
               THEN Fail(t)
               ELSE /\ db' = SetRows(FG \cup FP, newVal)
                    /\ written' = [written EXCEPT ![t] = @ \cup FG \cup FP]
                    /\ pc' = [pc EXCEPT ![t] = "commit"]
    /\ UNCHANGED cdb

-----------------------------------------------------------------------------
(* Statements Vitess leaves to MySQL (foreign_key_checks on).              *)

\* INSERT INTO C (p) VALUES (1)
\* InnoDB locks the parent row in P in shared mode, then inserts.
InsertC(t) ==
    /\ pc[t] = "ins"
    /\ LET r  == "c2"
           RP == Probe("P", OldKey)
           FP == Found("P", OldKey)
       IN /\ Free(t, RP, "S")
          /\ Free(t, {r}, "X")
          /\ FP # {} => CanInsert(t, "C", OldKey)
          /\ rlocks' = rlocks \cup RLocks(t, RP, "S") \cup RLocks(t, {r}, "X")
          /\ glocks' = glocks \cup (IF FP = {} THEN FkGap(t, "P", OldKey) ELSE {})
          /\ IF FP = {}
               THEN Fail(t)   \* "Cannot add or update a child row"
               ELSE /\ db' = [db EXCEPT ![r] = OldKey]
                    /\ written' = [written EXCEPT ![t] = @ \cup {r}]
                    /\ pc' = [pc EXCEPT ![t] = "commit"]
    /\ UNCHANGED cdb

\* UPDATE C SET p = 1 WHERE id = c1
UpdateC(t) ==
    /\ pc[t] = "upd"
    /\ LET r  == "c1"
           RP == Probe("P", OldKey)
           FP == Found("P", OldKey)
       IN /\ Free(t, RP, "S")
          /\ Free(t, {r}, "X")
          /\ FP # {} => CanInsert(t, "C", OldKey)
          /\ rlocks' = rlocks \cup RLocks(t, RP, "S") \cup RLocks(t, {r}, "X")
          /\ glocks' = glocks \cup (IF FP = {} THEN FkGap(t, "P", OldKey) ELSE {})
          /\ IF FP = {}
               THEN Fail(t)
               ELSE /\ db' = [db EXCEPT ![r] = OldKey]
                    /\ written' = [written EXCEPT ![t] = @ \cup {r}]
                    /\ pc' = [pc EXCEPT ![t] = "commit"]
    /\ UNCHANGED cdb

-----------------------------------------------------------------------------
(* Transaction end *)

Release(t) ==
    /\ rlocks' = {l \in rlocks : l[1] # t}
    /\ glocks' = {g \in glocks : g[1] # t}

Commit(t) ==
    /\ pc[t] = "commit"
    /\ cdb' = [r \in Rows |-> IF r \in written[t] THEN db[r] ELSE cdb[r]]
    /\ Release(t)
    /\ written' = [written EXCEPT ![t] = {}]
    /\ pc' = [pc EXCEPT ![t] = "done"]
    /\ UNCHANGED db

Rollback(t) ==
    /\ db' = [r \in Rows |-> IF r \in written[t] THEN cdb[r] ELSE db[r]]
    /\ Release(t)
    /\ written' = [written EXCEPT ![t] = {}]
    /\ pc' = [pc EXCEPT ![t] = "aborted"]
    /\ UNCHANGED cdb

Abort(t) ==
    /\ pc[t] = "abort"
    /\ Rollback(t)

\* A transaction can be rolled back at any statement boundary: an InnoDB
\* deadlock victim, a NOWAIT lock that is not free (getUpdateLock and
\* getVerifyLock use NOWAIT for tables with unique keys), or the client.
SpontaneousAbort(t) ==
    /\ pc[t] \notin {"done", "aborted", "abort"}
    /\ Rollback(t)

Terminated == \A t \in Txns : pc[t] \in {"done", "aborted"}

-----------------------------------------------------------------------------

Init ==
    /\ db = InitialRows
    /\ cdb = InitialRows
    /\ rlocks = {}
    /\ glocks = {}
    /\ written = [t \in Txns |-> {}]
    /\ pc = [t \in Txns |-> FirstStep(Workload[t])]

Next ==
    \/ \E t \in Txns :
         \/ Selection(t)
         \/ VerifyChild(t)
         \/ CascadeUpdateP(t)
         \/ CascadeDeleteP(t)
         \/ ParentStmt(t)
         \/ InsertC(t)
         \/ UpdateC(t)
         \/ Commit(t)
         \/ Abort(t)
         \/ SpontaneousAbort(t)
    \/ Terminated /\ UNCHANGED vars

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

TypeOK ==
    /\ db \in [Rows -> {OldKey, NewKey, NULL, ABSENT}]
    /\ cdb \in [Rows -> {OldKey, NewKey, NULL, ABSENT}]
    /\ rlocks \subseteq (Txns \X Rows \X {"S", "X"})
    /\ glocks \subseteq (Txns \X {"GP", "P", "C"} \X {OldKey, NewKey})
    /\ written \in [Txns -> SUBSET Rows]

\* Lock table sanity: no two transactions hold conflicting record locks,
\* and every uncommitted write is covered by the writer's exclusive lock.
LocksConsistent ==
    /\ \A l1, l2 \in rlocks :
         (l1[1] # l2[1] /\ l1[2] = l2[2]) => (l1[3] = "S" /\ l2[3] = "S")
    /\ \A t \in Txns : \A r \in written[t] : <<t, r, "X">> \in rlocks

\* The property under test: the committed database never holds a child row
\* whose parent does not exist.
FkIntegrity ==
    /\ \A r \in PRows :
         cdb[r] \notin {NULL, ABSENT} => \E g \in GPRows : cdb[g] = cdb[r]
    /\ \A r \in CRows :
         cdb[r] \notin {NULL, ABSENT} => \E q \in PRows : cdb[q] = cdb[r]

=============================================================================
