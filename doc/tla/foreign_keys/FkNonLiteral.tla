---------------------------- MODULE FkNonLiteral ----------------------------
(***************************************************************************)
(* Vitess's cascade for an UPDATE whose new foreign key value is not a    *)
(* literal, compared with MySQL's own cascade.                            *)
(*                                                                         *)
(* Schema:                                                                 *)
(*   parent(id PRIMARY KEY, k UNIQUE)                                      *)
(*   child(id PRIMARY KEY, p) FOREIGN KEY (p) REFERENCES parent(k)         *)
(*                            ON UPDATE CASCADE | SET NULL                 *)
(*                                                                         *)
(* Statement: UPDATE parent SET k = k + Delta                              *)
(*                                                                         *)
(* SemTable.HasNonLiteralForeignKeyUpdate makes Vitess plan this with      *)
(* VerifyAllFKs, so every statement runs with foreign_key_checks=OFF and  *)
(* the cascade runs one row at a time                                      *)
(* (FkCascade.executeNonLiteralExprFkChild). For the u_tbl7 and           *)
(* u_multicol_tbl1 cases in planbuilder/testdata/foreignkey_cases.json    *)
(* the plan is:                                                            *)
(*                                                                         *)
(*   Selection: SELECT k, k <=> k + Delta, k + Delta FROM parent FOR UPDATE *)
(*   for each selected row (old, new) with old # new:                      *)
(*     CASCADE:  UPDATE child SET p = :new WHERE p IN ((:old))             *)
(*     SET NULL: UPDATE child SET p = NULL WHERE p IN ((:old))             *)
(*                 AND (:new IS NULL OR p NOT IN ((:new)))                 *)
(*   Parent:    UPDATE /*+ SET_VAR(foreign_key_checks=OFF) */ parent       *)
(*                SET k = k + Delta                                         *)
(*                                                                         *)
(* MySQL executes an UPDATE one row at a time in the order its access     *)
(* path returns rows, checks the unique key after each row, and (with     *)
(* foreign_key_checks on) cascades each row to its children before moving *)
(* on. Vitess's Selection and its Parent UPDATE are separate statements,  *)
(* so MySQL can return the Selection's rows in a different order from the *)
(* one in which it updates them. For example, the Selection reads only k   *)
(* and its expressions, so it can be covered by the index on k, while the *)
(* UPDATE of k scans the primary key.                                      *)
(*                                                                         *)
(* The model picks the initial rows, the Selection order and the UPDATE   *)
(* order nondeterministically and runs Vitess's plan step by step.        *)
(* MatchesMySQL requires the result to equal MySQL's for the same UPDATE  *)
(* order.                                                                  *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    ChildAction,  \* "CASCADE" or "SETNULL"
    Delta,        \* the statement is UPDATE parent SET k = k + Delta
    PRows,        \* parent row ids, e.g. {"p1", "p2"}
    CRows,        \* child row ids, e.g. {"c1", "c2"}
    InitKeys,     \* key values the parent rows start with
    NULL          \* a model value: SQL NULL

ASSUME ChildAction \in {"CASCADE", "SETNULL"}
ASSUME Delta \in Int /\ Delta # 0

NewKey(k) == k + Delta

\* All orderings of a finite set, as sequences.
Perms(S) == {f \in [1..Cardinality(S) -> S] :
               \A i, j \in 1..Cardinality(S) : i # j => f[i] # f[j]}

\* Initial parent keys: unique, as the referenced column must be.
InitParents == {f \in [PRows -> InitKeys] :
                  \A r1, r2 \in PRows : r1 # r2 => f[r1] # f[r2]}

\* Initial children: each references an existing parent key or is NULL.
InitChildren(pk) == [CRows -> {pk[r] : r \in PRows} \cup {NULL}]

-----------------------------------------------------------------------------
(* MySQL's semantics for one parent row update. *)

\* Children after parent value old changes to new, under ChildAction.
CascadeRow(ck, old, new) ==
    [c \in CRows |->
       IF ck[c] = old
         THEN IF ChildAction = "CASCADE" THEN new ELSE NULL
         ELSE ck[c]]

\* UPDATE parent SET k = k + Delta, one row at a time in order ord, with the
\* unique check after each row. With cascade = TRUE, InnoDB applies the
\* foreign key action to the children after each row (foreign_key_checks
\* on). Returns [ok, parent, child]; ok = FALSE is a duplicate key error,
\* which rolls the statement back.
RECURSIVE RunUpdate(_, _, _, _, _)
RunUpdate(pk, ck, ord, i, cascade) ==
    IF i > Len(ord) THEN [ok |-> TRUE, parent |-> pk, child |-> ck]
    ELSE LET r   == ord[i]
             old == pk[r]
             new == NewKey(old)
         IN IF \E r2 \in PRows : r2 # r /\ pk[r2] = new
              THEN [ok |-> FALSE, parent |-> pk, child |-> ck]
              ELSE RunUpdate([pk EXCEPT ![r] = new],
                             IF cascade THEN CascadeRow(ck, old, new) ELSE ck,
                             ord, i + 1, cascade)

-----------------------------------------------------------------------------

VARIABLES
    pk0, ck0,   \* initial parent keys and child values
    selOrder,   \* order in which the Selection returns parent rows
    updOrder,   \* order in which MySQL updates parent rows
    parent,     \* parent keys as Vitess's plan runs
    child,      \* child values as Vitess's plan runs
    i,          \* next Selection row the cascade handles
    pc          \* "children", "parent", "done" or "error"

vars == <<pk0, ck0, selOrder, updOrder, parent, child, i, pc>>

MySQL == RunUpdate(pk0, ck0, updOrder, 1, TRUE)

Init ==
    /\ pk0 \in InitParents
    /\ ck0 \in InitChildren(pk0)
    /\ selOrder \in Perms(PRows)
    /\ updOrder \in Perms(PRows)
    /\ parent = pk0
    /\ child = ck0
    /\ i = 1
    /\ pc = "children"

\* One iteration of FkCascade.executeNonLiteralExprFkChild. The Selection
\* ran before any write, so it saw the initial keys.
CascadeChild ==
    /\ pc = "children"
    /\ i <= Len(selOrder)
    /\ LET old == pk0[selOrder[i]]
           new == NewKey(old)
       IN child' =
            IF old = new THEN child   \* the row's k is unchanged: skipped
            ELSE [c \in CRows |->
                    IF child[c] = old
                      THEN IF ChildAction = "CASCADE" THEN new ELSE NULL
                      ELSE child[c]]
    /\ i' = i + 1
    /\ UNCHANGED <<pk0, ck0, selOrder, updOrder, parent, pc>>

ChildrenDone ==
    /\ pc = "children"
    /\ i > Len(selOrder)
    /\ pc' = "parent"
    /\ UNCHANGED <<pk0, ck0, selOrder, updOrder, parent, child, i>>

\* The Parent UPDATE runs with foreign_key_checks=OFF: no cascade, but the
\* unique check still applies. A duplicate key error fails the statement and
\* vtgate rolls back everything the plan wrote (see FkPartialExec).
ParentUpdate ==
    /\ pc = "parent"
    /\ LET res == RunUpdate(parent, child, updOrder, 1, FALSE)
       IN IF res.ok
            THEN /\ parent' = res.parent
                 /\ pc' = "done"
                 /\ UNCHANGED child
            ELSE /\ parent' = pk0
                 /\ child' = ck0
                 /\ pc' = "error"
    /\ UNCHANGED <<pk0, ck0, selOrder, updOrder, i>>

Next ==
    \/ CascadeChild
    \/ ChildrenDone
    \/ ParentUpdate
    \/ pc \in {"done", "error"} /\ UNCHANGED vars

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

\* Every child references an existing parent.
FkIntegrity ==
    pc = "done" =>
      \A c \in CRows : child[c] # NULL => \E r \in PRows : parent[r] = child[c]

\* The statement succeeds exactly when MySQL's does, with the same rows.
MatchesMySQL ==
    /\ pc = "done"  => /\ MySQL.ok
                       /\ parent = MySQL.parent
                       /\ child = MySQL.child
    /\ pc = "error" => ~MySQL.ok

=============================================================================
