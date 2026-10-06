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
(* Statement: UPDATE parent SET k = <expr>, where <expr> gives each row an *)
(* arbitrary new key (for example k + 1, or another column).              *)
(*                                                                         *)
(* SemTable.HasNonLiteralForeignKeyUpdate makes Vitess plan this with      *)
(* VerifyAllFKs, so every statement runs with foreign_key_checks=OFF and  *)
(* the cascade runs one row at a time                                      *)
(* (FkCascade.executeNonLiteralExprFkChild). For the u_tbl7 and           *)
(* u_multicol_tbl1 cases in planbuilder/testdata/foreignkey_cases.json    *)
(* the plan is:                                                            *)
(*                                                                         *)
(*   Selection: SELECT k, k <=> <expr>, <expr> FROM parent FOR UPDATE      *)
(*   for each changed row (old, new):                                      *)
(*     CASCADE:  UPDATE child SET p = :new WHERE p IN ((:old))             *)
(*     SET NULL: UPDATE child SET p = NULL WHERE p IN ((:old))             *)
(*                 AND (:new IS NULL OR p NOT IN ((:new)))                 *)
(*   Parent:    UPDATE /*+ SET_VAR(foreign_key_checks=OFF) */ parent       *)
(*                SET k = <expr>                                           *)
(*                                                                         *)
(* MySQL executes an UPDATE one row at a time in the order its access     *)
(* path returns rows, checks the unique key after each row, and (with     *)
(* foreign_key_checks on) cascades each row to its children before moving *)
(* on. Vitess's Selection and its Parent UPDATE are separate statements,  *)
(* so MySQL can return the Selection's rows in a different order from the *)
(* one in which it updates them: the Selection reads only k and its       *)
(* expressions, so the index on k can cover it, while the UPDATE of k     *)
(* scans the primary key.                                                  *)
(*                                                                         *)
(* CascadeOrder chooses the order of the per-row child updates:            *)
(*   "selection":  the order the Selection returned the rows (Vitess      *)
(*                 before the fix).                                        *)
(*   "dependency": nonLiteralUpdateOrder in engine/fk_cascade.go. A row   *)
(*                 whose new key is another changed row's old key runs    *)
(*                 after that row. If no such order exists (the keys move *)
(*                 in a cycle) the statement fails before any write.      *)
(*                 Vitess uses this order only when the parent columns    *)
(*                 contain a primary or unique key, as in this model.     *)
(*                 Without one, MySQL's own cascade matches children      *)
(*                 again in the order it updates the rows, so Vitess      *)
(*                 keeps the Selection order.                             *)
(*                                                                         *)
(* The model picks the initial rows, the new keys, the Selection order,   *)
(* the dependency order (any valid one) and the UPDATE order              *)
(* nondeterministically. MatchesMySQL requires the result to equal        *)
(* MySQL's for the same UPDATE order.                                     *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, Sequences, TLC

CONSTANTS
    ChildAction,   \* "CASCADE" or "SETNULL"
    CascadeOrder,  \* "selection" or "dependency"
    PRows,         \* parent row ids, e.g. {"p1", "p2", "p3"}
    CRows,         \* child row ids, e.g. {"c1", "c2"}
    InitKeys,      \* key values the parent rows start with
    NewKeys,       \* key values the update can assign
    NULL           \* a model value: SQL NULL

ASSUME ChildAction \in {"CASCADE", "SETNULL"}
ASSUME CascadeOrder \in {"selection", "dependency"}

\* All orderings of a finite set, as sequences.
Perms(S) == {f \in [1..Cardinality(S) -> S] :
               \A i, j \in 1..Cardinality(S) : i # j => f[i] # f[j]}

\* Initial parent keys: unique, as the referenced column must be. A unique
\* key allows several NULLs.
InitParents == {f \in [PRows -> InitKeys \cup {NULL}] :
                  \A r1, r2 \in PRows : (r1 # r2 /\ f[r1] # NULL) => f[r1] # f[r2]}

\* Initial children: each references an existing parent key or is NULL.
InitChildren(pk) == [CRows -> ({pk[r] : r \in PRows} \ {NULL}) \cup {NULL}]

\* SQL equality: NULL equals nothing.
Eq(a, b) == a # NULL /\ b # NULL /\ a = b

-----------------------------------------------------------------------------
(* MySQL's semantics for one parent row update. *)

\* Children after parent value old changes to new, under ChildAction.
CascadeRow(ck, old, new) ==
    [c \in CRows |->
       IF Eq(ck[c], old)
         THEN IF ChildAction = "CASCADE" THEN new ELSE NULL
         ELSE ck[c]]

\* UPDATE parent SET k = <expr>, one row at a time in order ord, with the
\* unique check after each row. With cascade = TRUE, InnoDB applies the
\* foreign key action to the children after each row (foreign_key_checks
\* on). Returns [ok, parent, child]; ok = FALSE is a duplicate key error,
\* which rolls the statement back.
RECURSIVE RunUpdate(_, _, _, _, _, _)
RunUpdate(pk, ck, nk, ord, i, cascade) ==
    IF i > Len(ord) THEN [ok |-> TRUE, parent |-> pk, child |-> ck]
    ELSE LET r   == ord[i]
             old == pk[r]
             new == nk[r]
         IN IF \E r2 \in PRows : r2 # r /\ Eq(pk[r2], new)
              THEN [ok |-> FALSE, parent |-> pk, child |-> ck]
              ELSE RunUpdate([pk EXCEPT ![r] = new],
                             IF cascade /\ old # new THEN CascadeRow(ck, old, new) ELSE ck,
                             nk, ord, i + 1, cascade)

\* ord is a dependency order: a changed row whose new key is the old key of
\* another changed row comes after that row.
DepOrder(pk, nk, ord) ==
    \A i, j \in 1..Len(ord) :
      ~(i < j /\ pk[ord[j]] # nk[ord[j]] /\ pk[ord[i]] # nk[ord[i]]
              /\ Eq(nk[ord[i]], pk[ord[j]]))

-----------------------------------------------------------------------------

VARIABLES
    pk0, ck0,   \* initial parent keys and child values
    nk,         \* each parent row's new key
    selOrder,   \* order of the per-row child updates
    updOrder,   \* order in which MySQL updates parent rows
    parent,     \* parent keys as Vitess's plan runs
    child,      \* child values as Vitess's plan runs
    i,          \* next row the cascade handles
    pc          \* "children", "done" or "error"

vars == <<pk0, ck0, nk, selOrder, updOrder, parent, child, i, pc>>

MySQL == RunUpdate(pk0, ck0, nk, updOrder, 1, TRUE)

HasDepOrder == \E o \in Perms(PRows) : DepOrder(pk0, nk, o)

Init ==
    /\ pk0 \in InitParents
    /\ ck0 \in InitChildren(pk0)
    /\ nk \in [PRows -> NewKeys \cup {NULL}]
    /\ updOrder \in Perms(PRows)
    /\ parent = pk0
    /\ child = ck0
    /\ i = 1
    /\ IF CascadeOrder = "selection"
         THEN /\ selOrder \in Perms(PRows)
              /\ pc = "children"
         ELSE IF HasDepOrder
                THEN /\ selOrder \in {o \in Perms(PRows) : DepOrder(pk0, nk, o)}
                     /\ pc = "children"
                ELSE \* a cycle: the duplicate key error comes before any write
                     /\ selOrder = <<>>
                     /\ pc = "error"

\* One iteration of FkCascade.executeNonLiteralExprFkChild. The Selection
\* ran before any write, so it saw the initial keys.
CascadeChild ==
    /\ pc = "children"
    /\ i <= Len(selOrder)
    /\ LET old == pk0[selOrder[i]]
           new == nk[selOrder[i]]
       IN child' = IF old = new THEN child   \* unchanged row: skipped
                   ELSE CascadeRow(child, old, new)
    /\ i' = i + 1
    /\ UNCHANGED <<pk0, ck0, nk, selOrder, updOrder, parent, pc>>

\* The Parent UPDATE runs with foreign_key_checks=OFF: no cascade, but the
\* unique check still applies. A duplicate key error fails the statement and
\* vtgate rolls back everything the plan wrote (see FkPartialExec).
ParentUpdate ==
    /\ pc = "children"
    /\ i > Len(selOrder)
    /\ LET res == RunUpdate(parent, child, nk, updOrder, 1, FALSE)
       IN IF res.ok
            THEN /\ parent' = res.parent
                 /\ pc' = "done"
                 /\ UNCHANGED child
            ELSE /\ parent' = pk0
                 /\ child' = ck0
                 /\ pc' = "error"
    /\ UNCHANGED <<pk0, ck0, nk, selOrder, updOrder, i>>

Next ==
    \/ CascadeChild
    \/ ParentUpdate
    \/ pc \in {"done", "error"} /\ UNCHANGED vars

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

\* Every child references an existing parent.
FkIntegrity ==
    pc = "done" =>
      \A c \in CRows : child[c] # NULL => \E r \in PRows : Eq(parent[r], child[c])

\* The statement succeeds exactly when MySQL's does, with the same rows.
MatchesMySQL ==
    /\ pc = "done"  => /\ MySQL.ok
                       /\ parent = MySQL.parent
                       /\ child = MySQL.child
    /\ pc = "error" => ~MySQL.ok

=============================================================================
