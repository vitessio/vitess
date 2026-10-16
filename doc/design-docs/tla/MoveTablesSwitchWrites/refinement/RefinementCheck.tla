---------------------------- MODULE RefinementCheck ----------------------------
(***************************************************************************)
(* Checks that the state space reductions in MoveTablesSwitchWrites do    *)
(* not lose behavior: every behavior of the unreduced model                *)
(* (MoveTablesSwitchWritesRef) is, under the mapping below, a behavior of *)
(* the reduced model, including its fairness conditions.                   *)
(***************************************************************************)
EXTENDS MoveTablesSwitchWritesRef

\* The phase GwCheck would move a "gwCheck" request to.
MRs == [r \in Reqs |-> IF rs[r] = "gwCheck"
                       THEN IF invalid[r] THEN "gwDone" ELSE "send"
                       ELSE rs[r]]

Finished(r)    == MRs[r] \in {"idle", "acked", "failed"}
AttemptLive(r) == MRs[r] \in {"gwBuf", "buffered", "send"}
DestLive(r)    == MRs[r] \in {"gwBuf", "buffered", "send", "passed", "mdl"}

Opt == INSTANCE MoveTablesSwitchWrites WITH
    Diagnostics  <- TRUE,
    \* Not part of the unreduced model; with both FALSE the generations stay 0.
    StartupEvent <- FALSE,
    EventGenerations <- FALSE,
    kewGen       <- 0,
    evGen        <- 0,
    bufGen       <- 0,
    rs           <- MRs,
    try          <- [r \in Reqs |-> IF Finished(r) THEN 0 ELSE try[r]],
    lastVer      <- [r \in Reqs |-> IF Finished(r) THEN 0 ELSE lastVer[r]],
    capVer       <- [r \in Reqs |-> IF rs[r] = "load" THEN capVer[r] ELSE 0],
    dest         <- [r \in Reqs |-> IF DestLive(r) THEN dest[r] ELSE "src"],
    bufOnce      <- [r \in Reqs |-> AttemptLive(r) /\ bufOnce[r]],
    invalid      <- [r \in Reqs |-> AttemptLive(r) /\ invalid[r]],
    gwErr        <- [r \in Reqs |-> AttemptLive(r) /\ gwErr[r]],
    startedAfter <- [r \in Reqs |-> ~Finished(r) /\ startedAfter[r]]

\* Every step of the unreduced model is a step of the reduced one (or leaves
\* the mapped state unchanged).
OptSafety == Opt!Init /\ [][Opt!Next]_(Opt!vars)

\* The same, plus the reduced model's fairness conditions.
OptSpec == Opt!Spec
===============================================================================
