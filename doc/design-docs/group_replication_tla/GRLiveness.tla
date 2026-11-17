---------------------------- MODULE GRLiveness ----------------------------
(***************************************************************************)
(* Liveness of GRSafety: once the faults stop (their budgets are finite),  *)
(* the shard ends with a serving primary of its legitimate group.          *)
(*                                                                         *)
(* Every step of a tablet, of MySQL and of VTOrc is weakly fair: a step     *)
(* that stays enabled is taken. A join's success is strongly fair: a join  *)
(* that can complete infinitely often completes. With LOOP_RUNS, the sync  *)
(* loop serves again at most once per run, after its read of MySQL. Faults, clients, the      *)
(* outcomes of MySQL that VTOrc and the tablets must cope with (a join     *)
(* that fails or ends in a group of its own, a lost RPC), and the          *)
(* operator's reparents are not fair: they happen, or not, at most as     *)
(* often as their budgets allow.                                           *)
(***************************************************************************)
EXTENDS GRSafety

\* the steps of the tablets, MySQL and VTOrc come with the clean-up of the partition state (Next)
WFP(A) == WF_vars(A /\ PClean)
SFP(A) == SF_vars(A /\ PClean)

Fair ==
    /\ \A s \in Servers :
        /\ WFP(Deliver(s)) /\ WFP(Apply(s)) /\ WFP(ElectEnd(s)) /\ WFP(Restart(s))
        /\ WFP(BootComplete(s)) /\ WFP(AsyncApply(s)) /\ WFP(RefreshRec(s))
        /\ WFP(BootGraceExpire(s))
        /\ WFP(SyncRead(s)) /\ WFP(SyncStop(s)) /\ WFP(SyncDemote(s)) /\ WFP(LeaveForeign(s))
        /\ WFP(PrSnap(s)) /\ WFP(PrRead(s)) /\ WFP(PrAct(s))
        /\ WFP(SaSnap(s)) /\ WFP(SaRead(s)) /\ WFP(SaAct(s))
        /\ WFP(JoinStart(s)) /\ WFP(JoinRelease(s))
        \* a START ends: it joins a live group (strongly fair), or fails, or forms a group of its own
        /\ SFP(JoinComplete(s)) /\ WFP(JoinFail(s) \/ JoinStray(s))
        /\ WFP(FcRead(s)) /\ WFP(FcAct(s))
        /\ WFP(HBoot1(s)) /\ WFP(HBootGo(s)) /\ WFP(HBootGiveUp(s)) /\ WFP(HBootAbort(s))
        /\ WFP(StaleTopo(s)) /\ WFP(EndTerm(s))
        /\ WFP(VLeave(s))
        /\ WFP(PDemoteEnd(s) \/ PDemoteFail(s) \/ DrSnap(s)) /\ WFP(DrRead(s)) /\ WFP(DrAct(s)) /\ WFP(PPromote2(s))
        /\ WFP(UdSnap(s)) /\ WFP(UdRead(s)) /\ WFP(UdAct(s))
        /\ WFP(RwRead(s)) /\ WFP(RwAct(s))
        /\ WFP(IP1(s)) /\ WFP(IP2(s))
        /\ WFP(FenceDrop(s)) /\ WFP(NLeave(s))
    /\ \A i \in Incs : WFP(Elect(i)) /\ WFP(LeaveDead(i))
    /\ \A o \in Orcs :
        /\ WFP(OBegin(o)) /\ WFP(OIntent(o)) /\ WFP(OAdopt(o)) /\ WFP(OAdoptLater(o))
        \* the bootstrap RPC ends: its reply, or a timeout
        /\ WFP(OReply(o) \/ OTimeout(o))
        /\ WFP(OVotRead(o)) /\ WFP(OVotWrite(o)) /\ WFP(OSwap(o)) /\ WFP(OGrow(o)) /\ WFP(ORemove(o)) /\ WFP(ORemoveNoGroup(o))
    /\ WFP(OIntentExpire) /\ WFP(OUndo) /\ WFP(GraceExpire)
    /\ WFP(PDemote) /\ WFP(PWait) /\ WFP(PPromote) /\ WFP(PEnd) /\ WFP(PAbort)
    /\ WFP(PIRecord) /\ WFP(OAdoptUnrec) /\ WFP(OMoveToVoter) /\ WFP(OMoveFromDeleted)
    \* the scenario of a deleted record: the operator eventually deletes the record of a dead voter (only with
    \* DEL_DEAD, MaxDel > 0)
    /\ DEL_DEAD => WFP(ODelete)
    \* (fourth milestone) XCom expels an unreachable member, a view without quorum times out, an isolated
    \* member detects its partition, and the partition heals. These are timeouts of MySQL, which happen while the
    \* tablets' atomic decisions come and go (Free): strongly fair, so that a cycle of decisions (FLAG 1's
    \* alternating promotions) cannot postpone them for ever. One condition for all of them: each such step
    \* ends a part of a partition for good, so they cannot be taken infinitely often
    /\ SF_vars(PartEnd)

LSpec == Spec /\ Fair

\* a serving primary of the legitimate group, or a bound of the model blocks the way to one: no live group
\* of the recorded incarnation holds the voter majority, and no new incarnation or intent can be created
\* (the code is not limited by either)
LegitLive == \E i \in Incs : Alive(i) /\ RecOK(i, recInc) /\ VoterMaj(i)
Bound == (nextInc > MaxInc \/ nextTok > MaxTok) /\ ~LegitLive
EventuallyServes == <>[](Healthy \/ Bound)
\* ... or a listed voter died, and VTOrc cannot replace it: no live group of the recorded incarnation holds the
\* voter majority (P1: an operator runs ERS or the voter change), or no spare is left
\* (deleting its tablet record is the operator's signal)
NeedsOperator == \E v \in (voters \cap died) \ deleted : ~LegitLive \/ Servers \ (voters \cup died) = {}
EventuallyServesOrOp == <>[](Healthy \/ Bound \/ NeedsOperator)
\* a voter that died and whose record was deleted leaves the list
EventuallyShrinks == <>[](voters \cap died \cap deleted = {})
=============================================================================
