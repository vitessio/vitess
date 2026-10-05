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

Fair ==
    /\ \A s \in Servers :
        /\ WF_vars(Deliver(s)) /\ WF_vars(Apply(s)) /\ WF_vars(ElectEnd(s)) /\ WF_vars(Restart(s))
        /\ WF_vars(BootComplete(s)) /\ WF_vars(AsyncApply(s)) /\ WF_vars(RefreshRec(s))
        /\ WF_vars(BootGraceExpire(s))
        /\ WF_vars(SyncRead(s)) /\ WF_vars(SyncStop(s)) /\ WF_vars(SyncDemote(s)) /\ WF_vars(LeaveForeign(s))
        /\ WF_vars(PrSnap(s)) /\ WF_vars(PrRead(s)) /\ WF_vars(PrAct(s))
        /\ WF_vars(SaSnap(s)) /\ WF_vars(SaRead(s)) /\ WF_vars(SaAct(s))
        /\ WF_vars(JoinStart(s)) /\ WF_vars(JoinRelease(s))
        \* a START ends: it joins a live group (strongly fair), or fails, or forms a group of its own
        /\ SF_vars(JoinComplete(s)) /\ WF_vars(JoinFail(s) \/ JoinStray(s))
        /\ WF_vars(FcRead(s)) /\ WF_vars(FcAct(s))
        /\ WF_vars(HBoot1(s)) /\ WF_vars(HBootGo(s)) /\ WF_vars(HBootGiveUp(s)) /\ WF_vars(HBootAbort(s))
        /\ WF_vars(StaleTopo(s))
        /\ WF_vars(VLeave(s))
        /\ WF_vars(PDemoteEnd(s) \/ PDemoteFail(s) \/ DrSnap(s)) /\ WF_vars(DrRead(s)) /\ WF_vars(DrAct(s)) /\ WF_vars(PPromote2(s))
        /\ WF_vars(UdSnap(s)) /\ WF_vars(UdRead(s)) /\ WF_vars(UdAct(s))
        /\ WF_vars(RwRead(s)) /\ WF_vars(RwAct(s))
        /\ WF_vars(IP1(s)) /\ WF_vars(IP2(s))
        /\ WF_vars(FenceDrop(s)) /\ WF_vars(NLeave(s))
    /\ \A i \in Incs : WF_vars(Elect(i)) /\ WF_vars(LeaveDead(i))
    /\ \A o \in Orcs :
        /\ WF_vars(OBegin(o)) /\ WF_vars(OIntent(o)) /\ WF_vars(OAdopt(o)) /\ WF_vars(OAdoptLater(o))
        \* the bootstrap RPC ends: its reply, or a timeout
        /\ WF_vars(OReply(o) \/ OTimeout(o))
        /\ WF_vars(OVotRead(o)) /\ WF_vars(OVotWrite(o))
    /\ WF_vars(OIntentExpire) /\ WF_vars(OUndo) /\ WF_vars(GraceExpire)
    /\ WF_vars(PDemote) /\ WF_vars(PWait) /\ WF_vars(PPromote) /\ WF_vars(PEnd) /\ WF_vars(PAbort)
    /\ WF_vars(PIRecord) /\ WF_vars(OAdoptUnrec)

LSpec == Spec /\ Fair

\* a serving primary of the legitimate group, or a bound of the model blocks the way to one: no live group
\* of the recorded incarnation holds the voter majority, and no new incarnation or intent can be created
\* (the code is not limited by either)
LegitLive == \E i \in Incs : Alive(i) /\ RecOK(i, recInc) /\ VoterMaj(i)
Bound == (nextInc > MaxInc \/ nextTok > MaxTok) /\ ~LegitLive
EventuallyServes == <>[](Healthy \/ Bound)
=============================================================================
