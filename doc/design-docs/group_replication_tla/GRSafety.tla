----------------------------- MODULE GRSafety -----------------------------
(***************************************************************************)
(* Safety model of the Group Replication support of Vitess: one shard,     *)
(* its voters' MySQL Group Replication, their vttablets, and VTOrcs.       *)
(*                                                                         *)
(* See README.md next to this file for the design it follows, the Go code  *)
(* each action models, every abstraction, and what is not modeled.         *)
(*                                                                         *)
(* Each fix of the design is a CONSTANT switch: TRUE is the design on the  *)
(* branch, FALSE the behaviour before the fix.                             *)
(***************************************************************************)
EXTENDS Integers, FiniteSets, TLC

CONSTANTS
    Servers,        \* the voters: a host each, with mysqld and vttablet
    Orcs,           \* the VTOrcs
    NoServer, NoOrc,
    MaxInc,         \* bound: group incarnations (1 is the initial group)
    MaxTx,          \* bound: client transactions
    MaxTok,         \* bound: bootstrap intents written
    MaxCrash,       \* bound: host crashes
    MaxLeave,       \* bound: members that leave a live group (expulsion, clean leave)
    MaxLoss,        \* bound: groups that lose their majority (partition)
    MaxExpire,      \* bound: shard lock leases that expire under a live holder
    \* --- the fixes (TRUE = current design) ---
    LEGIT,          \* legitimate group = recorded incarnation + majority of the voters (NEW-1)
    BOOT_ALL,       \* bootstrap only with every voter reachable (majority bootstrap rejected)
    SERVE_LOCKED,   \* serve again only under the action lock on a fresh status, with generations (S7d r2)
    MA_DISABLED,    \* member action mysql_disable_super_read_only_if_primary disabled before every START
    FENCE,          \* the fence check
    FENCE_SNAPSHOT, \* a decision to make MySQL writable yields to a fence decided since its snapshot
    JOIN_GATE,      \* joins start only while the legitimate group is active on another tablet (NEW-2)
    INTENT,         \* VTOrc records a bootstrap intent before it bootstraps
    INTENT_FENCE,   \* a live intent fences a bootstrap on another tablet
    INTENT_PREFER,  \* among equal voters, VTOrc chooses the live intent's target again
    ADOPT,          \* adoption of the group of a bootstrap whose reply was lost (S7d r3)
    INC_CAS,        \* incarnation writes are a compare-and-swap (expected incarnation, intent token)
    NEW3_FIX,       \* StaleTopoPrimary only fixes the tablet type of a voter (NEW-3)
    CAND_BINLOG,    \* among the candidates, VTOrc prefers a voter that executed every voter's transactions
    BOOT_REQ,       \* the bootstrap RPC carries every voter's executed and received transactions; the
                    \* tablet applies its relay log, and refuses unless MySQL executed all of them
    BOOT_TOKEN,     \* the bootstrap RPC carries its intent's token and expected incarnation; the tablet
                    \* refuses an intent that the shard record no longer holds
    KEEP_NEWER_INTENT, \* recording an incarnation that is recorded already keeps an intent recorded
                       \* for it (a later bootstrap's)
    WITHDRAW_ON_REFUSAL, \* VTOrc withdraws its intent (compare-and-swap on the token) when the tablet
                         \* refuses the bootstrap definitively: MySQL lacks a required transaction, and
                         \* no START GROUP_REPLICATION runs
    \* --- unsafe variants, to validate the conditions of WITHDRAW_ON_REFUSAL (FALSE = the code) ---
    WITHDRAW_ANY_FAILURE, \* VTOrc withdraws its intent after any error or timeout of the bootstrap RPC
    DEFINITIVE_WHILE_STARTING, \* the tablet reports as definitive the refusal it sends while a START
                               \* GROUP_REPLICATION still runs (it gave up waiting for the START to end)
    \* --- a rule considered and rejected (FALSE = the code) ---
    CAND_EXECUTED,  \* only a voter that executed every voter's transactions may be the candidate
    \* --- environment and checking modes ---
    STALE_TOPO,     \* VTOrc's StaleTopoPrimary recovery runs
    STALE_REC,      \* a tablet decides on the shard record it read last (FALSE: on the current one)
    STUCK_CHECK,    \* no timeout longer than the shard lock's lease fires; Done marks healthy end states
    SPLIT,          \* decisions (snapshot, read, act) and the fence check (read, decide) interleave;
                    \* FALSE runs each as one atomic step
    INTENT_OUTLASTS_BOOT, \* timing assumption: an intent expires only while no VTOrc recovery and no
                         \* bootstrap is in flight (no stall outlasts the two-minute fence)
    DECIDE_CRASH,   \* a group decides a transaction while its primary crashes before committing it
    TOKEN_TOPO_TIMEOUT \* the tablet's read of the shard record for BOOT_TOKEN may time out; the check is
                       \* then skipped

Incs == 1..MaxInc
Tx   == 1..MaxTx
Toks == 1..MaxTok
Maj  == Cardinality(Servers) \div 2 + 1
NoIntent == [tgt |-> NoServer, prev |-> 0, tok |-> 0, born |-> 0]

VARIABLES
    \* ---- MySQL and Group Replication ----
    up,         \* the host (mysqld and vttablet) runs
    grp,        \* incarnation of the group s is an active member of, 0 for none
    st,         \* a START GROUP_REPLICATION runs in MySQL: "none", "join", "boot"
    sro,        \* super_read_only
    electing,   \* the primary election that made s the primary still runs
    exec,       \* transactions executed (binlog)
    recv,       \* transactions received into the relay log, not applied yet
    view,       \* members of incarnation i's view
    prim,       \* primary of incarnation i
    hist,       \* history of incarnation i: the bootstrapped member's data, plus every decided transaction
    dead,       \* incarnation i lost its majority: no commit, delivery, election or join any more
    nextInc,
    async,      \* the default channel replicates s asynchronously from this server (NoServer: off)
    \* ---- vttablet ----
    ttype,      \* "P" or "R"
    serving,    \* the query service serves as PRIMARY
    lk,         \* action lock: "free", "sync" (a decision of the sync loop), "join", "bootw", "boot"
    dph,        \* phase of the sync loop's decision: "idle", "pr_snap", "pr_read", "sa_snap", "sa_read"
    dOK,        \* the decision's read status satisfied the serving invariant
    dGen,       \* a not-serving reason was set since the decision captured its generation
    dFence,     \* a fence was decided since the decision's snapshot
    snap,       \* the sync loop's status read at the start of its run: "ok", "bad", "np" (not a primary)
    fenced,     \* groupReplicationFence.fenced
    fcPend,     \* the fence check read a status that must be fenced, and has not decided yet
    majInc,     \* the fence check's majorityIncarnation
    cInc,       \* the shard record's incarnation as the tablet read it last
    recentBoot, \* incarnation the tablet bootstrapped within groupReplicationBootstrapGrace
    dmt,        \* groupReplicationDemoted (DemotePrimary)
    breq,       \* bootstrap RPCs [o: VTOrc, tok: intent token, exp: expected incarnation, req: required
                \* transactions] that wait for the tablet's action lock
    bOrc,       \* the bootstrap RPC that the tablet runs
    bPrev,      \* recorded incarnation when that bootstrap started
    badServe,   \* the tablet serves on a decision that was not taken on a fresh status under the lock
    \* ---- shard record ----
    recInc,     \* Shard.group_replication_incarnation
    intent,     \* Shard.group_replication_bootstrap_intent
    nextTok,    \* next intent token
    intExp,     \* the current intent is older than the two-minute fence
    newest,     \* tablet with the newest primary term
    \* ---- VTOrc ----
    oph,        \* "idle", "chosen", "wait", "adopt"
    ocand,      \* bootstrap target
    oexp,       \* incarnation the shard record listed when the recovery read it
    otok,       \* token of the intent the recovery wrote or adopts
    oborn,      \* first incarnation created after that intent
    oreq,       \* every transaction that a voter executed or received, when the recovery read them
    orep,       \* reply of the bootstrap RPC: 0 none, -1 error, -2 definitive refusal, else the new
                \* incarnation
    lockOwner,  \* holder of the shard lock's lease
    \* ---- clients and history ----
    acked, nextTx,
    bootFrom,   \* for a bootstrapped incarnation i: the recorded incarnation it started from, and its target
    decisionAck, minorityAck, fenceUndone,
    adopted,    \* incarnations recorded for each intent token
    wfail,      \* STUCK_CHECK only: the bootstrap RPC of the current intent failed without a definitive
                \* refusal (an error or a timeout), so that only adoption or the intent's expiry ends it
    \* ---- budgets ----
    nCrash, nLeave, nLoss, nExpire

vM == <<up, grp, st, sro, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
vT == <<ttype, serving, lk, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc, cInc,
        recentBoot, dmt, breq, bOrc, bPrev, badServe>>
vS == <<recInc, intent, nextTok, intExp, newest>>
vO == <<oph, ocand, oexp, otok, oborn, oreq, orep, lockOwner>>
vH == <<acked, nextTx, bootFrom, decisionAck, minorityAck, fenceUndone, adopted, wfail>>
vB == <<nCrash, nLeave, nLoss, nExpire>>
vars == <<vM, vT, vS, vO, vH, vB>>

----------------------------------------------------------------------------
(* Helpers *)

VoterMaj(i)  == Cardinality(view[i]) >= Maj
Alive(i)     == view[i] # {} /\ ~dead[i]
\* mysql.IsGroupPrimary: ONLINE primary of a group with quorum
IsPrimQ(s)   == up[s] /\ grp[s] # 0 /\ prim[grp[s]] = s /\ ~dead[grp[s]]
\* MySQL accepts a commit that it can acknowledge
CanAccept(s) == IsPrimQ(s) /\ ~sro[s] /\ ~electing[s]
\* ground truth: the primary of the shard's legitimate group, with the voter majority
LegitMaj(s)  == IsPrimQ(s) /\ grp[s] = recInc /\ VoterMaj(grp[s])

\* groupReplicationServingReason = "" on a status read now, against the record incarnation c
ServeOK(s, c) == IsPrimQ(s) /\ ~electing[s] /\ (LEGIT => (grp[s] = c /\ VoterMaj(grp[s])))
\* IsLegitimatePrimary for the sync loop's promotion (trusts a group it bootstrapped itself)
PromoteLegit(s) == IsPrimQ(s) /\ (LEGIT => ((grp[s] = recInc \/ grp[s] = recentBoot[s]) /\ VoterMaj(grp[s])))

IntentLive(it) == it.tok # 0 /\ ~(it.tok = intent.tok /\ intExp)
\* shardGroupRecord.adoptableByIntent: the tablet's view of an intent that names it
AdoptableByIntent(s, i, it, rinc) ==
    ADOPT /\ INTENT /\ it.tok # 0 /\ it.tgt = s /\ it.prev = rinc /\ i # it.prev /\ IntentLive(it) /\ i >= it.born

\* another tablet reports an active member of the legitimate group with quorum
LegitActiveElsewhere(s) ==
    \E v \in Servers \ {s} : up[v] /\ grp[v] # 0 /\ ~dead[grp[v]] /\ (LEGIT => grp[v] = recInc)

Data(v) == exec[v] \cup recv[v]

\* the recorded incarnation as the tablet's sync loop and fence check see it
Rec(s) == IF STALE_REC THEN cInc[s] ELSE recInc
\* a fresh read of the shard record updates the tablet's copy
Refresh(s) == cInc' = IF STALE_REC THEN [cInc EXCEPT ![s] = recInc] ELSE cInc

SnapVal(s) == IF ~IsPrimQ(s) \/ electing[s] THEN "np" ELSE IF ServeOK(s, Rec(s)) THEN "ok" ELSE "bad"

Unlock(o) == IF lockOwner = o THEN NoOrc ELSE lockOwner

NoReq == [o |-> NoOrc, tok |-> 0, exp |-> 0, req |-> {}]
\* the VTOrc that sent the bootstrap RPC r to s still waits for its reply
Waiting(r, s) == r.o # NoOrc /\ oph[r.o] = "wait" /\ otok[r.o] = r.tok /\ ocand[r.o] = s
\* the reply of RPC r, if its VTOrc still waits for it
Reply(r, s, v) == orep' = IF Waiting(r, s) THEN [orep EXCEPT ![r.o] = v] ELSE orep

\* BOOT_TOKEN: the shard record still holds the RPC's intent, for the incarnation it expected (an RPC
\* without a token, before INTENT, is not checked). With TOKEN_TOPO_TIMEOUT the read may time out, and
\* the tablet then bootstraps without the check.
TokenOK(r) == ~BOOT_TOKEN \/ r.tok = 0 \/ (intent.tok = r.tok /\ recInc = r.exp)
TokenChecks == IF BOOT_TOKEN /\ TOKEN_TOPO_TIMEOUT THEN {TRUE, FALSE} ELSE {TRUE}
\* BOOT_REQ: the tablet applies its relay log if that covers what the RPC requires, then requires
\* MySQL's executed set to hold it
ApplyFor(s, r) == IF BOOT_REQ /\ ~(r.req \subseteq exec[s]) /\ r.req \subseteq Data(s)
                  THEN exec[s] \cup recv[s] ELSE exec[s]
ReqOK(s, r) == ~BOOT_REQ \/ r.req \subseteq ApplyFor(s, r)

\* With SPLIT = FALSE, a decision of the sync loop, or of the fence check, that started completes
\* before anything else happens: every other action is guarded by Free.
Busy == \E s \in Servers : dph[s] # "idle" \/ fcPend[s]
Free == SPLIT \/ ~Busy

----------------------------------------------------------------------------
Init ==
    \E p0 \in Servers :
        /\ up = [s \in Servers |-> TRUE]
        /\ grp = [s \in Servers |-> 1]
        /\ st = [s \in Servers |-> "none"]
        /\ sro = [s \in Servers |-> s # p0]
        /\ electing = [s \in Servers |-> FALSE]
        /\ exec = [s \in Servers |-> {}]
        /\ recv = [s \in Servers |-> {}]
        /\ view = [i \in Incs |-> IF i = 1 THEN Servers ELSE {}]
        /\ prim = [i \in Incs |-> IF i = 1 THEN p0 ELSE NoServer]
        /\ hist = [i \in Incs |-> {}]
        /\ dead = [i \in Incs |-> FALSE]
        /\ nextInc = 2
        /\ async = [s \in Servers |-> NoServer]
        /\ ttype = [s \in Servers |-> IF s = p0 THEN "P" ELSE "R"]
        /\ serving = [s \in Servers |-> s = p0]
        /\ lk = [s \in Servers |-> "free"]
        /\ dph = [s \in Servers |-> "idle"]
        /\ dOK = [s \in Servers |-> FALSE]
        /\ dGen = [s \in Servers |-> FALSE]
        /\ dFence = [s \in Servers |-> FALSE]
        /\ snap = [s \in Servers |-> IF s = p0 THEN "ok" ELSE "np"]
        /\ fenced = [s \in Servers |-> FALSE]
        /\ fcPend = [s \in Servers |-> FALSE]
        /\ majInc = [s \in Servers |-> IF s = p0 /\ FENCE THEN 1 ELSE 0]
        /\ cInc = [s \in Servers |-> 1]
        /\ recentBoot = [s \in Servers |-> 0]
        /\ dmt = [s \in Servers |-> FALSE]
        /\ breq = [s \in Servers |-> {}]
        /\ bOrc = [s \in Servers |-> NoReq]
        /\ bPrev = [s \in Servers |-> 0]
        /\ badServe = [s \in Servers |-> FALSE]
        /\ recInc = 1
        /\ intent = NoIntent
        /\ nextTok = 1
        /\ intExp = FALSE
        /\ newest = p0
        /\ oph = [o \in Orcs |-> "idle"]
        /\ ocand = [o \in Orcs |-> NoServer]
        /\ oexp = [o \in Orcs |-> 0]
        /\ otok = [o \in Orcs |-> 0]
        /\ oborn = [o \in Orcs |-> 0]
        /\ oreq = [o \in Orcs |-> {}]
        /\ orep = [o \in Orcs |-> 0]
        /\ lockOwner = NoOrc
        /\ acked = {}
        /\ nextTx = 1
        /\ bootFrom = [i \in Incs |-> <<0, NoServer>>]
        /\ decisionAck = FALSE
        /\ minorityAck = FALSE
        /\ fenceUndone = FALSE
        /\ adopted = [k \in Toks |-> {}]
        /\ wfail = FALSE
        /\ nCrash = 0 /\ nLeave = 0 /\ nLoss = 0 /\ nExpire = 0

----------------------------------------------------------------------------
(* Clients: through vtgate, or directly to MySQL *)

Commit(p) ==
    /\ Free
    /\ nextTx <= MaxTx /\ CanAccept(p)
    /\ hist' = [hist EXCEPT ![grp[p]] = @ \cup {nextTx}]
    /\ exec' = [exec EXCEPT ![p] = @ \cup {nextTx}]
    /\ acked' = acked \cup {nextTx}
    /\ nextTx' = nextTx + 1
    /\ minorityAck' = (minorityAck \/ ~VoterMaj(grp[p]))
    /\ decisionAck' = (decisionAck \/ (ttype[p] = "P" /\ serving[p] /\ badServe[p] /\ ~LegitMaj(p)))
    /\ UNCHANGED <<up, grp, st, sro, electing, recv, view, prim, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vB, bootFrom, fenceUndone, adopted, wfail>>

----------------------------------------------------------------------------
(* MySQL and Group Replication *)

\* XCom delivers the decided transactions to a member that is still in the view (relay log)
Deliver(s) ==
    /\ Free
    /\ up[s] /\ grp[s] # 0 /\ ~dead[grp[s]]
    /\ ~(hist[grp[s]] \subseteq Data(s))
    /\ recv' = [recv EXCEPT ![s] = @ \cup (hist[grp[s]] \ exec[s])]
    /\ UNCHANGED <<up, grp, st, sro, electing, exec, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vH, vB>>

\* the applier
Apply(s) ==
    /\ Free
    /\ up[s] /\ grp[s] # 0 /\ recv[s] # {}
    /\ exec' = [exec EXCEPT ![s] = @ \cup recv[s]]
    /\ recv' = [recv EXCEPT ![s] = {}]
    /\ UNCHANGED <<up, grp, st, sro, electing, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vH, vB>>

\* s is out of its group (exit state action READ_ONLY)
LeaveVars(s) ==
    /\ view' = [view EXCEPT ![grp[s]] = @ \ {s}]
    /\ prim' = [prim EXCEPT ![grp[s]] = IF @ = s THEN NoServer ELSE @]
    /\ grp' = [grp EXCEPT ![s] = 0]
    /\ sro' = [sro EXCEPT ![s] = TRUE]
    /\ electing' = [electing EXCEPT ![s] = FALSE]

\* a clean leave (shutdown, STOP), or an expulsion that the rest of the view outvotes
Leave(s) ==
    /\ Free
    /\ up[s] /\ grp[s] # 0 /\ ~dead[grp[s]] /\ nLeave < MaxLeave
    /\ LeaveVars(s)
    /\ nLeave' = nLeave + 1
    /\ UNCHANGED <<up, st, exec, recv, hist, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vH, nCrash, nLoss, nExpire>>

\* the members of a group that lost its majority leave it (unreachable_majority_timeout); they leave
\* together: until then they can neither commit, receive, elect nor be joined
LeaveDead(i) ==
    /\ Free
    /\ dead[i] /\ view[i] # {}
    /\ grp' = [s \in Servers |-> IF s \in view[i] THEN 0 ELSE grp[s]]
    /\ sro' = [s \in Servers |-> IF s \in view[i] THEN TRUE ELSE sro[s]]
    /\ electing' = [s \in Servers |-> IF s \in view[i] THEN FALSE ELSE electing[s]]
    /\ view' = [view EXCEPT ![i] = {}]
    /\ prim' = [prim EXCEPT ![i] = NoServer]
    /\ UNCHANGED <<up, st, exec, recv, hist, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vH, vB>>

\* a partition leaves no majority: the group blocks, and its members leave it later
LoseMajority(i) ==
    /\ Free
    /\ Alive(i) /\ Cardinality(view[i]) >= 2 /\ nLoss < MaxLoss
    /\ dead' = [dead EXCEPT ![i] = TRUE]
    /\ nLoss' = nLoss + 1
    /\ UNCHANGED <<up, grp, st, sro, electing, exec, recv, view, prim, hist, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vH, nCrash, nLeave, nExpire>>

Elect(i) ==
    /\ Free
    /\ Alive(i) /\ prim[i] = NoServer
    /\ \E q \in view[i] :
        /\ prim' = [prim EXCEPT ![i] = q]
        /\ electing' = [electing EXCEPT ![q] = TRUE]
    /\ UNCHANGED <<up, grp, st, sro, exec, recv, view, hist, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vH, vB>>

\* BEFORE_ON_PRIMARY_FAILOVER: the new primary applies everything decided, then the election ends
\* and Group Replication sets super_read_only, or clears it if the member action is enabled
ElectEnd(q) ==
    /\ Free
    /\ up[q] /\ electing[q] /\ grp[q] # 0 /\ ~dead[grp[q]]
    /\ exec' = [exec EXCEPT ![q] = @ \cup recv[q] \cup hist[grp[q]]]
    /\ recv' = [recv EXCEPT ![q] = {}]
    /\ electing' = [electing EXCEPT ![q] = FALSE]
    /\ sro' = [sro EXCEPT ![q] = MA_DISABLED]
    /\ UNCHANGED <<up, grp, st, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vH, vB>>

\* the host dies; relay_log_recovery drops the received backlog; vttablet restarts as REPLICA
CrashVars(s) ==
    /\ up[s] /\ nCrash < MaxCrash
    /\ up' = [up EXCEPT ![s] = FALSE]
    /\ IF grp[s] = 0 THEN UNCHANGED <<view, prim, grp, sro, electing, dead>>
       ELSE LET i == grp[s] IN
            /\ LeaveVars(s)
            \* the others outvote the dead member only with a majority of the old view
            /\ dead' = [dead EXCEPT ![i] = @ \/ ~(2 * (Cardinality(view[i]) - 1) > Cardinality(view[i]))
                                               \/ Cardinality(view[i]) = 1]
    /\ st' = [st EXCEPT ![s] = "none"]
    /\ recv' = [recv EXCEPT ![s] = {}]
    /\ async' = [async EXCEPT ![s] = NoServer]
    /\ ttype' = [ttype EXCEPT ![s] = "R"]
    /\ serving' = [serving EXCEPT ![s] = FALSE]
    /\ lk' = [lk EXCEPT ![s] = "free"]
    /\ dph' = [dph EXCEPT ![s] = "idle"]
    /\ dOK' = [dOK EXCEPT ![s] = FALSE]
    /\ dGen' = [dGen EXCEPT ![s] = FALSE]
    /\ dFence' = [dFence EXCEPT ![s] = FALSE]
    /\ snap' = [snap EXCEPT ![s] = "np"]
    /\ fenced' = [fenced EXCEPT ![s] = FALSE]
    /\ fcPend' = [fcPend EXCEPT ![s] = FALSE]
    /\ majInc' = [majInc EXCEPT ![s] = 0]
    /\ recentBoot' = [recentBoot EXCEPT ![s] = 0]
    /\ dmt' = [dmt EXCEPT ![s] = FALSE]
    /\ breq' = [breq EXCEPT ![s] = {}]
    /\ bOrc' = [bOrc EXCEPT ![s] = NoReq]
    /\ badServe' = [badServe EXCEPT ![s] = FALSE]
    /\ nCrash' = nCrash + 1
    /\ UNCHANGED <<exec, nextInc, cInc, bPrev>>
    /\ UNCHANGED <<vS, vO, acked, bootFrom, decisionAck, minorityAck, fenceUndone, adopted, wfail,
                   nLeave, nLoss, nExpire>>

Crash(s) ==
    /\ Free
    /\ CrashVars(s)
    /\ UNCHANGED <<hist, nextTx>>

\* DECIDE_CRASH: the group decides a transaction, and its primary crashes before it commits it: it is
\* never acknowledged, and the members that stay in the view receive it (Deliver), or apply it when
\* they elect a new primary. If the group then loses its majority, it can be left in a relay log only.
DecideCrash(p) ==
    /\ Free
    /\ DECIDE_CRASH
    /\ nextTx <= MaxTx /\ CanAccept(p)
    /\ hist' = [hist EXCEPT ![grp[p]] = @ \cup {nextTx}]
    /\ nextTx' = nextTx + 1
    /\ CrashVars(p)

Restart(s) ==
    /\ Free
    /\ ~up[s]
    /\ up' = [up EXCEPT ![s] = TRUE]
    /\ sro' = [sro EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<grp, st, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vH, vB>>

\* a join completes: distributed recovery from the primary; GR refuses a member with extra transactions
JoinComplete(s) ==
    /\ Free
    /\ up[s] /\ st[s] = "join"
    /\ \E i \in Incs :
        /\ Alive(i) /\ prim[i] # NoServer /\ exec[s] \subseteq hist[i]
        /\ grp' = [grp EXCEPT ![s] = i]
        /\ view' = [view EXCEPT ![i] = @ \cup {s}]
        /\ exec' = [exec EXCEPT ![s] = @ \cup exec[prim[i]]]
    /\ st' = [st EXCEPT ![s] = "none"]
    /\ sro' = [sro EXCEPT ![s] = TRUE]
    /\ lk' = [lk EXCEPT ![s] = IF @ = "join" THEN "free" ELSE @]
    /\ UNCHANGED <<up, electing, recv, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, serving, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc, cInc,
                   recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

\* NEW-1's MySQL mechanism: a join that finds no live group ends alone in a new incarnation
JoinStray(s) ==
    /\ Free
    /\ up[s] /\ st[s] = "join" /\ nextInc <= MaxInc
    /\ \A i \in Incs : ~Alive(i)
    /\ grp' = [grp EXCEPT ![s] = nextInc]
    /\ view' = [view EXCEPT ![nextInc] = {s}]
    /\ prim' = [prim EXCEPT ![nextInc] = s]
    /\ hist' = [hist EXCEPT ![nextInc] = exec[s]]
    /\ electing' = [electing EXCEPT ![s] = TRUE]
    /\ st' = [st EXCEPT ![s] = "none"]
    /\ nextInc' = nextInc + 1
    /\ lk' = [lk EXCEPT ![s] = IF @ = "join" THEN "free" ELSE @]
    /\ UNCHANGED <<up, sro, exec, recv, dead, async>>
    /\ UNCHANGED <<ttype, serving, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc, cInc,
                   recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

\* a join ends without a group (join timeout, refused)
JoinFail(s) ==
    /\ Free
    /\ st[s] = "join"
    /\ st' = [st EXCEPT ![s] = "none"]
    /\ lk' = [lk EXCEPT ![s] = IF @ = "join" THEN "free" ELSE @]
    /\ UNCHANGED <<up, grp, sro, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, serving, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc, cInc,
                   recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

\* a bootstrap START forms a group of one; MySQL applies the received backlog first (L4)
BootComplete(s) ==
    /\ Free
    /\ up[s] /\ st[s] = "boot" /\ nextInc <= MaxInc
    /\ exec' = [exec EXCEPT ![s] = @ \cup recv[s]]
    /\ recv' = [recv EXCEPT ![s] = {}]
    /\ grp' = [grp EXCEPT ![s] = nextInc]
    /\ view' = [view EXCEPT ![nextInc] = {s}]
    /\ prim' = [prim EXCEPT ![nextInc] = s]
    /\ hist' = [hist EXCEPT ![nextInc] = exec[s] \cup recv[s]]
    /\ electing' = [electing EXCEPT ![s] = TRUE]
    /\ st' = [st EXCEPT ![s] = "none"]
    /\ nextInc' = nextInc + 1
    /\ bootFrom' = [bootFrom EXCEPT ![nextInc] = <<bPrev[s], s>>]
    /\ bPrev' = [bPrev EXCEPT ![s] = 0]
    \* the RPC's handler, if it still runs: noteBootstrap, the reply (lost if VTOrc gave up), unlock
    /\ IF lk[s] = "boot"
       THEN /\ recentBoot' = [recentBoot EXCEPT ![s] = nextInc]
            /\ Reply(bOrc[s], s, nextInc)
            /\ lk' = [lk EXCEPT ![s] = "free"]
            /\ bOrc' = [bOrc EXCEPT ![s] = NoReq]
       ELSE UNCHANGED <<recentBoot, orep, lk, bOrc>>
    /\ UNCHANGED <<up, sro, dead, async>>
    /\ UNCHANGED <<ttype, serving, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc, cInc,
                   dmt, breq, badServe>>
    /\ UNCHANGED <<vS, oph, ocand, oexp, otok, oborn, oreq, lockOwner, vB, acked, nextTx, decisionAck,
                   minorityAck, fenceUndone, adopted, wfail>>

\* asynchronous replication on the default channel (only after StaleTopoPrimary without NEW3_FIX)
AsyncApply(s) ==
    /\ Free
    /\ async[s] # NoServer /\ up[s] /\ grp[s] = 0 /\ st[s] = "none" /\ up[async[s]]
    /\ ~(exec[async[s]] \subseteq exec[s])
    /\ exec' = [exec EXCEPT ![s] = @ \cup exec[async[s]]]
    /\ UNCHANGED <<up, grp, st, sro, electing, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vH, vB>>

----------------------------------------------------------------------------
(* vttablet: shard record, timers *)

RefreshRec(s) ==
    /\ Free
    /\ STALE_REC /\ up[s] /\ cInc[s] # recInc
    /\ Refresh(s)
    /\ UNCHANGED <<vM, ttype, serving, lk, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc,
                   recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

\* groupReplicationBootstrapGrace (1 minute) ends
BootGraceExpire(s) ==
    /\ Free
    /\ ~STUCK_CHECK /\ recentBoot[s] # 0
    /\ recentBoot' = [recentBoot EXCEPT ![s] = 0]
    /\ UNCHANGED <<vM, ttype, serving, lk, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

----------------------------------------------------------------------------
(* vttablet: the sync loop *)

\* a run of the loop reads MySQL's status, then may wait on the topology for a long time
SyncRead(s) ==
    /\ Free
    /\ up[s] /\ ttype[s] = "P" /\ snap[s] # SnapVal(s)
    /\ snap' = [snap EXCEPT ![s] = SnapVal(s)]
    /\ UNCHANGED <<vM, ttype, serving, lk, dph, dOK, dGen, dFence, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

\* enforceVoterMajority stops serving on the status read at the start of the run, without the lock
SyncStop(s) ==
    /\ Free
    /\ up[s] /\ ttype[s] = "P" /\ serving[s] /\ snap[s] = "bad"
    /\ serving' = [serving EXCEPT ![s] = FALSE]
    /\ badServe' = [badServe EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<vM, ttype, lk, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

\* pre-f782228: the loop serves again on the status it read at the start of its run, without the lock
SyncServeStale(s) ==
    /\ Free
    /\ ~SERVE_LOCKED
    /\ up[s] /\ ttype[s] = "P" /\ ~serving[s] /\ snap[s] = "ok"
    /\ serving' = [serving EXCEPT ![s] = TRUE]
    /\ badServe' = [badServe EXCEPT ![s] = ~LegitMaj(s)]
    /\ UNCHANGED <<vM, ttype, lk, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

\* demote (lost primary role) and demoteStalePrimary (MySQL out of its group), under the lock
SyncDemote(s) ==
    /\ Free
    /\ up[s] /\ lk[s] = "free" /\ ttype[s] = "P" /\ ~IsPrimQ(s)
    /\ ttype' = [ttype EXCEPT ![s] = "R"]
    /\ serving' = [serving EXCEPT ![s] = FALSE]
    /\ badServe' = [badServe EXCEPT ![s] = FALSE]
    /\ majInc' = [majInc EXCEPT ![s] = 0]
    /\ snap' = [snap EXCEPT ![s] = "np"]
    /\ dmt' = [dmt EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<vM, lk, dph, dOK, dGen, dFence, fenced, fcPend,
                   cInc, recentBoot, breq, bOrc, bPrev>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

\* isForeignGroup + leaveForeignGroupLocked: fence first, demote, STOP GROUP_REPLICATION
LeaveForeign(s) ==
    /\ Free
    /\ LEGIT
    /\ up[s] /\ lk[s] = "free" /\ grp[s] # 0
    /\ grp[s] # Rec(s) /\ grp[s] # recInc /\ grp[s] # recentBoot[s]
    /\ ~(IsPrimQ(s) /\ AdoptableByIntent(s, grp[s], intent, recInc))
    /\ LeaveVars(s)
    /\ ttype' = [ttype EXCEPT ![s] = "R"]
    /\ serving' = [serving EXCEPT ![s] = FALSE]
    /\ badServe' = [badServe EXCEPT ![s] = FALSE]
    /\ majInc' = [majInc EXCEPT ![s] = 0]
    /\ snap' = [snap EXCEPT ![s] = "np"]
    /\ fenced' = [fenced EXCEPT ![s] = FALSE]
    /\ fcPend' = [fcPend EXCEPT ![s] = FALSE]
    /\ Refresh(s)
    /\ UNCHANGED <<up, st, exec, recv, hist, dead, nextInc, async>>
    /\ UNCHANGED <<lk, dph, dOK, dGen, dFence, recentBoot, dmt, breq, bOrc, bPrev>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

\* promote + changeTypeLocked, step 1: the lock, a fresh shard record, the fence snapshot
PrSnap(s) ==
    /\ Free
    /\ up[s] /\ lk[s] = "free" /\ ttype[s] = "R" /\ PromoteLegit(s)
    /\ lk' = [lk EXCEPT ![s] = "sync"]
    /\ dph' = [dph EXCEPT ![s] = "pr_snap"]
    /\ dGen' = [dGen EXCEPT ![s] = FALSE]
    /\ dFence' = [dFence EXCEPT ![s] = FALSE]
    /\ Refresh(s)
    /\ UNCHANGED <<vM, ttype, serving, dOK, snap, fenced, fcPend, majInc,
                   recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

\* step 2: MySQL's status, read under the lock
PrRead(s) ==
    /\ dph[s] = "pr_snap"
    /\ dOK' = [dOK EXCEPT ![s] = ServeOK(s, Rec(s))]
    /\ dph' = [dph EXCEPT ![s] = "pr_read"]
    /\ UNCHANGED <<vM, ttype, serving, lk, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

\* step 3: the tablet becomes PRIMARY; it makes MySQL writable and serves only on that status,
\* and only if no fence was decided since the snapshot
PrAct(s) ==
    /\ dph[s] = "pr_read"
    /\ LET ok == dOK[s] /\ ~dGen[s] /\ ~(FENCE_SNAPSHOT /\ dFence[s]) IN
       /\ sro' = [sro EXCEPT ![s] = IF ok THEN FALSE ELSE @]
       /\ serving' = [serving EXCEPT ![s] = ok]
       /\ fenced' = [fenced EXCEPT ![s] = IF ok THEN FALSE ELSE @]
       /\ fenceUndone' = (fenceUndone \/ (ok /\ dFence[s]))
    /\ ttype' = [ttype EXCEPT ![s] = "P"]
    /\ newest' = s
    /\ dmt' = [dmt EXCEPT ![s] = FALSE]
    /\ snap' = [snap EXCEPT ![s] = "np"]
    /\ badServe' = [badServe EXCEPT ![s] = FALSE]
    /\ lk' = [lk EXCEPT ![s] = "free"]
    /\ dph' = [dph EXCEPT ![s] = "idle"]
    /\ dOK' = [dOK EXCEPT ![s] = FALSE]
    /\ dGen' = [dGen EXCEPT ![s] = FALSE]
    /\ dFence' = [dFence EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<up, grp, st, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<fcPend, majInc, cInc, recentBoot, breq, bOrc, bPrev>>
    /\ UNCHANGED <<recInc, intent, nextTok, intExp>>
    /\ UNCHANGED <<vO, vB, acked, nextTx, bootFrom, decisionAck, minorityAck, adopted, wfail>>

\* serveAgain, step 1: TryAcquire, the generation, the fence snapshot
SaSnap(s) ==
    /\ Free
    /\ SERVE_LOCKED
    /\ up[s] /\ lk[s] = "free" /\ ttype[s] = "P" /\ snap[s] = "ok" /\ ~dmt[s]
    /\ (~serving[s] \/ fenced[s] \/ sro[s])
    /\ lk' = [lk EXCEPT ![s] = "sync"]
    /\ dph' = [dph EXCEPT ![s] = "sa_snap"]
    /\ dGen' = [dGen EXCEPT ![s] = FALSE]
    /\ dFence' = [dFence EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<vM, ttype, serving, dOK, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

\* step 2: MySQL's status under the lock, against the shard record read before the lock
SaRead(s) ==
    /\ dph[s] = "sa_snap"
    /\ dOK' = [dOK EXCEPT ![s] = ServeOK(s, Rec(s))]
    /\ dph' = [dph EXCEPT ![s] = "sa_read"]
    /\ UNCHANGED <<vM, ttype, serving, lk, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

\* step 3: a reason stops serving; otherwise liftGroupReplicationFenceLocked, then
\* ClearGroupReplicationNotServing(gen)
SaAct(s) ==
    /\ dph[s] = "sa_read"
    /\ IF ~dOK[s]
       THEN /\ serving' = [serving EXCEPT ![s] = FALSE]
            /\ badServe' = [badServe EXCEPT ![s] = FALSE]
            /\ UNCHANGED <<sro, fenced, fenceUndone>>
       ELSE IF FENCE_SNAPSHOT /\ dFence[s]
       THEN UNCHANGED <<sro, fenced, fenceUndone, serving, badServe>>
       ELSE /\ sro' = [sro EXCEPT ![s] = FALSE]
            /\ fenced' = [fenced EXCEPT ![s] = FALSE]
            /\ fenceUndone' = (fenceUndone \/ dFence[s])
            /\ serving' = [serving EXCEPT ![s] = IF dGen[s] THEN @ ELSE TRUE]
            /\ badServe' = [badServe EXCEPT ![s] = IF dGen[s] THEN @ ELSE FALSE]
    /\ lk' = [lk EXCEPT ![s] = "free"]
    /\ dph' = [dph EXCEPT ![s] = "idle"]
    /\ dOK' = [dOK EXCEPT ![s] = FALSE]
    /\ dGen' = [dGen EXCEPT ![s] = FALSE]
    /\ dFence' = [dFence EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<up, grp, st, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, snap, fcPend, majInc, cInc, recentBoot,
                   dmt, breq, bOrc, bPrev>>
    /\ UNCHANGED <<vS, vO, vB, acked, nextTx, bootFrom, decisionAck, minorityAck, adopted, wfail>>

\* a join (the sync loop's rejoin, the startup join, VTOrc's GroupMemberNotOnline): a new epoch, the
\* fence reset, MySQL's START; the applier applies the relay backlog; the default channel is stopped
JoinStart(s) ==
    /\ Free
    /\ up[s] /\ lk[s] = "free" /\ st[s] = "none" /\ grp[s] = 0 /\ ttype[s] = "R"
    /\ JOIN_GATE => LegitActiveElsewhere(s)
    /\ lk' = [lk EXCEPT ![s] = "join"]
    /\ st' = [st EXCEPT ![s] = "join"]
    /\ exec' = [exec EXCEPT ![s] = @ \cup recv[s]]
    /\ recv' = [recv EXCEPT ![s] = {}]
    /\ async' = [async EXCEPT ![s] = NoServer]
    /\ fenced' = [fenced EXCEPT ![s] = FALSE]
    /\ fcPend' = [fcPend EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<up, grp, sro, electing, view, prim, hist, dead, nextInc>>
    /\ UNCHANGED <<ttype, serving, dph, dOK, dGen, dFence, snap, majInc, cInc,
                   recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

\* the joiner gives up while MySQL's START still runs (MySQL keeps running it); a START that ends
\* releases the lock with its end (JoinComplete, JoinStray, JoinFail)
JoinRelease(s) ==
    /\ Free
    /\ lk[s] = "join" /\ st[s] = "join"
    /\ lk' = [lk EXCEPT ![s] = "free"]
    /\ UNCHANGED <<vM, ttype, serving, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

----------------------------------------------------------------------------
(* vttablet: the fence check (no action lock; ordered with decisions by epochs and decisions) *)

\* groupReplicationJoinWatchWindow (30s after a start) is left out: fewer fences only add behaviours
Armed(s) == ttype[s] = "P" \/ fenced[s] \/ lk[s] \in {"join", "bootw", "boot"}

\* groupReplicationFenceReason, on the shard record the tablet read last
FenceReason(s) ==
    LET i == grp[s]
        recorded == ~LEGIT \/ i = Rec(s)
        maj == VoterMaj(i)
        notServable == ~maj \/ ~recorded
    IN  IF ~IsPrimQ(s) \/ lk[s] \in {"bootw", "boot"} THEN FALSE
        ELSE IF lk[s] = "join" THEN ~maj       \* no view id during the tablet's own START
        ELSE IF (~recorded /\ i = recentBoot[s]) \/ AdoptableByIntent(s, i, intent, Rec(s)) THEN FALSE
        ELSE IF maj /\ recorded THEN FALSE
        ELSE (~recorded /\ ~maj)
             \/ (ttype[s] = "P" /\ majInc[s] = i /\ notServable)
             \/ (fenced[s] /\ notServable)

NewMajInc(s) ==
    LET i == grp[s] IN
    IF ttype[s] = "P" /\ IsPrimQ(s) /\ lk[s] \notin {"join", "bootw", "boot"} /\ VoterMaj(i) /\ (~LEGIT \/ i = Rec(s))
    THEN i ELSE majInc[s]

\* the check reads MySQL; re-deciding a fence that holds is left out (it only refuses more decisions)
FcRead(s) ==
    /\ Free
    /\ FENCE /\ up[s] /\ Armed(s) /\ ~fcPend[s]
    /\ LET r == FenceReason(s) /\ (~fenced[s] \/ ~sro[s] \/ (ttype[s] = "P" /\ serving[s]))
           m == NewMajInc(s)
       IN /\ (r \/ m # majInc[s])
          /\ fcPend' = [fcPend EXCEPT ![s] = r]
          /\ majInc' = [majInc EXCEPT ![s] = m]
    /\ UNCHANGED <<vM, ttype, serving, lk, dph, dOK, dGen, dFence, snap, fenced,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

\* decide (under mu, in the same epoch, no bootstrap in progress): super_read_only, then the
\* not-serving reason of a PRIMARY tablet. A new epoch dropped fcPend already.
FcAct(s) ==
    /\ fcPend[s]
    /\ fcPend' = [fcPend EXCEPT ![s] = FALSE]
    /\ IF lk[s] \in {"bootw", "boot"}
       THEN UNCHANGED <<sro, fenced, dFence, dGen, serving, badServe>>
       ELSE /\ sro' = [sro EXCEPT ![s] = TRUE]
            /\ fenced' = [fenced EXCEPT ![s] = TRUE]
            /\ dFence' = [dFence EXCEPT ![s] = (dph[s] # "idle") \/ @]
            /\ IF ttype[s] = "P"
               THEN /\ serving' = [serving EXCEPT ![s] = FALSE]
                    /\ badServe' = [badServe EXCEPT ![s] = FALSE]
                    /\ dGen' = [dGen EXCEPT ![s] = (dph[s] # "idle") \/ @]
               ELSE UNCHANGED <<serving, badServe, dGen>>
    /\ UNCHANGED <<up, grp, st, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, lk, dph, dOK, snap, majInc, cInc, recentBoot, dmt, breq, bOrc, bPrev>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

----------------------------------------------------------------------------
(* vttablet: StartGroupReplication(bootstrap) RPC, from VTOrc *)

\* the RPC gets the action lock: stopServingBeforeBootstrap, a new epoch; an active member refuses, and
\* so does a superseded intent (BOOT_TOKEN); a START in progress is waited for; else the tablet applies
\* its relay log and checks what the RPC requires (BOOT_REQ), resets the fence, and MySQL's START
\* (bootstrap) runs. Only the refusal for a missing transaction, decided while no START runs, is
\* definitive (-2): this RPC did not start MySQL, and never will.
HBoot1(s) ==
    /\ Free
    /\ up[s] /\ lk[s] = "free"
    /\ \E r \in breq[s], checked \in TokenChecks :
        \* a superseded intent is refused before anything changes
        LET stale == checked /\ ~TokenOK(r) IN
        /\ breq' = [breq EXCEPT ![s] = @ \ {r}]
        /\ serving' = [serving EXCEPT ![s] = IF ttype[s] = "P" /\ ~stale THEN FALSE ELSE @]
        /\ badServe' = [badServe EXCEPT ![s] = IF ttype[s] = "P" /\ ~stale THEN FALSE ELSE @]
        /\ fcPend' = [fcPend EXCEPT ![s] = IF stale THEN @ ELSE FALSE]
        /\ IF stale \/ grp[s] # 0 \/ (st[s] = "none" /\ ~ReqOK(s, r))
           THEN /\ Reply(r, s, IF stale \/ grp[s] # 0 THEN -1 ELSE -2)
                /\ UNCHANGED <<lk, st, fenced, bOrc, bPrev, exec, recv>>
           ELSE /\ bOrc' = [bOrc EXCEPT ![s] = r]
                /\ UNCHANGED orep
                /\ IF st[s] # "none"
                   \* a bootstrap START in progress keeps the recorded incarnation it started from
                   THEN /\ lk' = [lk EXCEPT ![s] = "bootw"]
                        /\ UNCHANGED <<st, fenced, exec, recv, bPrev>>
                   ELSE /\ lk' = [lk EXCEPT ![s] = "boot"]
                        /\ bPrev' = [bPrev EXCEPT ![s] = recInc]
                        /\ st' = [st EXCEPT ![s] = "boot"]
                        /\ fenced' = [fenced EXCEPT ![s] = FALSE]
                        /\ exec' = [exec EXCEPT ![s] = ApplyFor(s, r)]
                        /\ recv' = [recv EXCEPT ![s] = IF ApplyFor(s, r) = exec[s] THEN @ ELSE {}]
    /\ UNCHANGED <<up, grp, sro, electing, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, dph, dOK, dGen, dFence, snap, majInc, cInc, recentBoot, dmt>>
    /\ UNCHANGED <<vS, oph, ocand, oexp, otok, oborn, oreq, lockOwner, vH, vB>>

\* stopOngoingGroupStartLocked: MySQL accepts the STOP once its START ended; a superseded intent is
\* refused before the STOP (BOOT_TOKEN), so that it never makes MySQL leave a group that a newer
\* bootstrap formed; MySQL leaves whatever the START joined or formed; then the checks of HBoot1 and the
\* bootstrap. A missing transaction is a definitive refusal: the START ended, and MySQL left its group.
HBootGo(s) ==
    /\ Free
    /\ up[s] /\ lk[s] = "bootw" /\ st[s] = "none"
    /\ \E checked \in TokenChecks :
        IF checked /\ ~TokenOK(bOrc[s])
        THEN /\ Reply(bOrc[s], s, -1)
             /\ lk' = [lk EXCEPT ![s] = "free"]
             /\ bOrc' = [bOrc EXCEPT ![s] = NoReq]
             /\ bPrev' = [bPrev EXCEPT ![s] = 0]
             /\ UNCHANGED <<view, prim, grp, sro, electing, st, fenced, fcPend, exec, recv>>
        ELSE /\ IF grp[s] # 0 THEN LeaveVars(s) ELSE UNCHANGED <<view, prim, grp, sro, electing>>
             /\ fcPend' = [fcPend EXCEPT ![s] = FALSE]
             /\ IF ReqOK(s, bOrc[s])
                THEN /\ lk' = [lk EXCEPT ![s] = "boot"]
                     /\ st' = [st EXCEPT ![s] = "boot"]
                     /\ fenced' = [fenced EXCEPT ![s] = FALSE]
                     /\ exec' = [exec EXCEPT ![s] = ApplyFor(s, bOrc[s])]
                     /\ recv' = [recv EXCEPT ![s] = IF ApplyFor(s, bOrc[s]) = exec[s] THEN @ ELSE {}]
                     /\ bPrev' = [bPrev EXCEPT ![s] = recInc]
                     /\ UNCHANGED <<orep, bOrc>>
                ELSE /\ Reply(bOrc[s], s, -2)
                     /\ lk' = [lk EXCEPT ![s] = "free"]
                     /\ bOrc' = [bOrc EXCEPT ![s] = NoReq]
                     /\ bPrev' = [bPrev EXCEPT ![s] = 0]
                     /\ UNCHANGED <<st, fenced, exec, recv>>
    /\ UNCHANGED <<up, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, serving, dph, dOK, dGen, dFence, snap, majInc, cInc, recentBoot,
                   dmt, breq, badServe>>
    /\ UNCHANGED <<vS, oph, ocand, oexp, otok, oborn, oreq, lockOwner, vH, vB>>

\* the START in progress did not end within groupReplicationStopOngoingStartTimeout: UNAVAILABLE, never
\* definitive (DEFINITIVE_WHILE_STARTING: the unsafe variant that reports it as definitive)
HBootGiveUp(s) ==
    /\ Free
    /\ lk[s] = "bootw"
    /\ Reply(bOrc[s], s, IF DEFINITIVE_WHILE_STARTING THEN -2 ELSE -1)
    /\ lk' = [lk EXCEPT ![s] = "free"]
    /\ bOrc' = [bOrc EXCEPT ![s] = NoReq]
    /\ bPrev' = [bPrev EXCEPT ![s] = IF st[s] = "boot" THEN @ ELSE 0]
    /\ UNCHANGED <<vM, ttype, serving, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, badServe>>
    /\ UNCHANGED <<vS, oph, ocand, oexp, otok, oborn, oreq, lockOwner, vH, vB>>

\* VTOrc gave up on the RPC: its context ends the handler; MySQL keeps running a START it issued
HBootAbort(s) ==
    /\ Free
    /\ lk[s] \in {"bootw", "boot"}
    /\ ~Waiting(bOrc[s], s)
    /\ lk' = [lk EXCEPT ![s] = "free"]
    /\ bOrc' = [bOrc EXCEPT ![s] = NoReq]
    /\ bPrev' = [bPrev EXCEPT ![s] = IF st[s] = "boot" THEN @ ELSE 0]
    /\ UNCHANGED <<vM, ttype, serving, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

----------------------------------------------------------------------------
(* VTOrc *)

BootReach == IF BOOT_ALL THEN Servers ELSE {v \in Servers : up[v]}

\* GroupNotBootstrapped: the shard lock, then a fresh status of every voter, and the candidate
OBegin(o) ==
    /\ Free
    /\ oph[o] = "idle" /\ lockOwner = NoOrc
    /\ IF BOOT_ALL THEN \A v \in Servers : up[v] ELSE Cardinality(BootReach) >= Maj
    /\ \A v \in BootReach : grp[v] = 0
    /\ LET All   == UNION {Data(v) : v \in BootReach}
           Has(c) == IF CAND_EXECUTED THEN exec[c] ELSE Data(c)
           Sup   == {c \in BootReach : All \subseteq Has(c)}
           P1    == IF INTENT /\ INTENT_PREFER /\ IntentLive(intent) /\ intent.tgt \in Sup
                    THEN {intent.tgt} ELSE Sup
           P1b   == IF CAND_BINLOG /\ \E c \in P1 : All \subseteq exec[c]
                    THEN {c \in P1 : All \subseteq exec[c]} ELSE P1
           Req   == IF BOOT_REQ THEN All ELSE {}
           P2    == IF \E c \in P1b : st[c] = "none" THEN {c \in P1b : st[c] = "none"} ELSE P1b
           P3    == IF \E c \in P2 : ttype[c] = "P" THEN {c \in P2 : ttype[c] = "P"} ELSE P2
       IN \E c \in P3 :
            \* STUCK_CHECK: a recovery whose intent the fence refuses changes nothing, and its loop
            \* would hide a stuck state from the deadlock check
            /\ STUCK_CHECK => ~(INTENT /\ INTENT_FENCE /\ IntentLive(intent) /\ intent.tgt # c)
            /\ lockOwner' = o
            /\ ocand' = [ocand EXCEPT ![o] = c]
            /\ oexp' = [oexp EXCEPT ![o] = recInc]
            /\ orep' = [orep EXCEPT ![o] = 0]
            /\ IF INTENT
               THEN /\ oph' = [oph EXCEPT ![o] = "chosen"]
                    /\ oreq' = [oreq EXCEPT ![o] = Req]
                    /\ UNCHANGED <<breq, otok, oborn>>
               ELSE \* before the intent: the bootstrap RPC right away
                    /\ oph' = [oph EXCEPT ![o] = "wait"]
                    /\ breq' = [breq EXCEPT ![c] = @ \cup {[o |-> o, tok |-> 0, exp |-> recInc, req |-> Req]}]
                    /\ UNCHANGED oreq
                    /\ otok' = [otok EXCEPT ![o] = 0]
                    /\ oborn' = [oborn EXCEPT ![o] = nextInc]
    /\ UNCHANGED <<vM, ttype, serving, lk, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vH, vB>>

\* WriteGroupReplicationBootstrapIntent (compare-and-swap, the fence), then the bootstrap RPC
OIntent(o) ==
    /\ Free
    /\ INTENT /\ oph[o] = "chosen"
    /\ IF recInc = oexp[o] /\ ~(INTENT_FENCE /\ IntentLive(intent) /\ intent.tgt # ocand[o])
       THEN /\ nextTok <= MaxTok
            /\ intent' = [tgt |-> ocand[o], prev |-> oexp[o], tok |-> nextTok, born |-> nextInc]
            /\ otok' = [otok EXCEPT ![o] = nextTok]
            /\ oborn' = [oborn EXCEPT ![o] = nextInc]
            /\ nextTok' = nextTok + 1
            /\ intExp' = FALSE
            /\ oph' = [oph EXCEPT ![o] = "wait"]
            /\ breq' = [breq EXCEPT ![ocand[o]] = @ \cup {[o |-> o, tok |-> nextTok, exp |-> oexp[o], req |-> oreq[o]]}]
            /\ UNCHANGED <<lockOwner, ocand, oexp>>
       ELSE /\ oph' = [oph EXCEPT ![o] = "idle"]
            /\ lockOwner' = Unlock(o)
            /\ ocand' = [ocand EXCEPT ![o] = NoServer]
            /\ oexp' = [oexp EXCEPT ![o] = 0]
            /\ UNCHANGED <<intent, otok, oborn, nextTok, breq, intExp>>
    \* the required set travels with the RPC
    /\ oreq' = [oreq EXCEPT ![o] = {}]
    /\ UNCHANGED <<vM, ttype, serving, lk, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, bOrc, bPrev, badServe>>
    /\ wfail' = FALSE
    /\ UNCHANGED <<recInc, newest, orep, acked, nextTx, bootFrom, decisionAck, minorityAck, fenceUndone,
                   adopted, vB>>

\* writeGroupReplicationIncarnation: compare-and-swap against the expected incarnation and the token
RecordOK(o, inc) ==
    \/ recInc = inc
    \/ ~INC_CAS
    \/ recInc = oexp[o] /\ (otok[o] # 0 => intent.tok = otok[o])
\* a write that finds inc recorded already clears only an intent for an earlier incarnation
KeepsIntent(inc) == KEEP_NEWER_INTENT /\ recInc = inc /\ intent.tok # 0 /\ intent.prev = inc
Record(o, inc) ==
    /\ recInc' = IF RecordOK(o, inc) THEN inc ELSE recInc
    /\ intent' = IF RecordOK(o, inc) /\ ~KeepsIntent(inc) THEN NoIntent ELSE intent
    /\ intExp' = IF RecordOK(o, inc) /\ ~KeepsIntent(inc) THEN FALSE ELSE intExp
    \* a write that finds the incarnation recorded already changes nothing, and is not counted
    /\ adopted' = IF RecordOK(o, inc) /\ otok[o] # 0 /\ recInc # inc
                  THEN [adopted EXCEPT ![otok[o]] = @ \cup {inc}] ELSE adopted

\* the recovery ends: the shard lock is released
OEnd(o) ==
    /\ oph' = [oph EXCEPT ![o] = "idle"]
    /\ lockOwner' = Unlock(o)
    /\ ocand' = [ocand EXCEPT ![o] = NoServer] /\ oexp' = [oexp EXCEPT ![o] = 0]
    /\ otok' = [otok EXCEPT ![o] = 0] /\ oborn' = [oborn EXCEPT ![o] = 0] /\ orep' = [orep EXCEPT ![o] = 0]

\* WithdrawGroupReplicationBootstrapIntent: removes the recovery's own intent, a compare-and-swap on its
\* token and on the incarnation it was recorded for; a newer intent, or an incarnation recorded since,
\* is left as it is
Withdraw(o) ==
    LET mine == otok[o] # 0 /\ intent.tok = otok[o] /\ recInc = intent.prev IN
    /\ intent' = IF mine THEN NoIntent ELSE intent
    /\ intExp' = IF mine THEN FALSE ELSE intExp

\* after a failed bootstrap RPC (reply v < 0, or a timeout: v = 0), VTOrc withdraws its intent
WithdrawsAfter(v) == INTENT /\ ((WITHDRAW_ON_REFUSAL /\ v = -2) \/ WITHDRAW_ANY_FAILURE)

\* the reply of the bootstrap RPC: record the incarnation, or adopt after an error, or withdraw the
\* intent after a definitive refusal
OReply(o) ==
    /\ Free
    /\ oph[o] = "wait" /\ orep[o] # 0
    /\ IF orep[o] > 0
       THEN /\ Record(o, orep[o])
            /\ oph' = [oph EXCEPT ![o] = "idle"]
            /\ lockOwner' = Unlock(o)
            /\ ocand' = [ocand EXCEPT ![o] = NoServer] /\ oexp' = [oexp EXCEPT ![o] = 0]
            /\ otok' = [otok EXCEPT ![o] = 0] /\ oborn' = [oborn EXCEPT ![o] = 0] /\ orep' = [orep EXCEPT ![o] = 0]
       ELSE /\ UNCHANGED <<recInc, adopted>>
            /\ IF WithdrawsAfter(orep[o])
               THEN /\ Withdraw(o)
                    /\ OEnd(o)
               ELSE /\ UNCHANGED <<intent, intExp>>
                    /\ IF ADOPT /\ INTENT
                       THEN /\ oph' = [oph EXCEPT ![o] = "adopt"]
                            /\ UNCHANGED <<lockOwner, ocand, oexp, otok, oborn, orep>>
                       ELSE OEnd(o)
    /\ wfail' = IF STUCK_CHECK /\ orep[o] = -1 /\ otok[o] # 0 /\ otok[o] = intent.tok THEN TRUE ELSE wfail
    /\ UNCHANGED <<vM, vT, nextTok, newest, oreq>>
    /\ UNCHANGED <<acked, nextTx, bootFrom, decisionAck, minorityAck, fenceUndone, vB>>

\* the RPC times out or its reply is lost; a request still waiting for the tablet's lock is cancelled
OTimeout(o) ==
    /\ Free
    /\ oph[o] = "wait"
    /\ breq' = [breq EXCEPT ![ocand[o]] = {r \in @ : ~(r.o = o /\ r.tok = otok[o])}]
    /\ IF WithdrawsAfter(0)
       THEN /\ Withdraw(o)
            /\ OEnd(o)
       ELSE /\ UNCHANGED <<intent, intExp>>
            /\ IF ADOPT /\ INTENT
               THEN /\ oph' = [oph EXCEPT ![o] = "adopt"]
                    /\ UNCHANGED <<lockOwner, ocand, oexp, otok, oborn>>
                    /\ orep' = [orep EXCEPT ![o] = 0]
               ELSE OEnd(o)
    /\ UNCHANGED <<vM, ttype, serving, lk, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, bOrc, bPrev, badServe>>
    /\ wfail' = IF STUCK_CHECK /\ otok[o] # 0 /\ otok[o] = intent.tok THEN TRUE ELSE wfail
    /\ UNCHANGED <<recInc, nextTok, newest, oreq, acked, nextTx, bootFrom, decisionAck, minorityAck,
                   fenceUndone, adopted, vB>>

\* AdoptGroupReplicationBootstrap: the target's status now, then the compare-and-swap
OAdopt(o) ==
    /\ Free
    /\ oph[o] = "adopt"
    /\ LET c == ocand[o] IN
       IF IsPrimQ(c) /\ grp[c] # oexp[o] /\ grp[c] >= oborn[o]
       THEN Record(o, grp[c])
       ELSE UNCHANGED <<recInc, intent, adopted, intExp>>
    /\ oph' = [oph EXCEPT ![o] = "idle"]
    /\ lockOwner' = Unlock(o)
    /\ ocand' = [ocand EXCEPT ![o] = NoServer] /\ oexp' = [oexp EXCEPT ![o] = 0]
    /\ otok' = [otok EXCEPT ![o] = 0] /\ oborn' = [oborn EXCEPT ![o] = 0] /\ orep' = [orep EXCEPT ![o] = 0]
    /\ UNCHANGED <<vM, vT, nextTok, newest, oreq>>
    /\ UNCHANGED <<acked, nextTx, bootFrom, decisionAck, minorityAck, fenceUndone, wfail, vB>>

\* GroupBootstrapNotRecorded: the shard lock, the shard record's intent, its target's group
OAdoptLater(o) ==
    /\ Free
    /\ ADOPT /\ INTENT
    /\ oph[o] = "idle" /\ lockOwner = NoOrc
    /\ intent.tok # 0 /\ intent.prev = recInc
    /\ IsPrimQ(intent.tgt) /\ grp[intent.tgt] # recInc
    /\ lockOwner' = o
    /\ oph' = [oph EXCEPT ![o] = "adopt"]
    /\ ocand' = [ocand EXCEPT ![o] = intent.tgt]
    /\ oexp' = [oexp EXCEPT ![o] = recInc]
    /\ otok' = [otok EXCEPT ![o] = intent.tok]
    /\ oborn' = [oborn EXCEPT ![o] = intent.born]
    /\ UNCHANGED <<vM, vT, vS, oreq, orep, vH, vB>>

\* the holder of the shard lock stalls longer than the lease; it keeps acting when it resumes
OLeaseExpire ==
    /\ Free
    /\ lockOwner # NoOrc /\ nExpire < MaxExpire
    /\ lockOwner' = NoOrc
    /\ nExpire' = nExpire + 1
    /\ UNCHANGED <<vM, vT, vS, oph, ocand, oexp, otok, oborn, oreq, orep, vH, nCrash, nLeave, nLoss>>

\* GroupReplicationBootstrapIntentFence (2 minutes) passes for the oldest live intent
OIntentExpire ==
    /\ Free
    /\ ~STUCK_CHECK
    /\ intent.tok # 0 /\ ~intExp
    /\ INTENT_OUTLASTS_BOOT =>
         /\ \A o \in Orcs : oph[o] = "idle"
         /\ \A s \in Servers : breq[s] = {} /\ lk[s] \notin {"bootw", "boot"} /\ st[s] # "boot"
    /\ intExp' = TRUE
    /\ UNCHANGED <<vM, vT, recInc, intent, nextTok, newest, vO, vH, vB>>

\* StaleTopoPrimary: forceDemotePrimary, the tablet type REPLICA; without NEW3_FIX also
\* setReplicationSource, which configures the default channel on a non-member
StaleTopo(t) ==
    /\ Free
    /\ STALE_TOPO
    /\ up[t] /\ ttype[t] = "P" /\ newest # t /\ lk[t] = "free"
    /\ sro' = [sro EXCEPT ![t] = TRUE]
    /\ serving' = [serving EXCEPT ![t] = FALSE]
    /\ badServe' = [badServe EXCEPT ![t] = FALSE]
    /\ dmt' = [dmt EXCEPT ![t] = TRUE]
    /\ ttype' = [ttype EXCEPT ![t] = "R"]
    /\ majInc' = [majInc EXCEPT ![t] = 0]
    /\ snap' = [snap EXCEPT ![t] = "np"]
    /\ async' = [async EXCEPT ![t] = IF ~NEW3_FIX /\ grp[t] = 0 /\ st[t] = "none" THEN newest ELSE @]
    /\ UNCHANGED <<up, grp, st, electing, exec, recv, view, prim, hist, dead, nextInc>>
    /\ UNCHANGED <<lk, dph, dOK, dGen, dFence, fenced, fcPend, cInc,
                   recentBoot, breq, bOrc, bPrev>>
    /\ UNCHANGED <<vS, vO, vH, vB>>

----------------------------------------------------------------------------
\* STUCK_CHECK: a state is a legitimate end state when a legitimate primary serves, or a bound is reached
Healthy == \E p \in Servers : ttype[p] = "P" /\ serving[p] /\ CanAccept(p) /\ LegitMaj(p)
\* ... or when only the expiry of the live intent, which this check leaves out, can end the wait that the
\* design accepts: the intent's bootstrap RPC failed without a definitive refusal, and its target is in
\* no group to adopt, nor runs a START
WaitsForExpiry ==
    wfail /\ IntentLive(intent) /\ grp[intent.tgt] = 0 /\ st[intent.tgt] = "none"
Done ==
    /\ Free
    /\ STUCK_CHECK
    /\ Healthy \/ nextInc > MaxInc \/ nextTok > MaxTok \/ WaitsForExpiry
    /\ UNCHANGED vars

Step ==
    \/ \E s \in Servers :
        \/ Commit(s) \/ Deliver(s) \/ Apply(s) \/ Leave(s) \/ ElectEnd(s)
        \/ Crash(s) \/ DecideCrash(s) \/ Restart(s) \/ JoinComplete(s) \/ JoinStray(s) \/ JoinFail(s)
        \/ BootComplete(s) \/ AsyncApply(s)
        \/ RefreshRec(s) \/ BootGraceExpire(s)
        \/ SyncRead(s) \/ SyncStop(s) \/ SyncServeStale(s) \/ SyncDemote(s) \/ LeaveForeign(s)
        \/ PrSnap(s) \/ PrRead(s) \/ PrAct(s) \/ SaSnap(s) \/ SaRead(s) \/ SaAct(s)
        \/ JoinStart(s) \/ JoinRelease(s) \/ FcRead(s) \/ FcAct(s)
        \/ HBoot1(s) \/ HBootGo(s) \/ HBootGiveUp(s) \/ HBootAbort(s)
        \/ StaleTopo(s)
    \/ \E i \in Incs : LoseMajority(i) \/ Elect(i) \/ LeaveDead(i)
    \/ \E o \in Orcs : OBegin(o) \/ OIntent(o) \/ OReply(o) \/ OTimeout(o) \/ OAdopt(o) \/ OAdoptLater(o)
    \/ OLeaseExpire \/ OIntentExpire
    \/ Done

Next == Step

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(* Properties *)

\* Every acknowledged write is in the legitimate group's history, and every MySQL that accepts
\* commits holds it.
NoLostAck ==
    /\ acked \subseteq hist[recInc]
    /\ \A p \in Servers : CanAccept(p) => acked \subseteq exec[p]

\* At most one MySQL accepts commits that it can acknowledge.
OneWritablePrimary == \A p, q \in Servers : (CanAccept(p) /\ CanAccept(q)) => p = q

\* No write is routed to a serving primary whose decision to serve was not taken on a fresh status
\* under the action lock, while it lacks the voter majority or the recorded incarnation (S7d r2).
NoDecisionAck == ~decisionAck

\* Expected to be violated: a serving primary whose view shrinks through clean leaves keeps its
\* view quorum and commits on a minority of the voters until the fence check fences it.
NoMinorityAck == ~minorityAck

\* At most one incarnation is recorded for each bootstrap intent.
AdoptOnce == \A k \in Toks : Cardinality(adopted[k]) <= 1

\* No decision made MySQL writable over a fence decided after the decision's snapshot.
FenceNotUndone == ~fenceUndone

\* No voter replicates asynchronously on the default channel (NEW-3).
NoAsyncVoter == \A s \in Servers : async[s] = NoServer

\* No two live groups were bootstrapped on different tablets from the same recorded incarnation.
\* (A bootstrap of the same target again is allowed: the tablet stops a START still in progress first.)
NoDualBootstrap ==
    \A i, j \in Incs :
        (bootFrom[i][1] # 0 /\ bootFrom[i][1] = bootFrom[j][1] /\ bootFrom[i][2] # bootFrom[j][2])
            => ~(Alive(i) /\ Alive(j))

Symm == Permutations(Servers) \cup Permutations(Orcs)
=============================================================================
