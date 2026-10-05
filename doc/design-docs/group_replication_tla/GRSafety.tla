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
    MaxProbe,       \* bound: re-probes of a stale intent's target (REPROBE_STALE_INTENT)
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
    REPROBE_STALE_INTENT, \* when a live intent names a voter that is no longer the candidate, and that is
                          \* reachable, in no group and runs no START, VTOrc sends that intent's bootstrap
                          \* to it again (same token, same expected incarnation, the current required set),
                          \* and withdraws the intent if it refuses definitively
    \* --- unsafe variants, to validate the conditions of WITHDRAW_ON_REFUSAL (FALSE = the code) ---
    WITHDRAW_ANY_FAILURE, \* VTOrc withdraws its intent after any error or timeout of the bootstrap RPC
    DEFINITIVE_WHILE_STARTING, \* the tablet reports as definitive the refusal it sends while a START
                               \* GROUP_REPLICATION still runs (it gave up waiting for the START to end)
    REPROBE_NO_REQ, \* the re-probe of REPROBE_STALE_INTENT does not carry the required transactions
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
    TOKEN_TOPO_TIMEOUT, \* the tablet's read of the shard record for BOOT_TOKEN may time out; the check is
                        \* then skipped
    EXPIRY_WAIT_OK, \* STUCK_CHECK: a state that only the expiry of a live intent can end, after its RPC
                    \* failed without a definitive refusal, counts as a healthy end state (WaitsForExpiry)
    \* ==== second milestone ====
    Prs,            \* the vtctld that runs PlannedReparentShard, EmergencyReparentShard and the initial
                    \* promotion (a model value; it holds the shard lock like a VTOrc)
    MaxVot,         \* bound: voter lists that VTOrc writes (GroupVotersOutOfDate)
    MaxPrs,         \* bound: reparents (PRS, ERS, initial promotion) that start
    MaxUndo,        \* bound: UndoDemotePrimary calls of VTOrc's PrimaryIsReadOnly recovery
    MaxSetRW,       \* bound: SetReadWrite calls (operator, PRS's recovery of a partial promotion)
    \* --- voter replacement ---
    VOTERS,         \* VTOrc's GroupVotersOutOfDate recovery runs: it selects the voters again and writes them
    VOTERS_NEED_GROUP, \* (fix, TRUE = code) VTOrc changes the voters only while a member of the recorded
                       \* incarnation is active with quorum in its view
    VOTERS_KEEP_MINORITY, \* (proposed, FALSE = code) ... and writes no list under which a view of the
                          \* recorded incarnation that lacks the majority of the listed voters would hold
                          \* the majority of the new ones
    VOT_SPLIT,      \* VTOrc's read of the voters' statuses and its write of the list are separate steps
    \* --- reparents and the RPCs that make MySQL writable ---
    PRS,            \* PlannedReparentShard runs
    ERS,            \* EmergencyReparentShard runs
    PRS_LEGIT,      \* (fix, TRUE = code) PRS's preflight requires the primary-elect in the shard's legitimate
                    \* group (recorded incarnation, voter majority); ERS's findGroupWithQuorum counts only
                    \* the members of that group
    UNDO_CHECK,     \* (fix, TRUE = code) UndoDemotePrimary serves and makes MySQL writable only on the
                    \* serving invariant (40305e2)
    UNDO_MATCH,     \* (fix, TRUE = code) VTOrc's PrimaryIsReadOnly recovery only on the legitimate primary
    SETRW_CHECK,    \* (fix, TRUE = code) SetReadWrite only on the serving invariant
    DEMOTE_FAIL,    \* environment: DemotePrimary may fail after it set super_read_only, and reverts
    \* --- InitPrimary ---
    INIT_EMPTY,     \* the shard never had a primary: no group, no incarnation recorded (recInc = 0)
    INIT_PRS,       \* the initial promotion of PlannedReparentShard runs: InitPrimary bootstraps the group
    INIT_WRITABLE,  \* (TRUE = code) InitPrimary makes MySQL writable before its decision to serve (the
                    \* exception); FALSE: MySQL stays read-only until a decision lets the tablet serve
    STANDALONE,     \* environment: a writable MySQL that Group Replication stopped (OFFLINE) takes writes on
                    \* its own; one in the ERROR state does not (its before_commit hook refuses them)
    MYSQLD_RESTART, \* environment: mysqld restarts while its vttablet keeps running (uses the crash budget)
    LOOP_RUNS,      \* the sync loop serves again at most once per run, after the run read MySQL's status (FALSE:
                    \* any number of times on the status it read last, which only adds behaviours)
    INIT_STABLE,    \* assumption: no host crashes, no member leaves and no group loses its majority while
                    \* the initial promotion runs (it completes, or fails without a fault)
    \* --- environment ---
    DIRECT_WRITES,  \* clients also write to MySQL directly (FALSE: only through vtgate, to a serving PRIMARY)
    GRACE_SETTLES,  \* timing: the voter replacement grace period (1 minute) outlasts the repair of a live
                    \* group: a voter is considered failed only once the live group of the recorded
                    \* incarnation has a primary whose election ended, after it went down
    \* --- fixes of the second milestone's findings (FALSE = the code at f528e9a) ---
    VOT_CAS,        \* the voter write is a compare-and-swap on the list and the incarnation VTOrc read
                    \* (VOT_SPLIT only)
    DEMOTE_REVERT_DECISION, \* DemotePrimary's revert goes through the serving decision (waits for the election
                            \* end, fence snapshot, serving invariant under the action lock); otherwise the
                            \* tablet stays PRIMARY, not serving, MySQL read-only
    INIT_GUARD,     \* the parts of the initial promotion's guard ({} = the code at f528e9a): it refuses
                    \* while a bootstrap intent is live ("intent"), an incarnation is recorded ("inc"), or a
                    \* tablet is an active member of a group ("active")
    VIEW_GTIDS,     \* environment: the bootstrap of a group logs a view change event with a GTID (-i for
                    \* incarnation i) in the bootstrapped member's binlog and in the group's history; a join
                    \* copies it with the rest. The view changes of joins and leaves are left out.
    VOT_REVALIDATE, \* (fix of finding 7, {} = the code) the parts of VTOrc's re-read of the voters right before
                    \* its voter write, atomic with the write (VOT_SPLIT only): "dropped", it refuses the write if a
                    \* voter that the new list drops is reachable and an active member, or holds transactions that
                    \* the kept voters' union lacks; "group", it refuses unless a member of the recorded
                    \* incarnation is still active with quorum (VOTERS_NEED_GROUP again); "reachable", it refuses
                    \* unless every voter that the selection dropped as failed is still unreachable and has not
                    \* been reachable since the selection (the grace criterion, evaluated again); "primary", it
                    \* refuses to drop the current primary of a live group
    INIT_RECORD_FAIL, \* environment: the initial promotion's write of the incarnation fails (a topology error, the
                      \* reparent's deadline): PRS fails, and the group InitPrimary bootstrapped stays unrecorded
    PRIMARY_MUST_BE_VOTER, \* (fix of finding 8, FALSE = the code) a tablet serves as PRIMARY only if its own MySQL
                           \* is a listed voter, in addition to the serving invariant
    FENCE_ON_DROP,  \* (fix of finding 8, FALSE = the code) the fence check fences a PRIMARY tablet whose server is no
                    \* longer a listed voter: not serving, super_read_only (not during a bootstrap)
    NONVOTER_LEAVES, \* (fix of finding 8, FALSE = the code) the tablet's sync loop makes an active member that is
                     \* not a listed voter leave its group, without waiting for VTOrc
    ADOPT_UNRECORDED, \* (fix of finding 6's liveness gap, FALSE = the code) VTOrc records the incarnation of a
                      \* group that nobody recorded (adoptUnrecordedGroup), when it is the only group the shard can
                      \* have
    VOT_PROMPT,     \* timing (VOT_SPLIT): VTOrc writes the voter list before a host that the list drops
                    \* restarts: its read of the statuses and its write are closer than a host restart
    JOIN_WAITS_RECORD \* (candidate, FALSE = the code) while the shard record lists no incarnation, a tablet
                      \* starts no join on its own: it waits until the bootstrap is recorded (VTOrc joins the
                      \* voters after it records its bootstrap; the voters of an initial promotion join after
                      \* PRS recorded it). A group bootstrapped before incarnations were recorded is not
                      \* rejoined on the tablets' own until one is recorded for it.

Incs == 1..MaxInc
Tx   == 1..MaxTx
Toks == 1..MaxTok
NoIntent == [tgt |-> NoServer, prev |-> 0, tok |-> 0, born |-> 0]
NoVot == [old |-> {}, new |-> {}, inc |-> 0, back |-> FALSE]

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
    orp,        \* the RPC the recovery waits for: 0 its own intent's, k > 0 the k-th re-probe (replies are
                \* matched to their RPC, as gRPC does: a re-probe reuses the VTOrc and the token)
    lockOwner,  \* holder of the shard lock's lease
    \* ---- clients and history ----
    acked, nextTx,
    bootFrom,   \* for a bootstrapped incarnation i: the recorded incarnation it started from, and its target
    decisionAck, minorityAck, fenceUndone,
    adopted,    \* incarnations recorded for each intent token
    wfail,      \* STUCK_CHECK only: the bootstrap RPC of the current intent failed without a definitive
                \* refusal (an error or a timeout), so that only adoption or the intent's expiry ends it
    \* ---- budgets ----
    nCrash, nLeave, nLoss, nExpire, nProbe,
    \* ==== second milestone ====
    voters,     \* Shard.group_replication_voters
    ovot,       \* VOT_SPLIT: [old: the list VTOrc o read, new: the list it selected, and writes next,
                \* inc: the incarnation the shard record listed]
    vMinor,     \* a voter write made a view that held fewer than a majority of the old voters hold a
                \* majority of the new ones
    pph,        \* the reparent's phase: "idle", "begun" (PRS), "demoting", "demoted", "promote",
                \* "prom_wait", "init", "init_run", "init_dec"
    pcur,       \* PRS: the current primary it demotes
    pel,        \* the primary-elect (InitPrimary's target)
    pprev,      \* initial promotion: the incarnation the shard record listed before InitPrimary
    dmWasRO,    \* DemotePrimary: MySQL was read-only when it started
    dmWasSrv,   \* DemotePrimary: the tablet served when it started
    dmR,        \* DemotePrimary: a not-serving reason was set while it ran
    dmF,        \* DemotePrimary: a fence was decided while it ran
    udReq,      \* an UndoDemotePrimary RPC waits for the tablet's action lock
    gone,       \* the host has been down for the voter replacement grace period
    ran,        \* LOOP_RUNS: this run of the sync loop has tried to serve again
    err,        \* STANDALONE: MySQL is out of its group in the ERROR state (expelled, or its group lost its
                \* majority), not stopped: Group Replication refuses its commits
    everP,      \* a tablet has been PRIMARY (the shard record's primary term is set)
    nVot, nPrs, nUndo, nSetRW

vM ==<<up, grp, st, sro, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
vT == <<ttype, serving, lk, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc, cInc,
        recentBoot, dmt, breq, bOrc, bPrev, badServe>>
vS == <<recInc, intent, nextTok, intExp, newest>>
vO == <<oph, ocand, oexp, otok, oborn, oreq, orep, orp, lockOwner>>
vH == <<acked, nextTx, bootFrom, decisionAck, minorityAck, fenceUndone, adopted, wfail>>
vB == <<nCrash, nLeave, nLoss, nExpire, nProbe>>
vN == <<voters, ovot, vMinor, pph, pcur, pel, pprev, dmWasRO, dmWasSrv, dmR, dmF, udReq, gone, everP,
        nVot, nPrs, nUndo, nSetRW, err, ran>>
vars == <<vM, vT, vS, vO, vH, vB, vN>>

----------------------------------------------------------------------------
(* Helpers *)

\* a majority of the voters V; policy.LegitimateGroup counts the listed voters ONLINE in the view
MajOf(V)     == Cardinality(V) \div 2 + 1
VoterMajIn(V, i) == Cardinality(view[i] \cap V) >= MajOf(V)
VoterMaj(i)  == VoterMajIn(voters, i)
\* the incarnation i is the recorded one c; none recorded (c = 0, a shard that never had a group, or one
\* bootstrapped before incarnations were recorded): only the voter rule applies
RecOK(i, c)  == c = 0 \/ i = c
Alive(i)     == view[i] # {} /\ ~dead[i]
\* mysql.IsGroupPrimary: ONLINE primary of a group with quorum
IsPrimQ(s)   == up[s] /\ grp[s] # 0 /\ prim[grp[s]] = s /\ ~dead[grp[s]]
\* MySQL accepts a commit that it can acknowledge
CanAccept(s) == IsPrimQ(s) /\ ~sro[s] /\ ~electing[s]
\* ground truth: the primary of the shard's legitimate group, with the voter majority
LegitMaj(s)  == IsPrimQ(s) /\ RecOK(grp[s], recInc) /\ VoterMaj(grp[s])

\* groupReplicationServingReason = "" on a status read now, against the record incarnation c
ServeOK(s, c) == IsPrimQ(s) /\ ~electing[s] /\ (LEGIT => (RecOK(grp[s], c) /\ VoterMaj(grp[s])))
                 /\ (PRIMARY_MUST_BE_VOTER => s \in voters)
\* IsLegitimatePrimary for the sync loop's promotion (trusts a group it bootstrapped itself)
PromoteLegit(s) == IsPrimQ(s) /\ (LEGIT => ((RecOK(grp[s], recInc) \/ grp[s] = recentBoot[s]) /\ VoterMaj(grp[s])))
                   /\ (PRIMARY_MUST_BE_VOTER => s \in voters)

IntentLive(it) == it.tok # 0 /\ ~(it.tok = intent.tok /\ intExp)
\* shardGroupRecord.adoptableByIntent: the tablet's view of an intent that names it
AdoptableByIntent(s, i, it, rinc) ==
    ADOPT /\ INTENT /\ it.tok # 0 /\ it.tgt = s /\ it.prev = rinc /\ i # it.prev /\ IntentLive(it) /\ i >= it.born

\* another tablet reports an active member of the legitimate group with quorum
LegitActiveElsewhere(s) ==
    \E v \in Servers \ {s} : up[v] /\ grp[v] # 0 /\ ~dead[grp[v]] /\ (LEGIT => RecOK(grp[v], recInc))

Data(v) == exec[v] \cup recv[v]
\* VIEW_GTIDS: the GTID of the view change event that the bootstrap of incarnation i logs
VGtid(i) == IF VIEW_GTIDS THEN {-i} ELSE {}

\* the recorded incarnation as the tablet's sync loop and fence check see it
Rec(s) == IF STALE_REC THEN cInc[s] ELSE recInc
\* a fresh read of the shard record updates the tablet's copy
Refresh(s) == cInc' = IF STALE_REC THEN [cInc EXCEPT ![s] = recInc] ELSE cInc

SnapVal(s) == IF ~IsPrimQ(s) \/ electing[s] THEN "np" ELSE IF ServeOK(s, Rec(s)) THEN "ok" ELSE "bad"

Unlock(o) == IF lockOwner = o THEN NoOrc ELSE lockOwner

NoReq == [o |-> NoOrc, tok |-> 0, exp |-> 0, req |-> {}, rp |-> 0]
\* the VTOrc that sent the bootstrap RPC r to s still waits for its reply
Waiting(r, s) == r.o # NoOrc /\ oph[r.o] = "wait" /\ otok[r.o] = r.tok /\ ocand[r.o] = s /\ orp[r.o] = r.rp
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
\* INIT_STABLE: faults wait for the initial promotion to end
NoFault == INIT_STABLE => pph \notin {"init", "init_run", "init_dec"}

----------------------------------------------------------------------------
\* The shard's group: incarnation 1 of all three voters, with a serving primary p0. With INIT_EMPTY, a
\* shard that never had a primary: no group, nothing recorded, every MySQL read-only.
Init ==
    \E p0 \in Servers :
        LET G == ~INIT_EMPTY IN
        /\ up = [s \in Servers |-> TRUE]
        /\ grp = [s \in Servers |-> IF G THEN 1 ELSE 0]
        /\ st = [s \in Servers |-> "none"]
        /\ sro = [s \in Servers |-> ~G \/ s # p0]
        /\ electing = [s \in Servers |-> FALSE]
        /\ exec = [s \in Servers |-> {}]
        /\ recv = [s \in Servers |-> {}]
        /\ view = [i \in Incs |-> IF i = 1 /\ G THEN Servers ELSE {}]
        /\ prim = [i \in Incs |-> IF i = 1 /\ G THEN p0 ELSE NoServer]
        /\ hist = [i \in Incs |-> {}]
        /\ dead = [i \in Incs |-> FALSE]
        /\ nextInc = IF G THEN 2 ELSE 1
        /\ async = [s \in Servers |-> NoServer]
        /\ ttype = [s \in Servers |-> IF G /\ s = p0 THEN "P" ELSE "R"]
        /\ serving = [s \in Servers |-> G /\ s = p0]
        /\ lk = [s \in Servers |-> "free"]
        /\ dph = [s \in Servers |-> "idle"]
        /\ dOK = [s \in Servers |-> FALSE]
        /\ dGen = [s \in Servers |-> FALSE]
        /\ dFence = [s \in Servers |-> FALSE]
        /\ snap = [s \in Servers |-> IF G /\ s = p0 THEN "ok" ELSE "np"]
        /\ fenced = [s \in Servers |-> FALSE]
        /\ fcPend = [s \in Servers |-> FALSE]
        /\ majInc = [s \in Servers |-> IF G /\ s = p0 /\ FENCE THEN 1 ELSE 0]
        /\ cInc = [s \in Servers |-> IF G THEN 1 ELSE 0]
        /\ recentBoot = [s \in Servers |-> 0]
        /\ dmt = [s \in Servers |-> FALSE]
        /\ breq = [s \in Servers |-> {}]
        /\ bOrc = [s \in Servers |-> NoReq]
        /\ bPrev = [s \in Servers |-> 0]
        /\ badServe = [s \in Servers |-> FALSE]
        /\ recInc = IF G THEN 1 ELSE 0
        /\ intent = NoIntent
        /\ nextTok = 1
        /\ intExp = FALSE
        /\ newest = IF G THEN p0 ELSE NoServer
        /\ oph = [o \in Orcs |-> "idle"]
        /\ ocand = [o \in Orcs |-> NoServer]
        /\ oexp = [o \in Orcs |-> 0]
        /\ otok = [o \in Orcs |-> 0]
        /\ oborn = [o \in Orcs |-> 0]
        /\ oreq = [o \in Orcs |-> {}]
        /\ orep = [o \in Orcs |-> 0]
        /\ orp = [o \in Orcs |-> 0]
        /\ lockOwner = NoOrc
        /\ acked = {}
        /\ nextTx = 1
        /\ bootFrom = [i \in Incs |-> <<0, NoServer>>]
        /\ decisionAck = FALSE
        /\ minorityAck = FALSE
        /\ fenceUndone = FALSE
        /\ adopted = [k \in Toks |-> {}]
        /\ wfail = FALSE
        /\ nCrash = 0 /\ nLeave = 0 /\ nLoss = 0 /\ nExpire = 0 /\ nProbe = 0
        /\ voters = Servers
        /\ ovot = [o \in Orcs |-> NoVot]
        /\ vMinor = FALSE
        /\ pph = "idle" /\ pcur = NoServer /\ pel = NoServer /\ pprev = 0
        /\ dmWasRO = [s \in Servers |-> FALSE]
        /\ dmWasSrv = [s \in Servers |-> FALSE]
        /\ dmR = [s \in Servers |-> FALSE]
        /\ dmF = [s \in Servers |-> FALSE]
        /\ udReq = [s \in Servers |-> FALSE]
        /\ gone = [s \in Servers |-> FALSE]
        /\ err = [s \in Servers |-> FALSE]
        /\ ran = [s \in Servers |-> FALSE]
        /\ everP = G
        /\ nVot = 0 /\ nPrs = 0 /\ nUndo = 0 /\ nSetRW = 0

----------------------------------------------------------------------------
(* Clients: through vtgate, or directly to MySQL *)

Commit(p) ==
    /\ Free
    /\ nextTx <= MaxTx /\ CanAccept(p)
    /\ DIRECT_WRITES \/ (ttype[p] = "P" /\ serving[p])
    /\ hist' = [hist EXCEPT ![grp[p]] = @ \cup {nextTx}]
    /\ exec' = [exec EXCEPT ![p] = @ \cup {nextTx}]
    /\ acked' = acked \cup {nextTx}
    /\ nextTx' = nextTx + 1
    /\ minorityAck' = (minorityAck \/ ~VoterMaj(grp[p]))
    /\ decisionAck' = (decisionAck \/ (ttype[p] = "P" /\ serving[p] /\ badServe[p] /\ ~LegitMaj(p)))
    /\ UNCHANGED <<up, grp, st, sro, electing, recv, view, prim, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vB, bootFrom, fenceUndone, adopted, wfail>>
    /\ UNCHANGED vN

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
    /\ UNCHANGED vN

\* the applier
Apply(s) ==
    /\ Free
    /\ up[s] /\ grp[s] # 0 /\ recv[s] # {}
    /\ exec' = [exec EXCEPT ![s] = @ \cup recv[s]]
    /\ recv' = [recv EXCEPT ![s] = {}]
    /\ UNCHANGED <<up, grp, st, sro, electing, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vH, vB>>
    /\ UNCHANGED vN

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
    /\ NoFault
    /\ LeaveVars(s)
    /\ nLeave' = nLeave + 1
    \* STANDALONE: an expulsion leaves MySQL in the ERROR state (a STOP that an operator issues outside
    \* Vitess is left out; Vitess's own STOPs, a restart, leave it OFFLINE)
    /\ err' = [err EXCEPT ![s] = STANDALONE]
    /\ UNCHANGED <<up, st, exec, recv, hist, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vH, nCrash, nLoss, nExpire, nProbe>>
    /\ UNCHANGED <<voters, ovot, vMinor, pph, pcur, pel, pprev, dmWasRO, dmWasSrv, dmR, dmF, udReq, gone, everP,
                   nVot, nPrs, nUndo, nSetRW, ran>>

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
    /\ err' = [s \in Servers |-> (STANDALONE /\ s \in view[i]) \/ err[s]]
    /\ UNCHANGED <<up, st, exec, recv, hist, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, pph, pcur, pel, pprev, dmWasRO, dmWasSrv, dmR, dmF, udReq, gone, everP,
                   nVot, nPrs, nUndo, nSetRW, ran>>

\* a partition leaves no majority: the group blocks, and its members leave it later
LoseMajority(i) ==
    /\ Free
    /\ Alive(i) /\ Cardinality(view[i]) >= 2 /\ nLoss < MaxLoss
    /\ NoFault
    /\ dead' = [dead EXCEPT ![i] = TRUE]
    /\ nLoss' = nLoss + 1
    /\ UNCHANGED <<up, grp, st, sro, electing, exec, recv, view, prim, hist, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vH, nCrash, nLeave, nExpire, nProbe>>
    /\ UNCHANGED vN

Elect(i) ==
    /\ Free
    /\ Alive(i) /\ prim[i] = NoServer
    /\ \E q \in view[i] :
        /\ prim' = [prim EXCEPT ![i] = q]
        /\ electing' = [electing EXCEPT ![q] = TRUE]
    /\ UNCHANGED <<up, grp, st, sro, exec, recv, view, hist, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vH, vB>>
    /\ UNCHANGED vN

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
    /\ UNCHANGED vN

\* the host dies; relay_log_recovery drops the received backlog; vttablet restarts as REPLICA
CrashVars(s) ==
    /\ up[s] /\ nCrash < MaxCrash
    /\ NoFault
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
                   nLeave, nLoss, nExpire, nProbe>>
    \* the RPCs in flight on the tablet end with it
    /\ dmWasRO' = [dmWasRO EXCEPT ![s] = FALSE] /\ dmWasSrv' = [dmWasSrv EXCEPT ![s] = FALSE]
    /\ dmR' = [dmR EXCEPT ![s] = FALSE] /\ dmF' = [dmF EXCEPT ![s] = FALSE]
    /\ udReq' = [udReq EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<voters, ovot, vMinor, pph, pcur, pel, pprev, gone, everP, nVot, nPrs, nUndo, nSetRW, err, ran>>

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
    /\ VOT_PROMPT => \A o \in Orcs : ~(oph[o] = "vot" /\ s \in ovot[o].old \ ovot[o].new)
    \* a host that a pending voter write drops is back: VTOrc's re-read would see it seen since the selection
    /\ ovot' = [o \in Orcs |-> IF oph[o] = "vot" /\ s \in ovot[o].old \ ovot[o].new
                               THEN [ovot[o] EXCEPT !.back = TRUE] ELSE ovot[o]]
    /\ up' = [up EXCEPT ![s] = TRUE]
    /\ sro' = [sro EXCEPT ![s] = TRUE]
    /\ gone' = [gone EXCEPT ![s] = FALSE]
    /\ err' = [err EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<grp, st, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vH, vB>>
    /\ UNCHANGED <<voters, vMinor, pph, pcur, pel, pprev, dmWasRO, dmWasSrv, dmR, dmF, udReq, everP,
                   nVot, nPrs, nUndo, nSetRW, ran>>

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
    /\ UNCHANGED vN

\* NEW-1's MySQL mechanism: a join that finds no live group ends alone in a new incarnation
JoinStray(s) ==
    /\ Free
    /\ up[s] /\ st[s] = "join" /\ nextInc <= MaxInc
    /\ \A i \in Incs : ~Alive(i)
    /\ grp' = [grp EXCEPT ![s] = nextInc]
    /\ view' = [view EXCEPT ![nextInc] = {s}]
    /\ prim' = [prim EXCEPT ![nextInc] = s]
    /\ hist' = [hist EXCEPT ![nextInc] = exec[s] \cup VGtid(nextInc)]
    /\ exec' = [exec EXCEPT ![s] = @ \cup VGtid(nextInc)]
    /\ electing' = [electing EXCEPT ![s] = TRUE]
    /\ st' = [st EXCEPT ![s] = "none"]
    /\ nextInc' = nextInc + 1
    /\ lk' = [lk EXCEPT ![s] = IF @ = "join" THEN "free" ELSE @]
    /\ UNCHANGED <<up, sro, recv, dead, async>>
    /\ UNCHANGED <<ttype, serving, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc, cInc,
                   recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>
    /\ UNCHANGED vN

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
    /\ UNCHANGED vN

\* a bootstrap START forms a group of one; MySQL applies the received backlog first (L4)
BootComplete(s) ==
    /\ Free
    /\ up[s] /\ st[s] = "boot" /\ nextInc <= MaxInc
    /\ exec' = [exec EXCEPT ![s] = @ \cup recv[s] \cup VGtid(nextInc)]
    /\ recv' = [recv EXCEPT ![s] = {}]
    /\ grp' = [grp EXCEPT ![s] = nextInc]
    /\ view' = [view EXCEPT ![nextInc] = {s}]
    /\ prim' = [prim EXCEPT ![nextInc] = s]
    /\ hist' = [hist EXCEPT ![nextInc] = exec[s] \cup recv[s] \cup VGtid(nextInc)]
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
       \* InitPrimary's START: noteBootstrap; InitPrimary goes on under the lock
       ELSE IF lk[s] = "init"
       THEN /\ recentBoot' = [recentBoot EXCEPT ![s] = nextInc]
            /\ UNCHANGED <<orep, lk, bOrc>>
       ELSE UNCHANGED <<recentBoot, orep, lk, bOrc>>
    /\ UNCHANGED <<up, sro, dead, async>>
    /\ UNCHANGED <<ttype, serving, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc, cInc,
                   dmt, breq, badServe>>
    /\ UNCHANGED <<vS, oph, ocand, oexp, otok, oborn, oreq, orp, lockOwner, vB, acked, nextTx, decisionAck,
                   minorityAck, fenceUndone, adopted, wfail>>
    /\ UNCHANGED vN

\* asynchronous replication on the default channel (only after StaleTopoPrimary without NEW3_FIX)
AsyncApply(s) ==
    /\ Free
    /\ async[s] # NoServer /\ up[s] /\ grp[s] = 0 /\ st[s] = "none" /\ up[async[s]]
    /\ ~(exec[async[s]] \subseteq exec[s])
    /\ exec' = [exec EXCEPT ![s] = @ \cup exec[async[s]]]
    /\ UNCHANGED <<up, grp, st, sro, electing, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vH, vB>>
    /\ UNCHANGED vN

----------------------------------------------------------------------------
(* vttablet: shard record, timers *)

RefreshRec(s) ==
    /\ Free
    /\ STALE_REC /\ up[s] /\ cInc[s] # recInc
    /\ Refresh(s)
    /\ UNCHANGED <<vM, ttype, serving, lk, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc,
                   recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>
    /\ UNCHANGED vN

\* groupReplicationBootstrapGrace (1 minute) ends
BootGraceExpire(s) ==
    /\ Free
    /\ ~STUCK_CHECK /\ recentBoot[s] # 0
    /\ recentBoot' = [recentBoot EXCEPT ![s] = 0]
    /\ UNCHANGED <<vM, ttype, serving, lk, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>
    /\ UNCHANGED vN

----------------------------------------------------------------------------
(* vttablet: the sync loop *)

\* a run of the loop reads MySQL's status, then may wait on the topology for a long time
SyncRead(s) ==
    /\ Free
    /\ up[s] /\ ttype[s] = "P" /\ (snap[s] # SnapVal(s) \/ ran[s])
    /\ snap' = [snap EXCEPT ![s] = SnapVal(s)]
    /\ ran' = [ran EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<vM, ttype, serving, lk, dph, dOK, dGen, dFence, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, pph, pcur, pel, pprev, dmWasRO, dmWasSrv, dmR, dmF, udReq, gone, everP,
                   nVot, nPrs, nUndo, nSetRW, err>>

\* enforceVoterMajority stops serving on the status read at the start of the run, without the lock
SyncStop(s) ==
    /\ Free
    /\ up[s] /\ ttype[s] = "P" /\ serving[s] /\ snap[s] = "bad"
    /\ serving' = [serving EXCEPT ![s] = FALSE]
    /\ badServe' = [badServe EXCEPT ![s] = FALSE]
    \* a not-serving reason, which a DemotePrimary in progress sees when it reverts
    /\ dmR' = [dmR EXCEPT ![s] = @ \/ lk[s] = "demote"]
    /\ UNCHANGED <<vM, ttype, lk, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev>>
    /\ UNCHANGED <<vS, vO, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, pph, pcur, pel, pprev, dmWasRO, dmWasSrv, dmF, udReq, gone, everP,
                   nVot, nPrs, nUndo, nSetRW, err, ran>>

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
    /\ UNCHANGED vN

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
    /\ UNCHANGED vN

\* isForeignGroup + leaveForeignGroupLocked: fence first, demote, STOP GROUP_REPLICATION
LeaveForeign(s) ==
    /\ Free
    /\ LEGIT
    /\ up[s] /\ lk[s] = "free" /\ grp[s] # 0
    /\ ~RecOK(grp[s], Rec(s)) /\ ~RecOK(grp[s], recInc) /\ grp[s] # recentBoot[s]
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
    /\ UNCHANGED vN

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
    /\ UNCHANGED vN

\* step 2: MySQL's status, read under the lock
PrRead(s) ==
    /\ dph[s] = "pr_snap"
    /\ dOK' = [dOK EXCEPT ![s] = ServeOK(s, Rec(s))]
    /\ dph' = [dph EXCEPT ![s] = "pr_read"]
    /\ UNCHANGED <<vM, ttype, serving, lk, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>
    /\ UNCHANGED vN

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
    \* the shard record's primary term is set
    /\ everP' = TRUE
    /\ UNCHANGED <<voters, ovot, vMinor, pph, pcur, pel, pprev, dmWasRO, dmWasSrv, dmR, dmF, udReq,
                   gone, nVot, nPrs, nUndo, nSetRW, err, ran>>

\* serveAgain, step 1: TryAcquire, the generation, the fence snapshot
SaSnap(s) ==
    /\ Free
    /\ SERVE_LOCKED
    /\ up[s] /\ lk[s] = "free" /\ ttype[s] = "P" /\ snap[s] = "ok" /\ ~dmt[s] /\ ~ran[s]
    /\ (~serving[s] \/ fenced[s] \/ sro[s])
    /\ lk' = [lk EXCEPT ![s] = "sync"]
    /\ dph' = [dph EXCEPT ![s] = "sa_snap"]
    /\ dGen' = [dGen EXCEPT ![s] = FALSE]
    /\ dFence' = [dFence EXCEPT ![s] = FALSE]
    /\ ran' = [ran EXCEPT ![s] = LOOP_RUNS]
    /\ UNCHANGED <<vM, ttype, serving, dOK, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, pph, pcur, pel, pprev, dmWasRO, dmWasSrv, dmR, dmF, udReq, gone, everP,
                   nVot, nPrs, nUndo, nSetRW, err>>

\* step 2: MySQL's status under the lock, against the shard record read before the lock
SaRead(s) ==
    /\ dph[s] = "sa_snap"
    /\ dOK' = [dOK EXCEPT ![s] = ServeOK(s, Rec(s))]
    /\ dph' = [dph EXCEPT ![s] = "sa_read"]
    /\ UNCHANGED <<vM, ttype, serving, lk, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>
    /\ UNCHANGED vN

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
    /\ UNCHANGED vN

\* a join (the sync loop's rejoin, the startup join, VTOrc's GroupMemberNotOnline): a new epoch, the
\* fence reset, MySQL's START; the applier applies the relay backlog; the default channel is stopped
JoinStart(s) ==
    /\ Free
    /\ up[s] /\ lk[s] = "free" /\ st[s] = "none" /\ grp[s] = 0 /\ ttype[s] = "R"
    \* only a listed voter joins the group (isGroupReplicationVoter)
    /\ s \in voters
    /\ JOIN_GATE => LegitActiveElsewhere(s)
    /\ JOIN_WAITS_RECORD => Rec(s) # 0
    /\ lk' = [lk EXCEPT ![s] = "join"]
    /\ st' = [st EXCEPT ![s] = "join"]
    /\ exec' = [exec EXCEPT ![s] = @ \cup recv[s]]
    /\ recv' = [recv EXCEPT ![s] = {}]
    /\ async' = [async EXCEPT ![s] = NoServer]
    /\ fenced' = [fenced EXCEPT ![s] = FALSE]
    /\ fcPend' = [fcPend EXCEPT ![s] = FALSE]
    \* the join stops MySQL's Group Replication first: it is no longer in the ERROR state
    /\ err' = [err EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<up, grp, sro, electing, view, prim, hist, dead, nextInc>>
    /\ UNCHANGED <<ttype, serving, dph, dOK, dGen, dFence, snap, majInc, cInc,
                   recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, pph, pcur, pel, pprev, dmWasRO, dmWasSrv, dmR, dmF, udReq, gone, everP,
                   nVot, nPrs, nUndo, nSetRW, ran>>

\* the joiner gives up while MySQL's START still runs (MySQL keeps running it); a START that ends
\* releases the lock with its end (JoinComplete, JoinStray, JoinFail)
JoinRelease(s) ==
    /\ Free
    /\ lk[s] = "join" /\ st[s] = "join"
    /\ lk' = [lk EXCEPT ![s] = "free"]
    /\ UNCHANGED <<vM, ttype, serving, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>
    /\ UNCHANGED vN

----------------------------------------------------------------------------
(* vttablet: the fence check (no action lock; ordered with decisions by epochs and decisions) *)

\* groupReplicationJoinWatchWindow (30s after a start) is left out: fewer fences only add behaviours
Armed(s) == ttype[s] = "P" \/ fenced[s] \/ lk[s] \in {"join", "bootw", "boot", "init"}

\* groupReplicationFenceReason, on the shard record the tablet read last
FenceReason(s) ==
    LET i == grp[s]
        recorded == ~LEGIT \/ RecOK(i, Rec(s))
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
    IF ttype[s] = "P" /\ IsPrimQ(s) /\ lk[s] \notin {"join", "bootw", "boot"} /\ VoterMaj(i) /\ (~LEGIT \/ RecOK(i, Rec(s)))
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
    /\ UNCHANGED vN

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
    \* a DemotePrimary in progress sees the fence, and the not-serving reason of a PRIMARY tablet
    /\ dmF' = [dmF EXCEPT ![s] = @ \/ (lk[s] = "demote" /\ lk[s] \notin {"bootw", "boot"})]
    /\ dmR' = [dmR EXCEPT ![s] = @ \/ (lk[s] = "demote" /\ ttype[s] = "P")]
    /\ UNCHANGED <<up, grp, st, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, lk, dph, dOK, snap, majInc, cInc, recentBoot, dmt, breq, bOrc, bPrev>>
    /\ UNCHANGED <<vS, vO, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, pph, pcur, pel, pprev, dmWasRO, dmWasSrv, udReq, gone, everP,
                   nVot, nPrs, nUndo, nSetRW, err, ran>>

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
    /\ UNCHANGED <<vS, oph, ocand, oexp, otok, oborn, oreq, orp, lockOwner, vH, vB>>
    /\ UNCHANGED vN

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
    /\ UNCHANGED <<vS, oph, ocand, oexp, otok, oborn, oreq, orp, lockOwner, vH, vB>>
    /\ UNCHANGED vN

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
    /\ UNCHANGED <<vS, oph, ocand, oexp, otok, oborn, oreq, orp, lockOwner, vH, vB>>
    /\ UNCHANGED vN

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
    /\ UNCHANGED vN

----------------------------------------------------------------------------
(* VTOrc *)

\* the listed voters whose status VTOrc read (all of them, or a reachable majority without BOOT_ALL)
BootReach == IF BOOT_ALL THEN voters ELSE {v \in voters : up[v]}

\* GroupNotBootstrapped: the shard lock, then a fresh status of every voter, and the candidate
\* REPROBE_STALE_INTENT: when the live intent names a voter t other than the candidate (t no longer holds
\* every transaction: it is not a candidate), and t answered, is in no group and runs no START on this
\* pass, VTOrc sends that intent's bootstrap to t again instead: the intent's token and expected
\* incarnation, the current required set. It does not write the intent, so its time is not refreshed.
\* At most one re-probe per pass. The re-probe relies on the withdrawal of WITHDRAW_ON_REFUSAL.
Reprobe(c) ==
    /\ REPROBE_STALE_INTENT /\ INTENT /\ WITHDRAW_ON_REFUSAL
    /\ IntentLive(intent) /\ intent.prev = recInc /\ intent.tgt # c
    /\ up[intent.tgt] /\ grp[intent.tgt] = 0 /\ st[intent.tgt] = "none"
    /\ intent.tgt \in voters
    /\ nProbe < MaxProbe
OBegin(o) ==
    /\ Free
    /\ oph[o] = "idle" /\ lockOwner = NoOrc
    /\ IF BOOT_ALL THEN \A v \in voters : up[v] ELSE Cardinality(BootReach) >= MajOf(voters)
    \* no tablet of the shard is an active member (a down host is in no group)
    /\ \A v \in Servers : grp[v] = 0
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
            /\ STUCK_CHECK => ~(INTENT /\ INTENT_FENCE /\ IntentLive(intent) /\ intent.tgt # c /\ ~Reprobe(c))
            /\ lockOwner' = o
            /\ orep' = [orep EXCEPT ![o] = 0]
            /\ IF Reprobe(c)
               THEN LET t == intent.tgt IN
                    /\ oph' = [oph EXCEPT ![o] = "wait"]
                    /\ ocand' = [ocand EXCEPT ![o] = t]
                    /\ oexp' = [oexp EXCEPT ![o] = recInc]
                    /\ otok' = [otok EXCEPT ![o] = intent.tok]
                    /\ oborn' = [oborn EXCEPT ![o] = intent.born]
                    /\ orp' = [orp EXCEPT ![o] = nProbe + 1]
                    /\ nProbe' = nProbe + 1
                    /\ breq' = [breq EXCEPT ![t] = @ \cup {[o |-> o, tok |-> intent.tok, exp |-> intent.prev,
                                                             req |-> IF REPROBE_NO_REQ THEN {} ELSE Req,
                                                             rp |-> nProbe + 1]}]
                    /\ UNCHANGED oreq
               ELSE
                    /\ ocand' = [ocand EXCEPT ![o] = c]
                    /\ oexp' = [oexp EXCEPT ![o] = recInc]
                    /\ UNCHANGED <<orp, nProbe>>
                    /\ IF INTENT
                       THEN /\ oph' = [oph EXCEPT ![o] = "chosen"]
                            /\ oreq' = [oreq EXCEPT ![o] = Req]
                            /\ UNCHANGED <<breq, otok, oborn>>
                       ELSE \* before the intent: the bootstrap RPC right away
                            /\ oph' = [oph EXCEPT ![o] = "wait"]
                            /\ breq' = [breq EXCEPT ![c] = @ \cup {[o |-> o, tok |-> 0, exp |-> recInc, req |-> Req, rp |-> 0]}]
                            /\ UNCHANGED oreq
                            /\ otok' = [otok EXCEPT ![o] = 0]
                            /\ oborn' = [oborn EXCEPT ![o] = nextInc]
    /\ UNCHANGED <<vM, ttype, serving, lk, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vH, nCrash, nLeave, nLoss, nExpire>>
    /\ UNCHANGED vN

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
            /\ breq' = [breq EXCEPT ![ocand[o]] = @ \cup {[o |-> o, tok |-> nextTok, exp |-> oexp[o], req |-> oreq[o], rp |-> 0]}]
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
    /\ UNCHANGED <<recInc, newest, orep, orp, acked, nextTx, bootFrom, decisionAck, minorityAck, fenceUndone,
                   adopted, vB>>
    /\ UNCHANGED vN

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
    /\ orp' = [orp EXCEPT ![o] = 0]

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
            /\ orp' = [orp EXCEPT ![o] = 0]
       ELSE /\ UNCHANGED <<recInc, adopted>>
            /\ IF WithdrawsAfter(orep[o])
               THEN /\ Withdraw(o)
                    /\ OEnd(o)
               ELSE /\ UNCHANGED <<intent, intExp>>
                    /\ IF ADOPT /\ INTENT
                       THEN /\ oph' = [oph EXCEPT ![o] = "adopt"]
                            /\ orp' = [orp EXCEPT ![o] = 0]
                            /\ UNCHANGED <<lockOwner, ocand, oexp, otok, oborn, orep>>
                       ELSE OEnd(o)
    /\ wfail' = IF STUCK_CHECK /\ orep[o] = -1 /\ otok[o] # 0 /\ otok[o] = intent.tok THEN TRUE ELSE wfail
    /\ UNCHANGED <<vM, vT, nextTok, newest, oreq>>
    /\ UNCHANGED <<acked, nextTx, bootFrom, decisionAck, minorityAck, fenceUndone, vB>>
    /\ UNCHANGED vN

\* the RPC times out or its reply is lost; a request still waiting for the tablet's lock is cancelled
OTimeout(o) ==
    /\ Free
    /\ oph[o] = "wait"
    /\ breq' = [breq EXCEPT ![ocand[o]] = {r \in @ : ~(r.o = o /\ r.tok = otok[o] /\ r.rp = orp[o])}]
    /\ IF WithdrawsAfter(0)
       THEN /\ Withdraw(o)
            /\ OEnd(o)
       ELSE /\ UNCHANGED <<intent, intExp>>
            /\ IF ADOPT /\ INTENT
               THEN /\ oph' = [oph EXCEPT ![o] = "adopt"]
                    /\ UNCHANGED <<lockOwner, ocand, oexp, otok, oborn>>
                    /\ orep' = [orep EXCEPT ![o] = 0]
                    /\ orp' = [orp EXCEPT ![o] = 0]
               ELSE OEnd(o)
    /\ UNCHANGED <<vM, ttype, serving, lk, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, bOrc, bPrev, badServe>>
    /\ wfail' = IF STUCK_CHECK /\ otok[o] # 0 /\ otok[o] = intent.tok THEN TRUE ELSE wfail
    /\ UNCHANGED <<recInc, nextTok, newest, oreq, acked, nextTx, bootFrom, decisionAck, minorityAck,
                   fenceUndone, adopted, vB>>
    /\ UNCHANGED vN

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
    /\ UNCHANGED <<vM, vT, nextTok, newest, oreq, orp>>
    /\ UNCHANGED <<acked, nextTx, bootFrom, decisionAck, minorityAck, fenceUndone, wfail, vB>>
    /\ UNCHANGED vN

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
    /\ UNCHANGED <<vM, vT, vS, oreq, orep, orp, vH, vB>>
    /\ UNCHANGED vN

\* the holder of the shard lock stalls longer than the lease; it keeps acting when it resumes
OLeaseExpire ==
    /\ Free
    /\ lockOwner # NoOrc /\ nExpire < MaxExpire
    /\ lockOwner' = NoOrc
    /\ nExpire' = nExpire + 1
    /\ UNCHANGED <<vM, vT, vS, oph, ocand, oexp, otok, oborn, oreq, orep, orp, vH, nCrash, nLeave, nLoss, nProbe>>
    /\ UNCHANGED vN

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
    /\ UNCHANGED vN

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
    /\ UNCHANGED vN

----------------------------------------------------------------------------
(* Second milestone *)

\* Shorthands for the UNCHANGED clauses of the actions below
vNp  == <<pph, pcur, pel, pprev>>
vNd  == <<dmWasRO, dmWasSrv, dmR, dmF>>
vNc  == <<nVot, nPrs, nUndo, nSetRW>>
vTd  == <<ttype, serving, lk, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc, cInc, recentBoot,
          dmt, breq, bOrc, bPrev, badServe>>

\* A MySQL outside of any group that is writable takes a client's write on its own: no group decides
\* it. Only UndoDemotePrimary, SetReadWrite or DemotePrimary's revert can leave MySQL so.
CommitAlone(p) ==
    /\ Free
    /\ STANDALONE /\ nextTx <= MaxTx /\ up[p] /\ grp[p] = 0 /\ st[p] = "none" /\ ~sro[p] /\ ~err[p]
    /\ DIRECT_WRITES \/ (ttype[p] = "P" /\ serving[p])
    /\ exec' = [exec EXCEPT ![p] = @ \cup {nextTx}]
    /\ acked' = acked \cup {nextTx}
    /\ nextTx' = nextTx + 1
    /\ minorityAck' = TRUE
    /\ UNCHANGED <<up, grp, st, sro, electing, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<vT, vS, vO, vB, bootFrom, decisionAck, fenceUndone, adopted, wfail>>
    /\ UNCHANGED vN


\* MYSQLD_RESTART: mysqld restarts (killed, or the OOM killer) while vttablet keeps running, with its
\* state, its RPCs and its type (NEW-6): MySQL leaves its group as in a crash, relay_log_recovery drops
\* the received backlog, and it comes back OFFLINE (Group Replication does not start on boot) and
\* super_read_only. Not while the tablet runs a START of its own, which the restart would end.
MysqldRestart(s) ==
    /\ Free
    /\ MYSQLD_RESTART /\ up[s] /\ nCrash < MaxCrash /\ NoFault
    /\ lk[s] \notin {"join", "bootw", "boot", "init"}
    /\ IF grp[s] = 0
       THEN /\ sro' = [sro EXCEPT ![s] = TRUE]
            /\ UNCHANGED <<view, prim, grp, electing, dead>>
       ELSE LET i == grp[s] IN
            /\ LeaveVars(s)
            /\ dead' = [dead EXCEPT ![i] = @ \/ ~(2 * (Cardinality(view[i]) - 1) > Cardinality(view[i]))
                                               \/ Cardinality(view[i]) = 1]
    /\ st' = [st EXCEPT ![s] = "none"]
    /\ recv' = [recv EXCEPT ![s] = {}]
    /\ async' = [async EXCEPT ![s] = NoServer]
    /\ err' = [err EXCEPT ![s] = FALSE]
    /\ nCrash' = nCrash + 1
    /\ UNCHANGED <<up, exec, hist, nextInc>>
    /\ UNCHANGED <<vT, vS, vO, vH, nLeave, nLoss, nExpire, nProbe>>
    /\ UNCHANGED <<voters, ovot, vMinor, vNp, vNd, udReq, gone, everP, vNc, ran>>

----------------------------------------------------------------------------
(* Voter replacement: VTOrc's GroupVotersOutOfDate (computeGroupReplicationVoters,
   updateGroupReplicationVoters, SelectGroupReplicationVoters, policy.SelectVoters) *)

\* updateGroupReplicationVoters' groupUp, on the statuses it read under the shard lock: a reachable
\* member of the recorded incarnation is active with quorum in its view (a member of another incarnation
\* has no quorum for VTOrc). The quorum is MySQL's, of the member's view: not the voter majority.
VotGroupUp ==
    \E s \in Servers : up[s] /\ grp[s] # 0 /\ ~dead[grp[s]] /\ RecOK(grp[s], recInc)

\* SelectVoters with three tablets, one per cell (group_replication_cross_cell), or with every eligible
\* tablet a voter (group_replication): every tablet that has not failed has a seat; the group primary
\* is up, so it keeps its seat. A tablet has failed when it has been unreachable for the replacement
\* grace period and no reachable member sees it as active; a down host is in no group, and the model
\* has no clocks: any set of down hosts may have failed. An empty list is never written.
VotChoices == {Servers \ F : F \in SUBSET {v \in Servers : ~up[v] /\ gone[v]}} \ {{}}

\* the live group of the recorded incarnation, if any, has a primary whose election ended: it executed
\* every transaction its group decided
Settled ==
    \A i \in Incs : Alive(i) /\ RecOK(i, recInc) => prim[i] # NoServer /\ ~electing[prim[i]]

\* the voter replacement grace period passes (the model has no clocks): one expiry marks every host that is
\* down then as failed; with GRACE_SETTLES only once the live group settled after they went down. A single
\* expiry until those hosts restart, and none once the voter writes are used up (gone is then normalized):
\* the selection takes any subset of the failed hosts, so the order of expiries adds no behaviour.
GraceExpire ==
    /\ Free
    /\ VOTERS /\ nVot < MaxVot
    /\ \A v \in Servers : ~gone[v]
    /\ \E v \in Servers : ~up[v]
    /\ GRACE_SETTLES => Settled
    /\ gone' = [v \in Servers |-> ~up[v]]
    /\ UNCHANGED <<vM, vT, vS, vO, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, vNp, vNd, udReq, everP, vNc, err, ran>>

\* the write makes a live view of the recorded incarnation that held fewer than a majority of the old
\* voters hold a majority of the new ones
MinorToMajor(old, new) ==
    \E i \in Incs : Alive(i) /\ RecOK(i, recInc) /\ ~VoterMajIn(old, i) /\ VoterMajIn(new, i)
\* the property's version (vMinor): the view reaches the new majority with members that were listed voters
\* already, i.e. through the removal of voters it lacks. A member that is not a listed voter and is in the
\* view joined it by distributed recovery, which gives it the group's history: re-adding it is not a shrink.
ShrinkToMinor(old, new) ==
    \E i \in Incs : Alive(i) /\ RecOK(i, recInc) /\ ~VoterMajIn(old, i)
                  /\ Cardinality(view[i] \cap old \cap new) >= MajOf(new)

\* the shard lock, the shard record, every tablet's status, the selection; then the write of the list
\* (UpdateShardFields, no compare-and-swap on the list it read, no lock check). VOT_SPLIT makes the
\* write a separate step, so that the statuses can change, and the lease expire, in between.
OVotRead(o) ==
    /\ Free
    /\ VOTERS /\ nVot < MaxVot
    /\ oph[o] = "idle" /\ lockOwner = NoOrc
    /\ VOTERS_NEED_GROUP => VotGroupUp
    /\ \E new \in VotChoices :
        /\ new # voters
        /\ VOTERS_KEEP_MINORITY => ~MinorToMajor(voters, new)
        /\ IF VOT_SPLIT
           THEN /\ oph' = [oph EXCEPT ![o] = "vot"]
                /\ ovot' = [ovot EXCEPT ![o] = [old |-> voters, new |-> new, inc |-> recInc, back |-> FALSE]]
                /\ lockOwner' = o
                /\ UNCHANGED <<voters, vMinor>>
           ELSE /\ voters' = new
                /\ vMinor' = (vMinor \/ ShrinkToMinor(voters, new))
                /\ UNCHANGED <<oph, ovot, lockOwner>>
    /\ nVot' = nVot + 1
    \* once the voter writes are used up, the grace state no longer matters
    /\ gone' = IF nVot + 1 >= MaxVot THEN [v \in Servers |-> FALSE] ELSE gone
    /\ UNCHANGED <<vM, vT, vS, ocand, oexp, otok, oborn, oreq, orep, orp, vH, vB>>
    /\ UNCHANGED <<vNp, vNd, udReq, everP, nPrs, nUndo, nSetRW, err, ran>>

\* the write (UpdateShardFields); with VOT_CAS a compare-and-swap on the list and the incarnation that the
\* read found, which fails if another VTOrc wrote the list, or a bootstrap recorded an incarnation, since
OVotWrite(o) ==
    /\ Free
    /\ oph[o] = "vot"
    /\ LET cas  == ~VOT_CAS \/ (voters = ovot[o].old /\ recInc = ovot[o].inc)
           kept == UNION {Data(k) : k \in ovot[o].new}
           \* "dropped" (recheckVoterChange): a reachable dropped voter that runs a START, is active in a foreign
           \* incarnation, or holds transactions beyond the kept voters' union refuses the write; one active in
           \* the legitimate group does not (it leaves in the same recovery). In the model every dropped voter
           \* was dropped as failed, so "reachable" refuses any reachable one first.
           drop == "dropped" \in VOT_REVALIDATE =>
                       \A d \in ovot[o].old \ ovot[o].new :
                           ~(up[d] /\ (st[d] # "none" \/ (grp[d] # 0 /\ ~RecOK(grp[d], recInc))
                                       \/ ~(Data(d) \subseteq kept)))
           \* "reachable": every voter the model drops had been unreachable for the grace period at the
           \* selection; the write is refused unless that still holds now: none has been reachable since (a
           \* START it runs is not visible as an active member yet, and it may have failed again)
           back == "reachable" \in VOT_REVALIDATE => ~ovot[o].back /\ \A d \in ovot[o].old \ ovot[o].new : ~up[d]
           grpUp == "group" \in VOT_REVALIDATE => VotGroupUp
           \* "primary": the write never drops the current primary of a live group (read fresh)
           keepP == "primary" \in VOT_REVALIDATE =>
                       \A d \in ovot[o].old \ ovot[o].new : ~(up[d] /\ grp[d] # 0 /\ prim[grp[d]] = d)
           w    == cas /\ drop /\ back /\ grpUp /\ keepP IN
       /\ voters' = IF w THEN ovot[o].new ELSE voters
       /\ vMinor' = (vMinor \/ (w /\ ShrinkToMinor(voters, ovot[o].new)))
    /\ oph' = [oph EXCEPT ![o] = "idle"]
    /\ ovot' = [ovot EXCEPT ![o] = NoVot]
    /\ lockOwner' = Unlock(o)
    /\ UNCHANGED <<vM, vT, vS, ocand, oexp, otok, oborn, oreq, orep, orp, vH, vB>>
    /\ UNCHANGED <<vNp, vNd, udReq, gone, everP, vNc, err, ran>>

\* leaveGroupReplication: an active member that is no longer a voter leaves the group (StopGroupReplication,
\* under its action lock) when the group keeps a majority of its members without it; it then replicates
\* asynchronously, which the model leaves out
VLeave(s) ==
    /\ Free
    /\ VOTERS /\ s \notin voters
    /\ up[s] /\ lk[s] = "free" /\ grp[s] # 0 /\ ~dead[grp[s]]
    /\ prim[grp[s]] \notin {s, NoServer}
    /\ 2 * (Cardinality(view[grp[s]]) - 1) > Cardinality(view[grp[s]])
    /\ LeaveVars(s)
    /\ fenced' = [fenced EXCEPT ![s] = FALSE]
    /\ fcPend' = [fcPend EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<up, st, exec, recv, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, serving, lk, dph, dOK, dGen, dFence, snap, majInc, cInc, recentBoot,
                   dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB, vN>>

----------------------------------------------------------------------------
(* PlannedReparentShard and EmergencyReparentShard (group replication paths), by the vtctld Prs under
   the shard lock; DemotePrimary, PromoteReplica, UndoDemotePrimary and SetReadWrite on the tablets *)

\* the reparent ends: the shard lock is released
PFree ==
    /\ pph' = "idle" /\ pcur' = NoServer /\ pel' = NoServer /\ pprev' = 0
    /\ lockOwner' = Unlock(Prs)

\* PRS's preflight (preflightChecks, checkGroupReplicationPrimaryElect): every tablet reachable; the
\* current primary is the PRIMARY tablet (none: an elect that is an ONLINE member with quorum); the
\* elect is a listed voter and an ONLINE member of the current primary's group, which with PRS_LEGIT
\* must be the shard's legitimate group (recorded incarnation, voter majority)
PBegin ==
    /\ Free
    /\ PRS /\ nPrs < MaxPrs /\ pph = "idle" /\ lockOwner = NoOrc /\ everP
    /\ \A v \in Servers : up[v]
    /\ \E el \in voters, cur \in Servers \cup {NoServer} :
        /\ el # cur
        /\ IF cur = NoServer THEN \A t \in Servers : ttype[t] # "P" ELSE ttype[cur] = "P"
        /\ grp[el] # 0 /\ ~dead[grp[el]]
        /\ cur # NoServer => grp[cur] = grp[el]
        /\ PRS_LEGIT => RecOK(grp[el], recInc) /\ VoterMaj(grp[el])
        /\ pcur' = cur /\ pel' = el
        /\ pph' = IF cur = NoServer THEN "promote" ELSE "begun"
    /\ lockOwner' = Prs
    /\ nPrs' = nPrs + 1
    /\ UNCHANGED <<vM, vT, vS, oph, ocand, oexp, otok, oborn, oreq, orep, orp, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, pprev, vNd, udReq, gone, everP, nVot, nUndo, nSetRW, err, ran>>

\* ERS (reparentShardLockedGroupReplication): a reachable ONLINE member of the shard's legitimate group
\* with quorum, which reports its primary (findGroupWithQuorum); the new primary is that primary, or,
\* with --new-primary or when the primary is not eligible, another ONLINE member of the view
\* (chooseGroupReplicationPrimary). ERS demotes nobody: PromoteReplica, then the reparent journal.
EBegin ==
    /\ Free
    /\ ERS /\ nPrs < MaxPrs /\ pph = "idle" /\ lockOwner = NoOrc
    /\ \E q \in Servers, el \in Servers :
        /\ up[q] /\ grp[q] # 0 /\ ~dead[grp[q]]
        /\ PRS_LEGIT => RecOK(grp[q], recInc) /\ VoterMaj(grp[q])
        /\ prim[grp[q]] # NoServer
        /\ el \in view[grp[q]] /\ up[el]
        /\ pel' = el
    /\ pph' = "promote" /\ pcur' = NoServer
    /\ lockOwner' = Prs
    /\ nPrs' = nPrs + 1
    /\ UNCHANGED <<vM, vT, vS, oph, ocand, oexp, otok, oborn, oreq, orep, orp, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, pprev, vNd, udReq, gone, everP, nVot, nUndo, nSetRW, err, ran>>

\* DemotePrimary on the current primary, under its action lock: a PRIMARY tablet whose MySQL is writable
\* stops serving; super_read_only; the demotion is noted (groupReplicationDemoted), so that only the
\* caller makes the tablet serve again. With DEMOTE_FAIL its last step, the read of the primary status,
\* may fail: the handler then reverts (PDemoteFail).
PDemote ==
    /\ Free
    /\ pph = "begun"
    /\ LET c == pcur IN
       /\ up[c] /\ lk[c] = "free"
       /\ serving' = [serving EXCEPT ![c] = IF ttype[c] = "P" /\ ~sro[c] THEN FALSE ELSE @]
       /\ badServe' = [badServe EXCEPT ![c] = IF ttype[c] = "P" /\ ~sro[c] THEN FALSE ELSE @]
       /\ sro' = [sro EXCEPT ![c] = TRUE]
       /\ IF DEMOTE_FAIL
          THEN /\ lk' = [lk EXCEPT ![c] = "demote"]
               /\ dmWasRO' = [dmWasRO EXCEPT ![c] = sro[c]]
               /\ dmWasSrv' = [dmWasSrv EXCEPT ![c] = serving[c]]
               /\ dmR' = [dmR EXCEPT ![c] = FALSE]
               /\ dmF' = [dmF EXCEPT ![c] = FALSE]
               /\ pph' = "demoting"
               /\ UNCHANGED dmt
          ELSE /\ dmt' = [dmt EXCEPT ![c] = ttype[c] = "P"]
               /\ pph' = "demoted"
               /\ UNCHANGED <<lk, vNd>>
    /\ UNCHANGED <<up, grp, st, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc, cInc, recentBoot,
                   breq, bOrc, bPrev>>
    /\ UNCHANGED <<vS, vO, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, pcur, pel, pprev, udReq, gone, everP, vNc, err, ran>>

\* the demotion completes
PDemoteEnd(c) ==
    /\ Free
    /\ lk[c] = "demote"
    /\ dmt' = [dmt EXCEPT ![c] = ttype[c] = "P"]
    /\ lk' = [lk EXCEPT ![c] = "free"]
    /\ dmWasRO' = [dmWasRO EXCEPT ![c] = FALSE] /\ dmWasSrv' = [dmWasSrv EXCEPT ![c] = FALSE]
    /\ dmR' = [dmR EXCEPT ![c] = FALSE] /\ dmF' = [dmF EXCEPT ![c] = FALSE]
    /\ pph' = IF pph = "demoting" /\ pcur = c THEN "demoted" ELSE pph
    /\ UNCHANGED <<vM, ttype, serving, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc, cInc,
                   recentBoot, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, pcur, pel, pprev, udReq, gone, everP, vNc, err, ran>>

\* the read of the primary status fails after super_read_only was set: demotePrimary's deferred reverts,
\* the last registered first. MySQL was writable: redoPreparedTransactionsAndSetReadWrite, whose only check
\* is checkGroupAllowsReadWrite (not a secondary of a group, no election running); then, if the tablet
\* served, SetServingUnlessGroupReplicationNotServing, which only consults the not-serving reasons. The
\* reparent fails.
PDemoteFail(c) ==
    /\ Free
    /\ lk[c] = "demote"
    /\ ~DEMOTE_REVERT_DECISION
    /\ LET redo == ~dmWasRO[c] /\ ~(grp[c] # 0 /\ ~IsPrimQ(c)) /\ ~(IsPrimQ(c) /\ electing[c])
           srv  == dmWasSrv[c] /\ ttype[c] = "P" /\ ~dmR[c] /\ ~serving[c]
       IN /\ sro' = [sro EXCEPT ![c] = IF redo THEN FALSE ELSE @]
          /\ fenceUndone' = (fenceUndone \/ (redo /\ dmF[c]))
          /\ serving' = [serving EXCEPT ![c] = IF srv THEN TRUE ELSE @]
          /\ badServe' = [badServe EXCEPT ![c] = IF srv THEN ~LegitMaj(c) ELSE @]
    /\ lk' = [lk EXCEPT ![c] = "free"]
    /\ dmWasRO' = [dmWasRO EXCEPT ![c] = FALSE] /\ dmWasSrv' = [dmWasSrv EXCEPT ![c] = FALSE]
    /\ dmR' = [dmR EXCEPT ![c] = FALSE] /\ dmF' = [dmF EXCEPT ![c] = FALSE]
    /\ IF pph = "demoting" /\ pcur = c THEN PFree ELSE UNCHANGED <<vNp, lockOwner>>
    /\ UNCHANGED <<up, grp, st, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc, cInc, recentBoot,
                   dmt, breq, bOrc, bPrev>>
    /\ UNCHANGED <<vS, oph, ocand, oexp, otok, oborn, oreq, orep, orp>>
    /\ UNCHANGED <<acked, nextTx, bootFrom, decisionAck, minorityAck, adopted, wfail, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, udReq, gone, everP, vNc, err, ran>>

\* DEMOTE_REVERT_DECISION: the revert waits for the end of an election, then decides under the action lock as
\* UndoDemotePrimary does: the not-serving generation and the fence snapshot (DrSnap, here), MySQL's status
\* (DrRead), then MySQL writable again only if it was writable and the decision allows it, the fence settled,
\* and the tablet serves again only if it served and no not-serving reason was set since the generation
\* (DrAct). Refused: the tablet stays PRIMARY, not serving, MySQL read-only. The reparent fails.
DrSnap(c) ==
    /\ Free
    /\ DEMOTE_REVERT_DECISION /\ lk[c] = "demote"
    /\ ~(IsPrimQ(c) /\ electing[c])
    /\ lk' = [lk EXCEPT ![c] = "sync"]
    /\ dph' = [dph EXCEPT ![c] = "dr_snap"]
    /\ dGen' = [dGen EXCEPT ![c] = FALSE]
    /\ dFence' = [dFence EXCEPT ![c] = FALSE]
    /\ Refresh(c)
    /\ dmR' = [dmR EXCEPT ![c] = FALSE] /\ dmF' = [dmF EXCEPT ![c] = FALSE]
    /\ IF pph = "demoting" /\ pcur = c THEN PFree ELSE UNCHANGED <<vNp, lockOwner>>
    /\ UNCHANGED <<vM, ttype, serving, dOK, snap, fenced, fcPend, majInc, recentBoot, dmt, breq, bOrc,
                   bPrev, badServe>>
    /\ UNCHANGED <<vS, oph, ocand, oexp, otok, oborn, oreq, orep, orp, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, dmWasRO, dmWasSrv, udReq, gone, everP, vNc, err, ran>>
DrRead(c) ==
    /\ dph[c] = "dr_snap"
    /\ dOK' = [dOK EXCEPT ![c] = ServeOK(c, Rec(c))]
    /\ dph' = [dph EXCEPT ![c] = "dr_read"]
    /\ UNCHANGED <<vM, ttype, serving, lk, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB, vN>>
DrAct(c) ==
    /\ dph[c] = "dr_read"
    /\ LET ok  == dOK[c] /\ ~(FENCE_SNAPSHOT /\ dFence[c])
           wr  == ok /\ ~dmWasRO[c]
           srv == ok /\ dmWasSrv[c] /\ ttype[c] = "P" /\ ~dGen[c]
       IN /\ sro' = [sro EXCEPT ![c] = IF wr THEN FALSE ELSE @]
          /\ fenced' = [fenced EXCEPT ![c] = IF wr THEN FALSE ELSE @]
          /\ fenceUndone' = (fenceUndone \/ (wr /\ dFence[c]))
          /\ serving' = [serving EXCEPT ![c] = IF srv THEN TRUE ELSE IF ok THEN @ ELSE FALSE]
          /\ badServe' = [badServe EXCEPT ![c] = IF srv \/ ~ok THEN FALSE ELSE @]
    /\ lk' = [lk EXCEPT ![c] = "free"]
    /\ dph' = [dph EXCEPT ![c] = "idle"]
    /\ dOK' = [dOK EXCEPT ![c] = FALSE]
    /\ dGen' = [dGen EXCEPT ![c] = FALSE]
    /\ dFence' = [dFence EXCEPT ![c] = FALSE]
    /\ dmWasRO' = [dmWasRO EXCEPT ![c] = FALSE] /\ dmWasSrv' = [dmWasSrv EXCEPT ![c] = FALSE]
    /\ UNCHANGED <<up, grp, st, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, snap, fcPend, majInc, cInc, recentBoot, dmt, breq, bOrc, bPrev>>
    /\ UNCHANGED <<vS, vO, vB, acked, nextTx, bootFrom, decisionAck, minorityAck, adopted, wfail>>
    /\ UNCHANGED <<voters, ovot, vMinor, vNp, dmR, dmF, udReq, gone, everP, vNc, err, ran>>

\* WaitForPosition on the elect: it applied what the demoted primary executed; or it times out, and PRS
\* undoes the demotion (UndoDemotePrimary, which waits for the old primary's action lock) and fails
PWait ==
    /\ Free
    /\ pph = "demoted"
    /\ \/ /\ up[pel] /\ exec[pcur] \subseteq exec[pel]
          /\ pph' = "promote"
          /\ UNCHANGED <<pcur, pel, pprev, lockOwner, udReq>>
       \/ /\ udReq' = [udReq EXCEPT ![pcur] = up[pcur] \/ @]
          /\ PFree
    /\ UNCHANGED <<vM, vT, vS, oph, ocand, oexp, otok, oborn, oreq, orep, orp, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, vNd, gone, everP, vNc, err, ran>>

\* PromoteReplica on the elect, under its action lock (promoteGroupMemberLocked): on an ONLINE member that
\* is not the primary, group_replication_set_as_primary, which Group Replication carries out at once: the
\* old primary becomes a secondary, super_read_only, and the elect's election starts. On a member without
\* quorum, or while the group has no primary, it fails. On a MySQL outside of any group, the asynchronous
\* path: changeTypeLocked, whose decision refuses to serve.
PPromote ==
    /\ Free
    /\ pph = "promote"
    /\ up[pel] /\ lk[pel] = "free"
    /\ LET e == pel
           i == grp[e]
       IN IF i # 0 /\ (dead[i] \/ prim[i] = NoServer)
          THEN /\ PFree
               /\ UNCHANGED <<prim, electing, sro, lk>>
          ELSE /\ IF i # 0 /\ prim[i] # e
                  THEN /\ prim' = [prim EXCEPT ![i] = e]
                       /\ electing' = [electing EXCEPT ![e] = TRUE, ![prim[i]] = FALSE]
                       /\ sro' = [sro EXCEPT ![prim[i]] = TRUE]
                  ELSE UNCHANGED <<prim, electing, sro>>
               /\ lk' = [lk EXCEPT ![e] = "prom"]
               /\ pph' = "prom_wait"
               /\ UNCHANGED <<pcur, pel, pprev, lockOwner>>
    /\ UNCHANGED <<up, grp, st, exec, recv, view, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, serving, dph, dOK, dGen, dFence, snap, fenced, fcPend, majInc, cInc,
                   recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, oph, ocand, oexp, otok, oborn, oreq, orep, orp, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, vNd, udReq, gone, everP, vNc, err, ran>>

\* waitForGroupPrimaryElected, then changeTypeLocked(PRIMARY): the decision of PrSnap, PrRead, PrAct; or the
\* RPC's deadline passes while MySQL is not the primary of its group with its election ended
PPromote2(e) ==
    /\ Free
    /\ lk[e] = "prom"
    /\ IF (IsPrimQ(e) /\ ~electing[e]) \/ grp[e] = 0
       THEN /\ lk' = [lk EXCEPT ![e] = "sync"]
            /\ dph' = [dph EXCEPT ![e] = "pr_snap"]
            /\ dGen' = [dGen EXCEPT ![e] = FALSE]
            /\ dFence' = [dFence EXCEPT ![e] = FALSE]
            /\ Refresh(e)
            /\ UNCHANGED <<vNp, lockOwner>>
       ELSE /\ lk' = [lk EXCEPT ![e] = "free"]
            /\ UNCHANGED <<dph, dGen, dFence, cInc>>
            /\ IF pph = "prom_wait" /\ pel = e THEN PFree ELSE UNCHANGED <<vNp, lockOwner>>
    /\ UNCHANGED <<vM, ttype, serving, dOK, snap, fenced, fcPend, majInc, recentBoot, dmt, breq, bOrc,
                   bPrev, badServe>>
    /\ UNCHANGED <<vS, oph, ocand, oexp, otok, oborn, oreq, orep, orp, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, vNd, udReq, gone, everP, vNc, err, ran>>

\* the promotion's decision is over: PopulateReparentJournal, a write on the new primary that succeeds only
\* if MySQL accepts it (a client's Commit in the same state covers it), and the reparent ends
PEnd ==
    /\ Free
    /\ pph = "prom_wait" /\ lk[pel] # "prom" /\ dph[pel] = "idle"
    /\ PFree
    /\ UNCHANGED <<vM, vT, vS, oph, ocand, oexp, otok, oborn, oreq, orep, orp, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, vNd, udReq, gone, everP, vNc, err, ran>>

\* an RPC of the reparent fails: its tablet is down, or its handler ended with the tablet's crash
PAbort ==
    /\ Free
    /\ \/ pph = "begun" /\ ~up[pcur]
       \/ pph = "demoting" /\ lk[pcur] # "demote"
       \/ pph \in {"promote", "init"} /\ ~up[pel]
       \/ pph = "prom_wait" /\ lk[pel] = "free" /\ dph[pel] = "idle" /\ ~up[pel]
       \/ pph = "init_run" /\ lk[pel] # "init"
    /\ PFree
    /\ UNCHANGED <<vM, vT, vS, oph, ocand, oexp, otok, oborn, oreq, orep, orp, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, vNd, udReq, gone, everP, vNc, err, ran>>

\* VTOrc's PrimaryIsReadOnly or PrimaryCurrentTypeMismatch: UndoDemotePrimary on a PRIMARY tablet whose
\* MySQL is read-only; with UNDO_MATCH only on the primary of the shard's legitimate group
\* (matchPrimaryOfGroupShard). The recovery's shard lock only orders it with other recoveries.
OUndo ==
    /\ Free
    /\ nUndo < MaxUndo
    /\ \E s \in Servers :
        /\ up[s] /\ ttype[s] = "P" /\ sro[s] /\ ~udReq[s]
        /\ UNDO_MATCH => LegitMaj(s)
        /\ udReq' = [udReq EXCEPT ![s] = TRUE]
    /\ nUndo' = nUndo + 1
    /\ UNCHANGED <<vM, vT, vS, vO, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, vNp, vNd, gone, everP, nVot, nPrs, nSetRW, err, ran>>

\* UndoDemotePrimary, under the action lock: waitForGroupElectionEnd, the not-serving generation, the
\* fence snapshot; MySQL's status: refused on a secondary of a group, and with UNDO_CHECK unless the
\* serving invariant holds (groupReplicationServingDecision); then MySQL writable (redo, checkGroupAllows-
\* ReadWrite: no election running), the fence settled, the demotion over, the tablet serves unless a
\* not-serving reason was set since the generation
UndoOK(s) ==
    /\ ~(grp[s] # 0 /\ ~IsPrimQ(s)) /\ ~(IsPrimQ(s) /\ electing[s])
    /\ UNDO_CHECK => ServeOK(s, Rec(s))
UdSnap(s) ==
    /\ Free
    /\ udReq[s] /\ up[s] /\ lk[s] = "free"
    /\ udReq' = [udReq EXCEPT ![s] = FALSE]
    /\ lk' = [lk EXCEPT ![s] = "sync"]
    /\ dph' = [dph EXCEPT ![s] = "ud_snap"]
    /\ dGen' = [dGen EXCEPT ![s] = FALSE]
    /\ dFence' = [dFence EXCEPT ![s] = FALSE]
    /\ Refresh(s)
    /\ UNCHANGED <<vM, ttype, serving, dOK, snap, fenced, fcPend, majInc, recentBoot, dmt, breq, bOrc,
                   bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, vNp, vNd, gone, everP, vNc, err, ran>>
UdRead(s) ==
    /\ dph[s] = "ud_snap"
    /\ dOK' = [dOK EXCEPT ![s] = UndoOK(s)]
    /\ dph' = [dph EXCEPT ![s] = "ud_read"]
    /\ UNCHANGED <<vM, ttype, serving, lk, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB, vN>>
UdAct(s) ==
    /\ dph[s] = "ud_read"
    /\ LET ok == dOK[s] /\ ~(FENCE_SNAPSHOT /\ dFence[s])
           srv == ok /\ ttype[s] = "P" /\ ~dGen[s]
       IN /\ sro' = [sro EXCEPT ![s] = IF ok THEN FALSE ELSE @]
          /\ fenced' = [fenced EXCEPT ![s] = IF ok THEN FALSE ELSE @]
          /\ fenceUndone' = (fenceUndone \/ (ok /\ dFence[s]))
          /\ dmt' = [dmt EXCEPT ![s] = IF ok THEN FALSE ELSE @]
          /\ serving' = [serving EXCEPT ![s] = IF srv THEN TRUE ELSE @]
          \* without UNDO_CHECK the tablet serves on a decision that did not check the serving invariant
          /\ badServe' = [badServe EXCEPT ![s] = IF srv THEN ~UNDO_CHECK /\ ~LegitMaj(s) ELSE @]
    /\ lk' = [lk EXCEPT ![s] = "free"]
    /\ dph' = [dph EXCEPT ![s] = "idle"]
    /\ dOK' = [dOK EXCEPT ![s] = FALSE]
    /\ dGen' = [dGen EXCEPT ![s] = FALSE]
    /\ dFence' = [dFence EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<up, grp, st, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, snap, fcPend, majInc, cInc, recentBoot, breq, bOrc, bPrev>>
    /\ UNCHANGED <<vS, vO, vB, acked, nextTx, bootFrom, decisionAck, minorityAck, adopted, wfail, vN>>

\* SetReadOnly(false) (SetReadWrite: an operator, PRS's recovery of a partial promotion), under the action
\* lock: the fence snapshot; checkGroupAllowsReadWrite; with SETRW_CHECK the serving invariant; MySQL
\* writable, and the fence settled. The tablet's serving state is left as it is.
RwOK(s) ==
    /\ ~(grp[s] # 0 /\ ~IsPrimQ(s)) /\ ~(IsPrimQ(s) /\ electing[s])
    /\ SETRW_CHECK => ServeOK(s, Rec(s))
RwSnap(s) ==
    /\ Free
    /\ nSetRW < MaxSetRW /\ up[s] /\ lk[s] = "free"
    /\ lk' = [lk EXCEPT ![s] = "sync"]
    /\ dph' = [dph EXCEPT ![s] = "rw_snap"]
    /\ dGen' = [dGen EXCEPT ![s] = FALSE]
    /\ dFence' = [dFence EXCEPT ![s] = FALSE]
    /\ Refresh(s)
    /\ nSetRW' = nSetRW + 1
    /\ UNCHANGED <<vM, ttype, serving, dOK, snap, fenced, fcPend, majInc, recentBoot, dmt, breq, bOrc,
                   bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, vNp, vNd, udReq, gone, everP, nVot, nPrs, nUndo, err, ran>>
RwRead(s) ==
    /\ dph[s] = "rw_snap"
    /\ dOK' = [dOK EXCEPT ![s] = RwOK(s)]
    /\ dph' = [dph EXCEPT ![s] = "rw_read"]
    /\ UNCHANGED <<vM, ttype, serving, lk, dGen, dFence, snap, fenced, fcPend, majInc,
                   cInc, recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB, vN>>
RwAct(s) ==
    /\ dph[s] = "rw_read"
    /\ LET ok == dOK[s] /\ ~(FENCE_SNAPSHOT /\ dFence[s]) IN
       /\ sro' = [sro EXCEPT ![s] = IF ok THEN FALSE ELSE @]
       /\ fenced' = [fenced EXCEPT ![s] = IF ok THEN FALSE ELSE @]
       /\ fenceUndone' = (fenceUndone \/ (ok /\ dFence[s]))
    /\ lk' = [lk EXCEPT ![s] = "free"]
    /\ dph' = [dph EXCEPT ![s] = "idle"]
    /\ dOK' = [dOK EXCEPT ![s] = FALSE]
    /\ dGen' = [dGen EXCEPT ![s] = FALSE]
    /\ dFence' = [dFence EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<up, grp, st, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, serving, snap, fcPend, majInc, cInc, recentBoot, dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vB, acked, nextTx, bootFrom, decisionAck, minorityAck, adopted, wfail, vN>>

----------------------------------------------------------------------------
(* The initial promotion: PlannedReparentShard on a shard that never had a primary
   (performInitialPromotion), InitPrimary on the tablet, then the incarnation recorded *)

\* case (1): no tablet is PRIMARY and the shard record has no primary term; every tablet reachable; the
\* elect's executed set holds every tablet's executed and received sets (checkPrimaryElectContainsAll-
\* Positions). The voters (selectInitialVoters) are all three already. The incarnation the shard record
\* lists is what the new one replaces (a compare-and-swap).
PIBegin ==
    /\ Free
    /\ INIT_PRS /\ nPrs < MaxPrs /\ pph = "idle" /\ lockOwner = NoOrc
    /\ ~everP /\ \A t \in Servers : ttype[t] # "P"
    /\ \A v \in Servers : up[v]
    \* INIT_GUARD (checkShardHasNoGroup, under the shard lock): no live bootstrap intent, no incarnation
    \* recorded, no tablet an active member of a group (its status read now)
    /\ "intent" \in INIT_GUARD => ~IntentLive(intent)
    /\ "inc" \in INIT_GUARD => recInc = 0
    /\ "active" \in INIT_GUARD => \A v \in Servers : grp[v] = 0
    /\ \E e \in voters :
        /\ \A v \in Servers : Data(v) \subseteq exec[e]
        /\ pel' = e
    /\ pprev' = recInc
    /\ pph' = "init"
    /\ lockOwner' = Prs
    /\ nPrs' = nPrs + 1
    /\ UNCHANGED <<vM, vT, vS, oph, ocand, oexp, otok, oborn, oreq, orep, orp, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, pcur, vNd, udReq, gone, everP, nVot, nUndo, nSetRW, err, ran>>

\* InitPrimary gets the action lock; bootstrapGroupForInitPrimaryLocked: a member of a group with other
\* members refuses; a member alone in its group keeps it; otherwise MySQL's bootstrap START, without
\* the checks of VTOrc's bootstrap (no intent, no required transactions), a new epoch. A START in
\* progress is refused (the model leaves out stopOngoingGroupStartLocked here).
IP1(e) ==
    /\ Free
    /\ pph = "init" /\ pel = e /\ up[e] /\ lk[e] = "free"
    /\ IF (grp[e] # 0 /\ Cardinality(view[grp[e]]) > 1) \/ (grp[e] = 0 /\ st[e] # "none")
       THEN /\ PFree
            /\ UNCHANGED <<st, lk, fenced, fcPend, bPrev>>
       ELSE /\ lk' = [lk EXCEPT ![e] = "init"]
            /\ pph' = "init_run"
            /\ IF grp[e] = 0
               THEN /\ st' = [st EXCEPT ![e] = "boot"]
                    /\ fenced' = [fenced EXCEPT ![e] = FALSE]
                    /\ fcPend' = [fcPend EXCEPT ![e] = FALSE]
                    /\ bPrev' = [bPrev EXCEPT ![e] = recInc]
               ELSE UNCHANGED <<st, fenced, fcPend, bPrev>>
            /\ UNCHANGED <<pcur, pel, pprev, lockOwner>>
    /\ UNCHANGED <<up, grp, sro, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, serving, dph, dOK, dGen, dFence, snap, majInc, cInc, recentBoot, dmt,
                   breq, bOrc, badServe>>
    /\ UNCHANGED <<vS, oph, ocand, oexp, otok, oborn, oreq, orep, orp, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, vNd, udReq, gone, everP, vNc, err, ran>>

\* waitForGroupPrimaryElected; checkGroupAllowsReadWrite; InitPrimary makes MySQL writable BEFORE its
\* decision to serve (the exception: the sidecar database, and the reparent journal its caller writes);
\* then changeTypeLocked(PRIMARY), whose decision refuses to serve (PrSnap, PrRead, PrAct): the group has
\* one voter. If MySQL is not the primary of its group (the group died meanwhile), InitPrimary fails.
IP2(e) ==
    /\ Free
    /\ pph = "init_run" /\ pel = e /\ lk[e] = "init" /\ st[e] = "none"
    /\ ~(IsPrimQ(e) /\ electing[e])
    /\ IF IsPrimQ(e)
       THEN /\ sro' = [sro EXCEPT ![e] = IF INIT_WRITABLE THEN FALSE ELSE @]
            /\ lk' = [lk EXCEPT ![e] = "sync"]
            /\ dph' = [dph EXCEPT ![e] = "pr_snap"]
            /\ dGen' = [dGen EXCEPT ![e] = FALSE]
            /\ dFence' = [dFence EXCEPT ![e] = FALSE]
            /\ Refresh(e)
            /\ pph' = "init_dec"
            /\ UNCHANGED <<pcur, pel, pprev, lockOwner>>
       ELSE /\ lk' = [lk EXCEPT ![e] = "free"]
            /\ PFree
            /\ UNCHANGED <<sro, dph, dGen, dFence, cInc>>
    /\ UNCHANGED <<up, grp, st, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, serving, dOK, snap, fenced, fcPend, majInc, recentBoot, dmt, breq, bOrc,
                   bPrev, badServe>>
    /\ UNCHANGED <<vS, oph, ocand, oexp, otok, oborn, oreq, orep, orp, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, vNd, udReq, gone, everP, vNc, err, ran>>

\* RecordGroupReplicationIncarnation: the elect's view incarnation, written with a compare-and-swap on the
\* incarnation read before InitPrimary (writeGroupReplicationIncarnation); the reparent ends
PIRecord ==
    /\ Free
    /\ pph = "init_dec" /\ dph[pel] = "idle" /\ lk[pel] # "sync"
    /\ \E fail \in IF INIT_RECORD_FAIL THEN BOOLEAN ELSE {FALSE} :
       LET inc == grp[pel]
           ok  == ~fail /\ inc # 0 /\ (recInc = pprev \/ recInc = inc)
       IN /\ recInc' = IF ok THEN inc ELSE recInc
          /\ intent' = IF ok /\ ~(recInc = inc /\ KeepsIntent(inc)) THEN NoIntent ELSE intent
          /\ intExp' = IF ok /\ ~(recInc = inc /\ KeepsIntent(inc)) THEN FALSE ELSE intExp
    /\ PFree
    /\ UNCHANGED <<vM, vT, nextTok, newest, oph, ocand, oexp, otok, oborn, oreq, orep, orp, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, vNd, udReq, gone, everP, vNc, err, ran>>

\* FENCE_ON_DROP: the fence check, after the shard record changed, fences a PRIMARY tablet whose server is no
\* longer a listed voter, as it fences a shrink (FcAct's fence branch, atomic)
FenceDrop(s) ==
    /\ Free
    /\ FENCE_ON_DROP /\ up[s] /\ ttype[s] = "P" /\ s \notin voters
    /\ lk[s] \notin {"bootw", "boot"}
    /\ ~(fenced[s] /\ sro[s] /\ ~serving[s])
    /\ sro' = [sro EXCEPT ![s] = TRUE]
    /\ fenced' = [fenced EXCEPT ![s] = TRUE]
    /\ dFence' = [dFence EXCEPT ![s] = (dph[s] # "idle") \/ @]
    /\ serving' = [serving EXCEPT ![s] = FALSE]
    /\ badServe' = [badServe EXCEPT ![s] = FALSE]
    /\ dGen' = [dGen EXCEPT ![s] = (dph[s] # "idle") \/ @]
    /\ dmF' = [dmF EXCEPT ![s] = @ \/ lk[s] = "demote"]
    /\ dmR' = [dmR EXCEPT ![s] = @ \/ lk[s] = "demote"]
    /\ UNCHANGED <<up, grp, st, electing, exec, recv, view, prim, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, lk, dph, dOK, snap, fcPend, majInc, cInc, recentBoot, dmt, breq, bOrc, bPrev>>
    /\ UNCHANGED <<vS, vO, vH, vB>>
    /\ UNCHANGED <<voters, ovot, vMinor, pph, pcur, pel, pprev, dmWasRO, dmWasSrv, udReq, gone, everP,
                   nVot, nPrs, nUndo, nSetRW, err, ran>>

\* NONVOTER_LEAVES: the sync loop makes an active member that is not a listed voter leave (a clean STOP)
NLeave(s) ==
    /\ Free
    /\ NONVOTER_LEAVES /\ s \notin voters
    /\ up[s] /\ lk[s] = "free" /\ grp[s] # 0 /\ ~dead[grp[s]]
    /\ LeaveVars(s)
    /\ fenced' = [fenced EXCEPT ![s] = FALSE]
    /\ fcPend' = [fcPend EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<up, st, exec, recv, hist, dead, nextInc, async>>
    /\ UNCHANGED <<ttype, serving, lk, dph, dOK, dGen, dFence, snap, majInc, cInc, recentBoot,
                   dmt, breq, bOrc, bPrev, badServe>>
    /\ UNCHANGED <<vS, vO, vH, vB, vN>>

\* ADOPT_UNRECORDED (adoptUnrecordedGroup, GroupBootstrapNotRecorded without an intent): under the shard lock,
\* while no incarnation is recorded and no intent is live, VTOrc records the incarnation of the group whose
\* ONLINE primary has quorum, when every voter answers, no tablet runs a START or is active in another group,
\* and the primary executed every transaction a voter executed or received; a compare-and-swap on the empty
\* incarnation and the absence of a live intent, which clears the intent. Atomic: the re-read and the write.
OAdoptUnrec ==
    /\ Free
    /\ ADOPT_UNRECORDED /\ lockOwner = NoOrc /\ \E o \in Orcs : oph[o] = "idle"
    /\ recInc = 0 /\ ~IntentLive(intent)
    /\ \E p \in Servers :
        /\ IsPrimQ(p)
        /\ \A v \in voters : up[v]
        /\ \A t \in Servers : up[t] => (st[t] = "none" /\ (grp[t] # 0 => grp[t] = grp[p]))
        /\ \A v \in voters : Data(v) \subseteq exec[p]
        /\ recInc' = grp[p]
    /\ intent' = NoIntent
    /\ intExp' = FALSE
    /\ UNCHANGED <<vM, vT, nextTok, newest, vO, vH, vB, vN>>

\* every action of the second milestone
Step2 ==
    \/ \E s \in Servers :
        \/ CommitAlone(s) \/ MysqldRestart(s) \/ VLeave(s) \/ PDemoteEnd(s) \/ PDemoteFail(s) \/ PPromote2(s)
        \/ DrSnap(s) \/ DrRead(s) \/ DrAct(s)
        \/ UdSnap(s) \/ UdRead(s) \/ UdAct(s) \/ RwSnap(s) \/ RwRead(s) \/ RwAct(s) \/ IP1(s) \/ IP2(s)
        \/ FenceDrop(s) \/ NLeave(s)
    \/ \E o \in Orcs : OVotRead(o) \/ OVotWrite(o)
    \/ GraceExpire \/ OAdoptUnrec \/ PBegin \/ EBegin \/ PDemote \/ PWait \/ PPromote \/ PEnd \/ PAbort \/ OUndo \/ PIBegin \/ PIRecord

----------------------------------------------------------------------------
\* STUCK_CHECK: a state is a legitimate end state when a legitimate primary serves, or a bound is reached
Healthy == \E p \in Servers : ttype[p] = "P" /\ serving[p] /\ CanAccept(p) /\ LegitMaj(p)
\* ... or when only the expiry of the live intent, which this check leaves out, can end the wait that the
\* design accepted before REPROBE_STALE_INTENT: the intent's bootstrap RPC failed without a definitive
\* refusal, and its target is in no group to adopt, nor runs a START. EXPIRY_WAIT_OK accepts it; without it,
\* the check finds it, and the re-probe ends it. A re-probe can also fail without a definitive refusal (a
\* timeout, a lost reply): the same wait, once the re-probe budget is used up, is the bound.
WaitsForExpiry ==
    wfail /\ IntentLive(intent) /\ grp[intent.tgt] = 0 /\ st[intent.tgt] = "none"
Done ==
    /\ Free
    /\ STUCK_CHECK
    /\ \/ Healthy \/ nextInc > MaxInc \/ nextTok > MaxTok
       \/ WaitsForExpiry /\ (EXPIRY_WAIT_OK \/ (REPROBE_STALE_INTENT /\ nProbe >= MaxProbe))
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
    \/ Step2
    \/ Done

Next == Step

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
(* Properties *)

\* Every acknowledged write is in the legitimate group's history, and every MySQL that accepts
\* commits holds it.
NoLostAck ==
    /\ recInc # 0 => acked \subseteq hist[recInc]
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

\* No voter write lets a live view of the recorded incarnation that held fewer than a majority of the
\* old voters hold a majority of the new ones.
NoVoterMinority == ~vMinor

\* No two live groups were bootstrapped on different tablets from the same recorded incarnation.
\* (A bootstrap of the same target again is allowed: the tablet stops a START still in progress first.)
NoDualBootstrap ==
    \A i, j \in Incs :
        (/\ bootFrom[i][2] # NoServer /\ bootFrom[j][2] # NoServer
         /\ bootFrom[i][1] = bootFrom[j][1] /\ bootFrom[i][2] # bootFrom[j][2])
            => ~(Alive(i) /\ Alive(j))

Symm == Permutations(Servers) \cup Permutations(Orcs)
\* for a configuration without VTOrc (Orcs = {})
SymmS == Permutations(Servers)
=============================================================================
