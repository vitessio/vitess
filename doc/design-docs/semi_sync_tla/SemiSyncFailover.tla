-------------------------- MODULE SemiSyncFailover --------------------------
(***************************************************************************)
(* Safety model of unplanned failover with MySQL semi-sync replication in  *)
(* Vitess: one shard of three tablets (mysqld + vttablet), the shard       *)
(* record and its lock, clients through vtgate, and one or two VTOrcs that *)
(* run EmergencyReparentShard (ERS) and the single-tablet recoveries.      *)
(*                                                                         *)
(* It follows the code on main (see README.md next to this file for the Go *)
(* code each action models, the abstractions, and what is left out).       *)
(* Each suspected bug has a CONSTANT switch: TRUE is the code on main,     *)
(* FALSE a proposed fix, so the model reproduces the bug and checks the    *)
(* fix.                                                                    *)
(***************************************************************************)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Tablets,        \* the tablets of the shard (all REPLICA or PRIMARY eligible)
    Orcs,           \* the VTOrcs
    None,
    InitPrimary,    \* the tablet that is the primary initially
    MaxTx,          \* bound: client transactions
    MaxCrash,       \* bound: mysqld crashes (kill -9 or host crash)
    MaxCut,         \* bound: network faults (a link cut or a node isolated)
    MaxExpire,      \* bound: shard lock leases that expire under a live holder
    MaxTabletRestart, \* bound: vttablet restarts of a replica (initializeReplication)
    MaxERS,         \* bound: ERS attempts (all VTOrcs together)
    \* --- the code on main (TRUE) or a fix (FALSE) ---
    RELAY_LOG_RECOVERY, \* relay_log_recovery=1: a mysqld restart drops the unapplied relay log
    REPOINT_DISCARDS,   \* STOP REPLICA + CHANGE REPLICATION SOURCE drops the unapplied relay log
    SRS_KEEPS_SESSIONS, \* SetReplicationSource on a PRIMARY tablet changes its type without
                        \* killing the sessions waiting for a semi-sync ACK, then disables
                        \* source-side semi-sync, which completes their commits
    SRS_STALE_ERRANT,   \* SetReplicationSource reads the position for the errant GTID check
                        \* before it disables source-side semi-sync
    SRS_KEEPS_WRITABLE, \* SetReplicationSource on a PRIMARY tablet leaves MySQL writable
    STARTUP_REPOINT,    \* a restarting replica's vttablet repoints it to the shard record's
                        \* primary without the shard lock (initializeReplication)
    RETRYING_IO,        \* ERS's StopReplicationAndGetStatus(IOTHREADONLY) does not stop a
                        \* receiver that is not connected (it keeps retrying the old primary)
    ERS_STALE_RECORD,   \* ERS runs on a shard record that a newer primary has not replaced yet
                        \* (its shard_sync is asynchronous); it demotes that newer primary
    DETACHED_REPOINT,   \* a successful ERS leaves its SetReplicationSource RPCs running after it
                        \* releases the shard lock (up to --wait-replicas-timeout)
    PROMOTE_DISCARDS,   \* PromoteReplica runs STOP REPLICA + RESET REPLICA ALL without applying
                        \* what the receiver got after ERS's relay log wait
    FIX_REPLICA_STALE,  \* fixReplica repoints to the shard record's primary even though another
                        \* tablet holds a newer primary term (before its shard_sync published it)
    FIX_PRIMARY_STALE   \* fixPrimary (PrimaryIsReadOnly, PrimarySemiSyncMustBeSet) makes the
                        \* shard record's primary writable without checking for a newer primary

Nodes == Tablets \cup Orcs \cup {"gate", "topo"}
Links == {l \in SUBSET Nodes : Cardinality(l) = 2}
Txs == 1..MaxTx
N == Cardinality(Tablets)

VARIABLES
    \* --- MySQL, per tablet ---
    up,         \* mysqld running
    binlog,     \* Seq(Txs): the binary log (every executed transaction, log_replica_updates)
    relay,      \* Seq(Txs): received, not yet applied
    src,        \* the configured replication source, or None
    io,         \* the receiver (IO thread) runs
    ssSrc,      \* rpl_semi_sync_source_enabled
    ssRep,      \* rpl_semi_sync_replica_enabled
    ro,         \* super_read_only
    wait,       \* transactions in the binlog that wait for a semi-sync ACK (AFTER_SYNC)
    \* --- clients ---
    origin,     \* [Txs -> Tablets \cup {None}]: where a transaction was committed first
    live,       \* transactions whose client still waits for the commit's outcome
    acked,      \* transactions acknowledged to their client
    unbacked,   \* history: transactions acknowledged while no other tablet held them
    \* --- vttablet, per tablet ---
    ttype,      \* "PRIMARY", "REPLICA" or "DRAINED" (the tablet's own type and its record)
    serving,    \* the query service serves (a PRIMARY accepts writes only while serving)
    tterm,      \* primary term of the tablet (0: never primary)
    \* --- topo ---
    shardPrimary, term, lock,
    \* --- network ---
    cut,        \* set of links that are down
    \* --- VTOrc ---
    phase,      \* per VTOrc: ERS phase
    ersOld,     \* per VTOrc: the shard record's primary when the ERS locked
    ersId,      \* per VTOrc: the number of its current ERS
    reached,    \* per VTOrc: tablets whose replication the ERS stopped (or that it demoted)
    pos,        \* per VTOrc: [Tablets -> SUBSET Txs]: their positions when reached
    cand,       \* per VTOrc: the tablet ERS promotes
    repointed,  \* per VTOrc: tablets that acknowledged the ERS's SetReplicationSource
    pending,    \* outstanding SetReplicationSource RPCs (they outlive the ERS)
    \* --- bounds ---
    nTx, nCrash, nCut, nExpire, nRestart, nERS

vars == <<up, binlog, relay, src, io, ssSrc, ssRep, ro, wait, origin, live, acked, unbacked,
          ttype, serving, tterm, shardPrimary, term, lock, cut,
          phase, ersOld, ersId, reached, pos, cand, repointed, pending,
          nTx, nCrash, nCut, nExpire, nRestart, nERS>>
mysqlVars == <<up, binlog, relay, src, io, ssSrc, ssRep, ro, wait>>
clientVars == <<origin, live, acked, unbacked>>
tabletVars == <<ttype, serving, tterm>>
orcVars == <<phase, ersOld, ersId, reached, pos, cand, repointed, pending>>
boundVars == <<nTx, nCrash, nCut, nExpire, nRestart, nERS>>

-----------------------------------------------------------------------------
(* Helpers *)

Rng(s) == {s[i] : i \in DOMAIN s}
Link(a, b) == {a, b} \notin cut
Committed(t) == Rng(binlog[t]) \ wait[t]
Combined(t) == Rng(binlog[t]) \cup Rng(relay[t])
PosOf(t, x) == CHOOSE i \in DOMAIN binlog[t] : binlog[t][i] = x
Min(S) == CHOOSE i \in S : \A j \in S : i <= j
Max(S) == CHOOSE i \in S : \A j \in S : i >= j

\* The semi_sync durability policy: every REPLICA (or PRIMARY eligible) tablet other than the
\* primary can ACK, one ACK is needed. DRAINED tablets are not ackers.
Acker(p, t) == t # p /\ ttype[t] \in {"PRIMARY", "REPLICA"}
Ackers(p) == {t \in Tablets : Acker(p, t)}

\* The receiver of r is connected to its source.
Connected(r) == src[r] # None /\ up[r] /\ io[r] /\ up[src[r]] /\ Link(r, src[r])

\* A tablet that accepts the writes vtgate sends it.
Writable(t) == up[t] /\ ~ro[t] /\ ttype[t] = "PRIMARY" /\ serving[t]

\* GTIDs on t that the parent p lacks, ignoring p's own (ErrantGTIDsOnReplica).
ErrantVs(set, p) == {x \in set \ Rng(binlog[p]) : origin[x] # p}

\* Complete the commits in rel: their clients get the OK if they still wait.
Release(t, rel) ==
    /\ wait' = [wait EXCEPT ![t] = @ \ rel]
    /\ acked' = acked \cup (rel \cap live)
    /\ live' = live \ rel

-----------------------------------------------------------------------------
Init ==
    /\ up = [t \in Tablets |-> TRUE]
    /\ binlog = [t \in Tablets |-> <<>>]
    /\ relay = [t \in Tablets |-> <<>>]
    /\ src = [t \in Tablets |-> IF t = InitPrimary THEN None ELSE InitPrimary]
    /\ io = [t \in Tablets |-> t # InitPrimary]
    /\ ssSrc = [t \in Tablets |-> t = InitPrimary]
    /\ ssRep = [t \in Tablets |-> t # InitPrimary]
    /\ ro = [t \in Tablets |-> t # InitPrimary]
    /\ wait = [t \in Tablets |-> {}]
    /\ origin = [x \in Txs |-> None]
    /\ live = {} /\ acked = {} /\ unbacked = {}
    /\ ttype = [t \in Tablets |-> IF t = InitPrimary THEN "PRIMARY" ELSE "REPLICA"]
    /\ serving = [t \in Tablets |-> TRUE]
    /\ tterm = [t \in Tablets |-> IF t = InitPrimary THEN 1 ELSE 0]
    /\ shardPrimary = InitPrimary /\ term = 1 /\ lock = None
    /\ cut = {}
    /\ phase = [o \in Orcs |-> "idle"]
    /\ ersOld = [o \in Orcs |-> None]
    /\ ersId = [o \in Orcs |-> 0]
    /\ reached = [o \in Orcs |-> {}]
    /\ pos = [o \in Orcs |-> [t \in Tablets |-> {}]]
    /\ cand = [o \in Orcs |-> None]
    /\ repointed = [o \in Orcs |-> {}]
    /\ pending = {}
    /\ nTx = 0 /\ nCrash = 0 /\ nCut = 0 /\ nExpire = 0 /\ nRestart = 0 /\ nERS = 0

-----------------------------------------------------------------------------
(* Clients and MySQL replication *)

\* A client commits through vtgate on a writable primary. With source-side semi-sync the commit
\* waits for an ACK (AFTER_SYNC: it is in the binlog, which replicas can read, but not acked);
\* without it, it is acknowledged at once.
Write(t) ==
    /\ nTx < MaxTx /\ Writable(t) /\ Link("gate", t)
    /\ LET x == nTx + 1 IN
       /\ nTx' = x
       /\ origin' = [origin EXCEPT ![x] = t]
       /\ binlog' = [binlog EXCEPT ![t] = Append(@, x)]
       /\ IF ssSrc[t]
            THEN /\ wait' = [wait EXCEPT ![t] = @ \cup {x}]
                 /\ live' = live \cup {x}
                 /\ UNCHANGED <<acked, unbacked>>
            ELSE /\ acked' = acked \cup {x}
                 /\ unbacked' = unbacked \cup {x}
                 /\ UNCHANGED <<wait, live>>
    /\ UNCHANGED <<up, relay, src, io, ssSrc, ssRep, ro, tabletVars, shardPrimary, term, lock, cut,
                   orcVars, nCrash, nCut, nExpire, nRestart, nERS>>

\* The receiver of r reads the next transaction of its source's binlog that r lacks (GTID
\* auto-position) into its relay log and, with semi-sync on both sides, ACKs it: MySQL completes
\* every waiting commit up to that binlog position.
Receive(r) ==
    LET s == src[r] IN
    /\ Connected(r)
    /\ \E i \in DOMAIN binlog[s] : binlog[s][i] \notin Combined(r)
    /\ LET i0 == Min({i \in DOMAIN binlog[s] : binlog[s][i] \notin Combined(r)})
           x == binlog[s][i0]
           rel == {y \in wait[s] : PosOf(s, y) <= i0}
       IN /\ relay' = [relay EXCEPT ![r] = Append(@, x)]
          /\ IF ssRep[r] /\ ssSrc[s]
               THEN Release(s, rel)
               ELSE UNCHANGED <<wait, acked, live>>
    /\ UNCHANGED <<up, binlog, src, io, ssSrc, ssRep, ro, origin, unbacked, tabletVars,
                   shardPrimary, term, lock, cut, orcVars, boundVars>>

\* The applier executes the head of the relay log.
Apply(r) ==
    /\ up[r] /\ relay[r] # <<>>
    /\ LET x == Head(relay[r]) IN
       binlog' = [binlog EXCEPT ![r] = IF x \in Rng(@) THEN @ ELSE Append(@, x)]
    /\ relay' = [relay EXCEPT ![r] = Tail(@)]
    /\ UNCHANGED <<up, src, io, ssSrc, ssRep, ro, wait, clientVars, tabletVars,
                   shardPrimary, term, lock, cut, orcVars, boundVars>>

\* kill -9 of mysqld (or of its host). The clients waiting on it get an error. Crash recovery
\* commits what the binlog holds (sync_binlog=1), so waiting commits stay in the binlog.
Crash(t) ==
    /\ nCrash < MaxCrash /\ up[t]
    /\ nCrash' = nCrash + 1
    /\ up' = [up EXCEPT ![t] = FALSE]
    /\ live' = live \ wait[t]
    /\ wait' = [wait EXCEPT ![t] = {}]
    /\ UNCHANGED <<binlog, relay, src, io, ssSrc, ssRep, ro, origin, acked, unbacked, tabletVars,
                   shardPrimary, term, lock, cut, orcVars, nTx, nCut, nExpire, nRestart, nERS>>

\* mysqld starts again: super_read_only, semi-sync off, replication not started
\* (skip_replica_start); with relay_log_recovery the unapplied relay log is gone.
Restart(t) ==
    /\ ~up[t]
    /\ up' = [up EXCEPT ![t] = TRUE]
    /\ ro' = [ro EXCEPT ![t] = TRUE]
    /\ ssSrc' = [ssSrc EXCEPT ![t] = FALSE]
    /\ ssRep' = [ssRep EXCEPT ![t] = FALSE]
    /\ io' = [io EXCEPT ![t] = FALSE]
    /\ relay' = [relay EXCEPT ![t] = IF RELAY_LOG_RECOVERY THEN <<>> ELSE @]
    /\ UNCHANGED <<binlog, src, wait, clientVars, tabletVars, shardPrimary, term, lock, cut,
                   orcVars, boundVars>>

\* Network faults: one link, or every link of a node; and healing all of them.
CutLink(l) ==
    /\ nCut < MaxCut /\ l \notin cut
    /\ nCut' = nCut + 1
    /\ cut' = cut \cup {l}
    /\ UNCHANGED <<mysqlVars, clientVars, tabletVars, shardPrimary, term, lock, orcVars,
                   nTx, nCrash, nExpire, nRestart, nERS>>

Isolate(n) ==
    /\ nCut < MaxCut
    /\ nCut' = nCut + 1
    /\ cut' = cut \cup {l \in Links : n \in l}
    /\ UNCHANGED <<mysqlVars, clientVars, tabletVars, shardPrimary, term, lock, orcVars,
                   nTx, nCrash, nExpire, nRestart, nERS>>

Heal ==
    /\ cut # {}
    /\ cut' = {}
    /\ UNCHANGED <<mysqlVars, clientVars, tabletVars, shardPrimary, term, lock, orcVars, boundVars>>

-----------------------------------------------------------------------------
(* vttablet *)

\* DemotePrimary(force): stop serving; the sessions still waiting after the shutdown grace
\* period are killed (their clients get an error); then source-side semi-sync is disabled,
\* which completes the waiting commits; then super_read_only. The tablet type is unchanged.
DemoteEffects(t) ==
    /\ serving' = [serving EXCEPT ![t] = FALSE]
    /\ live' = live \ wait[t]
    /\ wait' = [wait EXCEPT ![t] = {}]
    /\ ssSrc' = [ssSrc EXCEPT ![t] = FALSE]
    /\ ro' = [ro EXCEPT ![t] = TRUE]

\* The effects of SetReplicationSource(t, p), as setReplicationSourceLocked on main:
\*  1. a PRIMARY tablet changes its type to REPLICA (DBActionNone: MySQL stays writable)
\*  2. the position for the errant GTID check is read (before semi-sync changes)
\*  3. fixSemiSync(REPLICA) disables source-side semi-sync: waiting commits complete; their
\*     clients get the OK if their sessions were not killed
\*  4. the errant GTID check against p (needs p's PrimaryStatus)
\*  5. CHANGE REPLICATION SOURCE (drops the relay log) when the source changes or a heartbeat
\*     is given, and START REPLICA
\* hb: the RPC passes a heartbeat interval (VTOrc's fixReplica does, ERS does not).
\* The result is "ok" or "errant"; the effects up to step 3 happen in both cases.
SRSEffects(t, p, hb) ==
    LET wasPrimary == ttype[t] = "PRIMARY"
        killFirst == wasPrimary /\ ~SRS_KEEPS_SESSIONS
        stillLive == IF killFirst THEN live \ wait[t] ELSE live
        posRead == IF src[t] = None
                     THEN (IF SRS_STALE_ERRANT THEN Committed(t) ELSE Rng(binlog[t]))
                     ELSE Combined(t)
        errant == ErrantVs(posRead, p) # {}
    IN
    /\ ttype' = [ttype EXCEPT ![t] = IF wasPrimary THEN "REPLICA" ELSE @]
    /\ serving' = [serving EXCEPT ![t] = IF wasPrimary /\ killFirst THEN FALSE ELSE @]
    /\ ro' = [ro EXCEPT ![t] = IF wasPrimary /\ SRS_KEEPS_WRITABLE THEN @ ELSE TRUE]
    /\ ssSrc' = [ssSrc EXCEPT ![t] = FALSE]
    /\ ssRep' = [ssRep EXCEPT ![t] = ttype'[t] = "REPLICA"]
    /\ wait' = [wait EXCEPT ![t] = {}]
    /\ acked' = acked \cup (wait[t] \cap stillLive)
    /\ live' = (live \ wait[t])
    /\ unbacked' = unbacked \cup {x \in wait[t] \cap stillLive :
                                    \A u \in Tablets \ {t} : x \notin Combined(u)}
    /\ IF errant
         THEN UNCHANGED <<src, io, relay>>
         ELSE /\ src' = [src EXCEPT ![t] = p]
              /\ io' = [io EXCEPT ![t] = TRUE]
              /\ relay' = [relay EXCEPT ![t] =
                     IF REPOINT_DISCARDS /\ (src[t] # p \/ hb) THEN <<>> ELSE @]

\* The new primary's shard_sync publishes it in the shard record.
PublishPrimary(t) ==
    /\ ttype[t] = "PRIMARY" /\ shardPrimary # t /\ tterm[t] > term /\ Link(t, "topo")
    /\ shardPrimary' = t /\ term' = tterm[t]
    /\ UNCHANGED <<mysqlVars, clientVars, tabletVars, lock, cut, orcVars, boundVars>>

\* A PRIMARY tablet whose shard_sync sees a newer primary demotes itself and repoints to it.
SelfDemote(t) ==
    LET p == shardPrimary IN
    /\ ttype[t] = "PRIMARY" /\ p # t /\ p # None /\ term > tterm[t] /\ Link(t, "topo")
    /\ up[t] /\ Link(t, p)
    /\ DemoteEffects(t)
    /\ ttype' = [ttype EXCEPT ![t] = "REPLICA"]
    /\ ssRep' = [ssRep EXCEPT ![t] = TRUE]
    \* The errant check reads the position after the demotion: refused if t has commits p lacks.
    /\ IF ErrantVs(Rng(binlog[t]), p) # {}
         THEN UNCHANGED <<src, io, relay>>
         ELSE /\ src' = [src EXCEPT ![t] = p]
              /\ io' = [io EXCEPT ![t] = TRUE]
              /\ relay' = [relay EXCEPT ![t] = <<>>]
    /\ UNCHANGED <<up, binlog, origin, acked, unbacked, tterm, shardPrimary, term, lock, cut,
                   orcVars, boundVars>>

\* A replica's vttablet restarts (crash, OOM, rolling restart). With STARTUP_REPOINT,
\* initializeReplication reads the shard record without the shard lock and repoints the
\* replica to that primary with semi-sync ACKs, undoing an ERS that stopped its receiver.
TabletRestart(t) ==
    LET p == shardPrimary IN
    /\ nRestart < MaxTabletRestart /\ ttype[t] = "REPLICA" /\ up[t]
    /\ nRestart' = nRestart + 1
    /\ IF STARTUP_REPOINT /\ p # None /\ p # t /\ Link(t, "topo")
         THEN /\ src' = [src EXCEPT ![t] = p]
              /\ io' = [io EXCEPT ![t] = TRUE]
              /\ ssRep' = [ssRep EXCEPT ![t] = TRUE]
         ELSE UNCHANGED <<src, io, ssRep>>
    /\ UNCHANGED <<up, binlog, relay, ssSrc, ro, wait, clientVars, tabletVars, shardPrimary, term,
                   lock, cut, orcVars, nTx, nCrash, nCut, nExpire, nERS>>

-----------------------------------------------------------------------------
(* VTOrc *)

\* DeadPrimary: VTOrc cannot reach the primary, and no replica it reaches replicates from it.
DeadPrimary(o) ==
    LET p == shardPrimary IN
    /\ p # None
    /\ ~Link(o, p) \/ ~up[p]
    /\ \E r \in Tablets \ {p} : Link(o, r)
    /\ \A r \in Tablets \ {p} : Link(o, r) => ~(src[r] = p /\ Connected(r))

\* PrimarySemiSyncBlocked: the primary waits for ACKs although replicas have semi-sync enabled.
SemiSyncBlocked(o) ==
    LET p == shardPrimary IN
    /\ p # None /\ Link(o, p) /\ up[p] /\ ttype[p] = "PRIMARY"
    /\ wait[p] # {}
    /\ \E r \in Ackers(p) : ssRep[r]
    \* blocked for a while: no semi-sync replica is connected to ACK
    /\ ~\E r \in Ackers(p) : ssRep[r] /\ src[r] = p /\ Connected(r)

\* ERS, step 1: take the shard lock and read the shard record.
ERSLock(o) ==
    /\ phase[o] = "idle" /\ nERS < MaxERS
    /\ DeadPrimary(o) \/ SemiSyncBlocked(o)
    /\ lock = None /\ Link(o, "topo")
    /\ nERS' = nERS + 1
    /\ lock' = o
    /\ phase' = [phase EXCEPT ![o] = "stop"]
    /\ ersOld' = [ersOld EXCEPT ![o] = shardPrimary]
    /\ ersId' = [ersId EXCEPT ![o] = nERS + 1]
    /\ reached' = [reached EXCEPT ![o] = {}]
    /\ pos' = [pos EXCEPT ![o] = [t \in Tablets |-> {}]]
    /\ repointed' = [repointed EXCEPT ![o] = {}]
    /\ cand' = [cand EXCEPT ![o] = None]
    /\ UNCHANGED <<mysqlVars, clientVars, tabletVars, shardPrimary, term, cut, pending,
                   nTx, nCrash, nCut, nExpire, nRestart>>

\* ERS, step 2: StopReplicationAndGetStatus(IOTHREADONLY) on one tablet; a tablet that is not
\* a replica is demoted with DemotePrimary(force) instead.
ERSStop(o, t) ==
    /\ phase[o] = "stop" /\ t \notin reached[o]
    /\ Link(o, t) /\ up[t]
    /\ reached' = [reached EXCEPT ![o] = @ \cup {t}]
    /\ IF src[t] = None
         THEN /\ DemoteEffects(t)
              /\ pos' = [pos EXCEPT ![o][t] = Rng(binlog[t])]
              /\ UNCHANGED <<io, acked>>
         ELSE \* IOTHREADONLY leaves a receiver alone unless it is healthy (connected): a
              \* receiver that keeps retrying its source keeps running (N1, RETRYING_IO).
              /\ io' = [io EXCEPT ![t] = IF RETRYING_IO /\ ~Connected(t) THEN @ ELSE FALSE]
              /\ pos' = [pos EXCEPT ![o][t] = Combined(t)]
              /\ UNCHANGED <<serving, live, wait, ssSrc, ro, acked>>
    /\ UNCHANGED <<up, binlog, relay, src, ssRep, origin, unbacked, ttype, tterm,
                   shardPrimary, term, lock, cut, phase, ersOld, ersId, cand, repointed, pending, boundVars>>

\* ERS's DemotePrimary(force) on a live primary is cancelled once all other tablets answered
\* (stopReplicationAndBuildStatusMaps needs N-1 answers and cancels the rest): the tablet stopped
\* serving and killed the sessions waiting after the shutdown grace period, then fails on the
\* cancelled context and reverts to serving. New writes can then reach it again.
ERSDemoteCancelled(o, t) ==
    /\ phase[o] = "stop" /\ t \notin reached[o] /\ src[t] = None /\ ttype[t] = "PRIMARY"
    /\ Cardinality(reached[o]) >= N - 1
    /\ Link(o, t) /\ up[t] /\ wait[t] \cap live # {}
    /\ live' = live \ wait[t]
    /\ UNCHANGED <<mysqlVars, origin, acked, unbacked, tabletVars, shardPrimary, term, lock, cut,
                   orcVars, boundVars>>

\* haveRevoked for semi_sync: every tablet was reached, or every one of its ackers was.
Revoked(o) ==
    \A e \in Tablets : e \in reached[o] \/ Ackers(e) \subseteq reached[o]

\* The most advanced reached tablets (by executed plus received transactions).
MostAdvanced(o) ==
    {c \in reached[o] : ttype[c] # "DRAINED" /\ \A u \in reached[o] : pos[o][u] \subseteq pos[o][c]}

\* Stop replication and get the positions on everyone but the primary,
\* then try the rest of the reparent, which only aborts.
ERSAbortCleanup(o) ==
    \* restartReplicationOnStoppedReplicas (under the lock): restart the receivers ERS stopped.
    /\ io' = [t \in Tablets |-> IF t \in reached[o] /\ src[t] # None /\ lock = o /\ Link(o, t) /\ up[t]
                                THEN TRUE ELSE io[t]]
    /\ phase' = [phase EXCEPT ![o] = "unlock"]
    \* an aborted reparent cancels its SetReplicationSource RPCs (replCancel)
    /\ pending' = {m \in pending : m.ers # ersId[o] \/ m.orc # o}

\* ERS, step 3: once all but one tablet answered (the others are cancelled), check revocation
\* (unless the only failure is the shard record's primary), then pick the candidate.
ERSChoose(o) ==
    /\ phase[o] = "stop"
    /\ Cardinality(reached[o]) >= N - 1
    /\ LET ok == /\ reached[o] = Tablets \/ Tablets \ reached[o] = {ersOld[o]} \/ Revoked(o)
                 /\ lock = o
                 \* the fix: re-read the shard record; refuse if it changed since the lock, or
                 \* while a tablet holds a newer term than the record's
                 /\ ERS_STALE_RECORD \/ (shardPrimary = ersOld[o] /\ \A u \in Tablets : tterm[u] <= term)
                 /\ MostAdvanced(o) # {}
       IN IF ok
            THEN \E c \in MostAdvanced(o) :
                   \* a candidate needs a reached acker that it can repoint (forward progress)
                   /\ \E a \in reached[o] : Acker(c, a) /\ pos[o][a] \subseteq pos[o][c]
                   /\ cand' = [cand EXCEPT ![o] = c]
                   /\ phase' = [phase EXCEPT ![o] = "wait"]
                   /\ UNCHANGED <<io, pending>>
            ELSE /\ ERSAbortCleanup(o) /\ UNCHANGED cand
    /\ UNCHANGED <<up, binlog, relay, src, ssSrc, ssRep, ro, wait, clientVars, tabletVars,
                   shardPrimary, term, lock, cut, ersOld, ersId, reached, pos, repointed, boundVars>>

\* Any phase before the promotion can fail (timeouts, RPC errors) and abort.
ERSFail(o) ==
    /\ phase[o] \in {"stop", "wait", "quorum"}
    /\ ERSAbortCleanup(o)
    /\ UNCHANGED <<up, binlog, relay, src, ssSrc, ssRep, ro, wait, clientVars, tabletVars,
                   shardPrimary, term, lock, cut, ersOld, ersId, reached, pos, cand, repointed,
                   boundVars>>

\* ERS, step 4: the candidate applied its relay log; repoint every other tablet to it. The
\* SetReplicationSource RPCs run detached, with their own timeout.
ERSRepoint(o) ==
    LET c == cand[o] IN
    /\ phase[o] = "wait"
    /\ up[c] /\ Link(o, c) /\ relay[c] = <<>>
    /\ lock = o
    /\ pending' = pending \cup {[orc |-> o, ers |-> ersId[o], t |-> t, p |-> c] : t \in Tablets \ {c}}
    /\ phase' = [phase EXCEPT ![o] = "quorum"]
    /\ UNCHANGED <<mysqlVars, clientVars, tabletVars, shardPrimary, term, lock, cut,
                   ersOld, ersId, reached, pos, cand, repointed, boundVars>>

\* A SetReplicationSource RPC of an ERS reaches its tablet (or of fixReplica, inline below).
DeliverSRS(m) ==
    /\ m \in pending
    /\ Link(m.orc, m.t) /\ up[m.t] /\ Link(m.t, m.p) /\ up[m.p]
    /\ pending' = pending \ {m}
    /\ SRSEffects(m.t, m.p, FALSE)
    /\ repointed' = [repointed EXCEPT ![m.orc] =
                        IF cand[m.orc] = m.p /\ src'[m.t] = m.p THEN @ \cup {m.t} ELSE @]
    /\ UNCHANGED <<up, binlog, origin, tterm, shardPrimary, term, lock, cut,
                   phase, ersOld, ersId, reached, pos, cand, boundVars>>

\* An outstanding RPC times out.
DropSRS(m) ==
    /\ m \in pending
    /\ pending' = pending \ {m}
    /\ UNCHANGED <<mysqlVars, clientVars, tabletVars, shardPrimary, term, lock, cut,
                   phase, ersOld, ersId, reached, pos, cand, repointed, boundVars>>

\* ERS, step 5: once an acker was repointed, and still holding the lock, PromoteReplica:
\* RESET REPLICA ALL, source-side semi-sync, read-write, type PRIMARY with a new term.
ERSPromote(o) ==
    LET c == cand[o] IN
    /\ phase[o] = "quorum"
    /\ \E a \in repointed[o] : Acker(c, a)
    /\ lock = o /\ Link(o, c) /\ up[c]
    /\ src' = [src EXCEPT ![c] = None]
    /\ io' = [io EXCEPT ![c] = FALSE]
    /\ relay' = [relay EXCEPT ![c] = <<>>]
    \* the fix: stop the receiver, apply the relay log, then reset
    /\ binlog' = [binlog EXCEPT ![c] = IF PROMOTE_DISCARDS THEN @
                     ELSE @ \o SelectSeq(relay[c], LAMBDA x : x \notin Rng(binlog[c]))]
    /\ ssSrc' = [ssSrc EXCEPT ![c] = TRUE]
    /\ ro' = [ro EXCEPT ![c] = FALSE]
    /\ ttype' = [ttype EXCEPT ![c] = "PRIMARY"]
    /\ serving' = [serving EXCEPT ![c] = TRUE]
    /\ tterm' = [tterm EXCEPT ![c] = Max({tterm[u] : u \in Tablets} \cup {term}) + 1]
    /\ phase' = [phase EXCEPT ![o] = "unlock"]
    /\ UNCHANGED <<up, ssRep, wait, clientVars, shardPrimary, term, lock, cut,
                   ersOld, ersId, reached, pos, cand, repointed, pending, boundVars>>

ERSUnlock(o) ==
    /\ phase[o] = "unlock"
    /\ phase' = [phase EXCEPT ![o] = "idle"]
    /\ lock' = IF lock = o THEN None ELSE lock
    \* the fix: cancel the outstanding repoints before releasing the lock
    /\ pending' = IF DETACHED_REPOINT THEN pending
                  ELSE {m \in pending : m.ers # ersId[o] \/ m.orc # o}
    /\ UNCHANGED <<mysqlVars, clientVars, tabletVars, shardPrimary, term, cut,
                   ersOld, ersId, reached, pos, cand, repointed, boundVars>>

\* The lease of the shard lock expires under a holder that keeps going.
Expire ==
    /\ nExpire < MaxExpire /\ lock # None
    /\ nExpire' = nExpire + 1
    /\ lock' = None
    /\ UNCHANGED <<mysqlVars, clientVars, tabletVars, shardPrimary, term, cut, orcVars,
                   nTx, nCrash, nCut, nRestart, nERS>>

\* Single-tablet recoveries run under the shard lock, as one step each.
Recovery(o) ==
    /\ phase[o] = "idle" /\ lock = None /\ Link(o, "topo")
    /\ UNCHANGED <<lock, orcVars, boundVars>>

\* fixReplica (ReplicationStopped, NotConnectedToPrimary, ReplicaSemiSyncMustBeSet, ...): repoint a
\* REPLICA to the shard record's primary. Since #21251 it passes no heartbeat interval (except for
\* ReplicaMisconfigured, a wrong source), so only a change of source runs CHANGE.
FixReplica(o, t) ==
    LET p == shardPrimary IN
    /\ Recovery(o)
    /\ p # None /\ p # t /\ ttype[t] = "REPLICA" /\ ttype[p] = "PRIMARY"
    /\ Link(o, t) /\ Link(o, p) /\ up[t] /\ up[p] /\ Link(t, p)
    /\ src[t] # p \/ ~io[t] \/ ~ssRep[t]
    /\ FIX_REPLICA_STALE \/ \A u \in Tablets : tterm[u] <= tterm[p]
    /\ SRSEffects(t, p, FALSE)
    /\ UNCHANGED <<up, binlog, origin, tterm, shardPrimary, term, cut>>

\* fixPrimary (PrimaryIsReadOnly, PrimarySemiSyncMustBeSet): UndoDemotePrimary on the shard
\* record's primary: source-side semi-sync, read-write, serving. Without FIX_PRIMARY_STALE it
\* first checks that no tablet holds a newer primary term.
FixPrimary(o) ==
    LET p == shardPrimary IN
    /\ Recovery(o)
    /\ p # None /\ ttype[p] = "PRIMARY" /\ Link(o, p) /\ up[p]
    /\ ro[p] \/ ~ssSrc[p] \/ ~serving[p]
    /\ FIX_PRIMARY_STALE \/ \A u \in Tablets : tterm[u] <= tterm[p]
    /\ ssSrc' = [ssSrc EXCEPT ![p] = TRUE]
    /\ ro' = [ro EXCEPT ![p] = FALSE]
    /\ serving' = [serving EXCEPT ![p] = TRUE]
    /\ UNCHANGED <<up, binlog, relay, src, io, ssRep, wait, clientVars, ttype, tterm,
                   shardPrimary, term, cut>>

\* ReplicaIsWritable: set super_read_only on a writable replica.
FixReplicaWritable(o, t) ==
    /\ Recovery(o)
    /\ ttype[t] # "PRIMARY" /\ ~ro[t] /\ Link(o, t) /\ up[t]
    /\ ro' = [ro EXCEPT ![t] = TRUE]
    /\ UNCHANGED <<up, binlog, relay, src, io, ssSrc, ssRep, wait, clientVars, tabletVars,
                   shardPrimary, term, cut>>

\* StaleTopoPrimary: a PRIMARY tablet that is not the shard record's primary is force-demoted
\* (DemotePrimary(force)), changed to REPLICA and repointed.
StaleTopoPrimary(o, t) ==
    LET p == shardPrimary IN
    /\ Recovery(o)
    /\ ttype[t] = "PRIMARY" /\ p # t /\ p # None /\ tterm[p] > tterm[t]
    /\ Link(o, t) /\ up[t] /\ Link(t, p) /\ up[p]
    /\ DemoteEffects(t)
    /\ ttype' = [ttype EXCEPT ![t] = "REPLICA"]
    /\ ssRep' = [ssRep EXCEPT ![t] = TRUE]
    /\ IF ErrantVs(Rng(binlog[t]), p) # {}
         THEN UNCHANGED <<src, io, relay>>
         ELSE /\ src' = [src EXCEPT ![t] = p]
              /\ io' = [io EXCEPT ![t] = TRUE]
              /\ relay' = [relay EXCEPT ![t] = IF REPOINT_DISCARDS THEN <<>> ELSE @]
    /\ UNCHANGED <<up, binlog, origin, acked, unbacked, tterm, shardPrimary, term, cut>>

\* change-tablets-with-errant-gtid-to-drained: a REPLICA with GTIDs the primary lacks is DRAINED.
DrainErrant(o, t) ==
    LET p == shardPrimary IN
    /\ Recovery(o)
    /\ p # None /\ p # t /\ ttype[t] = "REPLICA" /\ Link(o, t) /\ Link(o, p) /\ up[t] /\ up[p]
    /\ ErrantVs(Rng(binlog[t]), p) # {}
    /\ ttype' = [ttype EXCEPT ![t] = "DRAINED"]
    /\ ssRep' = [ssRep EXCEPT ![t] = FALSE]
    /\ UNCHANGED <<up, binlog, relay, src, io, ssSrc, ro, wait, clientVars, serving, tterm,
                   shardPrimary, term, cut>>

-----------------------------------------------------------------------------
Next ==
    \/ \E t \in Tablets :
          \/ Write(t) \/ Receive(t) \/ Apply(t) \/ Crash(t) \/ Restart(t)
          \/ PublishPrimary(t) \/ SelfDemote(t) \/ TabletRestart(t)
    \/ \E l \in Links : CutLink(l)
    \/ \E n \in Tablets \cup Orcs : Isolate(n)
    \/ Heal
    \/ Expire
    \/ \E m \in pending : DeliverSRS(m) \/ DropSRS(m)
    \/ \E o \in Orcs :
          \/ ERSLock(o) \/ ERSChoose(o) \/ ERSFail(o) \/ ERSRepoint(o) \/ ERSPromote(o) \/ ERSUnlock(o)
          \/ \E t \in Tablets : ERSStop(o, t) \/ ERSDemoteCancelled(o, t)
          \/ FixPrimary(o)
          \/ \E t \in Tablets : FixReplica(o, t) \/ FixReplicaWritable(o, t)
                                \/ StaleTopoPrimary(o, t) \/ DrainErrant(o, t)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

TypeOK ==
    /\ shardPrimary \in Tablets \cup {None}
    /\ lock \in Orcs \cup {None}
    /\ acked \subseteq Txs /\ live \subseteq Txs

\* The newest primary: the tablet with the highest primary term. The shard record follows it
\* once its shard_sync publishes it.
Newest == CHOOSE t \in Tablets : \A u \in Tablets : tterm[u] <= tterm[t]

\* Every acknowledged write is in the binlog of the newest primary. (The binlog is durable: a
\* crashed primary still holds it.) A promotion that loses an acknowledged write violates this,
\* and so does a write that a deposed primary acknowledges.
NoLostAck == acked \subseteq Rng(binlog[Newest])

\* Every write was acknowledged while another tablet held it (the semi-sync contract).
NoUnbackedAck == unbacked = {}

\* At most one tablet accepts writes that it can acknowledge: writable, and its semi-sync is off
\* or a semi-sync replica is connected to it.
CanAck(t) == Writable(t) /\ (~ssSrc[t] \/ \E r \in Tablets : src[r] = t /\ ssRep[r] /\ Connected(r))
OneAckingPrimary == \A a, b \in Tablets : CanAck(a) /\ CanAck(b) => a = b

\* No reparent or repoint is in flight.
Quiescent == pending = {} /\ \A o \in Orcs : phase[o] = "idle"

\* Once nothing is in flight, a serving REPLICA that replicates holds no transaction that the
\* newest primary lacks.
NoErrantServingReplica ==
    Quiescent => \A t \in Tablets :
        (ttype[t] = "REPLICA" /\ t # Newest /\ src[t] # None)
            => ErrantVs(Rng(binlog[t]), Newest) = {}
=============================================================================
