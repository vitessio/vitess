# TLA+ model of the Group Replication safety protocol

`GRSafety.tla` models one shard of Vitess's Group Replication support (design: `doc/design-docs/GroupReplication.md`; every bug found so far: `doc/failover-audit/GroupReplication.md`). It covers MySQL Group Replication as far as safety needs it, the voters' vttablets (sync loop, fence check, bootstrap RPC), and up to two VTOrcs with a shard lock whose lease can expire. The model follows the code on the `group-replication-prototype` branch, not an idealized design. Each fix is a `CONSTANT` switch, so the model can check the design with the fix and reproduce the bug without it.

It is the first milestone. It is a bounded model check, not a proof. The bounds, and everything the model leaves out, are listed below.

## Running it

```
TLA2TOOLS_JAR=/path/to/tla2tools.jar ./run.sh              # every configuration, checked against its expected outcome
TLA2TOOLS_JAR=/path/to/tla2tools.jar ./run.sh new1 orcs    # some of them
```

`run.sh` runs TLC with `-workers auto` and prints, per configuration, whether the outcome matches the expected one (no error, a deadlock for the stuck-state check, or a violation of the named invariant), with the time, the distinct states, the depth and the trace length. It exits non-zero on an unexpected outcome. `TLA2TOOLS_JAR` defaults to `tla2tools.jar` next to the script, which `.gitignore` keeps out of the repository; download it from the TLA+ releases (the runs below used a nightly TLC, 2026.10.02). `TLC_HEAP` (default `8g`) sets the Java heap, `TLC_OUT` (default `out/`) the output directory, and `TLC_SIM_TRACES` (default 100,000 per worker) the behaviors of the configurations that run in simulation (`SIMULATED` in `run.sh`). The TLC outputs keep the full counterexample traces. The whole suite takes about an hour and a half on 4 cores.

## What is modeled

**Voters.** Three servers, each a host with a mysqld and a vttablet; all three are the shard's voters, which never change. A host crash takes down both.

**MySQL and Group Replication.**
- A group incarnation `i` has a view, a primary and a history `hist[i]`: the data of the member that bootstrapped it, plus every transaction decided in it. A commit is decided once a majority of the *current view* accepts it (MySQL's view quorum), and is acknowledged right away.
- *Accepted is not received.* A member receives a decided transaction into its relay log only through `Deliver`, and executes it through `Apply`. A group that loses its majority (`LoseMajority`, a partition) delivers nothing more, and its members then leave it: a transaction that only the old primary had received exists only in its binlog.
- `relay_log_recovery=ON`: a host crash drops the received, unapplied backlog. A bootstrap applies the backlog first (lab L4); so does a join.
- Elections: a group that keeps its majority elects a new primary when its primary leaves. Under `BEFORE_ON_PRIMARY_FAILOVER` the new primary applies everything decided before its election ends, and the end of the election sets `super_read_only` (with the member action disabled) or clears it (enabled). A bootstrap and a stray group start an election too.
- A clean leave shrinks the view and keeps quorum; an expulsion or a crash needs the rest of the view to outvote the member, otherwise the group loses its majority.
- A join completes into any live group whose history contains the joiner's binlog (GR refuses a member with extra transactions), recovering from the primary. A join that is still running when no live group is left ends alone in a new incarnation, as its primary (NEW-1's MySQL mechanism, `JoinStray`), or fails.
- A `START GROUP_REPLICATION` keeps running in MySQL after its client gave up, and MySQL refuses another `START` or a `STOP` until it ends.

**vttablet.**
- The sync loop reads MySQL's status at the start of a run, without the lock, and stops serving on it (`SyncRead`, `SyncStop`). Serving again (`serveAgain`) and every promotion (`changeTypeLocked`) are decisions in three steps under the action lock: take the lock, the not-serving generation and the fence snapshot; read MySQL's status; act. A decision makes MySQL writable and serves only if the serving invariant held on the status it read (recorded incarnation, ONLINE primary with quorum, election ended, majority of the voters in the view), no fence was decided since its snapshot, and no not-serving reason since its generation.
- Demotion of a primary that lost its role or whose MySQL is out of its group; leaving a foreign incarnation (fence first, then `STOP`), except a group the tablet bootstrapped within the grace, or the group of a live bootstrap intent that names the tablet; gated rejoins (only while another tablet reports an active member of the legitimate group).
- The fence check in two steps, without the action lock: read MySQL's view; decide under `mu` (in the same epoch, no bootstrap in progress), set `super_read_only`, and make a PRIMARY tablet stop serving. It fences stray incarnations without the voter majority, the shrink of a view in which it saw the voter majority in this incarnation, and keeps a fence that holds. Joins, bootstraps and leaves start new epochs, which drop a pending fence.
- The `StartGroupReplication(bootstrap)` RPC: it waits for the action lock, stops serving on a PRIMARY tablet, refuses an active member, waits for a `START` in progress to end (and then makes MySQL leave whatever that `START` formed or joined) or gives up, starts MySQL's bootstrap, notes the bootstrapped incarnation (trusted for the grace minute), and replies. If VTOrc gave up, the handler ends; a `START` it issued keeps running.

**VTOrc.** `GroupNotBootstrapped` under the shard lock: a fresh status of every voter, all reachable and none active, the candidate whose executed plus received set contains every other voter's (preferring the target of a live intent, then a member without a `START` in progress, then a PRIMARY tablet, then any: the lowest-alias tie-break is a nondeterministic choice, see "Symmetry"); the bootstrap intent (a compare-and-swap on the incarnation, refused while another target's intent is live); the RPC; then the incarnation recorded with a compare-and-swap on the expected incarnation and the intent's token, or, if the RPC failed or timed out, the adoption of the target's group. `GroupBootstrapNotRecorded` adopts later. A lease can expire under a VTOrc that stalls; the VTOrc keeps acting when it resumes. An intent expires after its two-minute fence. `StaleTopoPrimary` force-demotes a PRIMARY tablet that is not the newest, and without the NEW-3 fix configures the default channel on a voter outside its group.

**Clients** commit on any MySQL that accepts a commit (the ONLINE primary with quorum, not `super_read_only`, election ended), through vtgate or directly. A commit is acknowledged when it is decided.

## Properties

| Invariant | Meaning |
|---|---|
| `NoLostAck` | Every acknowledged write is in the history of the recorded incarnation, and every MySQL that accepts commits holds it. |
| `OneWritablePrimary` | At most one MySQL accepts commits that it can acknowledge. |
| `NoDecisionAck` | No write is acknowledged by a serving primary whose decision to serve was not taken under the action lock on a status read under it, while it lacks the voter majority or the recorded incarnation (the S7d r2 shape). |
| `AdoptOnce` | At most one incarnation is recorded for each bootstrap intent (by the reply or by adoption). |
| `FenceNotUndone` | No decision makes MySQL writable over a fence decided after the decision's snapshot. |
| `NoAsyncVoter` | No voter replicates on the default channel (NEW-3). |
| `NoDualBootstrap` | No two live groups were bootstrapped on different tablets from the same recorded incarnation. Checked under the timing assumption `INTENT_OUTLASTS_BOOT` (below). It is a structural property: a second group is read-only, not served and never recorded, so it costs availability, not data (see Finding 2). |
| `NoMinorityAck` | No write is acknowledged by a view that holds fewer than a majority of the voters. **Expected to be violated.** |

**`NoMinorityAck` is a documented, expected violation.** The probe defined it as "every acknowledged write was accepted by a view holding a majority of the voters". MySQL's quorum is that of the current view, not of the voters: when voters leave cleanly, a serving primary alone in its view keeps quorum and keeps committing, and it was writable before its view shrank, so nothing in MySQL fences it. The fence check sets `super_read_only` within about 150ms, and the sync loop stops serving within a second; the writes in between are acknowledged on one voter (G11: 4–20 writes). They are not lost: they are decided in the recorded incarnation (`NoLostAck` holds), and a group that later loses its majority is bootstrapped again only with every voter reachable, on the voter that holds them. The `minority` configuration shows the 4-step trace: a commit, a crash and a clean leave shrink the primary's view to itself, and the next commit is acknowledged by one voter. This is the same trade-off as a semi-sync primary that acknowledges before its replica stores the transaction; `paxos_single_leader` does not change it.

## Switches

Fixes (`TRUE` is the code on the branch): `LEGIT` (recorded incarnation plus voter majority, NEW-1), `BOOT_ALL` (bootstrap only with every voter reachable), `SERVE_LOCKED` (serve again only under the lock on a fresh status, with generations, S7d r2), `MA_DISABLED` (member action `mysql_disable_super_read_only_if_primary` disabled before every `START`), `FENCE`, `FENCE_SNAPSHOT`, `JOIN_GATE`, `INTENT`, `INTENT_FENCE`, `INTENT_PREFER`, `ADOPT` (S7d r3), `INC_CAS`, `NEW3_FIX`.

A proposed fix, not in the code: `CAND_EXECUTED`, the bootstrap candidate must hold every voter's transactions in its *executed* set (its binlog). See Finding 1 below.

Environment and checking modes: `STALE_TOPO` (VTOrc's `StaleTopoPrimary` runs), `STALE_REC` (a tablet decides on the shard record it read last instead of the current one), `SPLIT` (decisions and the fence check run as interleaved steps; `FALSE` makes each one atomic), `STUCK_CHECK` (no timeout longer than the shard lock's lease fires, and `Done` marks healthy end states, so that TLC's deadlock check finds stuck states), `INTENT_OUTLASTS_BOOT` (an intent expires only while no VTOrc recovery and no bootstrap is in flight: no VTOrc stalls past the two-minute fence).

## Configurations and bounds

Every configuration has 3 voters and starts from a group of all three with a serving primary. The base bounds are at most 2 new incarnations (`MaxInc = 3`), 2 transactions, 2 intents, and fault budgets of one crash, one leave of a live group (clean, or an expulsion), one loss of majority, and one lease expiry; each family below lowers or raises some of them. Faults that follow from another are free: the members of a group that lost its majority leave it, a crashed host restarts. The validation configurations use the bounds of their family.

The full model with every interleaving does not finish within an hour on this machine (4 cores, 15G): a run with two VTOrcs, two transactions and every step split reached 18M distinct states at depth 20 in 13 minutes with its queue still growing (the run log is summarized under "Results"). The current design is therefore checked by families of configurations, each with the features its properties need:

| Family | VTOrcs | Interleaving | Bounds | Checks |
|---|---|---|---|---|
| `tablet` | 1, no lease expiry | decisions and fence check split (`SPLIT`) | 1 transaction, `MaxInc = 2` | fence ordering, serving decisions, crashes and `relay_log_recovery`, NEW-1, the member action |
| `orcs` | 2, one lease expiry | decisions and fence check atomic | 1 transaction, no leave (a crash still removes a member) | intents, adoption, compare-and-swap, concurrent bootstraps, stalls |
| `integrated` | 2, two lease expiries | split | 2 transactions, `MaxInc = 4`, 3 intents, 2 of each fault | everything together, by simulation only (random behaviors, seed 1, depth 120) |

Why the split is sound for what each family checks:
- A family that makes a decision atomic only removes interleavings *inside* the decision. The `tablet` family checks every interleaving of the decisions with the fence check, crashes, leaves and elections, with one VTOrc. Two VTOrcs only change the shard record and MySQL's group through bootstraps, which the action lock serializes with the decisions in both families.
- `MaxInc = 2` in the `tablet` family allows one new incarnation: a bootstrap or a stray group. Every tablet-side scenario of the audit needs one (S7d r2, NEW-1, the majority bootstrap, S7d r3, the relay-log finding). The scenarios that need two (a stray next to a bootstrap, two bootstraps) are in the `orcs` family.
- One transaction is enough for every loss scenario: the bug is that one acknowledged transaction is missing. A second transaction adds states where members hold different prefixes; that is covered by simulation only (`tablet_tx2`, `integrated`).
- The `orcs` family has no separate leave: a VTOrc's decisions depend on whether members are active and what they hold, which crashes and losses of majority already vary; the shrink of a live group is the `tablet` family's concern.

Abstractions that reduce the state space, each of which only adds behaviors or removes indistinguishable ones:
- The fence check is not armed for the 30s after a join or bootstrap ends (`groupReplicationJoinWatchWindow`), and forgets `majorityIncarnation` when its tablet is demoted. Both only remove fences, which only adds unsafe behaviors.
- The fence check reads the current bootstrap intent rather than the tablet's cached copy; the intent only exempts a group that is read-only and not served.
- The members of a group that lost its majority leave it in one step: until they leave, they can neither commit, receive, elect nor be joined, and VTOrc does not bootstrap while any is active.
- A join releases the action lock when MySQL's `START` ends; giving up earlier is a separate step. The bootstrap RPC's handler finishes in the same step as MySQL's bootstrap.
- Bookkeeping that is rewritten before it is read again is reset when it ends (a decision's snapshot, a VTOrc's recovery, a bootstrap's expected incarnation), and an intent's expiry is a flag on the current intent rather than a counter.

**Symmetry.** Voters and VTOrcs are symmetric (`Permutations`). The code breaks ties between equal bootstrap candidates by the lowest alias; the model chooses any of them, which includes the code's choice, so the symmetry reduction is sound and the model has more behaviors than the code.

## Results

TLC 2026.10.02 (nightly), 4 workers, 8G heap, on a 4-core, 15G machine. "Exhaustive" means TLC explored the whole bounded state space.

### Validation: each fix off

Each configuration switches off the fixes in the second column and checks only the named invariant, so the counterexample is attributable to that fix. Every one of them keeps `CAND_EXECUTED` on, so that the finding below does not mask them. All exhaustive (the search stops at the first violation, breadth-first, so the trace is a shortest one). "States" is the number of states in the trace, including the initial state.

| Configuration | Bug (audit) | Switched off | Outcome | States | Time | Distinct states explored |
|---|---|---|---|---|---|---|
| `s7d_r2` | S7d r2: single-voter acks after a stale decision | `SERVE_LOCKED`, `MA_DISABLED` | `NoDecisionAck` violated | 10 | 4s | 18,551 |
| `s7d_r2_ma_off` | S7d r2, member action disabled, one leave | `SERVE_LOCKED` | no error | - | 7m34 | 4,359,180 |
| `s7d_r2_ma_off_leaves` | S7d r2, member action disabled, two leaves | `SERVE_LOCKED` | `NoDecisionAck` violated | 11 | 6s | 45,666 |
| `new1` | NEW-1: a stale member re-forms the group | `LEGIT` | `NoLostAck` violated | 10 | 3s | 10,856 |
| `majority_boot` | bootstrap from a reachable majority | `BOOT_ALL` | `NoLostAck` violated | 11 | 4s | 8,693 |
| `s7d_r3` | S7d r3: lost bootstrap reply, no adoption | `ADOPT` (`STUCK_CHECK`) | deadlock: stuck without a recorded group | 11 | 2s | 4,514 |
| `dual_nofence` | two VTOrcs bootstrap different tablets | `INTENT_FENCE` | `NoDualBootstrap` violated | 13 | 5s | 31,893 |
| `new3` | NEW-3: `StaleTopoPrimary` on a voter | `NEW3_FIX` | `NoAsyncVoter` violated | 7 | 2s | 2,369 |
| `fence_snapshot` | a decision older than a fence lifts it | `FENCE_SNAPSHOT` | `FenceNotUndone` violated | 13 | 7s | 66,062 |
| `no_cas` | incarnation writes without compare-and-swap | `INC_CAS` | `AdoptOnce` violated | 18 | 10s | 150,006 |
| `minority` | expected: a shrinking view | none | `NoMinorityAck` violated | 4 | 2s | 392 |

The traces, in short:
- `s7d_r2`: the primary s1 leaves its group, which then loses its majority; VTOrc bootstraps s1, whose tablet stops serving; Group Replication makes the bootstrapped primary writable (member action enabled), and the sync loop, on the status it read while s1 was the primary of three voters, serves again: a write is acknowledged by a group of one. This is the chaos run's interleaving.
- `s7d_r2_ma_off` and `s7d_r2_ma_off_leaves`: is the S7d r2 fix still needed now that the member action is disabled? The earlier probe concluded it was not, from the bootstrap path alone. With one leave per behavior (`s7d_r2_ma_off`, the `tablet` family's bounds) TLC finds no violation, exhaustively. With two leaves (`s7d_r2_ma_off_leaves`, exhaustive) it finds one in 11 states, without any VTOrc: the primary's view shrinks to itself and the loop stops serving; a voter rejoins and the next run of the loop reads MySQL's status as servable; the voter leaves again before that run acts, and the run, serving again on the status it read, makes the primary serve while its view holds one voter. MySQL is still writable from before (the fence check has not run yet; nothing in the model bounds its 150ms), so a write is acknowledged by one voter, through a decision that was not taken on a fresh status. The fix (serve again only under the action lock, on a status read under it, with generations) is therefore still needed: disabling the member action only covers the bootstrap shape of S7d r2. (A 24-step trace of an earlier version of the model, with two VTOrcs, was an artifact of a modeling error, a bootstrap reply matched to the wrong RPC, and is not reported.)
- `new1`: the primary dies while s2's join runs; the join forms a group of one; without the recorded incarnation and the voter majority the tablet promotes it and makes it writable, and it lacks the acknowledged write.
- `majority_boot`: s1, the only voter with two acknowledged writes, leaves and is down when the group loses its majority; VTOrc bootstraps s2 from the reachable majority.
- `s7d_r3`: VTOrc bootstraps s1; the reply is lost; without adoption s1 trusts its new group (unrecorded, read-only, not served) and nothing else can happen until a timeout longer than the shard lock's lease.
- `dual_nofence`: VTOrc o1 chooses s1 and stalls; its lease expires; o2 chooses s2; o1 bootstraps s1; without the fence, o2's intent for s2 is accepted while s1's intent is live, and two groups exist.
- `new3`: s1, the old primary, leaves; s2 is elected and promoted; `StaleTopoPrimary` on s1 configures the default channel.
- `fence_snapshot`: the fence check reads the primary after its view shrank to itself; a voter rejoins, and a serve-again decision takes its snapshot; the fence is decided; the decision reads a servable status and lifts the fence that was decided after its snapshot.
- `no_cas`: VTOrc o1's bootstrap RPC to s1 waits for a join `START` on s1, which ends in a stray group of one; o2, after o1's lease expired, adopts it (s1 is the intent's target); o1's handler then stops that group and bootstraps s1 again, and without the compare-and-swap o1 records the new incarnation for the same intent.

### Current design

| Configuration | Family | Bounds | Mode | Result | Distinct states | Depth | Time |
|---|---|---|---|---|---|---|---|
| `current` | `tablet` | the code (`CAND_EXECUTED` off) | exhaustive | **`NoLostAck` violated** (14 states) | 43,077 | 14 | 6s |
| `tablet` | `tablet` | 1 VTOrc, 1 transaction, `MaxInc = 2`, split | exhaustive | no error | 11,492,340 | 58 | 15m03 |
| `stale_rec` | `tablet` | as `tablet`, decisions on the tablet's last shard record, atomic | exhaustive | no error | 7,798,563 | 51 | 7m02 |
| `s7d_r3_adopt` | `tablet` | stuck check, adoption on, 1 transaction, `MaxInc = 3`, no leave | exhaustive | no stuck state | 833,559 | 49 | 1m19 |
| `orcs` | `orcs` | 2 VTOrcs, 1 lease expiry, 1 transaction, no leave, atomic | exhaustive | no error | 6,189,725 | 53 | 7m31 |
| `orcs_stall` | `orcs` | as `orcs`, a VTOrc may stall past the intent fence (all but `NoDualBootstrap`) | exhaustive | no error | 19,534,718 | 57 | 24m12 |
| `tablet_tx2` | `tablet` | as `tablet`, 2 transactions | simulation | no error | 400,000 traces, 25.4M states, mean length 22 | - | 6m03 |
| `integrated` | all | 2 VTOrcs, split, 2 transactions, `MaxInc = 4`, 3 intents, 2 of each fault | simulation | **`NoDualBootstrap` violated** (26 states) | 111,000 traces, 8.9M states | - | 2m01 |
| `integrated_core` | all | as `integrated`, every invariant but `NoDualBootstrap` | simulation | no error | 400,000 traces, 32.0M states, mean length 31 | - | 6m55 |

Except `current`, every configuration of the current design runs with `CAND_EXECUTED` on (the proposed fix for the first finding below), and checks every invariant except `NoMinorityAck` (and `NoDualBootstrap` where noted). The simulations use seed 1, at most 120 steps per behavior, and 100,000 behaviors per worker; they explore a sample, not the whole space. The exhaustive runs with two transactions did not converge within the hour (their queues still grew after 10 minutes); the `tablet` and `orcs` families are exhaustive with one transaction, and two transactions are covered by simulation only.

Runs that were stopped because their queue still grew after 10 minutes (before the reductions listed above, unless noted):

| Run | Distinct states | Depth | Queue | Time |
|---|---|---|---|---|
| 2 VTOrcs, 2 transactions, every step split, `MaxInc = 3` | 18.0M | 20 | 10.6M, growing | 13m |
| `tablet` family with 2 transactions (final model) | 7.9M | 27 | 1.49M, growing | 10m |
| `orcs` family with 2 transactions, with leaves (final model) | 8.4M | 22 | 3.1M, growing | 7m |
| `stale_rec` with split decisions (final model) | 7.1M | 27 | 1.64M, growing | 9m |

## Finding 1: a bootstrap candidate whose transactions are only in its relay log

With every fix on, TLC finds a violation of `NoLostAck` (configuration `current`, 14 states, 6s, exhaustive), with one VTOrc and no lease expiry:

```
 1. Init          s1 is the primary of incarnation 1 {s1, s2, s3}
 2. Commit(s1)    transaction 1 is acknowledged; only s1 has it
 3. Leave(s1)     s1 leaves the group (expelled); {s2, s3} keep the majority
 4. SyncDemote(s1)
 5. Deliver(s2)   s2 receives transaction 1 into its relay log, does not apply it
 6. LoseMajority(1), 7. LeaveDead(1)   no member is left in a group
 8. OBegin(o1)    VTOrc reads every voter: s1 executed {1}, s2 received {1}, s3 {}; it chooses s2
 9. Crash(s2)     mysqld restarts: relay_log_recovery drops {1}
10. OIntent(o1)   the intent for s2, and the bootstrap RPC
11. Restart(s2)
12. HBoot1(s2)    the tablet starts MySQL's bootstrap: s2 holds nothing
13. BootComplete(s2)  incarnation 2 = {s2}, history {}
14. OReply(o1)    VTOrc records incarnation 2: the acknowledged transaction 1 is not in its history
```

VTOrc's `chooseGroupBootstrapCandidate` (`go/vt/vtorc/logic/group_replication_recovery.go:618`) compares each voter's **executed set unioned with its received set**: `memberGTIDSet` (`group_replication_recovery.go:724`) unions `PrimaryStatus.Position` with `GroupReplicationStatus.ReceivedTransactionSet` (`RECEIVED_TRANSACTION_SET` of the `group_replication_applier` channel, `go/mysql/group_replication.go:73`). Among voters whose sets are equal, it sorts by intent target, `START` in progress, PRIMARY type and then the lowest alias (`group_replication_recovery.go:676`), and takes the first that contains every other set (`:703`). A voter that holds an acknowledged transaction only in its relay log (it was delivered, not applied, when the group lost its majority) can win against the old primary that holds it in its binlog. Nothing checks the candidate's set again: `bootstrapGroupReplication` writes the intent and calls `StartGroupReplication(bootstrap)` (`:421`, `:430`), and the tablet's `startGroupReplicationLocked` (`go/vt/vttablet/tabletmanager/group_replication.go:384`) only refuses an active member. If the chosen voter's mysqld restarts between VTOrc's status read and MySQL's `START`, `relay_log_recovery=ON` (`config/mycnf/mysql84.cnf:13`) discards the received backlog, and the new group lacks the acknowledged transaction. The old primary still holds it, so it cannot join the new group (extra transactions), and the recorded incarnation's history has lost a write that a majority accepted and a client was told committed.

The window is short in practice (VTOrc's intent write, the RPC, the tablet's lock), but a VTOrc stall, a slow topology write or a tablet whose action lock is held stretches it, and a mysqld restart is a routine event.

**Go reproduction.** `TestBootstrapGroupReplicationPrefersTransactionsInTheBinlog` (`go/vt/vtorc/logic/group_replication_recovery_test.go`) sets up the state of the trace: voter 100 has executed 1-10 and received 1-15, voter 101 (the old primary) has executed 1-15, voter 102 has executed 1-10. VTOrc bootstraps 100 (the lowest alias); the test expects 101 and fails on the branch. It is not committed on `group-replication-prototype` (see the report).

**Possible fixes** (not made; `CAND_EXECUTED` models the first): choose a candidate whose *executed* set contains every voter's executed and received sets; the last primary of the last incarnation always qualifies, since it executed everything decided in its incarnation, and every voter is reachable. Or pass the candidate's expected set with the bootstrap RPC and have the tablet check its MySQL's set under the action lock before `START`. With `CAND_EXECUTED`, every exhaustive configuration of the current design passes (above).

## Finding 2: a bootstrap RPC of a superseded intent

The `integrated` simulation (two VTOrcs, two leaves, two lease expiries) finds a violation of `NoDualBootstrap` with every fix on, in 26 states. VTOrcs o1 and o2 both choose s1 (o1's lease expired in between) and both write an intent for it, o2's replacing o1's (the same target is not fenced). o2's RPC gets s1's action lock first, bootstraps incarnation 2, and o2 records it, which clears the intent. o1's RPC, for the superseded intent, still waits for s1's action lock. s1's MySQL then leaves its group of one (in the code: a mysqld restart while its vttablet keeps running; the model's clean leave). o1's RPC now runs: `StartGroupReplication(bootstrap)` (`vttablet/tabletmanager/rpc_group_replication.go:47`) and `startGroupReplicationLocked` (`group_replication.go:384`) only refuse an active member, so s1 bootstraps incarnation 3 from recorded incarnation 2. Meanwhile o2, finding every voter out of a group and no live intent, bootstraps s2 as incarnation 4 from recorded incarnation 2, as the design intends. Two groups bootstrapped from the same recorded incarnation are alive.

No acknowledged write is at risk: the compare-and-swap refuses to record s1's group (o1's intent token and expected incarnation are stale), the group is read-only (member action disabled) and not served (serving invariant), and s1 leaves it after the bootstrap grace minute. `integrated_core` checks the other invariants at the same bounds by simulation and finds nothing. The cost is availability: s1 is out of the shard's group for up to that minute. The intent fence assumes that a bootstrap RPC acts only while its intent is the shard's; the RPC carries no intent, so the tablet cannot tell. The exhaustive `orcs` family does not reach this, since it has no leave that keeps the vttablet running. A fix would pass the intent's token, or the expected recorded incarnation, with the RPC, and have the tablet refuse a bootstrap whose intent is no longer the shard record's. No Go test is included: the current RPC has no parameter that such a test could exercise.

## Conformance: actions and the code they model

Paths are relative to `go/vt/`; line numbers are on `group-replication-prototype` at the commit that adds this model.

| Action | Code | Abstractions |
|---|---|---|
| `Commit` | MySQL commit on the group primary | Decided and acknowledged in one step. Certification conflicts, flow control and transaction size limits are left out. Any client, through vtgate or directly. |
| `Deliver`, `Apply` | XCom delivery to the relay log; the applier | A member receives everything decided at once. |
| `Leave`, `LeaveDead`, `LoseMajority`, `Crash` | expulsion, clean `STOP`/shutdown, `unreachable_majority_timeout`, host crash | No partial partitions: a partition is either an expulsion outvoted by the rest of the view, or a loss of majority of the whole incarnation. Members of a dead group leave together. |
| `Elect`, `ElectEnd` | GR's election; `BEFORE_ON_PRIMARY_FAILOVER`; member action `mysql_disable_super_read_only_if_primary` | The election thread is visible as soon as the member is primary (MySQL can report PRIMARY a moment before; `servesReadOnly` covers that, and it is left out). The election's choice is any member (member weights are left out). |
| `JoinComplete`, `JoinStray`, `JoinFail`, `BootComplete` | MySQL's `START GROUP_REPLICATION` | Recovery copies the primary's binlog at once. A stray forms only when no live group exists (the chaos runs observed it when the joined group lost its majority). RECOVERING is not modeled. |
| `Restart` | host restart | vttablet restarts as REPLICA (`tm_init.go`, `checkPrimaryShip`). |
| `RefreshRec` | `getRecord`, `readShardGroupRecord` (`vttablet/tabletmanager/group_replication_sync.go:936`, `group_replication_legitimacy.go:157`) | Only with `STALE_REC`; otherwise tablets decide on the current shard record. |
| `BootGraceExpire` | `groupReplicationPeers.recentlyBootstrapped` (`group_replication_legitimacy.go:128`) | Any time (no clocks). |
| `SyncRead`, `SyncStop` | `reconcile` reads the status, `enforceVoterMajority` sets the not-serving reason on it (`group_replication_sync.go:215`, `:636`) | Only for a PRIMARY tablet. |
| `SyncServeStale` | the loop before f782228 | Only with `SERVE_LOCKED = FALSE`. |
| `SyncDemote` | `demote`, `demoteStalePrimary` (`group_replication_sync.go:506`, `:551`) | One action for both. |
| `LeaveForeign` | `isForeignGroup`, `leaveForeignGroup`, `leaveForeignGroupLocked` (`group_replication_sync.go:981`, `:1002`; `group_replication_legitimacy.go:426`), `adoptableByIntent` (`group_replication_fence.go:513`) | Fence, demotion and `STOP` in one step under the lock. Intent liveness is the flag `intExp`; the 30s clock-skew check of the incarnation time is the order of incarnation ids. |
| `PrSnap`, `PrRead`, `PrAct` | `promote` (`group_replication_sync.go:440`) and `changeTypeWithGroupRecordLocked` (`vttablet/tabletmanager/rpc_actions.go:172`): `waitForGroupElectionEnd`, `snapshot`, `applyGroupReplicationServingDecisionLocked` (`group_replication.go:1156`), `groupReplicationServingReason` (`:1060`), `ChangeTabletType`, `settleGroupReplicationFenceLocked` (`group_replication_fence.go:489`) | Make-writable and settle are one step: a fence decided between them leaves MySQL fenced either way. Prepared-transaction redo is left out. |
| `SaSnap`, `SaRead`, `SaAct` | `serveAgain` (`group_replication_sync.go:783`), `liftGroupReplicationFenceLocked` (`group_replication_fence.go:450`), `ClearGroupReplicationNotServing` (`tm_state.go:533`) | The shard record is the one read before the lock. The voters' `server_uuid` discovery is left out: every voter is always identifiable. |
| `JoinStart`, `JoinRelease` | `shouldRejoin`, `rejoin` (`group_replication_sync.go:1033`, `:1070`), `checkLegitimateGroupToJoin`, `legitimateGroupActiveElsewhere` (`group_replication_legitimacy.go:399`, `:356`), `startGroupReplicationLocked`, `joinGroupLocked` (`group_replication.go:384`, `:558`), VTOrc's `startGroupReplicationOnMember` and `joinVotersAfterBootstrap` (`vtorc/logic/group_replication_recovery.go:215`, `:543`) | Every join source is gated; `joinVotersAfterBootstrap`'s ungated joins start right after the record, when the gate holds, and their delay is covered by `JoinStray`. The gate's peer reads are atomic with the `START`. |
| `FcRead`, `FcAct` | `checkFence` (`group_replication_fence.go:229`), `groupReplicationFenceReason` (`:306`), `fenceGroupReplicationMember` (`:394`), `decide` (`:134`), `beginStart`/`reset` epochs (`:102`, `:123`), `armed` (`:188`) | Not armed in the 30s watch window. A fence that already holds is not re-decided (re-deciding only refuses more decisions). The `lock_wait_timeout` failure of the `SET` is left out. Unidentifiable voters are left out. |
| `HBoot1`, `HBootGo`, `HBootGiveUp`, `HBootAbort` | `StartGroupReplication` (`vttablet/tabletmanager/rpc_group_replication.go:47`), `stopServingBeforeBootstrap` (`group_replication.go:1005`), `startGroupReplicationLocked`, `stopOngoingGroupStartLocked` (`:526`), `finishGroupJoinLocked` (`:642`), `noteBootstrap` (`group_replication_legitimacy.go:119`) | The serving pause (`pauseServingLocked`) is left out; a PRIMARY tablet stops serving. RECOVERING-without-ONLINE is not modeled. |
| `OBegin` | `matchGroupNotBootstrapped` (`vtorc/inst/group_replication.go:576`), `bootstrapGroupReplication` (`vtorc/logic/group_replication_recovery.go:373`), `chooseGroupBootstrapCandidate` (`:618`), `memberGTIDSet` (`:724`) | The voters' statuses are one atomic snapshot. The 10s grace for a `START` in progress is "may proceed at any time", with the preference for members without one. Lowest alias is a nondeterministic choice. |
| `OIntent` | `WriteGroupReplicationBootstrapIntent` (`vtctl/reparentutil/group_replication_bootstrap_intent.go:71`), then the RPC | The lock check (`CheckShardLocked`) is left out of every topology write: writes are guarded only by their compare-and-swap, which over-approximates the check-then-write race. |
| `OReply`, `OTimeout`, `OAdopt`, `OAdoptLater` | `RecordGroupReplicationBootstrap`, `AdoptGroupReplicationBootstrap`, `adoptableGroupIncarnation` (`group_replication_bootstrap_intent.go:218`, `:199`, `:158`), `writeGroupReplicationIncarnation` (`vtctl/reparentutil/group_replication.go:293`), `adoptGroupReplicationBootstrap` and `matchGroupBootstrapNotRecorded` (`vtorc/logic/group_replication_recovery.go:486`, `vtorc/inst/group_replication.go:586`) | The adoption's status read and its write are one step. A reply is matched to its RPC by the VTOrc and the intent token: a retried RPC never gets the reply of an earlier one. |
| `OLeaseExpire` | etcd lease (`topo/etcd2topo/lock.go:247`) | At most once (`MaxExpire`). |
| `OIntentExpire` | `GroupReplicationBootstrapIntentFence` (2 minutes) | With `INTENT_OUTLASTS_BOOT`, only while no recovery or bootstrap is in flight. |
| `StaleTopo`, `AsyncApply` | `reconcileStaleTopoPrimary` (`vtorc/logic/topology_recovery.go:1677`), `forceDemotePrimary`, `setReplicationSource`, `setReplicationSourceLocked` (`vttablet/tabletmanager/rpc_replication.go:1091`) | The tablet follows the type VTOrc writes into the topology. |

## What the model does not cover

- Liveness, beyond the stuck-state check of S7d r3. No fairness, no timing: every timeout is a nondeterministic step, so the model cannot say how long an outage lasts.
- PRS, ERS, `InitPrimary` and `InitShardPrimary` (and so the `InitPrimary` exception, which is writable before it serves), `UndoDemotePrimary`, `SetReadWrite`, `DemotePrimary` except through `StaleTopoPrimary`, the migration (`MigrateReplicationMode`) and its pauses, voter replacement (`GroupVotersOutOfDate`), the group primary move (NEW-4), backups.
- vtgate. Clients write to any MySQL that accepts commits, which is stronger than vtgate's routing; the probe's routing variants are not repeated.
- The durability policy: the shard is always under a Group Replication policy with all three voters listed. `offline_mode`, semi-sync, two-phase commit, heartbeats and replication lag are left out.
- MySQL: member weights, RECOVERING, ERROR states that wedge a member (NEW-5), auto-rejoin, certification, flow control, the 1–2s blocks of status reads during a `START`, a partition that does not cut the whole group in two, more than one partition at a time.
- Bounds: exhaustively, at most one crash, one leave, one loss of majority and one lease expiry per behavior, one transaction, two intents and two new incarnations (one in the `tablet` family); by simulation only, two of each fault, two transactions, three intents and three new incarnations. Bugs that need more are outside the checked space, and simulation samples its space.

## Next milestone

- Decide on the two findings, then switch `CAND_EXECUTED` (or the tablet-side check) into the code's column and make `integrated` pass.
- PRS and ERS with the reparent journal, `DemotePrimary`/`UndoDemotePrimary`, `SetReadWrite`, and `InitPrimary`'s exception (writable before it serves); voter replacement (`GroupVotersOutOfDate`), which changes the voters the invariants count.
- Liveness with fairness: every voter back eventually leads to a serving legitimate primary, which needs a smaller model or a liveness-specific abstraction.
- Partial partitions (a majority that cannot reach the primary, asymmetric VTOrc reachability) and the semantics of a member that is ONLINE without quorum.
- More exhaustive coverage: two transactions in the `tablet` family through further symmetry or a `VIEW`, and leaves in the `orcs` family.
