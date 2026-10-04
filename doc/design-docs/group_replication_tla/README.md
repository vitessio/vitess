# TLA+ model of the Group Replication safety protocol

`GRSafety.tla` models one shard of Vitess's Group Replication support (design: `doc/design-docs/GroupReplication.md`; every bug found so far: `doc/failover-audit/GroupReplication.md`). It covers MySQL Group Replication as far as safety needs it, the voters' vttablets (sync loop, fence check, bootstrap RPC), and up to two VTOrcs with a shard lock whose lease can expire. The model follows the code on the `group-replication-prototype` branch, not an idealized design. Each fix is a `CONSTANT` switch, so the model can check the design with the fix and reproduce the bug without it.

It is the first milestone. It is a bounded model check, not a proof. The bounds, and everything the model leaves out, are listed below.

## Running it

```
TLA2TOOLS_JAR=/path/to/tla2tools.jar ./run.sh              # every configuration, checked against its expected outcome
TLA2TOOLS_JAR=/path/to/tla2tools.jar ./run.sh new1 orcs    # some of them
```

`run.sh` runs TLC with `-workers auto` and prints, per configuration, whether the outcome matches the expected one (no error, a deadlock for the stuck-state check, or a violation of the named invariant), with the time, the distinct states, the depth and the trace length. It exits non-zero on an unexpected outcome. `TLA2TOOLS_JAR` defaults to `tla2tools.jar` next to the script, which `.gitignore` keeps out of the repository; download it from the TLA+ releases (the runs below used a nightly TLC, 2026.10.02). `TLC_HEAP` (default `8g`) sets the Java heap, `TLC_OUT` (default `out/`) the output directory, and `TLC_SIM_TRACES` (default 100,000 per worker) the behaviors of the configurations that run in simulation (`SIMULATED` in `run.sh`). The TLC outputs keep the full counterexample traces. The whole suite takes about an hour and three quarters on 4 cores.

## What is modeled

**Voters.** Three servers, each a host with a mysqld and a vttablet; all three are the shard's voters, which never change. A host crash takes down both.

**MySQL and Group Replication.**
- A group incarnation `i` has a view, a primary and a history `hist[i]`: the data of the member that bootstrapped it, plus every transaction decided in it. A commit is decided once a majority of the *current view* accepts it (MySQL's view quorum), and is acknowledged right away.
- *Accepted is not received.* A member receives a decided transaction into its relay log only through `Deliver`, and executes it through `Apply`. A group that loses its majority (`LoseMajority`, a partition) delivers nothing more, and its members then leave it: a transaction that only the old primary had received exists only in its binlog.
- `relay_log_recovery=ON`: a host crash drops the received, unapplied backlog. A bootstrap applies the backlog first (lab L4); so does a join, when its `START` begins (lab, "Bootstrap candidate" in the audit: the joiner then has the transactions in its binlog, and MySQL refuses it if the group lacks them).
- With `DECIDE_CRASH`, a group decides a transaction and its primary crashes before committing it (`DecideCrash`): never acknowledged, it reaches the other members' relay logs, and after a loss of majority can exist in a relay log only.
- Elections: a group that keeps its majority elects a new primary when its primary leaves. Under `BEFORE_ON_PRIMARY_FAILOVER` the new primary applies everything decided before its election ends, and the end of the election sets `super_read_only` (with the member action disabled) or clears it (enabled). A bootstrap and a stray group start an election too.
- A clean leave shrinks the view and keeps quorum; an expulsion or a crash needs the rest of the view to outvote the member, otherwise the group loses its majority.
- A join completes into any live group whose history contains the joiner's binlog (GR refuses a member with extra transactions), recovering from the primary. A join that is still running when no live group is left ends alone in a new incarnation, as its primary (NEW-1's MySQL mechanism, `JoinStray`), or fails.
- A `START GROUP_REPLICATION` keeps running in MySQL after its client gave up, and MySQL refuses another `START` or a `STOP` until it ends.

**vttablet.**
- The sync loop reads MySQL's status at the start of a run, without the lock, and stops serving on it (`SyncRead`, `SyncStop`). Serving again (`serveAgain`) and every promotion (`changeTypeLocked`) are decisions in three steps under the action lock: take the lock, the not-serving generation and the fence snapshot; read MySQL's status; act. A decision makes MySQL writable and serves only if the serving invariant held on the status it read (recorded incarnation, ONLINE primary with quorum, election ended, majority of the voters in the view), no fence was decided since its snapshot, and no not-serving reason since its generation.
- Demotion of a primary that lost its role or whose MySQL is out of its group; leaving a foreign incarnation (fence first, then `STOP`), except a group the tablet bootstrapped within the grace, or the group of a live bootstrap intent that names the tablet; gated rejoins (only while another tablet reports an active member of the legitimate group).
- The fence check in two steps, without the action lock: read MySQL's view; decide under `mu` (in the same epoch, no bootstrap in progress), set `super_read_only`, and make a PRIMARY tablet stop serving. It fences stray incarnations without the voter majority, the shrink of a view in which it saw the voter majority in this incarnation, and keeps a fence that holds. Joins, bootstraps and leaves start new epochs, which drop a pending fence.
- The `StartGroupReplication(bootstrap)` RPC, which carries the VTOrc, the intent's token, the incarnation it expects and the required transactions: it waits for the action lock; refuses a superseded intent (`BOOT_TOKEN`: the shard record no longer holds the token, or lists another incarnation) before anything changes; stops serving on a PRIMARY tablet, refuses an active member, waits for a `START` in progress to end (and then, the intent checked again, makes MySQL leave whatever that `START` formed or joined) or gives up; applies its relay log if that covers the required transactions, and refuses unless MySQL then executed them all (`BOOT_REQ`); starts MySQL's bootstrap, notes the bootstrapped incarnation (trusted for the grace minute), and replies. If VTOrc gave up, the handler ends; a `START` it issued keeps running. With `TOKEN_TOPO_TIMEOUT`, any read of the shard record for the token check may time out, and the check is then skipped.

**VTOrc.** `GroupNotBootstrapped` under the shard lock: a fresh status of every voter, all reachable and none active, the candidate whose executed plus received set contains every other voter's (preferring the target of a live intent, then, with `CAND_BINLOG`, a member that executed all of them, then a member without a `START` in progress, then a PRIMARY tablet, then any: the lowest-alias tie-break is a nondeterministic choice, see "Symmetry"), and the union of every voter's executed and received set, which the RPC requires (`BOOT_REQ`); the bootstrap intent (a compare-and-swap on the incarnation, refused while another target's intent is live); the RPC; then the incarnation recorded with a compare-and-swap on the expected incarnation and the intent's token, or, if the RPC failed or timed out, the adoption of the target's group. A write that finds the incarnation recorded already clears only an intent for an earlier incarnation (`KEEP_NEWER_INTENT`). `GroupBootstrapNotRecorded` adopts later. A lease can expire under a VTOrc that stalls; the VTOrc keeps acting when it resumes. An intent expires after its two-minute fence. `StaleTopoPrimary` force-demotes a PRIMARY tablet that is not the newest, and without the NEW-3 fix configures the default channel on a voter outside its group.

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
| `NoDualBootstrap` | No two live groups were bootstrapped on different tablets from the same recorded incarnation. Checked under the timing assumption `INTENT_OUTLASTS_BOOT` (below), and with the shard record answering the tablet (`TOKEN_TOPO_TIMEOUT` off). It is a structural property: a second group is read-only, not served and never recorded, so it costs availability, not data (see Finding 2). |
| `NoMinorityAck` | No write is acknowledged by a view that holds fewer than a majority of the voters. **Expected to be violated.** |

**`NoMinorityAck` is a documented, expected violation.** The probe defined it as "every acknowledged write was accepted by a view holding a majority of the voters". MySQL's quorum is that of the current view, not of the voters: when voters leave cleanly, a serving primary alone in its view keeps quorum and keeps committing, and it was writable before its view shrank, so nothing in MySQL fences it. The fence check sets `super_read_only` within about 150ms, and the sync loop stops serving within a second; the writes in between are acknowledged on one voter (G11: 4–20 writes). They are not lost: they are decided in the recorded incarnation (`NoLostAck` holds), and a group that later loses its majority is bootstrapped again only with every voter reachable, on the voter that holds them. The `minority` configuration shows the 4-step trace: a commit, a crash and a clean leave shrink the primary's view to itself, and the next commit is acknowledged by one voter. This is the same trade-off as a semi-sync primary that acknowledges before its replica stores the transaction; `paxos_single_leader` does not change it.

## Switches

Fixes (`TRUE` is the code on the branch): `LEGIT` (recorded incarnation plus voter majority, NEW-1), `BOOT_ALL` (bootstrap only with every voter reachable), `SERVE_LOCKED` (serve again only under the lock on a fresh status, with generations, S7d r2), `MA_DISABLED` (member action `mysql_disable_super_read_only_if_primary` disabled before every `START`), `FENCE`, `FENCE_SNAPSHOT`, `JOIN_GATE`, `INTENT`, `INTENT_FENCE`, `INTENT_PREFER`, `ADOPT` (S7d r3), `INC_CAS`, `NEW3_FIX`; and for the two findings below, `CAND_BINLOG` and `BOOT_REQ` (finding 1), `BOOT_TOKEN` and `KEEP_NEWER_INTENT` (finding 2).

A rule considered and rejected (`FALSE` is the code): `CAND_EXECUTED`, only a voter that executed every voter's transactions may be the candidate. With `DECIDE_CRASH` it gets stuck (`cand_strict`); see Finding 1.

Environment and checking modes: `DECIDE_CRASH` (a primary crashes after its group decided a transaction, before committing it), `TOKEN_TOPO_TIMEOUT` (the tablet's read of the shard record for the token check may time out, and the check is skipped), `STALE_TOPO` (VTOrc's `StaleTopoPrimary` runs), `STALE_REC` (a tablet decides on the shard record it read last instead of the current one), `SPLIT` (decisions and the fence check run as interleaved steps; `FALSE` makes each one atomic), `STUCK_CHECK` (no timeout longer than the shard lock's lease fires, and `Done` marks healthy end states, so that TLC's deadlock check finds stuck states), `INTENT_OUTLASTS_BOOT` (an intent expires only while no VTOrc recovery and no bootstrap is in flight: no VTOrc stalls past the two-minute fence).

## Configurations and bounds

Every configuration has 3 voters and starts from a group of all three with a serving primary. The base bounds are at most 2 new incarnations (`MaxInc = 3`), 2 transactions, 2 intents, and fault budgets of one crash, one leave of a live group (clean, or an expulsion), one loss of majority, and one lease expiry; each family below lowers or raises some of them. Faults that follow from another are free: the members of a group that lost its majority leave it, a crashed host restarts. The validation configurations use the bounds of their family.

The full model with every interleaving does not finish within an hour on this machine (4 cores, 15G): a run with two VTOrcs, two transactions and every step split reached 18M distinct states at depth 20 in 13 minutes with its queue still growing (the run log is summarized under "Results"). The current design is therefore checked by families of configurations, each with the features its properties need:

| Family | VTOrcs | Interleaving | Bounds | Checks |
|---|---|---|---|---|
| `tablet` | 1, no lease expiry | decisions and fence check split (`SPLIT`) | 1 transaction, `MaxInc = 2` | fence ordering, serving decisions, crashes and `relay_log_recovery`, NEW-1, the member action, the bootstrap candidate |
| `orcs` | 2, one lease expiry | decisions and fence check atomic | 1 transaction, no leave (a crash still removes a member); `stale_rpc*` and `dup_record`: one clean leave, no transaction | intents, adoption, compare-and-swap, concurrent bootstraps, stalls, superseded intents |
| `integrated` | 2, two lease expiries | split | 2 transactions, `MaxInc = 4`, 3 intents, 2 of each fault | everything together, by simulation only (random behaviors, seed 1, depth 120) |

Why the split is sound for what each family checks:
- A family that makes a decision atomic only removes interleavings *inside* the decision. The `tablet` family checks every interleaving of the decisions with the fence check, crashes, leaves and elections, with one VTOrc. Two VTOrcs only change the shard record and MySQL's group through bootstraps, which the action lock serializes with the decisions in both families.
- `MaxInc = 2` in the `tablet` family allows one new incarnation: a bootstrap or a stray group. Every tablet-side scenario of the audit needs one (S7d r2, NEW-1, the majority bootstrap, S7d r3, finding 1). The scenarios that need two (a stray next to a bootstrap, two bootstraps) are in the `orcs` family.
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

Each configuration switches off the fixes in the second column and checks only the named invariant, so the counterexample is attributable to that fix. Every other fix stays on. All exhaustive; the search stops at the first violation. TLC's search is breadth-first, but with several workers the first violation found can be one or two states longer than the shortest one (the original model gives 11 states for `s7d_r3` with one worker and 12 with two). "States" is the number of states in the trace, including the initial state.

| Configuration | Bug (audit) | Switched off | Outcome | States | Time | Distinct states explored |
|---|---|---|---|---|---|---|
| `s7d_r2` | S7d r2: single-voter acks after a stale decision | `SERVE_LOCKED`, `MA_DISABLED` | `NoDecisionAck` violated | 10 | 2s | 17,071 |
| `s7d_r2_ma_off` | S7d r2, member action disabled, one leave | `SERVE_LOCKED` | no error | - | 5m10 | 4,094,061 |
| `s7d_r2_ma_off_leaves` | S7d r2, member action disabled, two leaves | `SERVE_LOCKED` | `NoDecisionAck` violated | 11 | 4s | 47,434 |
| `new1` | NEW-1: a stale member re-forms the group | `LEGIT` | `NoLostAck` violated | 10 | 2s | 12,175 |
| `majority_boot` | bootstrap from a reachable majority | `BOOT_ALL` | `NoLostAck` violated | 11 | 5s | 10,281 |
| `s7d_r3` | S7d r3: lost bootstrap reply, no adoption | `ADOPT` (`STUCK_CHECK`) | deadlock: stuck without a recorded group | 12 | 4s | 4,253 |
| `dual_nofence` | two VTOrcs bootstrap different tablets | `INTENT_FENCE` | `NoDualBootstrap` violated | 14 | 4s | 20,932 |
| `new3` | NEW-3: `StaleTopoPrimary` on a voter | `NEW3_FIX` | `NoAsyncVoter` violated | 8 | 3s | 2,610 |
| `fence_snapshot` | a decision older than a fence lifts it | `FENCE_SNAPSHOT` | `FenceNotUndone` violated | 13 | 12s | 68,982 |
| `no_cas` | incarnation writes without compare-and-swap | `INC_CAS` | `AdoptOnce` violated | 18 | 30s | 416,552 |
| `relay_cand` | finding 1: a candidate with acknowledged transactions in its relay log only | `CAND_BINLOG`, `BOOT_REQ` | `NoLostAck` violated | 13 | 3s | 40,423 |
| `stale_rpc` | finding 2: a bootstrap RPC of a superseded intent (2 VTOrcs, a clean leave, no transaction) | `BOOT_TOKEN` | `NoDualBootstrap` violated | 18 | 11s | 274,971 |
| `dup_record` | finding 2, second path: a late reply clears a newer intent (as `stale_rpc`) | `KEEP_NEWER_INTENT` | `NoDualBootstrap` violated | 22 | 40s | 1,017,819 |
| `minority` | expected: a shrinking view | none | `NoMinorityAck` violated | 4 | 0s | 378 |
| `cand_strict` | the rejected rule: only a voter that executed every transaction bootstraps (`CAND_EXECUTED`, with `DECIDE_CRASH`, `STUCK_CHECK`) | `CAND_BINLOG`, `BOOT_REQ` | deadlock: no candidate | 6 | 1s | 686 |
| `stale_rpc_timeout` | expected: the token check skipped when the topology does not answer (`TOKEN_TOPO_TIMEOUT`) | none | `NoDualBootstrap` violated | 18 | 12s | 273,827 |

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
- `relay_cand`, `stale_rpc`, `dup_record`, `cand_strict`, `stale_rpc_timeout`: see the two findings below.

### Current design

| Configuration | Family | Bounds | Mode | Result | Distinct states | Depth | Time |
|---|---|---|---|---|---|---|---|
| `current` | `tablet` | the code: 1 VTOrc, 1 transaction, `MaxInc = 2`, split | exhaustive | no error | 11,182,538 | 58 | 12m06 |
| `tablet` | `tablet` | as `current`, and a primary may crash after its group decided a transaction (`DECIDE_CRASH`) | exhaustive | no error | 14,061,270 | 58 | 14m26 |
| `stale_rec` | `tablet` | as `tablet`, decisions on the tablet's last shard record, atomic | exhaustive | no error | 8,334,302 | 49 | 5m44 |
| `s7d_r3_adopt` | `tablet` | stuck check, adoption on, `DECIDE_CRASH`, 1 transaction, `MaxInc = 3`, no leave | exhaustive | no stuck state | 778,689 | 48 | 55s |
| `orcs` | `orcs` | 2 VTOrcs, 1 lease expiry, 1 transaction, no leave, atomic, `DECIDE_CRASH` | exhaustive | no error | 4,998,912 | 50 | 4m26 |
| `orcs_stall` | `orcs` | as `orcs`, a VTOrc may stall past the intent fence (all but `NoDualBootstrap`) | exhaustive | no error | 19,418,822 | 57 | 16m34 |
| `stale_rpc_fixed` | `orcs` | as `stale_rpc` (2 VTOrcs, 1 lease expiry, 1 clean leave, 1 loss of majority, no transaction, `MaxInc = 4`, 3 intents), `NoDualBootstrap` only | exhaustive | no error | 8,719,869 | 53 | 6m42 |
| `tablet_tx2` | `tablet` | as `tablet`, 2 transactions | simulation | no error | 400,000 traces, 25.5M states, mean length 26 | - | 5m29 |
| `integrated` | all | 2 VTOrcs, split, 2 transactions, `MaxInc = 4`, 3 intents, 2 of each fault, `DECIDE_CRASH` | simulation | no error | 400,000 traces, 31.3M states, mean length 26 | - | 6m53 |
| `integrated_core` | all | as `integrated`, every invariant but `NoDualBootstrap` | simulation | no error | 400,000 traces, 31.3M states, mean length 25 | - | 6m11 |

Every configuration of the current design runs with every fix on, and checks every invariant except `NoMinorityAck` (and `NoDualBootstrap` where noted; `integrated` now checks it too). The simulations use seed 1, at most 120 steps per behavior, and 100,000 behaviors per worker; they explore a sample, not the whole space. The exhaustive runs with two transactions did not converge within the hour (their queues still grew after 10 minutes); the `tablet` and `orcs` families are exhaustive with one transaction, and two transactions are covered by simulation only. With one transaction, `DECIDE_CRASH` and an acknowledged write exclude each other: the configurations with `DECIDE_CRASH` check that its unacknowledged transaction in a relay log neither blocks the bootstrap (`s7d_r3_adopt`) nor breaks the other invariants, and the simulations combine both.

Runs that were stopped because their queue still grew after 10 minutes (before the reductions listed above, unless noted; earlier versions of the model):

| Run | Distinct states | Depth | Queue | Time |
|---|---|---|---|---|
| 2 VTOrcs, 2 transactions, every step split, `MaxInc = 3` | 18.0M | 20 | 10.6M, growing | 13m |
| `tablet` family with 2 transactions | 7.9M | 27 | 1.49M, growing | 10m |
| `orcs` family with 2 transactions, with leaves | 8.4M | 22 | 3.1M, growing | 7m |
| `stale_rec` with split decisions | 7.1M | 27 | 1.64M, growing | 9m |

## Finding 1 (fixed): a bootstrap candidate whose transactions are only in its relay log

With every other fix on, the candidate rule before the fix violates `NoLostAck` (configuration `relay_cand`, 13 states, exhaustive), with one VTOrc and no lease expiry:

```
 1. Init          s1 is the primary of incarnation 1 {s1, s2, s3}
 2. Commit(s1)    transaction 1 is acknowledged; only s1 has it
 3. Deliver(s2)   s2 receives transaction 1 into its relay log, does not apply it
 4. LoseMajority(1)
 5. SyncDemote(s1)
 6. LeaveDead(1)  no member is left in a group
 7. OBegin(o1)    VTOrc reads every voter: s1 executed {1}, s2 received {1}, s3 {}; it chooses s2
 8. Crash(s2)     mysqld restarts: relay_log_recovery drops {1}
 9. Restart(s2)
10. OIntent(o1)   the intent for s2, and the bootstrap RPC
11. HBoot1(s2)    the tablet starts MySQL's bootstrap: s2 holds nothing
12. BootComplete(s2)  incarnation 2 = {s2}, history {}
13. OReply(o1)    VTOrc records incarnation 2: the acknowledged transaction 1 is not in its history
```

VTOrc compared each voter's executed set unioned with its received set (`memberGTIDSet`, from `PrimaryStatus.Position` and `GroupReplicationStatus.ReceivedTransactionSet`), and among equal sets took the lowest alias. Nothing checked the candidate's set again, neither VTOrc after its choice nor the tablet before `START`, and `relay_log_recovery=ON` discards the received backlog when mysqld restarts. `TestBootstrapGroupReplicationPrefersTransactionsInTheBinlog` reproduces the choice on the Go code.

**The fix** (design: "The bootstrap candidate"). VTOrc still requires a candidate whose executed and received set contains every voter's, prefers among them, after a live intent's target, one that *executed* all of them (`CAND_BINLOG`), and sends the union of every voter's executed and received sets with the RPC; under the action lock, right before `START`, the tablet applies its relay log if that covers what MySQL has not executed, and refuses unless MySQL then executed the whole set (`BOOT_REQ`). In the trace above, VTOrc chooses s1; had it chosen s2 (as the intent's target, say), s2's tablet refuses after the restart, and VTOrc chooses again.

**The rejected rule.** `CAND_EXECUTED` modeled the first proposal: only a voter that executed every voter's transactions may be the candidate. MySQL can leave a transaction in relay logs only: a group decides it while its primary crashes before committing it, the other members receive it, and the group then loses its majority. With `DECIDE_CRASH`, `cand_strict` finds the state in which no voter qualifies, and nothing else can happen (6 states): `DecideCrash(s1)`, `Restart(s1)`, `Deliver(s2)`, `LoseMajority(1)`, `LeaveDead(1)`; s2 alone holds transaction 1, in its relay log. The lab confirmed both halves of why the new group must hold such a transaction: a member whose relay log holds transactions the group lacks applies them when its join starts, and MySQL then refuses the join for good; and the applier channel's SQL thread can apply them without the group (`doc/failover-audit/GroupReplication.md`, "Bootstrap candidate").

## Finding 2 (fixed): a bootstrap RPC of a superseded intent

The `integrated` simulation found a violation of `NoDualBootstrap` with every other fix on (26 states); `stale_rpc` finds it exhaustively, in 18 states, with two VTOrcs, one lease expiry, one clean leave and no transaction. VTOrcs o1 and o2 both choose s1 (o1's lease expired in between) and both write an intent for it, o2's replacing o1's (the same target is not fenced). o2's RPC gets s1's action lock first, bootstraps incarnation 2, and o2 records it, which clears the intent. o1's RPC, for the superseded intent, still waits for s1's action lock. s1's MySQL then leaves its group of one (in the code: a mysqld restart while its vttablet keeps running; the model's clean leave). o1's RPC now runs, and s1 bootstraps incarnation 3 from recorded incarnation 2. Meanwhile o2, finding every voter out of a group and no live intent, bootstraps s2 as incarnation 4 from recorded incarnation 2, as the design intends. No acknowledged write is at risk: the compare-and-swap refuses to record s1's group, which is read-only and not served; the cost is that s1 is out of the shard's group until it leaves its own, after up to a minute.

**The fix.** The RPC carries the intent's token and the incarnation it was recorded for, and the tablet refuses a bootstrap whose intent the shard record no longer holds (`BOOT_TOKEN`): when the RPC gets the action lock, before the `STOP` of a `START` in progress, and right before MySQL's `START`. In the model each check is atomic with the step it guards. With it, TLC found a second path in 22 states (`dup_record`): o2 adopted the group that o1 bootstrapped, the group lost its majority again, and o2 recorded a new intent to bootstrap s1 again; o1's reply then arrived and recorded the same incarnation again, and that write cleared o2's newer intent while its bootstrap ran; o1, with no live intent to fence it, bootstrapped s2. A write that finds its incarnation recorded already now keeps an intent recorded for that incarnation (`KEEP_NEWER_INTENT`). With both, `stale_rpc_fixed` passes exhaustively, and the `integrated` simulation finds nothing.

**What remains.** The tablet's read of the shard record waits at most a second, and the tablet bootstraps without the check if the topology does not answer, so that the check never keeps the shard's group down. `stale_rpc_timeout` shows that this fallback can still reach the state, as before the fix (18 states). The model also assumes, with `INTENT_OUTLASTS_BOOT`, that no VTOrc stalls past the intent's two-minute fence.

## Conformance: actions and the code they model

Paths are relative to `go/vt/`; line numbers are on `group-replication-prototype` at the commit that adds this model.

| Action | Code | Abstractions |
|---|---|---|
| `Commit` | MySQL commit on the group primary | Decided and acknowledged in one step. Certification conflicts, flow control and transaction size limits are left out. Any client, through vtgate or directly. |
| `Deliver`, `Apply` | XCom delivery to the relay log; the applier | A member receives everything decided at once. |
| `DecideCrash` | the group decides a transaction while its primary crashes before committing it | Only with `DECIDE_CRASH`. Uses the crash budget and a transaction. |
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
| `HBoot1`, `HBootGo`, `HBootGiveUp`, `HBootAbort` | `StartGroupReplication` (`vttablet/tabletmanager/rpc_group_replication.go:47`), `stopServingBeforeBootstrap` (`group_replication.go:1005`), `startGroupReplicationLocked`, `stopOngoingGroupStartLocked` (`:526`), `finishGroupJoinLocked` (`:642`), `noteBootstrap` (`group_replication_legitimacy.go:119`) | The serving pause (`pauseServingLocked`) is left out; a PRIMARY tablet stops serving. RECOVERING-without-ONLINE is not modeled. The checks of the request (`checkGroupBootstrapIntentLocked`, `checkGroupBootstrapLocked` in `group_replication_bootstrap.go`) are atomic with the step they guard: the intent check when the RPC gets the lock, with the `STOP` of a `START` in progress, and with MySQL's `START`; the relay log's application (`ApplyGroupReplicationRelayLog`) and the GTID check with the `START`. A crash between them only loses the RPC. |
| `OBegin` | `matchGroupNotBootstrapped` (`vtorc/inst/group_replication.go:576`), `bootstrapGroupReplication`, `chooseGroupBootstrapCandidate`, `memberGTIDSets` (`vtorc/logic/group_replication_recovery.go`) | The voters' statuses are one atomic snapshot. The 10s grace for a `START` in progress is "may proceed at any time", with the preference for members without one. Lowest alias is a nondeterministic choice. |
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

- PRS and ERS with the reparent journal, `DemotePrimary`/`UndoDemotePrimary`, `SetReadWrite`, and `InitPrimary`'s exception (writable before it serves); voter replacement (`GroupVotersOutOfDate`), which changes the voters the invariants count.
- Liveness with fairness: every voter back eventually leads to a serving legitimate primary, which needs a smaller model or a liveness-specific abstraction.
- Partial partitions (a majority that cannot reach the primary, asymmetric VTOrc reachability) and the semantics of a member that is ONLINE without quorum.
- More exhaustive coverage: two transactions in the `tablet` family through further symmetry or a `VIEW`, and leaves in the `orcs` family.
