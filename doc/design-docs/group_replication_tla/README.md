# TLA+ model of the Group Replication safety protocol

`GRSafety.tla` models one shard of Vitess's Group Replication support (design: `doc/design-docs/GroupReplication.md`; every bug found so far: `doc/failover-audit/GroupReplication.md`). It covers MySQL Group Replication as far as safety needs it, the voters' vttablets (sync loop, fence check, bootstrap RPC), and up to two VTOrcs with a shard lock whose lease can expire. The model follows the code on the `group-replication-prototype` branch, not an idealized design. Each fix is a `CONSTANT` switch, so the model can check the design with the fix and reproduce the bug without it.

The first milestone covered the bootstrap protocol and the tablets' serving decisions. The second (see "Second milestone") adds voter replacement, `PlannedReparentShard`, `EmergencyReparentShard` and the initial promotion, the RPCs that make MySQL writable (`DemotePrimary` and its revert, `UndoDemotePrimary`, `SetReadWrite`), mysqld restarts under a running vttablet, a shard that never had a primary, and a liveness check with fairness (`GRLiveness.tla`). It found six problems in the code (findings 3 to 8); their fixes are on branch `gr-fixes5`, pending merge, and the model checks each of them. It is a bounded model check, not a proof. The bounds, and everything the model leaves out, are listed below.

## Running it

```
TLA2TOOLS_JAR=/path/to/tla2tools.jar ./run.sh              # every configuration, checked against its expected outcome
TLA2TOOLS_JAR=/path/to/tla2tools.jar ./run.sh new1 orcs    # some of them
```

`run.sh` runs TLC with `-workers auto` and prints, per configuration, whether the outcome matches the expected one (no error, a deadlock for the stuck-state check, or a violation of the named invariant), with the time, the distinct states, the depth and the trace length. It exits non-zero on an unexpected outcome. A configuration with the stuck-state check (`STUCK_CHECK`) runs with TLC's deadlock check, which reports a state without successors that `Done` does not mark as a healthy end state; every other configuration ignores states without successors (its budgets are used up). `live` checks the liveness module `GRLiveness.tla`. `TLA2TOOLS_JAR` defaults to `tla2tools.jar` next to the script, which `.gitignore` keeps out of the repository; download it from the TLA+ releases (the runs below used a nightly TLC, 2026.10.02). `TLC_HEAP` (default `8g`) sets the Java heap, `TLC_OUT` (default `out/`) the output directory, and `TLC_SIM_TRACES` (default 100,000 per worker) the behaviors of the configurations that run in simulation (`SIMULATED` in `run.sh`). The TLC outputs keep the full counterexample traces. The whole suite takes about four hours on 4 cores.

## What is modeled

**Voters.** Three servers, each a host with a mysqld and a vttablet. All three are the shard's voters; in the first milestone the list never changes (voter replacement: "Second milestone"). A host crash takes down both.

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
- The `StartGroupReplication(bootstrap)` RPC, which carries the VTOrc, the intent's token, the incarnation it expects and the required transactions: it waits for the action lock; refuses a superseded intent (`BOOT_TOKEN`: the shard record no longer holds the token, or lists another incarnation) before anything changes; stops serving on a PRIMARY tablet, refuses an active member, waits for a `START` in progress to end (and then, the intent checked again, makes MySQL leave whatever that `START` formed or joined) or gives up; applies its relay log if that covers the required transactions, and refuses unless MySQL then executed them all (`BOOT_REQ`); starts MySQL's bootstrap, notes the bootstrapped incarnation (trusted for the grace minute), and replies. If VTOrc gave up, the handler ends; a `START` it issued keeps running. With `TOKEN_TOPO_TIMEOUT`, any read of the shard record for the token check may time out, and the check is then skipped. Only the refusal for a missing transaction, decided while no `START` runs (when the RPC gets the lock, or once the `START` it waited for ended and MySQL left whatever that `START` formed), is a definitive refusal; every other refusal, and the `UNAVAILABLE` of an RPC that gave up waiting for a `START`, is an error.

**VTOrc.** `GroupNotBootstrapped` under the shard lock: a fresh status of every voter, all reachable and none active, the candidate whose executed plus received set contains every other voter's (preferring the target of a live intent, then, with `CAND_BINLOG`, a member that executed all of them, then a member without a `START` in progress, then a PRIMARY tablet, then any: the lowest-alias tie-break is a nondeterministic choice, see "Symmetry"), and the union of every voter's executed and received set, which the RPC requires (`BOOT_REQ`); the bootstrap intent (a compare-and-swap on the incarnation, refused while another target's intent is live); the RPC; then the incarnation recorded with a compare-and-swap on the expected incarnation and the intent's token, or, if the RPC failed or timed out, the adoption of the target's group. After a definitive refusal, VTOrc withdraws its intent instead (`WITHDRAW_ON_REFUSAL`): a compare-and-swap that removes it only while the shard record holds its token, for the incarnation it was recorded for. When a live intent names a voter other than the candidate (it no longer holds every transaction), and that voter is up, in no group and runs no `START`, VTOrc sends that intent's bootstrap to it again instead of fencing itself (`REPROBE_STALE_INTENT`): the intent's token and expected incarnation, the current required set, no write of the intent; the reply is handled as the intent's own (record, adopt, or withdraw after a definitive refusal). A reply is matched to the RPC that VTOrc waits for (`orp`, the request's `rp`), as gRPC matches it to its call: a re-probe reuses the VTOrc and the token. Like every topology write of the model, the withdrawal is guarded by its compare-and-swap only, not by the lock check, so that a VTOrc whose lease expired may withdraw too. A write that finds the incarnation recorded already clears only an intent for an earlier incarnation (`KEEP_NEWER_INTENT`). `GroupBootstrapNotRecorded` adopts later. A lease can expire under a VTOrc that stalls; the VTOrc keeps acting when it resumes. An intent expires after its two-minute fence. `StaleTopoPrimary` force-demotes a PRIMARY tablet that is not the newest, and without the NEW-3 fix configures the default channel on a voter outside its group.

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
| `NoVoterMinority` | No voter write lets a live view of the recorded incarnation that held fewer than a majority of the old voters hold a majority of the new ones (second milestone). |

**`NoMinorityAck` is a documented, expected violation.** The probe defined it as "every acknowledged write was accepted by a view holding a majority of the voters". MySQL's quorum is that of the current view, not of the voters: when voters leave cleanly, a serving primary alone in its view keeps quorum and keeps committing, and it was writable before its view shrank, so nothing in MySQL fences it. The fence check sets `super_read_only` within about 150ms, and the sync loop stops serving within a second; the writes in between are acknowledged on one voter (G11: 4–20 writes). They are not lost: they are decided in the recorded incarnation (`NoLostAck` holds), and a group that later loses its majority is bootstrapped again only with every voter reachable, on the voter that holds them. The `minority` configuration shows the 4-step trace: a commit, a crash and a clean leave shrink the primary's view to itself, and the next commit is acknowledged by one voter. This is the same trade-off as a semi-sync primary that acknowledges before its replica stores the transaction; `paxos_single_leader` does not change it.

## Switches

Fixes (`TRUE` is the code on the branch): `LEGIT` (recorded incarnation plus voter majority, NEW-1), `BOOT_ALL` (bootstrap only with every voter reachable), `SERVE_LOCKED` (serve again only under the lock on a fresh status, with generations, S7d r2), `MA_DISABLED` (member action `mysql_disable_super_read_only_if_primary` disabled before every `START`), `FENCE`, `FENCE_SNAPSHOT`, `JOIN_GATE`, `INTENT`, `INTENT_FENCE`, `INTENT_PREFER`, `ADOPT` (S7d r3), `INC_CAS`, `NEW3_FIX`; and for the two findings below, `CAND_BINLOG` and `BOOT_REQ` (finding 1), `BOOT_TOKEN` and `KEEP_NEWER_INTENT` (finding 2); `WITHDRAW_ON_REFUSAL`, the withdrawal of an intent whose bootstrap its target refused definitively (see "Withdrawing a refused intent"); and `REPROBE_STALE_INTENT`, the bootstrap of a live intent sent again to its target once that target is no longer the candidate (see "Re-probing a stale intent's target").

Unsafe variants of that withdrawal, which validate its conditions (`FALSE` is the code): `WITHDRAW_ANY_FAILURE`, VTOrc withdraws its intent after any error or timeout of the RPC; and `DEFINITIVE_WHILE_STARTING`, the tablet reports as definitive the refusal it sends after giving up on a `START` in progress. Of the re-probe: `REPROBE_NO_REQ`, the re-probe does not carry the required transactions.

A rule considered and rejected (`FALSE` is the code): `CAND_EXECUTED`, only a voter that executed every voter's transactions may be the candidate. With `DECIDE_CRASH` it gets stuck (`cand_strict`); see Finding 1.

Environment and checking modes: `DECIDE_CRASH` (a primary crashes after its group decided a transaction, before committing it), `TOKEN_TOPO_TIMEOUT` (the tablet's read of the shard record for the token check may time out, and the check is skipped), `STALE_TOPO` (VTOrc's `StaleTopoPrimary` runs), `STALE_REC` (a tablet decides on the shard record it read last instead of the current one), `SPLIT` (decisions and the fence check run as interleaved steps; `FALSE` makes each one atomic), `STUCK_CHECK` (no timeout longer than the shard lock's lease fires, and `Done` marks healthy end states, so that TLC's deadlock check finds stuck states; a recovery whose intent the fence refuses is not started, since it changes nothing and its loop would hide a stuck state; and a state that only the expiry of the live intent can end, after its RPC failed without a definitive refusal while its target is in no group and runs no `START`, counts as a healthy end state (`WaitsForExpiry`) only with `EXPIRY_WAIT_OK`, as the wait the design accepted before the re-probe, or once the re-probe budget `MaxProbe` is used up), `INTENT_OUTLASTS_BOOT` (an intent expires only while no VTOrc recovery and no bootstrap is in flight: no VTOrc stalls past the two-minute fence).

## Configurations and bounds

Every configuration has 3 voters and starts from a group of all three with a serving primary. The base bounds are at most 2 new incarnations (`MaxInc = 3`), 2 transactions, 2 intents, 2 re-probes (`MaxProbe`, reached only with two crashes: a target must lose a transaction after VTOrc chose it), and fault budgets of one crash, one leave of a live group (clean, or an expulsion), one loss of majority, and one lease expiry; each family below lowers or raises some of them. Faults that follow from another are free: the members of a group that lost its majority leave it, a crashed host restarts. The validation configurations use the bounds of their family.

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
| `s7d_r2` | S7d r2: single-voter acks after a stale decision | `SERVE_LOCKED`, `MA_DISABLED` | `NoDecisionAck` violated | 11 | 3s | 17,857 |
| `s7d_r2_ma_off` | S7d r2, member action disabled, one leave | `SERVE_LOCKED` | no error | - | 4m53 | 3,564,658 |
| `s7d_r2_ma_off_leaves` | S7d r2, member action disabled, two leaves | `SERVE_LOCKED` | `NoDecisionAck` violated | 11 | 6s | 45,947 |
| `new1` | NEW-1: a stale member re-forms the group | `LEGIT` | `NoLostAck` violated | 10 | 3s | 13,216 |
| `majority_boot` | bootstrap from a reachable majority | `BOOT_ALL` | `NoLostAck` violated | 11 | 2s | 6,575 |
| `s7d_r3` | S7d r3: lost bootstrap reply, no adoption | `ADOPT` (`STUCK_CHECK`) | deadlock: stuck without a recorded group | 11 | 1s | 3,772 |
| `dual_nofence` | two VTOrcs bootstrap different tablets | `INTENT_FENCE` | `NoDualBootstrap` violated | 13 | 3s | 33,111 |
| `new3` | NEW-3: `StaleTopoPrimary` on a voter | `NEW3_FIX` | `NoAsyncVoter` violated | 7 | 2s | 2,501 |
| `fence_snapshot` | a decision older than a fence lifts it | `FENCE_SNAPSHOT` | `FenceNotUndone` violated | 13 | 8s | 57,731 |
| `no_cas` | incarnation writes without compare-and-swap | `INC_CAS` | `AdoptOnce` violated | 18 | 20s | 429,311 |
| `relay_cand` | finding 1: a candidate with acknowledged transactions in its relay log only | `CAND_BINLOG`, `BOOT_REQ` | `NoLostAck` violated | 13 | 6s | 40,570 |
| `stale_rpc` | finding 2: a bootstrap RPC of a superseded intent (2 VTOrcs, a clean leave, no transaction) | `BOOT_TOKEN` | `NoDualBootstrap` violated | 18 | 14s | 224,082 |
| `dup_record` | finding 2, second path: a late reply clears a newer intent (as `stale_rpc`) | `KEEP_NEWER_INTENT` | `NoDualBootstrap` violated | 22 | 52s | 1,158,510 |
| `minority` | expected: a shrinking view | none | `NoMinorityAck` violated | 4 | 1s | 455 |
| `cand_strict` | the rejected rule: only a voter that executed every transaction bootstraps (`CAND_EXECUTED`, with `DECIDE_CRASH`, `STUCK_CHECK`) | `CAND_BINLOG`, `BOOT_REQ` | deadlock: no candidate | 6 | 1s | 937 |
| `stale_rpc_timeout` | expected: the token check skipped when the topology does not answer (`TOKEN_TOPO_TIMEOUT`) | none | `NoDualBootstrap` violated | 18 | 13s | 243,636 |
| `withdraw_any` | unsafe variant of the withdrawal: VTOrc withdraws after any error or timeout (1 VTOrc, no transaction, no crash) | `WITHDRAW_ANY_FAILURE` on | `NoDualBootstrap` violated | 12 | 2s | 551 |
| `withdraw_starting` | unsafe variant of the withdrawal: a refusal sent while a `START` runs reported as definitive (as `withdraw_any`) | `DEFINITIVE_WHILE_STARTING` on | `NoDualBootstrap` violated | 19 | 1s | 3,166 |
| `refusal_stuck` | a refused intent fences the voter that holds the lost transaction (`STUCK_CHECK`, `DECIDE_CRASH`, two crashes) | `WITHDRAW_ON_REFUSAL` | deadlock: stuck until the intent expires | 14 | 7s | 72,895 |
| `reprobe_stuck` | an intent whose RPC failed without a definitive refusal fences the voter that holds the transaction its target lost (as `refusal_stuck`, with the withdrawal, the wait not accepted) | `REPROBE_STALE_INTENT`, `EXPIRY_WAIT_OK` | deadlock: stuck until the intent expires | 13 | 11s | 55,187 |
| `reprobe_noreq` | unsafe variant of the re-probe: it does not carry the required transactions (1 VTOrc, 2 transactions, atomic) | `REPROBE_NO_REQ` on | `NoLostAck` violated | 17 | 13s | 192,618 |

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
- `relay_cand`, `stale_rpc`, `dup_record`, `cand_strict`, `stale_rpc_timeout`: see the two findings below; `withdraw_any`, `withdraw_starting`, `refusal_stuck`: see "Withdrawing a refused intent".

### Current design

| Configuration | Family | Bounds | Mode | Result | Distinct states | Depth | Time |
|---|---|---|---|---|---|---|---|
| `current` | `tablet` | the code: 1 VTOrc, 1 transaction, `MaxInc = 2`, split | exhaustive | no error | 9,545,958 | 58 | 10m38 |
| `tablet` | `tablet` | as `current`, and a primary may crash after its group decided a transaction (`DECIDE_CRASH`) | exhaustive | no error | 12,010,419 | 58 | 13m04 |
| `stale_rec` | `tablet` | as `tablet`, decisions on the tablet's last shard record, atomic | exhaustive | no error | 7,007,168 | 47 | 5m16 |
| `s7d_r3_adopt` | `tablet` | stuck check (the wait for an intent's expiry not accepted), adoption on, `DECIDE_CRASH`, 1 transaction, `MaxInc = 3`, no leave | exhaustive | no stuck state | 801,537 | 48 | 1m04 |
| `orcs` | `orcs` | 2 VTOrcs, 1 lease expiry, 1 transaction, no leave, atomic, `DECIDE_CRASH` | exhaustive | no error | 4,499,270 | 50 | 4m22 |
| `orcs_stall` | `orcs` | as `orcs`, a VTOrc may stall past the intent fence (all but `NoDualBootstrap`) | exhaustive | no error | 18,396,118 | 57 | 17m27 |
| `stale_rpc_fixed` | `orcs` | as `stale_rpc` (2 VTOrcs, 1 lease expiry, 1 clean leave, 1 loss of majority, no transaction, `MaxInc = 4`, 3 intents), `NoDualBootstrap` only | exhaustive | no error | 6,906,994 | 52 | 6m06 |
| `tablet_tx2` | `tablet` | as `tablet`, 2 transactions | simulation | no error | 400,000 traces, 25.5M states, mean length 26 | - | 6m05 |
| `integrated` | all | 2 VTOrcs, split, 2 transactions, `MaxInc = 4`, 3 intents, 2 of each fault, `DECIDE_CRASH` | simulation | no error | 400,000 traces, 31.3M states, mean length 25 | - | 7m17 |
| `integrated_core` | all | as `integrated`, every invariant but `NoDualBootstrap` | simulation | no error | 400,000 traces, 31.3M states, mean length 26 | - | 7m50 |
| `refusal_withdraw` | `tablet` | stuck check with the withdrawal, without the re-probe, the wait for an intent's expiry accepted: 1 VTOrc, 1 transaction, `DECIDE_CRASH`, two crashes, `MaxInc = 3`, no leave, split; every invariant | exhaustive | no stuck state, no error | 5,359,373 | 54 | 7m54 |
| `withdraw_orcs` | `orcs` | as `orcs`, with two crashes, so that a definitive refusal, the withdrawal and the re-probe are reachable | exhaustive | no error | 23,996,701 | 56 | 23m17 |
| `reprobe` | `tablet` | as `refusal_withdraw`, with the re-probe (`MaxProbe = 2`), the wait for an intent's expiry not accepted; every invariant | exhaustive | no stuck state, no error | 5,371,499 | 54 | 8m06 |

With the re-probe on, the counts of the configurations with at most one crash are unchanged: a target can only stop being the candidate after VTOrc chose it by losing a transaction in a restart, which needs a second crash; `withdraw_orcs` grew from 23,878,616 to 23,996,701 states. The runs of `reprobe` and `refusal_withdraw` shared the machine with builds and end-to-end tests. The ghost that records which incarnation a bootstrap `START` started from is set only when the `START` begins (see `HBoot1` under "Conformance"), which removed states in which an RPC that never bootstrapped carried it, from the first version of this table. Every configuration of the current design runs with every fix on (`refusal_withdraw` without the re-probe, as noted), and checks every invariant except `NoMinorityAck` (and `NoDualBootstrap` where noted; `integrated` now checks it too). The simulations use seed 1, at most 120 steps per behavior, and 100,000 behaviors per worker; they explore a sample, not the whole space. The exhaustive runs with two transactions did not converge within the hour (their queues still grew after 10 minutes); the `tablet` and `orcs` families are exhaustive with one transaction, and two transactions are covered by simulation only. With one transaction, `DECIDE_CRASH` and an acknowledged write exclude each other: the configurations with `DECIDE_CRASH` check that its unacknowledged transaction in a relay log neither blocks the bootstrap (`s7d_r3_adopt`) nor breaks the other invariants, and the simulations combine both.

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

## Withdrawing a refused intent

A live intent fences a bootstrap on any other voter until it expires. With every other fix on and the withdrawal off, the stuck-state check finds the state in which only the intent's expiry, which `STUCK_CHECK` leaves out, could let the shard's group be bootstrapped again (`refusal_stuck`, 14 states):

```
 1. Init              s1 is the primary of incarnation 1 {s1, s2, s3}
 2. DecideCrash(s1)   the group decides transaction 1; s1 crashes before it commits it
 3. Restart(s1)
 4. Deliver(s2)       s2 and s3 receive 1 into their relay logs, and do not apply it
 5. Deliver(s3)
 6. LoseMajority(1)
 7. LeaveDead(1)
 8. OBegin(o1)        no voter executed 1; s2 and s3 hold it: VTOrc chooses s2
 9. Crash(s2)         s2's mysqld restarts: relay_log_recovery discards 1
10. Restart(s2)
11. OIntent(o1)       the intent for s2, and the RPC
12. HBoot1(s2)        MySQL lacks 1 and runs no START: a definitive refusal
13. OReply(o1)        VTOrc keeps the intent, and adopts
14. OAdopt(o1)        nothing to adopt: only s3 can bootstrap, and the intent for s2 fences it
```

**The fix** (`WITHDRAW_ON_REFUSAL`): the tablet's refusal for a missing transaction, decided while no `START` runs, is definitive (reply -2), and VTOrc withdraws its intent with a compare-and-swap on the token and on the incarnation the intent was recorded for. With it, `refusal_withdraw` (the same bounds, every invariant) finds no stuck state and no violation. The stuck-state check then accepts one kind of end state, `WaitsForExpiry`: the intent's RPC failed without a definitive refusal (an error or a timeout), and its target is in no group and runs no `START`. TLC finds that state first without that rule: VTOrc chooses s2, s2's host crashes before the RPC arrives, the RPC times out, s2 restarts without transaction 1; VTOrc cannot tell that failure from a lost reply, and keeps the intent. That wait was the design's until the re-probe (see "Re-probing a stale intent's target").

**The conditions matter.** Each unsafe variant violates `NoDualBootstrap`, exhaustively, with one VTOrc, no transaction, no crash, `MaxInc = 3`, `MaxTok = 3`:
- `withdraw_any` (12 states), VTOrc withdraws after any failure: `LoseMajority(1)`, `LeaveDead(1)`, `OBegin(o1)` and `OIntent(o1)` choose s1, `HBoot1(s1)` starts MySQL's bootstrap, `OTimeout(o1)` withdraws the intent, `OBegin(o1)` chooses s2 (s1 runs a `START`), `BootComplete(s1)`, `OIntent(o1)`, `HBoot1(s2)`, `BootComplete(s2)`: two groups from incarnation 1.
- `withdraw_starting` (19 states), the tablet reports as definitive the refusal it sends after giving up on a `START` in progress: the bootstrap RPC to s2 times out while MySQL's bootstrap runs, and VTOrc's adoption finds no group yet; on its next pass, VTOrc chooses s2 again (the live intent's target) and the RPC waits for that `START`, gives up (`HBootGiveUp`), and reports a definitive refusal; VTOrc withdraws the intent and bootstraps s1, and s2's `START` completes.

**Safety with the withdrawal.** The exhaustive configurations of the current design do not reach a definitive refusal: with one crash, a candidate that holds a transaction in its relay log only cannot also lose it in a restart (TLC finds a refusal within 11 states in `refusal_withdraw` and `withdraw_orcs`, which allow two crashes). `withdraw_orcs` (two VTOrcs, one lease expiry, two crashes, `DECIDE_CRASH`, every invariant) and the `integrated` simulation (two crashes) check the withdrawal next to the other mechanisms: see "Current design".

**What remains.** A bootstrap RPC that fails without a definitive refusal keeps its intent, by design: VTOrc cannot tell a target that did not run the bootstrap from a lost reply. That includes an RPC that reaches the target while its mysqld restarts. The re-probe below ends that wait once the target answers again.

## Re-probing a stale intent's target

With `EXPIRY_WAIT_OK` off, the stuck-state check finds the wait that `refusal_withdraw` accepts (`reprobe_stuck`, 13 states, the bounds of `refusal_withdraw`):

```
 1. Init              s1 is the primary of incarnation 1 {s1, s2, s3}
 2. DecideCrash(s1)   the group decides transaction 1; s1 crashes before it commits it
 3. Restart(s1)
 4. Deliver(s2)       s2 and s3 receive 1 into their relay logs, and do not apply it
 5. Deliver(s3)
 6. LoseMajority(1)
 7. LeaveDead(1)
 8. OBegin(o1)        no voter executed 1; s2 and s3 hold it: VTOrc chooses s2
 9. Crash(s2)         s2's mysqld restarts: relay_log_recovery discards 1
10. Restart(s2)
11. OIntent(o1)       the intent for s2, and the RPC
12. OTimeout(o1)      the RPC times out: VTOrc keeps the intent
13. OAdopt(o1)        nothing to adopt; only s3 holds 1, and the intent for s2 fences it until it expires
```

The RPC's failure is not a definitive refusal, and VTOrc no longer chooses s2, so nothing ever refuses the intent's bootstrap: only the intent's expiry, which `STUCK_CHECK` leaves out, ends the wait. In the code that is two minutes after the intent. The same state follows an RPC that reaches the tablet while mysqld is down, and fails on its first MySQL read.

**The fix** (`REPROBE_STALE_INTENT`, in `OBegin`): when the live intent names a voter other than the candidate, and that voter is up, in no group and runs no `START`, VTOrc sends that intent's bootstrap to it again, with the intent's token and expected incarnation and the current required set, and without writing the intent. The tablet needs no change: s2 lacks transaction 1 and no `START` runs, so it refuses definitively, and VTOrc withdraws the intent; its next `OBegin` bootstraps s3. `reprobe` (the same bounds, `MaxProbe = 2`, every invariant, `EXPIRY_WAIT_OK` off) finds no stuck state and no violation: 5,371,499 distinct states, exhaustively. The code bootstraps the candidate in the same pass, under the same shard lock; the model releases the lock after the withdrawal and lets any VTOrc begin a new recovery, which includes the code's continuation, since the model's topology writes are guarded by their compare-and-swap only.

**Why the re-probe is safe**, which `reprobe`, `withdraw_orcs` and the `integrated` simulation check with it on:
- A target that bootstraps after all executed every transaction the re-probe requires, the current union of every voter's executed and received sets, so its group loses nothing; it is recorded as the intent's, with the compare-and-swap.
- A definitive refusal still proves that no bootstrap starts from the intent, although its token was sent twice. The first RPC failed before VTOrc sent the second. If its handler still waits for the action lock, the token check refuses it once the intent is withdrawn; before that, its own required set does: it was computed on an earlier pass, and the union of the voters' executed and received sets only shrinks while no group runs, so it holds what the target lacks now. With two VTOrcs, the re-probe of an intent that the other VTOrc wrote is the same RPC, and the withdrawal the same compare-and-swap.
- The re-probe needs the required set: `reprobe_noreq` (`REPROBE_NO_REQ`, two transactions, `MaxProbe = 1`, atomic steps) violates `NoLostAck` in 17 states. s1 commits transaction 1 (acknowledged), the group decides 2 while s1 crashes, s2 receives both, the group loses its majority; VTOrc chooses s2, s2 restarts without them, the RPC times out; the next pass chooses s1 and re-probes s2, which bootstraps without 1, and VTOrc records it.
- Two variants are not separate configurations, since they add nothing to what is checked: a re-probe while a `START` runs only meets the tablet's existing rule (it waits for the `START`, and refuses definitively only once MySQL left what it formed; reporting the refusal while the `START` runs is `withdraw_starting`), and a re-probe that rewrote the intent or refreshed its time would only extend the fence, which no safety property sees and the stuck-state check, without expiry, cannot measure.

**What remains.** A re-probe that fails without a definitive refusal keeps the intent, as the first RPC did; VTOrc re-probes on each pass while the target answers, at most once per pass. While the target does not answer, or runs a `START`, the intent fences the other voters until it expires.

## Second milestone

### What it adds

> **The voter rules changed after this milestone.** VTOrc's voter changes were redesigned (see "Voters" in the design): one voter per cell under the only policy, `group_replication_cross_cell`; SwapVoter, GrowVoter, RemoveVoter (a deleted tablet record) and RemoveVoterNoGroup, each decided on one fresh read under the shard lock with the settled check (P1), the voter-in-no-view check (P2) and the spare check (P3), then a compare-and-swap; and the move of a group primary that is not a voter. The code no longer has `voterChangeRefusal` (`VOTERS_KEEP_MINORITY`), `recheckVoterChange` (`VOT_REVALIDATE`), VTOrc's leave of members that are not voters (`VLeave`) or the re-seat of such a primary, and does not rely on `GRACE_SETTLES`. The configurations below model the earlier rules; the redesign is modelled in "Third milestone: the voter redesign".

- **Voter replacement** (`VOTERS`, VTOrc's `GroupVotersOutOfDate`). The shard record lists the voters (`voters`), and the tablets, VTOrc, PRS and ERS count the voter majority against the list it holds now. A host that is down for the replacement grace period has failed (`GraceExpire`; with `GRACE_SETTLES`, only once the live group of the recorded incarnation has a primary whose election ended after the host went down). VTOrc, under the shard lock, while a member of the recorded incarnation is active with quorum (`VOTERS_NEED_GROUP`), writes a list without some failed voters (`OVotRead`); with `VOT_SPLIT`, the write is a separate step (`OVotWrite`), so that the statuses can change and the lease expire in between. A member that is no longer a voter leaves the group when the group keeps a majority of its members without it (`VLeave`). The model has no spare tablet: a voter is replaced only by a smaller list.
- **PlannedReparentShard** (`PRS`): the preflight under the shard lock, `DemotePrimary` on the current primary (it stops serving and sets `super_read_only`), the wait for the primary-elect (`UndoDemotePrimary` on the old primary when it fails), `PromoteReplica` (`group_replication_set_as_primary` on a member that is not the group's primary, then the promotion's decision), the reparent journal. With `DEMOTE_FAIL`, the demotion's last step, the read of the primary status, fails after `super_read_only` was set, and the handler's deferred reverts run.
- **EmergencyReparentShard** (`ERS`): a reachable member of the legitimate group with quorum that reports its primary; that primary, or another ONLINE member of its view, is promoted with `PromoteReplica`. ERS demotes nobody.
- **The RPCs that make MySQL writable**: `UndoDemotePrimary`, sent by VTOrc's `PrimaryIsReadOnly` recovery, and `SetReadOnly(false)` (`SetReadWrite`), each a decision under the action lock in three steps, like the sync loop's.
- **The initial promotion** (`INIT_EMPTY`, `INIT_PRS`): a shard that never had a primary, with no group and nothing recorded, every MySQL read-only. PRS's `performInitialPromotion` calls `InitPrimary`, which bootstraps a group of one and makes MySQL writable before its decision to serve (the exception, `INIT_WRITABLE`), and records the new incarnation with a compare-and-swap. VTOrc bootstraps the same shard on its own (`GroupNotBootstrapped`). Until the first incarnation is recorded, the incarnation is unknown, and the code then applies the voter rule only (`policy.LegitimateGroup.inIncarnation`).
- **Environment**: a writable MySQL outside of any group takes writes on its own (`STANDALONE`; one in the ERROR state does not, since its `before_commit` hook refuses them); mysqld restarts while its vttablet keeps running, its state and its RPCs (`MYSQLD_RESTART`), and comes back OFFLINE and `super_read_only`; with `VIEW_GTIDS`, the bootstrap of a group logs a view change event with a GTID in its member's binlog and in the group's history.
- **Liveness** (`GRLiveness.tla`, below).

`NoVoterMinority` is the new invariant: no voter write lets a live view of the recorded incarnation that held fewer than a majority of the old voters hold a majority of the new ones, with members that were listed voters already. A member that is in the view but not listed joined it through distributed recovery, which gave it the group's history: listing it again is not a shrink. VTOrc's rule (`VOTERS_KEEP_MINORITY`, below) is stricter: it counts every member of the view.

`EventuallyServes` (`GRLiveness.tla`) is the liveness property: once the faults stop (their budgets are finite), the shard ends with a serving primary of its legitimate group, `<>[](Healthy \/ Bound)`, where `Bound` holds when no live group of the recorded incarnation holds the voter majority and the bounds forbid a new intent or incarnation, which the code is not limited by. Every step of the tablets, MySQL and VTOrc is weakly fair, the completion of a join strongly fair. Faults, clients, the outcomes MySQL may choose (a join that fails or ends in a group of its own, a lost RPC) and the operator's reparents are not fair.

Switches. Fixes that are in the code at f528e9a (`TRUE`): `VOTERS_NEED_GROUP`, `PRS_LEGIT` (PRS's preflight requires the primary-elect in the legitimate group, and ERS's `findGroupWithQuorum` counts only members of it), `UNDO_CHECK` and `UNDO_MATCH` (40305e2), `SETRW_CHECK`. The fixes of findings 3 to 8, implemented on branch `gr-fixes5` (`FALSE` or `{}` is the code at f528e9a): `VOTERS_KEEP_MINORITY` and `VOT_CAS` (finding 3), `DEMOTE_REVERT_DECISION` (finding 4), `INIT_GUARD` (finding 5, a set of parts: `"intent"`, `"inc"`, `"active"`), `JOIN_WAITS_RECORD` and `ADOPT_UNRECORDED` (finding 6), `VOT_REVALIDATE` (finding 7, a set of parts: `"dropped"`, `"reachable"`, `"group"`, `"primary"`), `PRIMARY_MUST_BE_VOTER`, `FENCE_ON_DROP` and `NONVOTER_LEAVES` (finding 8). Environment and modes: `VOTERS`, `VOT_SPLIT`, `PRS`, `ERS`, `DEMOTE_FAIL`, `INIT_EMPTY`, `INIT_PRS`, `INIT_WRITABLE` (`FALSE`: `InitPrimary` leaves MySQL read-only until a decision lets the tablet serve), `STANDALONE`, `MYSQLD_RESTART`, `LOOP_RUNS` (the sync loop serves again at most once per run, after the run read MySQL's status), `INIT_STABLE` (no fault while the initial promotion runs), `DIRECT_WRITES` (`FALSE`: clients write only through vtgate, to a serving PRIMARY), `GRACE_SETTLES`, `VIEW_GTIDS` (MySQL 8.4.11 logs no view change GTID in the lab, so `FALSE` is the realistic setting and `TRUE` a variant), `INIT_RECORD_FAIL` (the initial promotion's write of the incarnation may fail), `VOT_PROMPT` (timing: no host that a pending voter write drops restarts before the write). Bounds: `MaxVot` (voter writes), `MaxPrs` (reparents), `MaxUndo`, `MaxSetRW`.

### Configurations and bounds

Each configuration's header says what it checks. The second milestone adds four families; every one has one VTOrc and atomic decisions (`SPLIT = FALSE`), and new incarnations and intents are lowered to keep the reparents exhaustive:

| Family | Bounds | Checks |
|---|---|---|
| `voters` | `MaxInc = 2`, 1 intent, 1 transaction, 2 crashes, 1 leave, 1 loss of majority, 1 voter write | voter replacement; `voters_split1` splits the read from the write |
| `reparent` | no new incarnation, 1 transaction, 1 reparent, 1 crash or mysqld restart, 1 leave, no loss of majority, 1 `UndoDemotePrimary`, standalone commits; `setrw`: 1 `SetReadWrite` and no reparent; with a failing `DemotePrimary`: 2 leaves and no crash, or 1 crash or mysqld restart and no leave, no `UndoDemotePrimary` | PRS, ERS, `DemotePrimary` and its revert, `UndoDemotePrimary`, `SetReadWrite` |
| `init` | an empty shard, 1 transaction, 2 initial promotions, 1 crash, 1 leave, 1 loss of majority, standalone commits; alone: `MaxInc = 3`; next to VTOrc: `MaxInc = 2`, 1 intent | the initial promotion, alone or next to VTOrc's bootstrap of the shard |
| `live` | no transaction, `MaxInc = 2`; `live`: 1 crash, 1 loss of majority, 1 intent; `live_init*`: an empty shard, 1 crash, no loss, 2 intents, 1 initial promotion; `live_voters*`: 2 crashes, no loss, 1 voter write, split; no symmetry (TLC's liveness checking does not support it) | `EventuallyServes` |

The bounds of a family that finds a bug are those of the configuration that checks its fix, so that the fix is checked against the scenario.

### Results

Validation, each fix off, exhaustive, stopped at the first violation (the code at f528e9a unless noted):

| Configuration | Bug | Switched off | Outcome | States | Time | Distinct states explored |
|---|---|---|---|---|---|---|
| `voters_minority` | finding 3: a voter write that a minority view survives | `VOTERS_KEEP_MINORITY` | `NoVoterMinority` violated | 7 | 2s | 2,892 |
| `voters_nogroup` | voter write while no member of the group is active with quorum | `VOTERS_NEED_GROUP` | `NoLostAck` violated | 12 | 5s | 47,319 |
| `voters_nosettle` | a failed voter dropped before the live group applied its write | `GRACE_SETTLES` (timing) | `NoLostAck` violated | 13 | 5s | 38,396 |
| `voters_split_slow` | finding 7: a voter write on stale statuses | `VOT_REVALIDATE` | `NoLostAck` violated | 22 | 1m43 | 1,658,173 |
| `voters_split_drop` | finding 7, re-read of the dropped voters only | `"reachable"`, `"group"` | `NoLostAck` violated | 22 | 1m46 | 1,684,467 |
| `voters_split_noreach` | finding 7, without the grace criterion re-evaluated (finding 8 shape) | `"reachable"` | `NoLostAck` violated | 22 | 1m55 | 1,768,657 |
| `voters_split_nonvoter` | finding 8 candidate alone: a primary must be a voter | `"reachable"`; `PRIMARY_MUST_BE_VOTER` on | `NoLostAck` violated | 22 | 2m00 | 1,859,310 |
| `undo_nocheck` | `UndoDemotePrimary` without the serving invariant (40305e2) | `UNDO_CHECK`, `UNDO_MATCH` | `NoLostAck` violated | 7 | 6s | 91,455 |
| `setrw_nocheck` | `SetReadWrite` without the serving invariant | `SETRW_CHECK` | `NoLostAck` violated | 7 | 2s | 4,381 |
| `prs_demote_fail` | finding 4: the revert of a failed `DemotePrimary` serves without a decision | `DEMOTE_REVERT_DECISION` | `NoDecisionAck` violated | 7 | 1s | 2,529 |
| `prs_demote_fence` | finding 4: the revert lifts a fence | `DEMOTE_REVERT_DECISION` | `FenceNotUndone` violated | 9 | 2s | 6,600 |
| `prs_demote_restart` | finding 4: the revert makes an OFFLINE MySQL writable | `DEMOTE_REVERT_DECISION` | `NoLostAck` violated | 6 | 1s | 1,135 |
| `init_orc` | finding 5: the initial promotion next to VTOrc's bootstrap | `INIT_GUARD` | `NoDualBootstrap` violated | 12 | 2s | 2,291 |
| `init_orc_vgtid` | finding 5, with view change GTIDs | `INIT_GUARD` | `NoDualBootstrap` violated | 10 | 2s | 2,187 |
| `init_guard_noint` | finding 5, guard without the live intent | `"intent"` | `NoDualBootstrap` violated | 10 | 2s | 1,579 |
| `init_guard_intent` | finding 5, guard without the active members | `"active"` | `NoDualBootstrap` violated | 16 | 3s | 17,993 |
| `init_orc_lost` | finding 6 on the code | `JOIN_WAITS_RECORD` | `NoLostAck` violated | 16 | 9s | 133,765 |
| `init_orc_unrec` | finding 6 with the fixes of findings 3 to 5 | `JOIN_WAITS_RECORD` | `NoLostAck` violated | 18 | 4s | 35,303 |
| `init_fault` | finding 6 on the initial promotion (faults during it) | `INIT_STABLE`, `JOIN_WAITS_RECORD` | `OneWritablePrimary` violated | 21 | 9s | 108,099 |
| `init_rerun` | a failed initial promotion run again on another voter (no write at risk) | `INIT_GUARD`, `JOIN_WAITS_RECORD` | `NoDualBootstrap` violated | 11 | 2s | 2,182 |
| `init_direct` | a write acknowledged by the `InitPrimary` target before its incarnation is recorded (direct clients) | the exception (`INIT_WRITABLE`) | `NoLostAck` violated | 20 | 6s | 78,415 |
| `live_init_prs_fail` | finding 6's fix: a failed incarnation write leaves a group nobody records | `ADOPT_UNRECORDED` | `EventuallyServes` violated | 14 | 4s | 4,718 |

Current design, with the fixes of findings 3 to 5 (`VOTERS_KEEP_MINORITY`, `VOT_CAS`, `DEMOTE_REVERT_DECISION`, `INIT_GUARD`) unless noted:

| Configuration | Family | Bounds | Mode | Result | Distinct states | Depth | Time |
|---|---|---|---|---|---|---|---|
| `voters` | `voters` | the code, every invariant but `NoVoterMinority` | exhaustive | no error | 2,473,645 | 48 | 2m55 |
| `voters_fixed` | `voters` | 1 voter write, 2 crashes, 1 leave | exhaustive | no error | 2,465,042 | 48 | 2m52 |
| `voters_split1` | `voters` | as `voters_fixed`, the read and the write split | exhaustive | no error | 4,149,334 | 51 | 6m00 |
| `voters_split_prompt` | `voters` | as `voters_split1`, the code without the re-read, under `VOT_PROMPT` | exhaustive | no error | 2,527,122 | 48 | 3m07 |
| `voters_code` | `voters` | as `voters_split1`, with every rule shipped for findings 7 and 8 | exhaustive | no error | 4,149,334 | 51 | 5m00 |
| `voters_split` | `voters` | 2 VTOrcs, 2 voter writes, 1 lease expiry, split | simulation | no error | 400,000 traces, 10,327,337 states, mean length 18 | - | 4m01 |
| `prs` | `reparent` | 1 PRS, 1 `UndoDemotePrimary`, 1 crash or mysqld restart, 1 leave, no new incarnation | exhaustive | no error | 11,085,041 | 45 | 10m30 |
| `ers` | `reparent` | as `prs`, with an ERS | exhaustive | no error | 6,807,588 | 42 | 6m49 |
| `setrw` | `reparent` | 1 `SetReadWrite`, no reparent | exhaustive | no error | 157,684 | 27 | 12s |
| `prs_fixed` | `reparent` | a failing demotion, 2 leaves | exhaustive | no error | 1,241,538 | 40 | 1m36 |
| `prs_fixed_restart` | `reparent` | a failing demotion, 1 crash or mysqld restart | exhaustive | no error | 73,672 | 30 | 8s |
| `init_alone` | `init` | the initial promotion alone, `INIT_STABLE`, view change GTIDs | exhaustive | no error | 253,075 | 43 | 23s |
| `init_direct_ro` | `init` | `InitPrimary` read-only until it serves (`INIT_WRITABLE` off), direct clients, with the fixes, `NoLostAck` | exhaustive | no error | 347,809 | 44 | 22s |
| `init_orc_guard` | `init` | next to VTOrc, without `JOIN_WAITS_RECORD`, `NoDualBootstrap` | simulation | no error | 400,000 traces, 11,014,189 states, mean length 18 | - | 3m00 |
| `init_orc_fixed` | `init` | next to VTOrc, with `JOIN_WAITS_RECORD` | exhaustive | no error | 6,282,978 | 61 | 7m08 |
| `init_orc_fixed_vgtid` | `init` | as `init_orc_fixed`, view change GTIDs | exhaustive | no error | 6,720,329 | 61 | 7m23 |
| `init_fault_fixed` | `init` | faults during the initial promotion, with `JOIN_WAITS_RECORD` | exhaustive | no error | 279,905 | 44 | 19s |
| `init_orc_adopt` | `init` | as `init_orc_fixed`, a failing incarnation write, `ADOPT_UNRECORDED` | exhaustive | no error | 6,942,486 | 61 | 7m29 |
| `live` | `live` | 1 crash, 1 loss of majority, 1 re-bootstrap | exhaustive | no error | 70,749 | 38 | 35s |
| `live_init` | `live` | an empty shard that VTOrc initializes, `JOIN_WAITS_RECORD` | exhaustive | no error | 109,337 | 47 | 56s |
| `live_init_prs` | `live` | as `live_init`, with an initial promotion | exhaustive | no error | 376,104 | 50 | 3m31 |
| `live_init_prs_adopt` | `live` | as `live_init_prs`, a failing incarnation write, `ADOPT_UNRECORDED` | exhaustive | no error | 583,320 | 53 | 6m03 |
| `live_voters` | `live` | a voter fails and is replaced, the shipped voter rules, 2 crashes | exhaustive | no error | 102,714 | 40 | 46s |
| `live_voters_fence` | `live` | as `live_voters`, a re-read without `"reachable"` and `"primary"`: the list can drop the primary, which is fenced | exhaustive | no error | 128,892 | 43 | 56s |
| `fixed` | all | every second-milestone feature and fix, 2 of each fault | simulation | no error | 400,000 traces, 15,700,929 states, mean length 27 | - | 5m46 |

Until this milestone, `run.sh` passed `-deadlock`, which disables TLC's deadlock check, to every configuration expected to pass: the stuck-state checks of `s7d_r3_adopt`, `refusal_withdraw` and `reprobe` did not run. They run now, and find no stuck state, with the first milestone's state counts (801,537; 5,359,373; 5,371,499). ERS without its legitimacy check (`PRS_LEGIT` off) finds no violation in 10 minutes (7.4M states, depth 18, not exhaustive): the promotion's serving decision refuses what the check would; the check is defense in depth. Each run of the second milestone was capped at 15 minutes (the machine was shared): `current` explored 8,461,130 of its 9,545,958 states without error before the cap, and `tablet`, `orcs_stall` and `withdraw_orcs` (13, 17 and 23 minutes in the first milestone) were not run again; no action of the second milestone is enabled in them, and every other first-milestone configuration that passes reproduces its count exactly.

## Finding 3 (fixed on `gr-fixes5`, pending merge): a voter write that a minority view survives

VTOrc's `GroupVotersOutOfDate` recovery writes a new voter list when `SelectGroupReplicationVoters` drops failed voters, once a member of the recorded incarnation is active with quorum in its view (`groupUp` in `updateGroupReplicationVoters`). MySQL's quorum is that of the view, not of the voters: a view that shrank through clean leaves keeps it. `voters_minority` (`NoVoterMinority`, 7 states):

```
 1. Init          s1 is the primary of incarnation 1 {s1, s2, s3}; voters {s1, s2, s3}
 2. Commit(s1)    transaction 1 is acknowledged
 3. Crash(s2)     the view is {s1, s3}
 4. Leave(s3)     a clean leave: the view {s1} keeps its quorum, and lacks the voter majority (s1 does not serve)
 5. Crash(s3)
 6. GraceExpire   s2 and s3 have been unreachable for the replacement grace period
 7. OVotRead(o1)  s1 is active with quorum: VTOrc writes the voters {s1}
```

s1 then holds the voter majority of the list, and serves: every write is acknowledged by one host until the others rejoin. `voters` (the code, every other invariant) finds no lost write in its bounds, since s1 holds everything the group decided: the cost is durability, one copy of every acknowledged write. `TestUpdateGroupReplicationVotersKeepsSeatsOfMinorityView` reproduces it on the Go code.

**The fix** (`VOTERS_KEEP_MINORITY`, `VOT_CAS`): VTOrc never writes a list under which a view that lacks a majority of the current voters would hold a majority of the new ones (`voterChangeRefusal` in `SelectGroupReplicationVoters`); the write is a compare-and-swap on the list and the incarnation the selection read. In the code, a current voter counts in a view when it is ONLINE there, a new one when it is ONLINE or RECOVERING, and a new voter that is unreachable and that no reachable member reports active counts in every view, since VTOrc cannot see a view of unreachable members; the replacement of a failed voter by a spare in its cell stays allowed. The model has no spare tablet and VTOrc sees every view, so the rule is the ground-truth `MinorToMajor`. `voters_fixed` (every invariant) and `voters_split1` (the read and the write separate) find no violation. With one VTOrc the compare-and-swap never fails; two VTOrcs with a split write and a lease expiry (`voters_split`, simulation) find no violation either, and no configuration with three tablets violates `NoVoterMinority` without it (2 VTOrcs, 1.1M states in 200s, not exhaustive): it needs a spare tablet, which the model does not have.

Two conditions of the code are needed too: without `VOTERS_NEED_GROUP`, a failed voter that holds an acknowledged write loses its seat (`voters_nogroup`, `NoLostAck`), and without the timing assumption `GRACE_SETTLES` (the one-minute grace outlasts the election of the live group's primary) so does a failed voter whose write the new primary did not apply yet (`voters_nosettle`, `NoLostAck`).

## Finding 4 (fixed on `gr-fixes5`, pending merge): `DemotePrimary`'s revert skips the serving decision

When `demotePrimary` fails after it set `super_read_only` (its last step, the read of the primary status), its deferred reverts run: `redoPreparedTransactionsAndSetReadWrite`, whose only group check is `checkGroupAllowsReadWrite` (not a secondary, no election running), then `SetServingUnlessGroupReplicationNotServing`, which consults only the not-serving reasons. Neither checks the serving invariant or the fence. `prs_demote_fail` (`NoDecisionAck`, 7 states):

```
 1. Init          s1 is the primary of incarnation 1 {s1, s2, s3}
 2. Leave(s2)
 3. PBegin        PRS from s1 to s3
 4. Leave(s3)     s1's view is {s1}: quorum, no voter majority
 5. PDemote       s1 stops serving, super_read_only; the read of the primary status will fail
 6. PDemoteFail   the revert: MySQL writable (s1 is its group's primary, no election), the tablet serves
 7. Commit(s1)    acknowledged by one voter, on a decision that never checked the voter majority
```

The same revert lifts a fence decided during the demotion (`prs_demote_fence`, `FenceNotUndone`), and after a mysqld restart during the demotion makes an OFFLINE MySQL writable and serves it, which then takes writes outside of any group (`prs_demote_restart`, `NoLostAck`, 6 states). `TestDemotePrimaryRevertKeepsServingInvariant` reproduces it on the Go code.

**The fix** (`DEMOTE_REVERT_DECISION`, `revertDemotionWithGroupDecisionLocked`): under Group Replication the revert is a serving decision under the action lock, as `UndoDemotePrimary` and every promotion are: it waits for the end of an election, takes the fence snapshot, decides on MySQL's status read under the lock, and makes MySQL writable, and the tablet serve, only if the decision allows it and no fence was decided since the snapshot. Otherwise the tablet stays PRIMARY, not serving, MySQL read-only, and the sync loop serves again once a decision allows it. `prs_fixed` (the same bounds, every invariant) finds no violation.

## Finding 5 (fixed on `gr-fixes5`, pending merge): the initial promotion next to VTOrc's bootstrap

On a shard that never had a primary, PRS takes the initial promotion path: no tablet is PRIMARY and the shard record has no primary term. VTOrc bootstraps such a shard too (`GroupNotBootstrapped`), and until a majority of the voters joined its group no tablet is PRIMARY. A PRS that takes the shard lock in between calls `InitPrimary`, which bootstraps a second group next to VTOrc's. `init_orc` (`NoDualBootstrap`, 12 states), and also with the view change GTIDs that would make a bootstrapped member's binlog differ from the others' (`VIEW_GTIDS`; `init_orc_vgtid`, 10 states, through a bootstrap `START` that has not logged its view change yet):

```
 2. Crash(s1)       (with Restart, no part in the scenario: with several workers the trace can be
 3. Restart(s1)      longer than the shortest one)
 4. OBegin(o1)      VTOrc chooses s1 on the empty shard
 5. OIntent(o1)     the intent for s1, and the bootstrap RPC
 6. HBoot1(s1)      MySQL's bootstrap START runs on s1
 7. OTimeout(o1)    the RPC times out; VTOrc keeps the intent
 8. OAdopt(o1)      nothing to adopt yet; VTOrc releases the shard lock
 9. BootComplete(s1)  VTOrc's group {s1}, not recorded yet
10. PIBegin         PRS: no PRIMARY tablet, no primary term, every tablet reachable, s2's executed set contains
                    every tablet's: the initial promotion of s2
11. IP1(s2)         InitPrimary bootstraps s2
12. BootComplete(s2)  a second group, from the same (empty) recorded incarnation
```

`TestPlannedReparentGroupReplicationInitialPromotionAfterBootstrap` reproduces it on the Go code.

**The fix** (`INIT_GUARD`, `checkShardHasNoGroup` in `performInitialPromotion`): under a group replication policy, the initial promotion refuses while a bootstrap intent is live, an incarnation is recorded, or any tablet's MySQL is an active member of a group, read under the shard lock. Each part is needed: without the live intent, the trace above (`init_guard_noint`, 10 states); without the active members, an intent that expired while its target's group exists unrecorded, which another voter joined and left, so that it holds that group's view change and passes the containment check (`init_guard_intent`, 16 states). With all three, `init_orc_guard` finds no two groups (`NoDualBootstrap`, 400,000 simulated behaviors); the other invariants fail through finding 6, whose fix `init_orc_fixed` adds.

## Finding 6 (fix on `gr-fixes5`, pending merge): a stray group serves before the first incarnation is recorded

While the shard record lists no incarnation, any group counts as the shard's group for the tablets (`inIncarnation` returns true), and the tablets rejoin one on their own (`legitimateGroupActiveElsewhere`). With every fix of findings 3 to 5 on, `init_orc_unrec` (`NoLostAck`, 18 states; `init_orc_lost` on the code, 16 states):

```
 2-3. Crash(s1), Restart(s1)  (no part in the scenario)
 4. OBegin(o1)      VTOrc chooses s1 on the empty shard
 5. OIntent(o1)     the intent for s1, and the bootstrap RPC
 6. HBoot1(s1)
 7. BootComplete(s1)  incarnation 1 {s1}; the RPC's reply is on its way
 8. JoinStart(s2)   s2's sync loop: s1 is active in a group, and no incarnation is recorded
 9. Leave(s1)       s1 leaves (or crashes): no live group is left
10. JoinStray(s2)   s2's join ends alone in a new group, incarnation 2
11. ElectEnd(s2)
12. JoinStart(s3)   s3 joins s2's group: no incarnation is recorded
13. JoinComplete(s3)  {s2, s3}: the voter majority
14-16. PrSnap, PrRead, PrAct(s2)  s2 is promoted and serves: the voter rule holds, the incarnation is unknown
17. Commit(s2)      transaction 1 is acknowledged in incarnation 2
18. OReply(o1)      VTOrc records incarnation 1 (compare-and-swap on the empty incarnation and its token):
                    the acknowledged write is not in the recorded group's history
```

The tablets of incarnation 2 then find it foreign and leave it, and the write is lost. It needs VTOrc to record the bootstrap after the stray group formed, which MySQL took 9–20s to do in the lab: a VTOrc stall or a slow topology write between the RPC's reply and the record, which no timeout bounds. The initial promotion has the same window: `init_fault` (`OneWritablePrimary`) finds it when the `InitPrimary` target leaves its group before PRS records it, which `init_alone` excludes with the assumption `INIT_STABLE`; and with the fix of finding 5, a second initial promotion that runs while a voter's join is still starting (not yet an active member) makes its target writable next to the stray group that join forms (`OneWritablePrimary`, 21 states). With clients that write to MySQL directly and `InitPrimary` read-only until it serves, the initial promotion's own group serves under the unknown incarnation, loses its majority, and a stray group of a join serves without the write (`NoLostAck`, 23 states, `init_direct_ro` without the fix). The earlier `init_orc_lost` configuration had recorded this trace as an expected violation without noticing that it contains no reparent; it is a finding.

**The fix** (`JOIN_WAITS_RECORD`): while the shard record lists no incarnation, no join starts: not the tablet's own (at startup, in the sync loop), not VTOrc's `GroupMemberNotOnline` recovery, not a join of a new voter by VTOrc's voter update. The joins that follow a recorded bootstrap (VTOrc's, the migration's) are unchanged, and so are `InitPrimary` and the serving decisions. In the model every join is `JoinStart`, and voter replacement has no spare to join, so the rule is one condition. With it and the fixes of findings 3 to 5, `init_orc_fixed`, `init_orc_fixed_vgtid` and `init_fault_fixed` (as `init_fault`, without `INIT_STABLE`) find no violation, and the shard still initializes: `live_init` (VTOrc's bootstrap) and `live_init_prs` (with an initial promotion next to it) satisfy `EventuallyServes`. Two narrower versions are not enough: one that waits only while a bootstrap intent is live misses the initial promotion, which writes no intent, and one that also waits while the shard never had a primary term misses a target that `InitPrimary` made PRIMARY before PRS recorded the incarnation.

**What remains: a group that nobody records.** VTOrc's group needs no join to be recorded: its reply, or its adoption, which needs only the intent in the shard record, expired or not. The initial promotion's group does: when PRS's write of the incarnation fails after `InitPrimary` bootstrapped its target (a topology error, the reparent's deadline; `INIT_RECORD_FAIL`), the group has no intent to adopt, the voters may not join it, the guard of finding 5 refuses another initial promotion while its member is active, and VTOrc bootstraps only a shard with no active voter. `live_init_prs_fail` violates `EventuallyServes` (13 states, then stuttering): `PIBegin`, `IP1(s2)`, `BootComplete(s2)`, `ElectEnd(s2)`, `IP2(s2)` (MySQL writable, the exception), `PrRead`, `PrAct(s2)` (PRIMARY, not serving: one voter), `PIRecord` fails, and nothing is enabled that leads to a serving primary. The code before the fix let the voters join that group, under the unknown incarnation. **The fix** (`ADOPT_UNRECORDED`, `adoptUnrecordedGroup` on `gr-fixes5`): VTOrc records the incarnation of a group that nobody recorded, under the shard lock, when it is the only group the shard can have: no incarnation recorded and no live intent, its primary ONLINE with quorum, every voter answering, no tablet running a `START` or active in another group, and the primary holding every transaction a voter executed or received; the write is a compare-and-swap on the empty incarnation and the absence of a live intent. With it, `live_init_prs_adopt` satisfies `EventuallyServes` (583,320 states), and `init_orc_adopt` (as `init_orc_fixed`, with the failing write) finds no violation (6,942,486 states).

## Finding 7 (fix on `gr-fixes5`, pending merge): a voter write on stale statuses

With the read of the statuses and the write of the list as separate steps (`VOT_SPLIT`), and the fixes of finding 3 on, `voters_split_slow` (`NoLostAck`, 22 states):

```
 2. Crash(s2)
 3. GraceExpire       s2 has failed
 4. OVotRead(o1)      VTOrc selects the voters {s1, s3}, under the shard lock
 5. Restart(s2)
 6. JoinStart(s2)
 7. JoinComplete(s2)  s2 is back in the group
 8. Leave(s1)
 9. Elect(1)
10. ElectEnd(s2)      s2 is the primary
11-13. PrSnap, PrRead, PrAct(s2)  s2 serves
14. Commit(s2)        transaction 1 is acknowledged: decided with s3, received by s2 only
15. Crash(s2)         {s3} loses its majority
16. LeaveDead(1)
17. OVotWrite(o1)     the list {s1, s3}: the compare-and-swap holds, the list and the incarnation did not change
18-22. OBegin, OIntent, HBoot1, BootComplete, OReply(o1)  every listed voter is reachable: VTOrc bootstraps
                      s1, which lacks transaction 1, and records it
```

The compare-and-swap of finding 3 compares the list and the incarnation, which did not change; the statuses the selection was made on did. It needs VTOrc to stall between its read and its write, under the shard lock, for longer than s2's restart, rejoin, election, commit and second failure.

**The fix** (`VOT_REVALIDATE`, `recheckVoterChange`): right before the compare-and-swap, VTOrc reads again, and refuses the write unless a member of the recorded incarnation is still active with quorum in its view (`"group"`, the condition of `VOTERS_NEED_GROUP`, without the voter majority, which would refuse the replacements finding 3 allows); no voter dropped as unreachable answers now or was reached since the selection (`"reachable"`); and no voter dropped for another reason runs a `START`, is active in a foreign incarnation, does not answer, or holds transactions beyond the kept voters' union (`"dropped"`; an active voter of the legitimate group may be dropped, since `SelectVoters` drops one when a non-voter primary takes its cell's seat, and it leaves in the same recovery). The re-read and the write are one step in the model: the residual assumption is no stall between them. `voters_split1` (every invariant) finds no violation. Each part is needed: with `"dropped"` alone, the trace above (`voters_split_drop`, 22 states: s2 is down again at the write); with `"dropped"` and `"group"`, s2 is back and promoted as a voter before the write, and the write drops the group's primary (`voters_split_noreach`, 22 states). Under the timing assumption that no host the list drops restarts between the read and the write (`VOT_PROMPT`), the code without the re-read passes too (`voters_split_prompt`).

## Finding 8 (fix on `gr-fixes5`, pending merge): a primary that is not a voter

A join checks voter status when it starts; MySQL finishes a `START` whose client gave up, and only `GroupVotersOutOfDate` makes a member that is no longer a voter leave, when the group keeps a majority without it. Meanwhile Group Replication may elect it, and it counts in the view's majority. The serving invariant requires a majority of the voters ONLINE in the primary's view, not that the primary is a voter: a non-voter primary acknowledges a write that no voter holds in its binlog, and the bootstrap, which requires only the listed voters, loses it. In the model the only way a voter loses its seat while it is active is the stale write of finding 7: `voters_split_noreach` is the trace (s2, back and promoted while still a voter, is dropped by the write and acknowledges a write as a non-voter primary); the code reaches the state through paths the three-tablet model has no spare tablet for. A tablet that serves as PRIMARY only if its own MySQL is a listed voter (`PRIMARY_MUST_BE_VOTER`) is not enough on its own: in that trace s2 keeps serving, its MySQL writable, after the write until its next decision (`voters_split_nonvoter`, `NoLostAck`, 22 states). In the model, the `"reachable"` part of finding 7's re-read is what closes it.

**The fix as shipped** keeps more as defense in depth: the re-read also refuses to drop the current primary of the live group (`"primary"`); a PRIMARY tablet whose server is no longer a listed voter is fenced at the next fence check (`FENCE_ON_DROP`); a tablet serves as PRIMARY only on a voter's MySQL (`PRIMARY_MUST_BE_VOTER`); and an active non-voter leaves its group on the tablet's own (`NONVOTER_LEAVES`). `voters_code`, with all of them, finds no violation (4,149,334 states, the same as `voters_split1`: the re-read refuses before the other rules act), and with them a failed voter is still replaced and the shard ends with a serving primary (`live_voters`, `EventuallyServes`, 102,714 states; `live_voters_fence`, with a re-read weak enough that the list drops the primary, so that the fence on a dropped primary fires, also ends with a serving primary, 128,892 states).

## Third milestone: the voter redesign

The redesign (one voter per cell, `group_replication_cross_cell` only) replaces the voter selection of findings 1, 3, 7 and 8's VTOrc parts. VTOrc's automatic actions are each one fresh read under the shard lock, then a compare-and-swap on the voter list and the incarnation:
- **SwapVoter(v, x)**: v failed (unreachable for the grace period, or its tablet record deleted), x a spare in v's cell. Preconditions: P1, a settled legitimate source p ≠ v (the ONLINE primary with quorum of the recorded incarnation, its election ended, its view holding a majority of the voters); P2, v answers no more and no member reports it; P3, x answers, is a REPLICA in no group, runs no `START`, and executed nothing p lacks.
- **GrowVoter(x)**: a cell with an eligible tablet and no voter; P1, P3, and p's view holds a majority of the grown list.
- **GroupPrimaryNotVoter**: the group's primary is not a voter, or is a voter whose tablet record is deleted (it cannot be PRIMARY: `ChangeType` fails without a record); VTOrc moves the primary to an ONLINE voter of its view. The list does not change.
- **RemoveVoter(v)**, the only shrink: v's tablet record is deleted (the operator's signal), v is active in no view and not in p's view, P1 with p ≠ v, and no spare passes P3.
- **RemoveVoterNoGroup(v)**: no group runs, no `START`, no live intent, v's record is deleted and v does not answer (if it answered, the bootstrap could include it), and every other voter whose record is not deleted answers, inactive, with no `START`: VTOrc removes it, and the bootstrap then needs only the remaining voters. What only v held is lost; the operator accepted it by deleting the record. `DeleteTablets` refuses the shard's PRIMARY unless `AllowPrimary` is set; a tablet whose record is deleted cannot be promoted or made writable, and its restart re-creates the record.

There is no operator command and no automatic shrink otherwise. The tablet keeps fix 8 (a primary must be a voter, the fence on a dropped primary, non-voters leave).

### What the model adds

- A fourth tablet as a spare (`Spares`; `InitVoters = Servers \ Spares`). Every server is a cell of its own, and a spare may replace any voter, which over-approximates "in v's cell". `Seats` is the number of cells with an eligible tablet.
- `VOT_MODE`: `"select"` is the selection of the code at 8c9013e (every earlier configuration, unchanged); `"swap"` the redesign: `OSwap`, `OGrow`, `OMoveToVoter`, `ORemove`, `ORemoveNoGroup`. With `VOT_SPLIT` the compare-and-swap is a separate step (`OVotWrite`, on the list and the incarnation), and `VOT_PROMPT` is the timing assumption that no host the pending list drops restarts in between.
- Switches of the redesign's checks: `P1_SETTLED` (P1's election-ended check, which replaces the timing assumption `GRACE_SETTLES`), `SPARE_CHECK` (P3), `REMOVE_CHECKS` (`"p1"`, `"noview"`), `NOGROUP_DEL` (RemoveVoterNoGroup only for a deleted record).
- The environment: `ODelete` (an operator deletes a voter's tablet record, `MaxDel`; not the PRIMARY's unless `DEL_NOT_PRIMARY` is off, for `AllowPrimary`), `Die` (a host dies for good, `MaxDie`), `EnvJoin` (a tablet that is not a voter starts a join, `ENV_SPARE_JOIN`). Directed scenarios start with voters already down past their grace period (`InitDown`) or with deleted records (`InitDeleted`); `DIE_FIRST`, `DEL_DEAD` and `SEQ_FAULTS` order the faults of the liveness scenarios.
- Invariants: `VoterCount` (in swap mode the list shrinks only for a deleted record, and grows only up to `Seats`), `NoNonVoterServes` (a serving PRIMARY whose MySQL takes writes is a listed voter), `NoLostAckExceptDeleted` (`NoLostAck`, except the transactions whose only holders are deleted voters, or were when VTOrc removed one: the accepted loss), `NoLostAckAfterDelete` (`NoLostAck` for the transactions acknowledged after the first deletion), `NoLostAckAfterMove` (the same, except what a primary acknowledged after its own record was deleted, before VTOrc moved the primary role: the window of `AllowPrimary`). Ghost variables record the accepted transactions (`lostOK`, `delAcked`, `delTx`). `NoVoterMinority` counts every member of a view in swap mode (fix 1's strict rule). Liveness: `EventuallyServes`, `EventuallyServesOrOp` (or a dead voter that VTOrc cannot replace needs an operator), `EventuallyShrinks` (a dead voter whose record was deleted leaves the list).

### Results

Every run was capped at 15 minutes. Families: four tablets, one VTOrc, atomic decisions, one transaction; swap: 1 crash, 1 leave, 1 loss of majority, one re-bootstrap (`MaxInc = 2`, one intent), one voter write; the directed configurations start in the state the scenario needs.

| Configuration | Checks | Mode | Outcome | Distinct states | Depth | Time |
|---|---|---|---|---|---|---|
| `swap_fixed` | SwapVoter: P1, P2, P3; 4 tablets, 1 crash, 1 leave, 1 loss, 1 re-bootstrap, 1 voter write | exhaustive | no error | 2,318,089 | 46 | 8m53 |
| `swap_code` | as `swap_fixed`, with 8a, 8b, 8c (the code to implement) | exhaustive | no error | 2,318,089 | 46 | 7m12 |
| `grow_fixed` | GrowVoter: 2 voters and a tablet in a cell without one (Seats 3) | exhaustive | no error | 1,886,788 | 49 | 7m58 |
| `swap_split_prompt` | directed (s3 down past its grace at first), read and compare-and-swap split, `VOT_PROMPT` | exhaustive | no error | 885,208 | 45 | 2m42 |
| `delete_remove` | directed (s3 down, record deleted, no spare): RemoveVoter, then faults and a re-bootstrap | exhaustive | no error | 2,281,713 | 49 | 5m14 |
| `delete_swap` | directed, with a spare: SwapVoter of a deleted voter without its grace period | exhaustive | no error | 684,593 | 43 | 1m58 |
| `delete_remove_noview` | directed: s3 deleted while active; RemoveVoter without the "noview" conditions | exhaustive | no error | 3,269,297 | 46 | 7m35 |
| `delete_nogroup` | RemoveVoterNoGroup, one deletion (not of the PRIMARY) in any state; `NoLostAckExceptDeleted`, `NoLostAckAfterDelete` | exhaustive | no error | 4,776,821 | 45 | 11m56 |
| `delete_primary` | as `delete_nogroup`, AllowPrimary: GroupPrimaryNotVoter for a deleted primary; `NoLostAckAfterMove` | exhaustive | no error | 2,584,093 | 49 | 6m07 |
| `swap_spare_active` | without P3, a non-voter may join on its own; simulation | simulation | no error | 160,000 traces, 9,002,070 states | - | 11m22 |
| `swap_nosettle` | without P1's settled check | exhaustive | `NoLostAck` violated, 14 states | 45,527 | 14 | 9s |
| `swap_split_slow` | directed, split, without `VOT_PROMPT` | exhaustive | `NoLostAck` violated, 20 states | 1,097,493 | 22 | 2m18 |
| `delete_remove_nop1` | RemoveVoter without P1 | exhaustive | `NoVoterMinority` violated, 8 states | 9,014 | 8 | 4s |
| `delete_nogroup_lost` | the accepted loss: a write only the deleted voter held | exhaustive | `NoLostAck` violated, 13 states | 64,835 | 13 | 11s |
| `delete_nogroup_nodel` | RemoveVoterNoGroup without the deleted record | exhaustive | `NoLostAckExceptDeleted` violated, 11 states | 13,359 | 12 | 5s |
| `delete_primary_window` | AllowPrimary: the window before the move | exhaustive | `NoLostAckAfterDelete` violated, 12 states | 182,306 | 14 | 24s |
| `live_swap` | a voter dies, the spare replaces it, then a crash | exhaustive | no error | 56,892 | 38 | 1m06 |
| `live_delete` | a voter dies, another crashes, the operator deletes the dead one's record (any order); `EventuallyShrinks` too | exhaustive | no error | 20,436 | 32 | 19s |
| `live_nonvoter_primary` | a non-voter is elected; GroupPrimaryNotVoter moves the primary | exhaustive | no error | 5,211 | 19 | 9s |
| `live_delete_active` | a live voter's record is deleted, the primary crashes; the primary role moves off the deleted voter | exhaustive | no error | 3,612 | 17 | 6s |
| `live_delete_active_nomove` | as `live_delete_active`, without that move | exhaustive | `EventuallyServes` violated, 8 (then stuttering) states | 1,906 | 10 | 4s |
| `live_nospare` | a voter dies, another crashes, no spare, no deletion | exhaustive | `EventuallyServes` violated, 6 (then stuttering) states | 1,351 | 8 | 4s |
| `live_nospare_op` | as `live_nospare`, `EventuallyServesOrOp` | exhaustive | no error | 6,852 | 26 | 8s |

**What the model found.**
- **P1's settled check is needed** (`swap_nosettle`, `NoLostAck`, 14 states): s1 commits, leaves and crashes, s2 is elected and still applies the backlog, VTOrc swaps s1 for the spare s4 on s2 as the source, the group loses its majority before s2 applied the write, and the bootstrap from {s2, s3, s4} lacks it. With the election-ended check (`primary_election_in_progress = false`), the source applied everything the group decided.
- **The read-to-write window remains** (`swap_split_slow`, `NoLostAck`, 20 states, directed: s3 is down past its grace period): VTOrc reads and decides to swap s3 for s4; before the compare-and-swap, s3 restarts, rejoins as a voter, is elected, acknowledges a write that only it received, and crashes; the write then lands (same list, same incarnation), and the bootstrap from {s1, s2, s4} lacks the write. The redesign keeps the read and the write about two seconds apart, under the shard lock: `VOT_PROMPT` is the residual assumption, and `swap_split_prompt` passes under it.
- **RemoveVoter needs P1** (`delete_remove_nop1`, `NoVoterMinority`, 8 states): s1 commits and crashes, s2 leaves, the operator deletes s1's and s2's records, and VTOrc removes both, which leaves the view {s3}, a minority of the old voters, with the majority of the list {s3}.
- **A lost majority with a dead voter** (the first `live_delete`, `EventuallyServes`, 8 states, now REVISION 2): a voter dies, another crashes, the group loses its majority, and the bootstrap needs every listed voter, the dead one included. RemoveVoter needs a live group, so the deletion of the dead voter's record did not help. RemoveVoterNoGroup is the answer: with it, `live_delete` (the death, the crash and the deletion in any order) ends with a serving primary and the dead voter out of the list.
- **The accepted loss** (`delete_nogroup_lost`, `NoLostAck`, 13 states): s1 commits a write that only it receives, leaves and crashes, the group loses its majority, the operator deletes s1's record, VTOrc removes s1 and bootstraps from {s2, s3}: the write that only the deleted voter held is lost, and the deletion is the operator's acceptance. Without the deleted-record condition (`delete_nogroup_nodel`), the same removal loses a write that no deleted voter held (`NoLostAckExceptDeleted`, 11 states). The configurations with deleted records therefore check `NoLostAckExceptDeleted` for `NoLostAck`.
- **What a deleted voter acknowledges after its deletion** (REVISION 2b). The first version of RemoveVoterNoGroup lost writes acknowledged after the deletion: a deleted voter that restarted, was elected and served, or a serving PRIMARY whose record was deleted and kept serving (`delete_nogroup`, `NoLostAckAfterDelete`, 12 states: s2 leaves, s1's record is deleted, s1 acknowledges a write that only it received, crashes, the group dies, VTOrc removes s1 and bootstraps from {s2, s3}). Four rules close it, all in the model now: a tablet whose record is deleted cannot be promoted or made writable, and its restart re-creates the record; RemoveVoterNoGroup needs v not to answer; `DeleteTablets` refuses the PRIMARY without `AllowPrimary`; and VTOrc moves the primary role off a deleted voter. `delete_nogroup` then passes `NoLostAckAfterDelete`. With `AllowPrimary` (`delete_primary`), what the deleted primary acknowledges before the move is the accepted window (`delete_primary_window`, `NoLostAckAfterDelete`), and nothing acknowledged otherwise after the deletion is lost (`NoLostAckAfterMove`).
- **A deleted voter elected primary** (`live_delete_active_nomove`, `EventuallyServes` violated): an operator deletes the record of a live REPLICA voter, which stays an active member; the primary crashes and the group elects the deleted voter; its tablet cannot become PRIMARY, and nothing serves. Moving the primary role off it (`live_delete_active`) restores liveness.
- **Without a spare and without a deletion** (`live_nospare`, `EventuallyServes` violated, 6 states then stuttering): a voter dies and another crashes; the shard waits for an operator (`live_nospare_op` holds with `EventuallyServesOrOp`).

**What the model cannot distinguish.** (The fourth milestone revisits this with per-member views: P3 is then needed for safety, RemoveVoter's "not in p's view" still is not; see "Fourth milestone".) It has one view per incarnation (no partial partitions). P1 requires the source's view to hold a majority of the voters, so no other view of the recorded incarnation exists that a new or removed voter could tip over. Two of the redesign's conditions are therefore implied by P1 for safety in the model, and matter for availability and for partial partitions, which the model leaves out:
- P3's "not an active member, no `START`" (`swap_spare_active`, without P3 and with a non-voter that joins on its own: no violation, 4.4M states in 13 minutes and in simulation). An active spare is in p's own view, and Group Replication refuses a joiner with extra transactions.
- RemoveVoter's "active in no view, not in p's view" (`delete_remove_noview`, directed, exhaustive: no violation). Removing an active member v ≠ p only lowers p's view's count of the new voters, which can stop the group from serving until v leaves; every acknowledged write is executed by p, which stays a voter, and the bootstrap needs every listed voter.

`swap_code` and `swap_fixed` explore the same states: no tablet that is not a voter is ever active under the redesign's rules, so fix 8's rules never act there. `live_nonvoter_primary` exercises them: a non-voter that joins on its own and is elected is moved off by GroupPrimaryNotVoter, and the shard serves.

## Fourth milestone: partial partitions

The third milestone's model had one view per incarnation: every member of a group agreed on its membership, so P1 (the source's view holds a majority of the voters) left no other view of the recorded incarnation that a new or removed voter could tip over. This milestone gives each MySQL its own view of the same incarnation, and has VTOrc read those views as the code does.

### What the model adds

- **Partitions inside a group** (`MaxPart`): `Isolate(i, S)` cuts a side S, a minority or half of the membership, off the view of incarnation i. The other side lists S's members UNREACHABLE (`unr`): they still count in the membership, so the quorum is a majority of the ONLINE and UNREACHABLE members (`QuorumOK`); it expels them only with that quorum (`Expel`; an expelled primary is replaced by an election). A side without quorum blocks, and `unreachable_majority_timeout` then makes its members leave (`Block`, then `LeaveDead`).
- **Stale views**: an isolated member keeps its old view. Until it detects the partition (`Detect`) it reports the old view ONLINE, with quorum and with its old primary: the hung or isolated primary of S2 and S7c, which believes it is in a majority view. After `Detect` it reports only its own side ONLINE, without quorum. `Heal` puts a member that was not expelled back into the view (it receives what it missed); a member that was expelled, or that gave up, leaves its group (ERROR). An isolated member that crashes, leaves or rejoins drops its old view (`PClean`).
- **XCom's constraint**: commits, deliveries, elections, joins and expulsions need the real quorum of the current membership. `CanAccept` (a commit MySQL can acknowledge) and the ground truth of the properties (`LegitMaj`, `Healthy`, `MinorToMajor`) use the real view (`RealPrimQ`); certification blocks the isolated side. `group_replication_set_as_primary` (GroupPrimaryNotVoter) needs the group's consensus, so it moves the primary only on a real primary.
- **What VTOrc and the tablets read**: each MySQL's own `replication_group_members` (`MyMem`, `MyOnl`, `MyPrim`, `MyQuorum`), with `fillGroupReplicationMemberStatus`'s `HasQuorum` (more than half of its members ONLINE) and `IsGroupPrimary`. `IsPrimQ` is that read, and the sync loop, the fence check and VTOrc's recoveries use it, so an isolated primary that has not detected its partition looks like the primary of a majority view to its tablet and to VTOrc. The redesign's checks read, as `group_replication_voters.go` does:
  - P1 (`settledPrimary`): from the primary's own view: ONLINE primary with quorum, the view's incarnation, the election ended, a majority of the voters ONLINE in its view.
  - P2 (`activeAnywhere`, `READ_VIEWS`): from the view of every reachable member of the recorded incarnation (a tablet with a record whose MySQL is active in its own view, an isolated one included): none reports v ONLINE. A member listed UNREACHABLE is not active. `READ_VIEWS = FALSE` keeps the third milestone's ground truth (v in no view, and not isolated), so the earlier configurations are unchanged.
  - RemoveVoter's "not in p's view" (`inPrimaryView`): v is in p's view in no state, UNREACHABLE included (`MyMem`). `REMOVE_CHECKS = {"p1", "p2"}` keeps P2 and drops it.
  - P3 (`spare`): from the spare's own status: no group (an isolated member still reports itself ONLINE), no `START`, not in the ERROR state, its executed set within p's.
- **Witnesses** (`wit_*` configurations, expected violated: each shows that the scenario is reachable): a stale P1 source (`WitStaleP1`), a voter P2 reads as gone while the majority side lists it UNREACHABLE (`WitP2Unreach`), a removal that only "not in p's view" refuses (`WitPview`), a swap and a removal whose every check holds on an isolated source (`WitSwapStale`, `WitRemoveStale`), a grow on an isolated source (`WitGrowStale`), and a spare that is an active member, isolated or not, when P1 and P2 hold (`WitActiveSpare`, `WitStaleSpare`).

Abstractions: the partition is between the MySQL members; VTOrc reaches every tablet that is up (a deleted record is not read, and stands for a tablet VTOrc cannot reach whose MySQL may run). `Block` is final: a side that timed out leaves even if the partition heals later. RECOVERING members are not modeled (a join is in the view once it completes). PRS and ERS (not in these configurations) still read the ground truth.

### Configurations

The `partition` family: one VTOrc, atomic decisions, one transaction, `READ_VIEWS`, the redesign's rules with the tablet rules of finding 8 (as `swap_code`), no leave and no loss of majority other than the partitions' own (a side without quorum times out), one re-bootstrap (`MaxInc = 2`, one intent), one voter write.
- `swap_partial`: four tablets, directed: s3 is down past its grace period at first, the spare s4 may join on its own (`ENV_SPARE_JOIN`), one partition, no crash. `swap_partial_nop3`: without P3. `swap_partial_nop3_lost`: the same, `NoLostAck` only. `swap_partial_crash`, `swap_partial_nop3_crash`: with one crash.
- `remove_partial`: three voters, s3's record deleted while its MySQL is an active member, no spare, two partitions, one crash. `remove_partial_noview`: without "not in p's view" (`REMOVE_CHECKS = {"p1", "p2"}`).
- `grow_partial`: two voters and a third tablet in a cell without one, one partition, one crash.
- `wit_swap_partial`, `wit_remove_partial`, `wit_grow_partial`, `wit_flag1_fixed`: the witnesses, not in `run.sh`: TLC stops at the first violated invariant, so each witness is checked alone (keep one `INVARIANT` line).

### Results

| Configuration | Checks | Mode | Outcome | Distinct states | Depth | Time |
|---|---|---|---|---|---|---|
| `swap_partial` | SwapVoter, P1, P2, P3 on per-member views; directed, 1 partition | exhaustive | no error | 2,369,438 | 44 | 6m28 |
| `swap_partial_nop3` | as `swap_partial`, without P3 | exhaustive | `NoVoterMinority` violated, 6 states | 714 | 8 | 2s |
| `swap_partial_nop3_lost` | as `swap_partial_nop3`, `NoLostAck` only | exhaustive | no error | 2,407,196 | 44 | 5m55 |
| `swap_partial_crash` | as `swap_partial`, 1 crash | capped (13 min) | no error in the explored part | 4,862,867 | - | 13m01 |
| `swap_partial_nop3_crash` | as `swap_partial_nop3`, 1 crash | exhaustive | `NoVoterMinority` violated, 5 states | 1,128 | 8 | 2s |
| `remove_partial` | RemoveVoter with every check; s3 deleted while active, 2 partitions, 1 crash | exhaustive | no error | 6,209,802 | 49 | 12m41 |
| `remove_partial_noview` | as `remove_partial`, without "not in p's view" | exhaustive | no error | 6,257,718 | 49 | 11m46 |
| `grow_partial` | GrowVoter, 1 partition, 1 crash | exhaustive | no error | 677,207 | 44 | 1m27 |

Every witness is reachable (shortest traces): a stale P1 source (2 to 3 states), a voter P2 reads as gone while the majority side lists it UNREACHABLE (5), a removal only "not in p's view" refuses (2), a swap and a removal whose every check holds on an isolated source (2, 3), a grow on an isolated source (2), an active spare (3), an isolated spare (4). So the passing configurations do exercise the stale source, the UNREACHABLE voter and the isolated spare.

**What the model found.**
- **P3's "not an active member" is needed for safety once views diverge** (`swap_partial_nop3`, `NoVoterMinority`, 6 states). s3 is down past its grace period; the spare s4 joins the group on its own, so the view is {s1, s2, s4} with s1 the primary; s1 is cut off and has not detected it yet: it still reports the view {s1, s2, s4} ONLINE, itself the primary, with quorum. The real view is {s2, s4}, which keeps XCom's quorum (2 of 3 members) but holds one of the three voters. VTOrc reads s1 as a settled legitimate source (P1: its own view holds s1 and s2 ONLINE), s3 as gone (P2: no reachable member reports it), and, without P3, takes the active member s4 as the spare: the list {s1, s2, s4} makes the real view {s2, s4}, which held a minority of the old voters, hold a majority of the new ones. In the one-view model P1's view was the real view, which then held a majority of the old voters, so a swap could not do this (`swap_spare_active`). The code has P3 (`spare` rejects an active member, a `START`, a member in a group), and `swap_partial` passes with it. No acknowledged write is lost: certification needs the real quorum, and the view that gains the majority is the one that has it (`swap_partial_nop3_lost`, exhaustive), so the violation is of the voter-majority invariant (a view becomes legitimate through a voter write), the property of finding 3, not of `NoLostAck`.
- **RemoveVoter's "not in p's view" is not needed for safety in the model** (`remove_partial_noview`, exhaustive, no error, with the removal it alone refuses reachable in 2 states). Argument: with P1 and P2, the only removals it refuses are of a voter v that p's view lists UNREACHABLE, or that an isolated p still lists. (1) p on the real majority side: v is on a side without quorum and cannot commit; what it acknowledged before the cut was certified by a majority of the membership, so p's side holds it and p, settled, executed it; p's view holds a majority of the voters ONLINE without v, so a majority of the list without v too, and no view gains a majority through the removal. (2) p isolated: P2 reads the view of every reachable member; a real view with quorum that holds v holds at least one other member, which reports v ONLINE unless VTOrc cannot read it. In the three-tablet model only v's record is deleted, so that other member is read and P2 refuses. The condition keeps its role for availability (a removed UNREACHABLE member may come back), and for the case the model leaves out: a majority side whose other members' vttablets VTOrc cannot reach while their MySQL runs (the model ties vttablet and mysqld, except for a deleted record).
- `grow_partial` passes with a grow on an isolated source reachable: GrowVoter adds a tablet in no group, so the new voter is in no view, and the source's view must hold a majority of the grown list.

### FLAG 1 of the chaos soak: a stale primary takes the primary term

The soak found a primary that is paused, or cut off and healed before it learns of its expulsion, and keeps its stale full view: on resume its tablet's sync loop sees MySQL ONLINE and PRIMARY with a majority of the voters in that view, promotes the tablet and writes the shard record with a newer primary term; the legitimate primary's tablet sees another tablet in the record, ends its primary term (`endPrimaryTerm` on an active member changes only the tablet type, MySQL stays writable) and becomes REPLICA until it decides again. The model now has that path: the stale views above, the sync loop's promotion on MySQL's own status (`PrSnap`, `PromoteLegit` read through `IsPrimQ`), and `EndTerm` (`END_TERM`). The ghost `stRec` records a promotion that names a member outside every view with quorum while another member is the legitimate primary; `NoStalePrimaryRecorded` is `~stRec`.

- **Reproduced** (`flag1_stale`, `NoStalePrimaryRecorded`, 12 states): s1, the primary, commits and is cut off before it detects it; the majority {s2, s3} expels it and elects s2, whose tablet promotes (the record names s2); s1's tablet ends its term; its sync loop reads MySQL: ONLINE PRIMARY with quorum, all three voters ONLINE in the stale view, the recorded incarnation: it promotes and takes the term while s2 is the legitimate primary. s2's tablet then ends its term with MySQL writable, and promotes again once its sync loop runs; the two can alternate until s1 detects its partition.
- **No safety impact**: `flag1_safety` (every invariant, 8,393 states, exhaustive) and `flag1_safety_faults` (one crash, one leave, one re-bootstrap) find no lost write and no second commit-able primary: certification needs XCom's real quorum (`CanAccept`), so the stale side acknowledges nothing, and the writable MySQL of the demoted legitimate primary is the only one that can commit. The cost is routing: vtgate follows the record to a primary that cannot commit, until s1 detects its partition.
- **The proposed fix** (`PROMOTE_CHECK`): before the sync loop promotes its tablet while the shard record names another tablet, it reads that tablet's status and refuses if it is the ONLINE primary of a quorum view of the same incarnation (a tablet that does not answer does not refuse). It closes the soak's case (`flag1_fixed`), but not all of (a) (`flag1_fixed_faults`, `NoStalePrimaryRecorded`, 12 states): s1, the recorded primary, leaves and its tablet demotes; the group elects s2, which is cut off before its tablet takes the term; the majority {s1, s3} expels s2 and elects s3; s2's sync loop reads its stale view (ONLINE primary, quorum, three voters ONLINE) and promotes, since the record names s1, now a replica that refuses nothing. The record then names s2 while s3 is the legitimate primary, and s3's own promotion is refused while s2 still reads as the primary.
- **A wider check** (`PROMOTE_CHECK_ALL`): refuse while any other tablet of the shard reads as an active member of the same incarnation, with quorum in its view and another ONLINE primary. `flag1_fixed_all` and `flag1_fixed_all_faults` (exhaustive) pass with `NoStalePrimaryRecorded`, and `live_flag1_fixed_all` keeps `EventuallyServes` with a crash of the recorded primary.
- **The cost of either check**: the genuine new primary is held back too while the deposed one still answers with an undetected full view (it reads as the ONLINE primary with quorum): the witness `WitServesDuringStale` (the legitimate primary serves while the isolated old primary still believes it is the primary) is reachable without the fix (9 states) and needs the old primary's `Detect` with it (10 states). In MySQL that wait is the failure detector's (about 5 seconds), and a paused mysqld does not answer, so it does not refuse. `EventuallyServes` holds with and without the fixes (`live_flag1`, `live_flag1_fixed`, `live_flag1_fixed_all`): without them the two tablets can take the term in turn, but only until the stale member detects its partition.

| Configuration | Checks | Outcome | Distinct states | Depth | Time |
|---|---|---|---|---|---|
| `flag1_stale` | one partition, `END_TERM`; `NoStalePrimaryRecorded` | violated, 12 states | 1,172 | 13 | 3s |
| `flag1_safety` | as `flag1_stale`, every safety invariant | no error | 8,393 | 30 | 3s |
| `flag1_safety_faults` | plus one crash, one leave, one re-bootstrap | no error | 3,667,452 | 52 | 6m50 |
| `flag1_fixed` | `PROMOTE_CHECK`; every invariant and `NoStalePrimaryRecorded` | no error | 1,973 | 20 | 2s |
| `flag1_fixed_faults` | as `flag1_fixed`, with the faults | `NoStalePrimaryRecorded` violated, 12 states | 53,694 | 13 | 8s |
| `flag1_fixed_all` | `PROMOTE_CHECK_ALL` | no error | 1,973 | 20 | 2s |
| `flag1_fixed_all_faults` | `PROMOTE_CHECK_ALL`, with the faults | no error | 2,285,078 | 45 | 4m26 |
| `live_flag1` | liveness: one partition, one crash, no re-bootstrap; `EventuallyServes` | no error | 120,036 | 40 | 1m45 |
| `live_flag1_fixed` | as `live_flag1`, `PROMOTE_CHECK` | no error | 42,555 | 33 | 32s |
| `live_flag1_fixed_all` | as `live_flag1`, `PROMOTE_CHECK_ALL` | no error | 38,691 | 33 | 27s |

The partition timeouts (`Detect`, `Heal`, `Expel`, `Block`) are strongly fair in `GRLiveness.tla`: they are MySQL's timers, which run while the tablets' atomic decisions come and go; with weak fairness, the alternating promotions above kept `Detect` intermittently disabled and `live_flag1` reported a spurious `EventuallyServes` violation.

## Fifth milestone: forced ERS and a group of a single voter

`EmergencyReparentShard --group-replication-force-new-group` starts a new group from the voters that answer once the group lost its majority and its members left it, and VTOrc grows a group of a single voter back (JoinSpareBeforeGrow, then GrowVoter). See "Forced ERS" and "Voters" in the design.

### What the model adds

- `OForce(o)` (`FORCE`): under the shard lock, while no tablet that is up is an active member or runs a `START` and no intent is live, the forced reparent drops a set `U` of voters, neither empty nor all of them, keeping the voters outside `U`, which must be up. It is the voter write (atomic, like `ORemoveNoGroup`); the bootstrap that follows is VTOrc's `OBegin`/`OIntent`/`OReply` path, which the code's force path reuses (intent, required set, token, compare-and-swap of the incarnation, adoption, withdrawal). With `FORCE_DOWN`, every voter in `U` is down: the operator's assertion. Without it, a voter in `U` may run, cut off from the vtctld only: it reads the shard record (the model's tablets read the current record unless `STALE_REC`).
- The accepted loss (`Excused`): a transaction acknowledged before the force (`delTx` marks the first one after it) that no voter of the new list holds. The first run of `force_down` checked a static set instead, what no surviving voter held at the force, and found a transaction that a survivor held only in its relay log, discarded by a restart of its mysqld after the force (14 states): the same loss as if that restart had come before the force, so the rule is dynamic. A transaction acknowledged after the force is never excused (`NoLostAckAfterDelete`), nor one that a voter of the new list executed.
- `OJoinSpare(o)` (`JOIN_SPARE`): a group of a single voter `p`, alone in its view, with P1: a valid spare `x` (P3) starts its join (`JoinBody`); the list does not change. `OGrow` then accepts, for a group of a single voter only, a spare ONLINE in `p`'s view, of `p`'s incarnation, with no `START` and nothing `p` lacks, and checks the majority of the grown list in `p`'s view with it. `NLeave` gains the code's `memberMayLeave` under `JOIN_SPARE`: a member that is not a voter leaves only if it is not the group primary and its group keeps a majority of its members ONLINE without it.

### Results

| Configuration | Checks | Outcome | Distinct states | Depth | Time |
|---|---|---|---|---|---|
| `force_down` | forced reparent, the dropped voters down; two crashes, one leave, one loss of majority; every invariant, `NoLostAckExceptDeleted` and `NoLostAckAfterDelete` | no error (exhaustive) | 3,933,299 | 49 | 8m52 |
| `force_grow` | as `force_down`, with `JOIN_SPARE` and a second voter write: a dropped voter that restarts joins the single voter's group and takes a seat | no error (exhaustive) | 4,664,795 | 50 | 10m45 |
| `wit_force_grow` | witness: a spare that is not a voter is in the single voter's group (`WitJoinSpare`) | violated, 15 states | 146,110 | 15 | 19s |
| `force_lost` | as `force_down`, checking `NoLostAck`: the accepted loss | violated, 12 states | 26,700 | 12 | 7s |
| `force_live` | a dropped voter runs, cut off from the vtctld only | `NoNonVoterServes` violated, 3 states | 155 | 5 | 1s |

- **The accepted loss** (`force_lost`): s1, the primary, commits a write that no other member received yet, leaves the group and crashes; s2 crashes, and the group of s2 and s3 loses its majority; the operator drops s1 and s2, and the group is bootstrapped from s3: the write is lost, which is what the operator accepts.
- **A dropped voter that runs** (`force_live`): the forced reparent drops voters whose group still runs, cut off from the vtctld; their primary serves while it is no longer a voter, until its tablet reads the new list (`FenceDrop`, the code's `groupReplicationNotVoter`), next to the new group. A tablet that cannot reach the topology server does not read it at all, which the model leaves out: that is why the operator must make sure that the dropped voters are down.
- **Growth from a single voter**: a simulation of `force_grow` reaches a group of two voters, both ONLINE in the recorded incarnation, after the force left one (26 states: s1 and s2 dropped, s3 bootstrapped, s1 restarts, joins as a spare, and takes the seat).

## Sixth milestone: PlannedReparentShard to a tablet that is not a voter

PRS promotes a spare `x` by swapping it in first: for the voter `v` of its cell, or into a new seat (see "To a tablet that is not a voter" in the design). The model checks it against the faults of the voter redesign's family (a fourth tablet as the spare, one crash, one leave, one loss of majority, one re-bootstrap), and VTOrc's swap of a live voter, which the code does for a voter whose tablet type the policy no longer allows.

### What the model adds

- `PBeginSwap` (`PRS_SWAP`): PRS's preflight for a spare, under the shard lock: every tablet up, the current primary `cur` the settled legitimate primary (P1), `x` a valid spare (P3), and `v` any voter (each server is a cell of its own) or none (a grow). For `v # cur`, the voter write (atomic with the read, like VTOrc's) with the new list's majority in `cur`'s view without `v` and `x` (`SwapMajority`), then `PSwapJoin` waits for `x` to be ONLINE in the recorded group and the reparent goes on (`PDemote`, `PWait`, `PPromote`). For `v = cur`, the reparent demotes `cur` first.
- `PSwapDemoted`: after the demotion, on a fresh read, the swap of `cur` for `x`, with `SwapMajority`, P3, the demotion holding (`SWAP_DEMOTED`: `dmt`, the tablet's `groupReplicationDemoted`, and `super_read_only`) and the voters of the new list having executed what `cur` executed (`SWAP_HOLDS`). Then `PSwapJoin`, and `PWait` and `PPromote` promote `x`. When `x` does not join (and is not ONLINE in a view), PRS writes the old list back (`SWAP_REVERT_CHECK`: unless a view that lacks a majority of the new list would hold one of the old list) and undoes the demotion.
- `OSwapLive` (`SWAP_LIVE`): VTOrc's swap of a live voter `v`: P1 with `p # v`, P3, `SwapMajority`, no P2.

### Results

| Configuration | Checks | Outcome | Distinct states | Depth | Time |
|---|---|---|---|---|---|
| `prs_swap` | PRS to a spare, both cases, `DemotePrimary` may fail, writes to a MySQL out of its group; every invariant | no error, stopped with its queue growing (bounded) | 16,111,721 | - | 37m46 |
| `prs_swap_core` | as `prs_swap`, without the failed demotion and the writes out of the group | PENDING | | | |
| `swap_live_small` | VTOrc's swap of a live voter; one crash, one leave, no loss of majority; every invariant | no error (exhaustive) | 466,937 | 32 | 1m35 |
| `swap_live` | as `swap_live_small`, with a loss of majority and a re-bootstrap | no error, stopped with its queue growing (bounded) | 12,382,963 | - | 31m39 |
| `wit_prs_swap` | witness: PRS swaps the demoted primary out (`WitPrsSwapDemoted`) | violated, 5 states | 3,218 | 8 | 3s |
| `wit_prs_swap_serves` | witness: a spare that PRS swapped in serves as the primary (`WitPrsSwap`) | violated, 10 states | 242,858 | 12 | 41s |
| `prs_swap_norevertcheck` | `SWAP_REVERT_CHECK` off | `NoVoterMinority` violated, 7 states | 25,394 | 9 | 6s |
| `prs_swap_noholds` | `SWAP_HOLDS` off | `NoLostAck` violated, 15 states | 2,295,803 | 17 | 4m42 |
| `prs_swap_nodemoted` | `SWAP_DEMOTED` off | `NoNonVoterServes` violated, 18 states | 8,818,560 | 20 | 19m12 |

Each of the three checks of the swap of the demoted primary was found by the model, in this order, before the code had it:

- **The revert** (`prs_swap_norevertcheck`): the swap's join fails after a voter left the group; writing the old list back gives the view of the old primary and the remaining voter a majority of the old list that it lacks under the new one. A first version of the revert also wrote the old list back while `x` had joined and been elected after the old primary crashed (`NoNonVoterServes`, 14 states): PRS now reads `x`'s status after a failed join RPC, and goes on if `x` is ONLINE.
- **The new voters' transactions** (`prs_swap_noholds`): the old primary, the only voter that executed an acknowledged write, is swapped out, the swap fails, the old primary crashes, and VTOrc bootstraps the group from the new list, which no longer needs it. A first version of the wait counted a copy in a voter's relay log, which a restart of its mysqld discards (`NoLostAck`, 17 states).
- **The demotion** (`prs_swap_nodemoted`): the primary crashes and restarts as a REPLICA before PRS demotes it, so the demotion does not hold; its sync loop makes it serve again as the group's primary, and the swap then drops it from the voters.

## Conformance: actions and the code they model

Paths are relative to `go/vt/`; line numbers are on `group-replication-prototype` at the commit that adds this model, and for the second milestone's rows at f528e9a. The fixes on `gr-fixes5` are named by function: their lines may move before the merge.

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
| `HBoot1`, `HBootGo`, `HBootGiveUp`, `HBootAbort` | `StartGroupReplication` (`vttablet/tabletmanager/rpc_group_replication.go:47`), `stopServingBeforeBootstrap` (`group_replication.go:1005`), `startGroupReplicationLocked`, `stopOngoingGroupStartLocked` (`:526`), `finishGroupJoinLocked` (`:642`), `noteBootstrap` (`group_replication_legitimacy.go:119`) | The serving pause (`pauseServingLocked`) is left out; a PRIMARY tablet stops serving. RECOVERING-without-ONLINE is not modeled. The checks of the request (`checkGroupBootstrapIntentLocked`, `checkGroupBootstrapLocked` in `group_replication_bootstrap.go`) are atomic with the step they guard: the intent check when the RPC gets the lock, with the `STOP` of a `START` in progress, and with MySQL's `START`; the relay log's application (`ApplyGroupReplicationRelayLog`) and the GTID check with the `START`. A crash between them only loses the RPC. A refusal is definitive (`refuseGroupBootstrapLocked`, reply -2) only on the GTID check while no `START` runs: the code confirms it with a fresh read of MySQL's status (`start_in_progress`, from `performance_schema.processlist`) under the action lock, which the model's `st[s] = "none"` stands for. The ghost `bPrev`, the recorded incarnation that a bootstrap `START` started from (for `NoDualBootstrap`), is set when the `START` begins and kept while it runs; earlier versions set it when an RPC that waits for a `START` got the lock, and cleared it when such an RPC gave up, which hid the provenance of a bootstrap `START` that still ran. |
| `OBegin` | `matchGroupNotBootstrapped` (`vtorc/inst/group_replication.go:576`), `bootstrapGroupReplication`, `chooseGroupBootstrapCandidate`, `memberGTIDSets`; the re-probe: `staleGroupBootstrapIntentTarget`, `runGroupBootstrap` (`vtorc/logic/group_replication_recovery.go`) | The voters' statuses are one atomic snapshot. The 10s grace for a `START` in progress is "may proceed at any time", with the preference for members without one. Lowest alias is a nondeterministic choice. The re-probe's conditions are read on that snapshot; its token and voter conditions always hold in the model (one tablet per voter, every intent has a token). After a withdrawal, the code chooses again in the same pass; the model ends the recovery instead (see "Re-probing a stale intent's target"). |
| `OIntent` | `WriteGroupReplicationBootstrapIntent` (`vtctl/reparentutil/group_replication_bootstrap_intent.go:71`), then the RPC | The lock check (`CheckShardLocked`) is left out of every topology write: writes are guarded only by their compare-and-swap, which over-approximates the check-then-write race. |
| `OReply`, `OTimeout`, `OAdopt`, `OAdoptLater` | `RecordGroupReplicationBootstrap`, `AdoptGroupReplicationBootstrap`, `adoptableGroupIncarnation` (`group_replication_bootstrap_intent.go:218`, `:199`, `:158`), `writeGroupReplicationIncarnation` (`vtctl/reparentutil/group_replication.go:293`), `adoptGroupReplicationBootstrap` and `matchGroupBootstrapNotRecorded` (`vtorc/logic/group_replication_recovery.go:486`, `vtorc/inst/group_replication.go:586`); after a definitive refusal, `WithdrawGroupReplicationBootstrapIntent` (`group_replication_bootstrap_intent.go`) and `withdrawGroupReplicationBootstrapIntent` (`vtorc/logic/group_replication_recovery.go`) | The adoption's status read and its write are one step. A reply is matched to its RPC by the VTOrc and the intent token: a retried RPC never gets the reply of an earlier one. The definitive refusal travels as `StartGroupReplicationResponse.definitive_refusal`, and `tmclient.GroupBootstrapRefusedError` in VTOrc. |
| `OLeaseExpire` | etcd lease (`topo/etcd2topo/lock.go:247`) | At most once (`MaxExpire`). |
| `OIntentExpire` | `GroupReplicationBootstrapIntentFence` (2 minutes) | With `INTENT_OUTLASTS_BOOT`, only while no recovery or bootstrap is in flight. |
| `StaleTopo`, `AsyncApply` | `reconcileStaleTopoPrimary` (`vtorc/logic/topology_recovery.go:1677`), `forceDemotePrimary`, `setReplicationSource`, `setReplicationSourceLocked` (`vttablet/tabletmanager/rpc_replication.go:1091`) | The tablet follows the type VTOrc writes into the topology. |
| `VotGroupUp`, `OVotRead`, `OVotWrite` | `updateGroupReplicationVoters` (`vtorc/logic/group_replication_recovery.go:937`), `matchGroupVotersOutOfDate`, `computeGroupReplicationVoters`, `SelectGroupReplicationVoters` (`vtorc/inst/group_replication.go:615`, `:463`, `:219`), `policy.SelectVoters` (`vtctl/reparentutil/policy/group_replication.go:269`); with `VOTERS_KEEP_MINORITY` and `VOT_CAS`, `voterChangeRefusal` and the compare-and-swap in `updateGroupReplicationVoters` (`gr-fixes5`) | Every tablet that has not failed keeps its seat (three tablets, one per cell); the selection may drop any subset of the failed voters. VTOrc sees every view (no unreachable member that is up). With `VOT_SPLIT`, the selection and the write are separate steps; otherwise one. |
| `GraceExpire` | `GetGroupReplicationVoterReplacementGracePeriod` (`vtorc/inst/group_replication.go:249`) | No clocks: one expiry marks every host that is down then as failed, and only once the live group settled (`GRACE_SETTLES`). |
| `VLeave` | `leaveGroupReplication` (`vtorc/logic/group_replication_recovery.go:1061`) | `StopGroupReplication` under the action lock in one step; the asynchronous replication that follows is left out. |
| `PBegin` | `preflightChecks` (`vtctl/reparentutil/planned_reparenter.go:163`), `checkGroupReplicationPrimaryElect` (`vtctl/reparentutil/group_replication.go:138`) | Every tablet up; the elect a listed voter in the current primary's group. |
| `PDemote`, `PDemoteEnd`, `PDemoteFail` | `demotePrimary` (`vttablet/tabletmanager/rpc_replication.go:667`) and its deferred reverts (`:849`) | The only failure modeled is the last step's (the read of the primary status), after `super_read_only`. Semi-sync is left out. |
| `DrSnap`, `DrRead`, `DrAct` | `revertDemotionWithGroupDecisionLocked` (`vttablet/tabletmanager/rpc_replication.go`, `gr-fixes5`) | Only with `DEMOTE_REVERT_DECISION`. The redo of prepared transactions is left out (no two-phase commit). |
| `PWait` | `performGracefulPromotion` (`vtctl/reparentutil/planned_reparenter.go:251`): `WaitForPosition`, then `UndoDemotePrimary` on failure | The catch-up is "the elect executed what the old primary executed". |
| `PPromote`, `PPromote2`, `PEnd` | `PromoteReplica` (`vttablet/tabletmanager/rpc_replication.go:1389`), `promoteGroupMemberLocked`, `waitForGroupPrimaryElected` (`vttablet/tabletmanager/group_replication.go:939`, `:757`), `PopulateReparentJournal` (`rpc_replication.go:533`) | `group_replication_set_as_primary` takes effect at once. The journal is a write that succeeds only where MySQL accepts commits; a client's commit covers it. |
| `PAbort` | an RPC of the reparent that fails | Its tablet is down, or its handler ended with a crash. |
| `EBegin` | `reparentShardLockedGroupReplication`, `findGroupWithQuorum`, `chooseGroupReplicationPrimary` (`vtctl/reparentutil/emergency_reparenter_gr.go:214`, `:64`, `:130`) | One member's view stands for all: the code's failure when members disagree only removes behaviors. Member weights, `PreventCrossCellPromotion` and `SetReplicationSource` of the replicas are left out. |
| `OUndo`, `UdSnap`, `UdRead`, `UdAct` | `fixPrimary` (`vtorc/logic/topology_recovery.go:1573`), `tabletUndoDemotePrimary` (`vtorc/logic/tablet_discovery.go:461`), `UndoDemotePrimary` (`vttablet/tabletmanager/rpc_replication.go:896`) | The recovery's shard lock only orders it with other recoveries, and is left out. |
| `RwSnap`, `RwRead`, `RwAct` | `SetReadOnly` (`vttablet/tabletmanager/rpc_actions.go:79`), `checkGroupAllowsReadWrite` (`vttablet/tabletmanager/group_replication.go:345`), `redoPreparedTransactionsAndSetReadWrite` (`vttablet/tabletmanager/tm_init.go:978`) | Called at any time on any tablet, at most `MaxSetRW` times. |
| `PIBegin`, `IP1`, `IP2`, `PIRecord` | `performInitialPromotion`, `selectInitialVoters` (`vtctl/reparentutil/planned_reparenter.go:350`, `:432`), `InitPrimary` (`vttablet/tabletmanager/rpc_replication.go:459`), `bootstrapGroupForInitPrimaryLocked` (`vttablet/tabletmanager/group_replication.go:984`), `RecordGroupReplicationIncarnation` (`vtctl/reparentutil/group_replication.go:264`); with `INIT_GUARD`, `checkShardHasNoGroup` (`gr-fixes5`) | The voters are all three already. `stopOngoingGroupStartLocked` is left out of `InitPrimary`: a `START` in progress refuses it. |
| `CommitAlone` | MySQL with Group Replication stopped (OFFLINE) and `super_read_only` off | Only with `STANDALONE`; never in the ERROR state. |
| `MysqldRestart` | mysqld killed or restarted under a running vttablet (NEW-6) | Uses the crash budget; not while the tablet runs a `START` of its own. |
| `JoinStart` with `JOIN_WAITS_RECORD` | `checkLegitimateGroupToJoin` (`vttablet/tabletmanager/group_replication_legitimacy.go:399`), `matchGroupMemberNotOnline`, and VTOrc's voter update (`gr-fixes5`) | No join while the shard record lists no incarnation; every join of the model is `JoinStart`. |
| `OVotWrite` with `VOT_REVALIDATE` | `recheckVoterChange` (`vtorc/logic/group_replication_recovery.go`, `gr-fixes5`) | The re-read and the compare-and-swap are one step: no stall between them. Every dropped voter was dropped as failed, so `"reachable"` decides before `"dropped"`; the time since VTOrc last reached a voter is `ovot.back`. |
| `FenceDrop` | the fence check of a PRIMARY tablet that is no longer a listed voter (`gr-fixes5`) | Atomic; any time after the list changed, except during a bootstrap. |
| `NLeave` | the sync loop's leave of an active non-voter (`gr-fixes5`) | A clean `STOP`, any time. |
| `OAdoptUnrec` | `adoptUnrecordedGroup`, `matchGroupBootstrapNotRecorded`, `RecordUnrecordedGroupReplicationIncarnation` (`vtorc/logic/group_replication_recovery.go`, `vtorc/inst/group_replication.go`, `vtctl/reparentutil/group_replication.go`, `gr-fixes5`) | The statuses and the compare-and-swap in one step. |
| `OSwap`, `OGrow`, `OMoveToVoter`, `OMoveFromDeleted`, `ORemove`, `ORemoveNoGroup`, `ODelete` | the voter redesign (implemented in `go/vt/vtorc/inst/group_replication_voters.go`, `PlanGroupVoters`): SwapVoter, GrowVoter, GroupPrimaryNotVoter, RemoveVoter, RemoveVoterNoGroup; `DeleteTablets` | Each is one atomic read and compare-and-swap (or, with `VOT_SPLIT`, a read and a later `OVotWrite`). Every server is a cell of its own; a spare may replace any voter. |
| `Isolate`, `Detect`, `Expel`, `Block`, `Heal`, `PClean` | MySQL Group Replication: XCom's failure detector, expulsion (`group_replication_member_expel_timeout`), `group_replication_unreachable_majority_timeout`; `replication_group_members` as `fillGroupReplicationMemberStatus` reads it (`mysql/group_replication.go`) | Fourth milestone. A cut isolates a minority side of a view; its members keep their old view (ONLINE, with quorum, until `Detect`); the majority side lists them UNREACHABLE until it expels them. Commits, deliveries, elections, joins and expulsions need the real quorum. `Block` is final. |
| `MyMem`, `MyOnl`, `MyPrim`, `MyQuorum`, `IsPrimQ`; `P1`, `NoViewRead`, `MyMem(p)` in `ORemove`, `P3` | `IsGroupPrimary`, `HasQuorum`; `settledPrimary`, `activeAnywhere`, `inPrimaryView`, `spare` (`vtorc/inst/group_replication_voters.go`) | Each MySQL's own view, which VTOrc and the tablets read; `RealPrimQ` and `CanAccept` are the ground truth. `READ_VIEWS` selects the code's P2. |
| `EndTerm`; `PROMOTE_CHECK` in `PrSnap` | `endPrimaryTerm` on an active member (`vttablet/tabletmanager/shard_sync.go`); the proposed check before `groupReplicationSync.promote` (FLAG 1) | Only the tablet type changes; MySQL stays writable. The check reads the recorded tablet's status in the same step as the promotion's snapshot (atomic decisions). |
| `OForce` | `forceNewGroupReplicationGroup`, `planForcedGroup`, `writeForcedGroupVoters` (`vtctl/reparentutil/emergency_reparenter_gr_force.go`) | The read and the voter write are one step; the bootstrap is VTOrc's actions, which the force path reuses. |
| `OJoinSpare`; `OGrow` and `NLeave` with `JOIN_SPARE` | JoinSpareBeforeGrow and `spare(..., joined)` in `PlanGroupVoters` (`vtorc/inst/group_replication_voters.go`), `updateGroupReplicationVoters` (`vtorc/logic/group_replication_recovery.go`); `memberMayLeave` (`vttablet/tabletmanager/group_replication_sync.go`) | The decision and the start of the join are one step; the join's end is `JoinComplete`. |
| `PBeginSwap`, `PSwapDemoted`, `PSwapJoin` | `planGroupReplicationSwap`, `checkGroupSwap`, `swapInElect`, `swapAfterDemote`, `waitForNewVotersToHold`, `swapRevertRefusal` (`vtctl/reparentutil/planned_reparenter_gr_swap.go`), `performGracefulPromotion` | Each decision and its voter write are one step (the code's read and compare-and-swap, under the shard lock); the spare does not replicate asynchronously, so only the other voters can hold the demoted primary's transactions. |
| `OSwapLive` | the swap of an ineligible voter in `PlanGroupVoters` (`vtorc/inst/group_replication_voters.go`) | Any voter can be swapped: the model has no tablet types. |

## What the model does not cover

- Liveness beyond the `live*` configurations (one VTOrc, no transaction, at most two crashes, one loss of majority or one voter write, one initial promotion, no PRS or ERS of a running shard) and the stuck-state checks of S7d r3, of a refused intent and of a stale intent. No timing: every timeout is a step that fairness eventually takes, so the model cannot say how long an outage lasts.
- `InitShardPrimary` (its `InitPrimary` is the initial promotion's), the migration (`MigrateReplicationMode`) and its pauses, the group primary move (NEW-4), backups. PRS's catch-up is "the elect executed what the old primary executed"; ERS's `SetReplicationSource` on the other tablets, member weights and the cross-cell rules are left out.
- Voter replacement by another tablet: the model has three tablets, all voters, so a voter can only be dropped. The same-cell replacement that the voter rule allows, a spare that joins, and the voter compare-and-swap between two VTOrcs (which needs a spare to fail) are not checked. VTOrc sees every view: the code's rule for a new voter that is unreachable and in a view VTOrc cannot see is not exercised.
- vtgate. Clients write to any MySQL that accepts commits, which is stronger than vtgate's routing; the probe's routing variants are not repeated.
- The durability policy: the shard is always under a Group Replication policy. `offline_mode`, semi-sync, two-phase commit (and so the redo of prepared transactions), heartbeats and replication lag are left out.
- MySQL: member weights, RECOVERING, ERROR states that wedge a member (NEW-5), auto-rejoin, certification, flow control, the 1–2s blocks of status reads during a `START`, the view change events of joins and leaves. Partial partitions (fourth milestone) are between MySQL members only: VTOrc reaches every tablet that is up, vttablet and mysqld fail together (a deleted record stands for a tablet VTOrc cannot read whose MySQL may run), a side that timed out without quorum leaves even if the partition heals later, at most two partitions per behavior, and only in the `partition` and `flag1` families (PRS and ERS still read the ground truth).
- Partial partitions with VTOrc's reads split from its write (`VOT_SPLIT`), with two VTOrcs, or with a majority side whose other members' vttablets VTOrc cannot reach while their MySQL runs (the case where RemoveVoter's "not in p's view" could matter for safety).
- Timing: VTOrc's re-read before a voter write and the write are one step, and so are `adoptUnrecordedGroup`'s reads and its write; a stall between them is not modeled. The 15-minute cap per run kept `current` (8.5M of 9.5M states explored, no error) and `tablet`, `orcs_stall` and `withdraw_orcs` from being run again exhaustively in the second milestone; no action of the second milestone is enabled in them.
- Bounds: exhaustively, at most one crash, one leave, one loss of majority and one lease expiry per behavior, one transaction, two intents and two new incarnations (one in the `tablet`, `voters` and `reparent` families; two crashes, two leaves or one voter write and one reparent where a family says so); by simulation only, two of each fault, two transactions, three intents and three new incarnations. Bugs that need more are outside the checked space, and simulation samples its space.

## Next milestone

- Re-check the fixes once `gr-fixes5` is merged, against its final code and line numbers; `init_alone` without `INIT_STABLE` now that finding 6 is fixed.
- A fourth tablet: the voter replacement by a spare, the voter compare-and-swap between two VTOrcs, and the code's rule for unreachable new voters, with VTOrc's partial view of the statuses.
- Partial partitions with asymmetric VTOrc reachability (a tablet VTOrc cannot reach whose MySQL is in the majority side), with `VOT_SPLIT` and two VTOrcs, and with PRS and ERS reading per-member views.
- Liveness with the reparents and voter replacement (a failed demotion that the revert refuses ends with a serving primary), and a liveness validation: every known stuck state so far ends with an intent's expiry, which the bounds count.
- More exhaustive coverage: two transactions in the `tablet` family through further symmetry or a `VIEW`, leaves in the `orcs` family, and the integrated second-milestone configuration (`fixed`) beyond simulation.
