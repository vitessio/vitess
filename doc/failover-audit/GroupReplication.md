# Unplanned failover audit, repeated with MySQL Group Replication

The [failover audit](https://github.com/vitessio/vitess/blob/claude/vitess-failover-validation-nm4msw/doc/failover-audit/README.md) (branch `claude/vitess-failover-validation-nm4msw`) found unplanned-failover problems with cross-cell semi-sync and VTOrc. This document checks each finding against the Group Replication (GR) mode of this branch (`group_replication_cross_cell`, see `doc/design-docs/GroupReplication.md`), using the audit's chaos harness in a new GR mode.

Setup: 3 cells, one tablet per cell, one VTOrc per cell, a per-cell etcd plus a global etcd, vtgate in zone1, MySQL 8.4.11, 4 writers (40 ms interval) and a primary reader (100 ms) through vtgate. In GR mode the shard is created with `cross_cell` semi-sync, converted online with `vtctldclient MigrateReplicationMode --durability-policy group_replication_cross_cell`, and every scenario starts once all three voters are ONLINE (`member_expel_timeout=0`, `paxos_single_leader=ON`, `unreachable_majority_timeout=1s`, VTOrc failover grace 30s, default sync interval 1s).

Status legend: **not reproducible** (the failure mode cannot happen, with evidence), **reproduced**, **new variant** (the issue appears in a different form), **new** (GR-specific issue not in the audit).

## Summary table

"Gap" is the longest interval without an acknowledged write. Semi-sync numbers are from the audit (MySQL 8.0.46) unless marked "this env".

| Audit item | Semi-sync result (audit) | GR result | Evidence | Status |
|---|---|---|---|---|
| §2 relay-log discard on replica restart (S11, S11k) | 500 acked writes lost, ERS reports success | Relay-log discard still happens on a member (G11k: R1 held 401 certified, unapplied transactions `:1129-1529`; after `kill -9` and restart it reported `Received=""`, `Executed=:1-1128`). It cannot become silent loss: the group `{P,R1}` loses quorum, P leaves after ~12s, nothing is elected, and VTOrc only bootstraps the voter whose GTID set contains all others, and only when every voter is reachable. | G11k: no failover in 80s with P's host down, 0/3272 acked writes lost, writes back 64.7s after P restarted | not reproducible (fails closed) |
| §2 via `fixReplica` (S11c) | 500 acked writes lost | Async analyses (`ReplicaSemiSyncMustBeSet`, `fixReplica`) are suppressed for active members; no default channel exists to repoint. But see NEW-3: VTOrc's `StaleTopoPrimary` still configures the default channel on a voter that is out of the group. | code: `go/vt/vtorc/inst/analysis_dao.go` (GR suppression); S7d logs | not reproducible (different issue: NEW-3) |
| §2 NEW RISK: graceful leave shrinks the group | n/a (semi-sync blocks with no acker) | **Confirmed.** After R2 was expelled, a clean `mysqlctl shutdown` of R1 left P as a group of one (`view=1/1`) that kept committing: 1264 acked writes in ~13s existed only on P (`:1533-2788`). P's host then died: no failover (VTOrc logged 46 failed `GroupNotBootstrapped` attempts), writes blocked 153s until P returned; bootstrap on P, 0 lost. The same shrink happens through the tablet's own rejoin (`STOP GROUP_REPLICATION` before `START`) and through `unreachable_majority_timeout` leaves (S7c: zone1 served as `view=1/1`). | G11 report; S7c observer log `zone1 ... gr=ONLINE/PRIMARY view=1/1` | new variant (durability of one while a member is out; fails closed if the primary then dies) |
| §3A old primary rejoins with errant GTIDs / `super_read_only=OFF` (S3, S7d, V2) | S3 ×2, S7d, V2: errant GTIDs, SRO OFF | S3, V2, S4, S9i: minority primary cannot commit, `read_only` ON 6.8s after the fault, rejoined as ONLINE secondary, no errant GTIDs. **S7d (flapping, 11s isolated / 5s healed): acked writes lost in 3 of 4 runs** (652/1330, 637/2608, 664/3391), and the 4th ended with all three members OFFLINE for >2 min. | S7d reports and MySQL logs (NEW-1, NEW-2) | not reproducible for single faults; **new variant, worse, under flapping** |
| §3B failover blocked by an unreachable cell-local topo, lock-lease expiry (S9, S9i, S9b) | S9: 155s down, S9i: 133s, S9b: failover 58s after etcd returned | S9: gap 7.7s; S9i: 9.1s; S10: 7.7s. The group elects without topo and no shard lock is involved. But when the member the group elects has its own cell topo down, its tablet cannot become PRIMARY (the tablet record is written first): S9b gap 169.6s, G9b (deterministic) 104.6s, bounded only by when the test restored etcd. | S9b vttablet log: `failed to promote the tablet to PRIMARY error="context canceled updating tablet_type ... in the topo"` every 15s | fixed for S9/S9i; **new variant** for S9b (NEW-4) |
| §3C replication-only partition delays ERS 70s+ (S4) | 73s and 83s | Majority elects, old primary demotes itself: new primary +7.0s, gap 6.8s. | S4 report | not reproducible |
| §3D stale `fixReplica`/`SetReplicationSource` demotes the new primary | UNIT; this env (G3D semi-sync): primary made a replica of a replica, unwanted failover at +2.75s, 256 failed writes, old primary left with `super_read_only=0` | Before fix: tablet flipped to REPLICA for 0.61s (64 failed writes) until the sync loop re-promoted it. **Fixed** (cb582df): refused with `FAILED_PRECONDITION`, 0 failed writes. | G3D reports (GR before/after, semi-sync) | reproduced (brief), fixed |
| §3E vtgate routes to an isolated/old primary | UNIT; this env (G3E semi-sync): reads to old primary until +11.5s, old primary PRIMARY+writable 21s after failover, never reconverged | Isolated GR primary: vtgate reads until +6.7s (G3E), +6.8s (S4), mysqld read-only at +6.8s. **But the healthcheck gap itself is reproduced E2E**: in S7/S7b the old primary's vttablet stayed PRIMARY while its mysqld was down (and after restart, OFFLINE/read-only), and when the current primary's mysqld died vtgate routed primary reads to it: 30 (S7) and 24 (S7b) stale reads. | S7/S7b reports ("answered by old primary after the topo primary changed: 30") | bounded to ~7s for isolation; **reproduced** for the stale-PRIMARY case |
| §F wall-clock primary terms | PLAUSIBLE | Still applies: self-promotion uses `time.Now()` (`tm_state.go:215` via `group_replication_sync.go:236`). | code | applies |
| §F `PreventCrossCellPromotion` | makes ERS impossible with one tablet per cell | Ignored by the group's own election and the tablet's self-promotion (only the GR ERS path checks it, `emergency_reparenter_gr.go:111`). G1x with `--prevent-cross-cell-failover`: failover to zone3 in 7.5s. | G1x report | applies (not honoured) |
| §F `IncapacitatedPrimary` single vantage point | CODE | Still a single VTOrc's view, but gated by the 30s GR grace (`group_replication_recovery.go:65-83`); after that it runs PRS/ERS (a switchover, not loss). Not tested E2E. | code | reduced |
| §F ERS abort leaves IO threads stopped; `--ignore-replicas` bypasses revocation | UNIT | GR ERS path neither stops replication nor revokes; it only follows a quorum (`findGroupWithQuorum`). | code | not applicable |
| §F semi-sync fallback only prevented by my.cnf | CODE | Not applicable; the analogue is the group shrinking (row 3). | G11 | new variant |
| §F in-flight unacked commits become errant | E2E S8b | Minority commits are rolled back (`before_commit ... failed`, "transactions were unable to be certified and will now rollback"); no errant GTIDs in S2/S3/S4/V2. | MySQL error logs | not reproducible |
| §F replica startup repoints without lock; `InitPrimary` read-write before semi-sync | PLAUSIBLE | Voters join the group, never repoint; `InitPrimary` bootstraps first. Still applies to non-voter async replicas. | code | not applicable to voters |
| Held up: S1, S2, S5, S6, S7, S8, S10 | correct | S1 7.3s/gap 7.6s (semi-sync this env 3.2s); S6/S6b no failover, 0 failed writes; S8/S8b/S10 7.7s; S5/S5b and S7/S7b: no failover, fail closed, 0 lost (semi-sync S5 failed over after R1 resumed; GR waits for the dead primary: 223s). **S2: the resumed old primary stayed in ERROR forever (3/3 runs, NEW-5).** | reports | S2 regressed |
| V1/V2/V3 (single VTOrc) | V2: 1.6s dual-writable, errant GTIDs, 91s down | V1: no failover, 0 failed writes; V2: gap 7.6s, no errant; V3: 7.4s | reports | V2 fixed |

## New GR-specific issues

### NEW-1. A stale member re-forms the group on its own and becomes primary: acknowledged writes lost (S7d, 3 of 4 runs)

S7d isolates the current primary for 11s and heals for 5s, four times. In three of four GR runs a member that lacked the latest certified transactions ended up as the only member of a group, became PRIMARY in Vitess, and took writes. The members that held the acked transactions were then refused (`This member has more executed transactions than those present in the group`) or, in one run, admitted anyway.

| Run | Member alone in its view | View | Acked writes missing on final primary |
|---|---|---|---|
| 1 | zone3 | new incarnation `17907817905287113:1` | 652/1330 |
| r2 (with the fixes) | zone1 | new incarnation `17907850003440460:1` | 637/2608 |
| ar0 (`--group-replication-autorejoin-tries 0`) | zone3 | new incarnation `17907858161940982:1` | 664/3391 |
| r1 (with the fixes) | none; all members OFFLINE, see NEW-2 | | 0, but no primary for >2 min |

In each case the member had just left a group because of `unreachable_majority_timeout` or an expulsion, and Vitess made it join again (`START GROUP_REPLICATION`, never with the bootstrap flag) while the other members were themselves in ERROR or leaving. About 13s later (48s in r2) MySQL installed a new view incarnation containing only that member (run ar0, zone3: `16:30:03.043 Starting group replication (bootstrap: false)` then `16:30:16.19 Group membership changed to vm:13721 on view 17907858161940982:1`). In S7c the same shrink happened within one incarnation (zone1 alone at view `…:8` and `…:10`). The MySQL-level mechanism was not isolated; two raw-MySQL lab attempts (join while the other two members lose majority, with and without killing the `START`) did not reproduce it.

Vitess accepts such a group: `HasQuorum` is computed over the member's current view (`go/mysql/group_replication.go:169`), so a view of one has quorum, and the sync loop promotes the tablet (`group_replication_sync.go:142-143`). Nothing records which group incarnation holds the shard's data.

Suggested fixes (Vitess side):
- Measure quorum against the voters in the shard record: a member may be treated as primary (sync-loop promotion, VTOrc `GroupPrimaryNotInTopo`, ERS) only if a majority of the listed voters are ONLINE in its view. A primary whose ONLINE count drops below that majority should stop taking writes. This also closes the graceful-leave shrink (G11) and S7c.
- Record the group incarnation (view-id prefix) in the shard record when Vitess bootstraps, and treat a member reporting a different incarnation as a split group: never promote it, alert, and make it leave.
- Serialize membership changes: the tablet's rejoin loop, VTOrc `GroupMemberNotOnline`, VTOrc bootstrap and `StaleTopoPrimary` all issue `STOP`/`START GROUP_REPLICATION` concurrently on the same member during S7d.

### NEW-2. Bootstrap starved by concurrent joins: permanent outage (S7d r1, G11, G11k)

When no member is active, VTOrc's `GroupNotBootstrapped` picks the right member, but `StartGroupReplication(bootstrap)` fails with `errno 3724 This option cannot be set while START or STOP GROUP_REPLICATION is ongoing` as long as a join (`bootstrap=false`) is in flight on that member. Joins are started by the tablet's startup, its rejoin loop and VTOrc `GroupMemberNotOnline`, and each one blocks until MySQL's join timeout because no group exists. S7d r1: 114 failed bootstrap attempts, all members OFFLINE when the test gave up (>2 min). G11/G11k: bootstrap delayed ~50s after P came back (writes back after 65s).

Suggested fix: before bootstrapping, suspend the tablet's rejoin and stop the in-flight join (`STOP GROUP_REPLICATION`), or do not start a join when no seed reports an active group.

### NEW-3. `StaleTopoPrimary` configures async replication on a voter that is out of the group

`reconcileStaleTopoPrimary` force-demotes the old primary and calls `setReplicationSource` (`go/vt/vtorc/logic/topology_recovery.go:1686`). For a voter in ERROR/OFFLINE the tablet takes the async path and runs `CHANGE REPLICATION SOURCE … ; START REPLICA` on the default channel (S7d r2: zone1 at 16:16:05.64, right after `StaleTopoPrimary`; run 1: zone3 received the same kind of `SetReplicationSource` at 15:23:09.8), concurrently with GR rejoins (`Can't start replica IO THREAD of channel '' when group replication is running with single-primary mode`). The GR ERS path deliberately skips such voters (`reparentGroupReplicationTablets`); this recovery should too.

### NEW-4. The group elects a member whose cell topo is down; Vitess cannot promote it (S9b, G9b)

The group's election ignores topo. The elected member's tablet must write its own tablet record before it becomes PRIMARY (`tm_state.go:214-240`), which fails every 15s while its cell's etcd is down. VTOrc's `GroupPrimaryNotInTopo` and ERS use the same path. Nothing moves the group primary to a member whose cell topo works: G9b 104.6s without a primary (until the test restored etcd), S9b 169.6s including 49s for the etcd client to reconnect. Suggested fix: when promotion keeps failing, have the sync loop or VTOrc call `group_replication_set_as_primary` on a member in a healthy cell, or lower the member weight of tablets whose cell topo is unreachable.

### NEW-5. A resumed old primary wedges in ERROR forever (S2, 3 of 3 runs)

After SIGSTOP/SIGCONT the old primary is expelled. Its mysqld then deadlocks: a Vitess status query on `performance_schema.replication_group_communication_information` (issued by `(*Conn).GroupReplicationStatus`, `go/mysql/group_replication.go:107-116`, only to fill `PaxosSingleLeader`) waits forever in `Gcs_xcom_proxy_impl::xcom_client_get_event_horizon` while holding the GCS lock that `Gcs_operations::leave` (auto-rejoin) needs (gdb stacks: thread `connection` in `get_write_concurrency`, thread `gr_rejoin` in `pthread_rwlock_wrlock`). `KILL` does not interrupt it. On the Vitess side the query runs without a context (`mysqlctl.(*Mysqld).GroupReplicationStatus`), so `DemotePrimary(force)` from VTOrc's `StaleTopoPrimary` hangs inside it holding the tablet action lock, and every later `StartGroupReplication` RPC times out (S2 autorejoin0 goroutine dump: `demotePrimary` → `GroupReplicationStatus` → `group_replication.go:108`). With `--group-replication-autorejoin-tries 0` it still wedged. In one run VTOrc then dropped the wedged voter from the shard record, leaving 2 voters. Recovery needs a mysqld restart.

Suggested fix: do not read `replication_group_communication_information` on every status poll (read it once after a successful join/bootstrap and cache it, or report the configured `group_replication_paxos_single_leader`), and run the status queries with the caller's context so a hung query cannot hold the action lock.

### NEW-6. Old primary vttablet keeps type PRIMARY while its mysqld is down or OFFLINE

`endPrimaryTerm` returns early when the GR status cannot be read (mysqld down), and after a restart the member is OFFLINE, so the tablet stays PRIMARY (S1: ~30s, until VTOrc rejoined it; S7/S7b: >100s). Its own rejoin loop refuses to rejoin a PRIMARY tablet (`group_replication_sync.go:357`), so rejoin depends on VTOrc. Combined with the vtgate healthcheck gap (§3E) this produced the stale reads in S7/S7b.

### NEW-7. Other observations

- `vtctldclient StopReplication` on a voter is undone by VTOrc `GroupMemberNotOnline` within seconds (G11s), unlike the tablet's own rejoin, which honours it.
- `StartGroupReplication(bootstrap)` left `group_replication_bootstrap_group=ON` when `START` outlived the caller's context: the reset reused the expired context. A later join would bootstrap a second group. **Fixed** (a49bfc9, with a unit test).
- `TestStopGroupReplicationRestoresReadWriteOnPrimary` is flaky on this branch (2/40 runs without any change); `TestWaitForDBAGrants` fails in this environment.
- S8 does not exercise its fault in GR mode: VTOrc never starts an ERS for `DeadPrimary` within 60s (the group elects first).

## Fixes in this branch

| Commit | Change |
|---|---|
| a49bfc9 | `mysqlctl.StartGroupReplication`: reset `group_replication_bootstrap_group` with a fresh context and connection after a failed bootstrap. Unit test fails without the fix. |
| cb582df | `setReplicationSourceLocked`: refuse (`FAILED_PRECONDITION`) when MySQL is the primary of a group with quorum, before changing the tablet type. Unit test fails without the fix; G3D in GR mode goes from 64 failed writes to 0. |

NEW-1 to NEW-6 are addressed by the design changes below; NEW-4 (a group primary whose cell topo is down) is not.

## Fixes and re-run

The principle: Group Replication decides the primary and Vitess follows it, but only within the shard's **legitimate** group, and Vitess must not create the conditions for a new group (`doc/design-docs/GroupReplication.md`, "The legitimate group").

| Commit | Change | Issue |
|---|---|---|
| d410495 | `Shard.group_replication_incarnation` records the view-id incarnation of the group Vitess bootstraps (migration, PRS initial promotion, `InitShardPrimary`, VTOrc bootstrap; cleared by the reverse migration). `policy.LegitimateGroup`: a primary is followed only if it is the ONLINE primary with quorum, in the recorded incarnation, and a majority of the listed voters are ONLINE in its view. Used by the tablet sync loop, VTOrc (`GroupPrimaryNotInTopo`, `PromoteGroupPrimary`, voter selection, `GroupMemberNotOnline`), ERS and the PRS preflight. A member active in another incarnation makes its tablet leave that group and suspend its rejoins. | NEW-1 |
| dd9a60e | A PRIMARY whose view holds fewer than a majority of the voters stops serving (`replication group lost the majority of its voters`), keeps its type (vtgate buffers), and serves again once the majority is back. MySQL is left alone. | graceful-leave shrink (G11, S7c) |
| 67e4c28 | Tablet startup join, sync-loop rejoin and VTOrc `GroupMemberNotOnline` only start a join while another tablet reports an active member of the legitimate group with quorum. A bootstrap suspends the tablet's rejoins and stops a `START` in progress (errno 3724, or RECOVERING without a group). | NEW-2 |
| a0acd14 | No more `replication_group_communication_information` query: `paxos_single_leader` comes from `@@global.group_replication_paxos_single_leader`. Status reads run with the caller's context in mysqlctl, and the tablet bounds each read by 10s. | NEW-5 |
| 19cc6f9 | Under a GR policy, a PRIMARY tablet that is a listed voter and whose member is OFFLINE (or the plugin is off) is demoted to REPLICA, without touching MySQL. Not during a migration. | NEW-6 |
| 2fca566 | `StaleTopoPrimary` only fixes the tablet type of a listed voter under a GR policy. | NEW-3 |
| a46a462 | A voter that `GroupMemberNotOnline` applies to is not analyzed for `ClusterHasNoPrimary`/`PrimaryTabletDeleted`, which outranked it while the group had no legitimate primary. | found in the re-run: 71s without a primary |
| d744494 | The sync loop waits up to 1 min for its joins (a `START` whose client gave up keeps running in MySQL); any join stops a `START` in progress first; a joiner contacts the members just seen active first. | found in the re-run: errno 3724 for 37s |
| 4c6a7c2 | A promotion does not wait for voters that do not answer once the others establish the majority; voter `server_uuid`s are learned in the background. | found in the re-run: S2 promotion 2.4s after the election instead of 0.6s |
| 314ce5f | `--group-replication-autorejoin-tries` defaults to 0: the tablet's gated rejoin replaces MySQL's auto-rejoin, which blocked a member for 60s (status queries hung) and does not check that the legitimate group is active. | found in the re-run |
| 4bd7d21 | VTOrc saves the incarnation it recorded after a bootstrap in its own copy of the shard record. | found in the re-run: 21s until the next refresh |
| 63e122a | A reparent's `SetReplicationSource` (with a reparent journal entry) on a member that is still the group primary waits until the group moved the primary away; VTOrc's (without) is still refused (cb582df). | `TestGroupReplicationLifecycle`'s PRS failed on the base of these fixes |

**The MySQL mechanism of NEW-1** was observed several times during the re-runs, with auto-rejoin off: a join (`START GROUP_REPLICATION`, not a bootstrap) that is in progress when the group it joins loses its majority ends with the joiner alone in a new incarnation, ONLINE and PRIMARY. For example (final S7d run 1), VTOrc's `StartGroupReplication` on zone3 at 18:49:20, the group {zone2, zone1} lost its majority at 18:49:22, and at 18:49:39 zone3 reported view `…:1` with itself only. The tablet detected the foreign incarnation within a second and left the group; `STOP GROUP_REPLICATION` on such a member can take up to 30s while its communication engine exits. No component followed such a group in any run.

### Results

Same harness and setup as above, GR mode, binaries of 4bd7d21 for every run below (runs on intermediate commits led to the fixes a46a462 to 4bd7d21 and are not listed; 63e122a only changes the planned reparent path, which these scenarios do not use). "Lost" is acked writes missing on the final primary; "gap" is the report's longest interval between acknowledged writes; "converged" is the harness's convergence check after the writers stop.

| Scenario | Before (audit above) | After |
|---|---|---|
| S7d ×4 | run 1: 652/1330 lost; r2: 637/2608 lost; ar0: 664/3391 lost, 7 violations (errant GTIDs, not converged); r1: 0 lost, all members OFFLINE for >2 min | 0/3116, 0/3276, 0/1060, 0/3036 lost; 0 violations; converged in all 4 (19s after the writers stopped in run 3, before that in the others). Gaps 47.9s, 24.6s, see below, 48.1s |
| S2 ×3 | old primary wedged in ERROR in 3/3 runs, not converged; gap 9.05s; 0 lost | 0/3724, 0/3800, 0/3788 lost; gap 9.05s ×3; converged (all three ONLINE) in 3/3; 1 violation per run, see below |
| G11 | P committed 1264 acked writes alone for 13s; writes back 68.9s after P restarted; gap 153.1s; 0 lost | P stops serving 1.8s after R1's shutdown; writes back 13.4s after P restarted; gap 108.9s (P is down for 84s by design); 0/3680 lost; 0 violations |
| G11k | writes back 66.7s after P restarted; gap 170.0s; 0 lost | writes back 13.3s after P restarted; gap 115.6s; 0/3546 lost; 0 violations |
| S1 | gap 7.57s, converged 5.2s, 0 lost | gap 7.52s, converged 5.7s, 0/2140 lost, 0 violations |
| S3 | gap 9.04s, 0 lost, 1 violation (one sample of two writable primaries) | gap 9.04s, 0/3828 lost, 0 violations |

**S7d availability.** After the second isolation, the group has two members (the isolated first primary could not rejoin within the 5s heal) and loses its majority when its primary is isolated: both members end in ERROR, and writes stop until the group is bootstrapped again once every voter is reachable. That took 44.9s, 21.5s and 45.1s in runs 1, 2 and 4 (the flapping continues meanwhile), and in run 3 no write succeeded from +42s to the end of the writes (+108s): the group was re-formed at +72s and lost its majority to the next isolation 0.3s later. Before the fixes this looked better only because Vitess followed groups of one, which lost the acknowledged writes. The report's "longest write gap" did not count an outage that lasts until the writers stop, hence 9.0s for run 3 (fixed since, see "S7d availability" below).

**S2 violation.** Both mysqld and vttablet of the old primary are frozen. For 0 to 200ms after SIGCONT its vttablet still reports PRIMARY and its mysqld is writable, until the tablet sees that its member was expelled; MySQL keeps `read_only=OFF` for 3s more (the tablet is REPLICA by then). The same 1-sample violation was reported before the fixes; the resumed member cannot commit (no majority), and vtgate follows the newer primary term.

**Still open.** NEW-4 (a group primary whose cell topo is down cannot be promoted). Joins in progress while a group loses its majority still create groups of one in MySQL; Vitess leaves them, but the member then needs a new join. Recovery after a total loss of the majority is bounded by MySQL: members stay in ERROR, or answer no status query, for up to a minute while their communication engine exits, and a bootstrap needs every voter readable.

The end-to-end tests `TestGroupReplicationLifecycle` and `TestGroupReplicationOneVoterPerCell` (`go/test/endtoend/reparent/grouprepl`) pass at 63e122a. On the base of these fixes (85b8695), and at 4bd7d21, the planned reparent step of `TestGroupReplicationLifecycle` failed: the old primary refused PRS's `SetReplicationSource` 8ms after its demotion because its member was still the group primary (cb582df); 63e122a fixes it.

## S7d availability

After the fixes above, S7d (the current primary isolated 11s and healed 5s, four times) lost no acknowledged write, but writes stopped for 21–69s once the group lost its majority. This section breaks those outages down, from the chaos reports, the event logs, and the MySQL, vttablet and VTOrc logs of the runs above and of new runs, and fixes the part that Vitess added.

### Metric

The report's "longest write gap" only measured intervals between two acknowledged writes, so an outage that lasted until the writers stopped did not count (run 3 reported 9.0s while no write succeeded for 66s). It now also measures the last acknowledged write to the time the writers stopped, and reports the total time without an acknowledged write, as the sum of the outages of at least 1s (ec8c0ef). The check of writes acknowledged by the deposed primary counted, as a violation, the writes of an old primary that the group elected again during the ~200ms before the shard record named it; it now counts the old primary as back from its vttablet's last non-PRIMARY sample before it reported PRIMARY again (2d33a57).

### Semi-sync baseline (this environment)

Two S7d runs with the default `cross_cell` semi-sync: every isolation stops writes for the whole 11s (the ERS completes when the partition heals), 45.1s and 45.9s without an acknowledged write in total, longest gap 11.3s and 12.1s, 0 acknowledged writes lost, but 4 violations per run: two samples of two writable PRIMARY tablets, 20 and 16 writes acknowledged by the deposed primary, and the old primary left with `super_read_only=0` (not converged).

### Where the time went (before)

Run 1 of the final runs above, fault at 18:55:18.7 (times relative to it):

| Time | What happened | Who | Wait |
|---|---|---|---|
| +0.0 | zone1, the primary, isolated. | harness | |
| +5.8 to +9.0 | zone1 loses the majority and leaves after 1s (`unreachable_majority_timeout`); zone2 is elected, its tablet promotes itself at +7.6, writes resume at +9.0. | GR, sync loop | GR's failure detection (unavoidable) |
| +11.0 | Heal. | | |
| +11.9 to +14.8 | VTOrc `GroupMemberNotOnline` runs `StartGroupReplication` on zone1. Its `STOP GROUP_REPLICATION` waits 2.9s: MySQL's own leave, started at +6.8, is still in progress ("Skipping leave operation: concurrent attempt to leave the group is on-going"). | VTOrc, MySQL | MySQL leave |
| +14.8 | `START GROUP_REPLICATION` on zone1. | tablet | |
| +16.1 | zone2, the primary, isolated 1.2s into zone1's join. The group {zone2, zone3} has no majority without zone1. | harness | cadence |
| +21.2 to +22.7 | zone3 and zone2 lose the majority and leave: writes stop. | GR | unavoidable once zone1 is not back |
| +24.2 | zone1's join ends with zone1 alone in a new incarnation. Its tablet leaves that group within 1s (the `STOP` takes 4.8s) and **suspends its own rejoins until VTOrc's `GroupMemberNotOnline`**. | MySQL, tablet | avoidable: suspension |
| +27.1 | Heal. zone3's and zone2's leaves complete at +27.3 and +30.3, zone1 leaves its foreign group at +30.4. | MySQL | MySQL leave |
| +32.2 to +43.2 | zone2 isolated again. It holds the latest acknowledged writes; nothing can be bootstrapped without it. | harness | unavoidable (bootstrap needs every voter) |
| +43.2 to +44.6 | Heal; VTOrc bootstraps zone2 0.3s later (done 1.4s after the heal). | VTOrc | |
| +44.6 to +48.2 | zone3's sync loop starts its join at +46.6 (join gate, 2s interval). zone1 is suspended, and VTOrc's `GroupMemberNotOnline` fails 13 times (two VTOrcs) with "no primary tablet found": the group only gets a primary tablet once a majority of voters is in it. | tablet, VTOrc | avoidable: no primary tablet, rejoin gate |
| +48.2 to +59.3 | zone2 isolated again, alone in its group; zone3's join stalls. | harness | |
| +59.3 to +63.7 | Heal; zone3's pending join completes after 4.0s (MySQL retries its connections), zone2 is promoted, writes resume at +63.7. VTOrc can only make zone1 rejoin now (+63.9, in the group at +65.9). | MySQL, sync loop | MySQL join retry |

The other runs show the same pattern, and two more avoidable delays:

- Run 3: VTOrc's `GroupMemberNotOnline` checked that the group was active, then waited 9.2s for the `FullStatus` of an isolated tablet before it started the join. The group had lost its majority meanwhile; the join ended alone in a new incarnation, which counts as an active member and kept VTOrc from bootstrapping the group for 20s (bootstrap at +89.3 instead of about +71).
- In new runs with some fixes: VTOrc bootstrapped the group on the old primary while its tablet was still PRIMARY (its sync loop was blocked for 15s on the unreachable topology), so the bootstrap made MySQL writable on a PRIMARY tablet, which acknowledged writes for 2.3s with a single voter in its group (none were lost; the member survived). A later run showed a race of the same kind: the sync loop lifted the not-serving state that the bootstrap had set, because MySQL was not a group primary yet, and one write was acknowledged before its next run. Separately, in run 2, VTOrc's `PrimaryIsReadOnly` recovery called `UndoDemotePrimary` on a PRIMARY tablet whose MySQL was in the ERROR state, which cleared `super_read_only` on it (Group Replication's `before_commit` hook still refused the commits).

What MySQL 8.4.11 contributes, measured over these runs:

- A member that leaves after `unreachable_majority_timeout` takes 5–27s (typically 8s) to complete its leave, not before the partition heals: 0.2–4s after it in most cases, up to 22s. A `STOP GROUP_REPLICATION` meanwhile waits for it, and its status queries can time out.
- A join takes 1.5–2s to be admitted and 2–3s to be ONLINE. The 5s heal windows of S7d leave little room once the leave (2–4s after the heal) is added.
- A join in progress when the group loses its majority either ends with the joiner alone in a new incarnation 9–20s later, or stays blocked for up to a minute (54s in one run), during which `STOP` fails with "Another instance of START/STOP GROUP_REPLICATION command is executing" (errno 3663). Leaving such a foreign group takes 4–6s.
- Adding a member stalls commits for about 2.5s.

### Fixes

| Commit | Change |
|---|---|
| 62882d9 | A tablet that leaves a foreign group no longer suspends its rejoins: its sync loop rejoins the legitimate group once another tablet reports it active, like any voter (the join gate of 67e4c28 made the suspension redundant). |
| 419bae0 | VTOrc's `GroupMemberNotOnline` runs while the shard has no primary tablet, like `PromoteGroupPrimary` and `UpdateGroupReplicationVoters`. |
| d6a5d08 | VTOrc's check before a join returns as soon as the answers settle it, and waits at most 2s for the other tablets. After a bootstrap, VTOrc makes the other voters join the new group right away, in the background. |
| 40305e2 | A PRIMARY tablet stops serving before it bootstraps a group under a group replication policy with several voters, and serves again once a majority of the voters is ONLINE (the sync loop now reads the not-serving reason from the tablet state). `UndoDemotePrimary` requires the primary of the legitimate group under such a policy. |
| 0407786 | The sync loop leaves the not-serving reason alone while the tablet is PRIMARY and its MySQL is not a group primary (the race above). |

Each change has a unit test that fails without it.

Evaluated and not changed:

- `--group-replication-unreachable-majority-timeout=0`, which keeps a member without majority in its group instead of leaving (so a group of two that loses its majority resumes on its own when the partition heals). One S7d run: 47.4s without acknowledged writes, longest gap 19.5s, and 3 violations: an isolated primary stays writable (`super_read_only=OFF`, commits block) while its tablet cannot reach the topology to demote itself. The default stays 1s.
- Aborting a join in progress when its group loses its majority: MySQL refuses the `STOP` while such a `START` runs (errno 3663).
- Bootstrapping while a voter is unreachable or its status unreadable: that voter may hold acknowledged writes the others lack.

### Results

GR mode, binaries of 0407786 (the harness of 2d33a57), same setup as above. The "before" numbers of the final runs 1–4 of the previous section are estimated from their event logs (the start of an outage is its first failed write minus the 3s client timeout), since their reports predate the metric; the estimate matches the new metric within 2.5s on the runs below.

| S7d | Longest write gap | Total without an acknowledged write | Acked writes lost | Violations |
|---|---|---|---|---|
| Semi-sync `cross_cell` ×2 | 11.3s, 12.1s | 45.1s, 45.9s | 0, 0 | 4, 4 |
| GR before (final runs 1–4 above) | 47.9s, 24.5s, 69.2s (until the writers stopped), 48.1s | 56.9s, 53.9s, 78.2s, 57.1s | 0 | 0 |
| GR after ×4 | 9.1s, 31.9s, 51.6s, 9.8s | 38.8s, 52.6s, 60.7s, 36.9s | 0/4716, 0/3564, 0/2802, 0/4919 | 0 |

In runs 1 and 4 every isolation cost about 9s, like a single failover (plus a 2.5s commit stall when the last member rejoins). In runs 2 and 3, a join was in progress when the next isolation cut the group to a minority, and the long outage is MySQL's: leaves of 3–5s after each heal, a foreign group from the join, and the next isolation 1.7s after the members were free. In run 2 writes resumed 4.5s after the following heal (bootstrap 1.4s, joins 2s, promotion 1s). In run 3 the group was bootstrapped 3.5s after that heal, the joins VTOrc started right away were cut off by the next isolation 0.4s later, and they completed 7.3s after the last heal.

Regression checks (GR mode, same binaries): S2 gap 9.05s, 0/3816 lost, the known 1-sample violation of the resumed old primary; G11 gap 107.8s (P is down for 84s by design), writes back 9.2s after P restarted (13.4s before), 0/3676 lost, 0 violations; S1 gap 7.48s, 0/2256 lost, 0 violations; S3 gap 9.04s, 0/3720 lost, 0 violations. `TestGroupReplicationLifecycle` and `TestGroupReplicationOneVoterPerCell` pass.

**Still open.** The long outages left in S7d come from MySQL: slow leaves after `unreachable_majority_timeout`, and joins that a loss of majority turns into a group of one or blocks for up to a minute. VTOrc's detection of a bootstrappable group waits for a successful discovery of every voter, which took up to 3.5s after a heal because discoveries of an isolated tablet time out after 10s. The tablet's sync loop can be blocked for 15s by a topology call while its cell is cut off, which delays its reaction to the group (its demotion, or serving again).

## Not tested

- S11/S11b/S11c and S12/S13 as written: they use `SOURCE_DELAY`, `fixReplica` and the default channel, which GR members do not have. G11/G11k/G11s replace them.
- Builtin backups (same graceful leave as G11, for the whole backup), 5-member groups, clock skew, and `IncapacitatedPrimary` with a lossy link.
- The MySQL-level cause of NEW-1 and NEW-5 (MySQL 8.4.11 only).

## Reproducing

```
go/test/endtoend/vtorc/chaos/chaos_run.sh -test.run '^TestS7dFlappingPrimaryLong$' -test.v -test.timeout 30m   # semi-sync
CHAOS_DURABILITY=group_replication_cross_cell go/test/endtoend/vtorc/chaos/chaos_run.sh -test.run '^TestG11GracefulLeaveThenPrimaryDies$' -test.v -test.timeout 30m
```

`chaos_run.sh` must run as root; it builds the test binary, drops to `RUN_USER` (default `ubuntu`) with `CAP_NET_ADMIN`, and deletes the run's VTDATAROOT afterwards. `CHAOS_TABLET_EXTRA_ARGS` adds vttablet flags. Reports go to `/home/$RUN_USER/chaos-results/<scenario>/`.
