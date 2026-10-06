# Unplanned failover audit, repeated with MySQL Group Replication

The [failover audit](https://github.com/vitessio/vitess/blob/claude/vitess-failover-validation-nm4msw/doc/failover-audit/README.md) (branch `claude/vitess-failover-validation-nm4msw`) found unplanned-failover problems with cross-cell semi-sync and VTOrc. This document checks each finding against the Group Replication (GR) mode of this branch (`group_replication_cross_cell`, see `doc/design-docs/GroupReplication.md`), using the audit's chaos harness in a new GR mode.

Setup: 3 cells, one tablet per cell, one VTOrc per cell, a per-cell etcd plus a global etcd, vtgate in zone1, MySQL 8.4.11, 4 writers (40 ms interval) and a primary reader (100 ms) through vtgate. In GR mode the shard is created with `cross_cell` semi-sync, converted online with `vtctldclient MigrateReplicationMode --durability-policy group_replication_cross_cell`, and every scenario starts once all three voters are ONLINE (`member_expel_timeout=0`, `paxos_single_leader=ON`, `unreachable_majority_timeout=1s`, VTOrc failover grace 30s, default sync interval 1s).

Status legend: **not reproducible** (the failure mode cannot happen, with evidence), **reproduced**, **new variant** (the issue appears in a different form), **new** (GR-specific issue not in the audit).

## Summary table

"Gap" is the longest interval without an acknowledged write. Semi-sync numbers are from the audit (MySQL 8.0.46) unless marked "this env".

| Audit item | Semi-sync result (audit) | GR result | Evidence | Status |
|---|---|---|---|---|
| §2 relay-log discard on replica restart (S11, S11k) | 500 acked writes lost, ERS reports success | Relay-log discard still happens on a member (G11k: R1 held 401 certified, unapplied transactions `:1129-1529`; after `kill -9` and restart it reported `Received=""`, `Executed=:1-1128`). It cannot become silent loss: the group `{P,R1}` loses quorum, P leaves after ~12s, nothing is elected, and VTOrc only bootstraps a voter whose GTID set contains all others, only when every voter is reachable, and only once that voter has executed them all (see "Bootstrap candidate and superseded bootstrap intents"). | G11k: no failover in 80s with P's host down, 0/3272 acked writes lost, writes back 64.7s after P restarted | not reproducible (fails closed) |
| §2 via `fixReplica` (S11c) | 500 acked writes lost | Async analyses (`ReplicaSemiSyncMustBeSet`, `fixReplica`) are suppressed for active members; no default channel exists to repoint. But see NEW-3: VTOrc's `StaleTopoPrimary` still configures the default channel on a voter that is out of the group. | code: `go/vt/vtorc/inst/analysis_dao.go` (GR suppression); S7d logs | not reproducible (different issue: NEW-3) |
| §2 NEW RISK: graceful leave shrinks the group | n/a (semi-sync blocks with no acker) | **Confirmed.** After R2 was expelled, a clean `mysqlctl shutdown` of R1 left P as a group of one (`view=1/1`) that kept committing: 1264 acked writes in ~13s existed only on P (`:1533-2788`). P's host then died: no failover (VTOrc logged 46 failed `GroupNotBootstrapped` attempts), writes blocked 153s until P returned; bootstrap on P, 0 lost. The same shrink happens through the tablet's own rejoin (`STOP GROUP_REPLICATION` before `START`) and through `unreachable_majority_timeout` leaves (S7c: zone1 served as `view=1/1`). Since the serving invariant, the primary stops serving when its view shrinks; since the fence check, MySQL is also `super_read_only` within about 0.11s (12 single-voter writes, from 32–84 over 0.25–0.81s, see "Fence check"). | G11 report; S7c observer log `zone1 ... gr=ONLINE/PRIMARY view=1/1` | new variant (durability of one while a member is out; fails closed if the primary then dies) |
| §3A old primary rejoins with errant GTIDs / `super_read_only=OFF` (S3, S7d, V2) | S3 ×2, S7d, V2: errant GTIDs, SRO OFF | S3, V2, S4, S9i: minority primary cannot commit, `read_only` ON 6.8s after the fault, rejoined as ONLINE secondary, no errant GTIDs. **S7d (flapping, 11s isolated / 5s healed): acked writes lost in 3 of 4 runs** (652/1330, 637/2608, 664/3391), and the 4th ended with all three members OFFLINE for >2 min. | S7d reports and MySQL logs (NEW-1, NEW-2) | not reproducible for single faults; **new variant, worse, under flapping** |
| §3B failover blocked by an unreachable cell-local topo, lock-lease expiry (S9, S9i, S9b) | S9: 155s down, S9i: 133s, S9b: failover 58s after etcd returned | S9: gap 7.7s; S9i: 9.1s; S10: 7.7s. The group elects without topo and no shard lock is involved. But when the member the group elects has its own cell topo down, its tablet cannot become PRIMARY (the tablet record is written first): S9b gap 169.6s, G9b (deterministic) 104.6s, bounded only by when the test restored etcd. Since the NEW-4 fix VTOrc moves the group primary to a member whose cell topo answers: G9b gap 22.2s and 20.4s, S9b 23.1s. | S9b vttablet log: `failed to promote the tablet to PRIMARY error="context canceled updating tablet_type ... in the topo"` every 15s; G9b/S9b reports after the fix | fixed for S9/S9i (S9i regressed to 139s with the legitimacy check and is fixed again, 9.0s: see "S9i regression"); new variant for S9b (NEW-4), **fixed** (see "NEW-4 fix") |
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

**Fixed:** VTOrc moves the group primary to a voter whose cell topo answers (see "NEW-4 fix").

### NEW-5. A resumed old primary wedges in ERROR forever (S2, 3 of 3 runs)

After SIGSTOP/SIGCONT the old primary is expelled. Its mysqld then deadlocks: a Vitess status query on `performance_schema.replication_group_communication_information` (issued by `(*Conn).GroupReplicationStatus`, `go/mysql/group_replication.go:107-116`, only to fill `PaxosSingleLeader`) waits forever in `Gcs_xcom_proxy_impl::xcom_client_get_event_horizon` while holding the GCS lock that `Gcs_operations::leave` (auto-rejoin) needs (gdb stacks: thread `connection` in `get_write_concurrency`, thread `gr_rejoin` in `pthread_rwlock_wrlock`). `KILL` does not interrupt it. On the Vitess side the query runs without a context (`mysqlctl.(*Mysqld).GroupReplicationStatus`), so `DemotePrimary(force)` from VTOrc's `StaleTopoPrimary` hangs inside it holding the tablet action lock, and every later `StartGroupReplication` RPC times out (S2 autorejoin0 goroutine dump: `demotePrimary` → `GroupReplicationStatus` → `group_replication.go:108`). With `--group-replication-autorejoin-tries 0` it still wedged. In one run VTOrc then dropped the wedged voter from the shard record, leaving 2 voters. Recovery needs a mysqld restart.

Suggested fix: do not read `replication_group_communication_information` on every status poll (read it once after a successful join/bootstrap and cache it, or report the configured `group_replication_paxos_single_leader`), and run the status queries with the caller's context so a hung query cannot hold the action lock.

### NEW-6. Old primary vttablet keeps type PRIMARY while its mysqld is down or OFFLINE

`endPrimaryTerm` returns early when the GR status cannot be read (mysqld down), and after a restart the member is OFFLINE, so the tablet stays PRIMARY (S1: ~30s, until VTOrc rejoined it; S7/S7b: >100s). Its own rejoin loop refuses to rejoin a PRIMARY tablet (`group_replication_sync.go:357`), so rejoin depends on VTOrc. Combined with the vtgate healthcheck gap (§3E) this produced the stale reads in S7/S7b.

### NEW-7. Other observations

- `vtctldclient StopReplication` on a voter is undone by VTOrc `GroupMemberNotOnline` within seconds (G11s), unlike the tablet's own rejoin, which honours it.
- `StartGroupReplication(bootstrap)` left `group_replication_bootstrap_group=ON` when `START` outlived the caller's context: the reset reused the expired context. A later join would bootstrap a second group. **Fixed** (a49bfc9, with a unit test).
- `TestStopGroupReplicationRestoresReadWriteOnPrimary` was flaky early on this branch (2/40 runs). It no longer fails: 0 of 2,900 runs, 2,400 of them on 8 loaded parallel processes. `TestWaitForDBAGrants` fails in this environment.
- `TestPausedPrimaryKeepsTheServingInvariant` failed in about 1% of runs under load. This came from a race in the tablet, not the test. `legitimateGroup` built the group from the voter server_uuids it knew, then looked for the voters it could not identify. The sync loop's background fetch (`warmVoterServerUUIDs`) could learn them in between: nothing was missing any more, so it returned the group built before, which lacked those voters. A primary with its voter majority could then stop serving for "lost the majority of its voters" until the next sync run, and a promotion could be refused once the same way. **Fixed**: the group is built again when no voter is missing. 0 of 4,800 runs fail under the load that failed 22 of 2,400 before.
- `go test -race` reports a race in `TestGetDetectionAnalysisGroupReplication`. It comes from a bug in `go/viperutil` that is the same on `main`, not from this branch: see "Outside Group Replication: config reads race with config writes".
- S8 does not exercise its fault in GR mode: VTOrc never starts an ERS for `DeadPrimary` within 60s (the group elects first).

## Fixes in this branch

| Commit | Change |
|---|---|
| a49bfc9 | `mysqlctl.StartGroupReplication`: reset `group_replication_bootstrap_group` with a fresh context and connection after a failed bootstrap. Unit test fails without the fix. |
| cb582df | `setReplicationSourceLocked`: refuse (`FAILED_PRECONDITION`) when MySQL is the primary of a group with quorum, before changing the tablet type. Unit test fails without the fix; G3D in GR mode goes from 64 failed writes to 0. |

NEW-1 to NEW-6 are addressed by the design changes below; NEW-4 (a group primary whose cell topo is down) by the last of them (see "NEW-4 fix").

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

**Still open.** Joins in progress while a group loses its majority still create groups of one in MySQL; Vitess leaves them, but the member then needs a new join. Recovery after a total loss of the majority is bounded by MySQL: members stay in ERROR, or answer no status query, for up to a minute while their communication engine exits, and a bootstrap needs every voter readable.

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

- `--group-replication-unreachable-majority-timeout=0` (a flag at the time; the setting is now fixed at 1s), which keeps a member without majority in its group instead of leaving (so a group of two that loses its majority resumes on its own when the partition heals). One S7d run: 47.4s without acknowledged writes, longest gap 19.5s, and 3 violations: an isolated primary stays writable (`super_read_only=OFF`, commits block) while its tablet cannot reach the topology to demote itself. The default stays 1s.
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

**Still open.** The long outages left in S7d come from MySQL: slow leaves after `unreachable_majority_timeout`, and joins that a loss of majority turns into a group of one or blocks for up to a minute. VTOrc's detection of a bootstrappable group waits for a successful discovery of every voter, which took up to 3.5s after a heal because discoveries of an isolated tablet time out after 10s. The tablet's sync loop can be blocked for 15s by a topology call while its own cell is cut off, which delays its reaction to the group (its demotion, or serving again); reads of the tablet records of another cell that is cut off no longer block it (see "S9i regression").

## S9i regression: a partitioned cell's topo blocked the promotion

S9i partitions the primary's cell, including its cell topo server, from the other cells. The audit measured a 9.1s gap. Re-run on the binaries of 6aa6c59, it took 139.7s and 138.9s (READ_ONLY) and 140.0s and 139.6s (OFFLINE_MODE): the group elected a member in another cell 7s after the fault, but no tablet became PRIMARY until the partition healed. 0 acked writes lost, 0 violations.

**Cause.** The legitimacy check (d410495) reads the shard's tablet records, to find the voters in the member's view by their MySQL address, with `GetTabletMapForShard` under the sync-loop step's 15s deadline. `GetShardReplication` of the partitioned cell did not answer until that deadline, and the reads of the tablet records of the other cells, which follow it with the same context, then failed with `context deadline exceeded`. The elected member found 0 of 3 voters in its view (`not the primary of the shard's legitimate group ... online_voters=0`), every step. VTOrc's `PromoteGroupPrimary` failed the same way (`Failed to recover: partial result: zone3`, after 15s under the shard lock). The audit's run predates the legitimacy check.

**Fix (627f1f0).** `topo.GetTabletMapForShardWithCellTimeout` reads each cell with its own deadline (2s) and returns the tablets of the cells that answer, with a partial result. The tablet uses it for the group record and the join seeds, identifies a voter without a tablet record by its known `server_uuid`, and reuses the tablet records it read within 30s while they identify every voter, so the promotion does not wait for the partitioned cell. VTOrc's `PromoteGroupPrimary` and its join check use it too and accept a partial result, and VTOrc identifies a voter without a tablet record by the `server_uuid` it last discovered. Unit tests reproduce the partition with a memory topo cell that does not answer, and fail without the fix.

| S9i, READ_ONLY | Longest write gap | Topo primary changed | Acked writes lost |
|---|---|---|---|
| Audit (before the legitimacy check) | 9.1s | | 0 |
| 6aa6c59 ×2 | 139.7s, 138.9s | +139.5s, +138.7s | 0/2060, 0/2016 |
| Per-cell deadline only ×2 | 10.4s, 10.3s | +10.5s, +9.5s | 0/3936, 0/3780 |
| 627f1f0 (with the reuse of the tablet records) ×2 | 9.04s, 9.05s | +7.0s, +7.0s | 0/3652, 0/3729 |

## NEW-4 fix: moving the group primary out of a cell whose topo is down

G9b kills the etcd of the cell of the member the group will elect, then the primary's mysqld; S9b kills the etcd of a replica's cell, and the group elects that replica in about half of the runs (by `server_uuid`). The elected member's tablet cannot write its own record, so it cannot become PRIMARY, and VTOrc's `PromoteGroupPrimary` went through the same tablet.

**Fix.** `PromoteGroupPrimary` already reads the shard's tablets cell by cell with a 2s deadline each; `topo.GetTabletMapAndFailedCellsForShard` now also names the cells that did not answer. When the group primary's cell is one of them, VTOrc, under the shard lock:

1. re-reads the status of the tablets of the cells that answered, and requires the members of the shard's legitimate group among them (recorded incarnation, a majority of the voters ONLINE in their view) to agree that the analyzed member is still their primary, and its tablet not to be PRIMARY (in the shard record, or in its own state if its vttablet answers within 2s; the move does not depend on it otherwise);
2. waits until the member has not been the topology primary for 5s since VTOrc first saw `GroupPrimaryNotInTopo`: a tablet promotes itself within a sync interval, the per-cell read already waited 2s, and a brief topo unavailability (an etcd leader election) does not move the primary;
3. picks an ONLINE voter of that view in a cell that answered, allowed by the durability policy (the rules of PRS's GR path), with the highest member weight, then the lowest alias, and calls `PromoteReplica` on it: `group_replication_set_as_primary`, then the tablet type. It writes the reparent journal. No new RPC.

A member whose cell answers is never moved away from. A member that the primary was moved away from is neither moved to nor away from again for a minute. Without an eligible target (or within that minute), VTOrc logs why and skips that tablet's recovery for 10s (`GroupPrimaryMoveBackoff`). The GR recoveries of a single tablet also refresh the tablet records cell by cell, keeping those of the cells that do not answer: the refresh used to wait 15s for the cut-off cell and then refresh nothing.

The stuck tablet's own promotion can still write PRIMARY to its record if its etcd comes back during the attempt. Its term is taken right after it checked that its MySQL is the group primary (`changeTypeLocked`), so before the move took effect and older than the new primary's term; making MySQL writable is then refused on a group secondary, so the tablet stays REPLICA; and `StaleTopoPrimary` fixes the record. A tablet that became PRIMARY just before the move demotes itself in its reconcile loop. In the runs below the stuck attempt timed out after the move, and no other attempt started.

Unit tests (`go/vt/vtorc/logic/group_replication_move_test.go`) cut the cell off with an unreachable memory topo cell: the move, no move when the cell answers, without an eligible target, for a foreign incarnation or a minority view, when the tablet already runs as PRIMARY, when the group elected another primary meanwhile, within the hold period (both directions), and the grace period; target selection; and the refresh.

Results (GR mode, binaries of this change):

| Scenario | Before | After | Group elected | Shard primary changed | Acked writes lost | Violations |
|---|---|---|---|---|---|---|
| G9b run 1 | gap 104.6s (after etcd was restored) | gap 22.2s | +6.7s | +22.2s (move at +21.1s) | 0/3784 | 0 |
| G9b run 2 | | gap 20.4s | +6.7s | +20.5s (move at +20.4s) | 0/3776 | 0 |
| S9b, elected member in the cut-off cell | gap 169.6s | gap 23.1s | | +23.3s (move at +23.0s) | 0/3804 | 0 |
| S9b, elected member in another cell | | gap 7.1s, no move | | +7.0s | 0/3825 | 0 |
| S1 | gap 6.95s | gap 7.64s | | +7.5s | 0/2228 | 0 |
| S3 | gap 9.05s | gap 9.05s | | +6.75s | 0/3640 | 0 |

Where the time goes (G9b run 1, from the primary's death): election +6.7s; VTOrc discovers the new group primary at +8.0s but detects `GroupPrimaryNotInTopo` only at +17.0s, because while a cell's topo hangs every topology refresh blocks VTOrc's main loop for `--topo-information-refresh-duration` (3s here), so analysis runs every 3s; the recovery's refresh and the per-cell read take 2s each; `group_replication_set_as_primary` and the type change 1.1s. `TestGroupReplicationLifecycle` and `TestGroupReplicationOneVoterPerCell` pass.

**Still open.** The detection delay above. A move needs VTOrc to have discovered the elected member through its vttablet; otherwise the ERS fallback applies after `--group-replication-failover-grace-period`. Under an asymmetric partition, VTOrcs of different cells may disagree on which cell answers; the hold against moving back is per VTOrc.

## Exit state action

`group_replication_exit_state_action` decides what MySQL does to a member that leaves its group involuntarily (`unreachable_majority_timeout`, an expulsion, an applier error). `READ_ONLY` (Vitess's default) sets `super_read_only`; `OFFLINE_MODE` (MySQL 8.4's default) also sets `offline_mode`, which disconnects and refuses every user without `CONNECTION_ADMIN` or `SUPER`: vttablet's app, allprivs and filtered users, and, in the runs below, which used the XCom communication stack, the replication user. Since the MySQL communication stack (see below), the replication user has `CONNECTION_ADMIN` and keeps its connections. MySQL never clears `offline_mode` itself, not even when the member rejoins or becomes the primary; since 35adb1d the tablet clears it once the member is ONLINE in the shard's legitimate group with a majority of the voters in its view, when it bootstraps the group, and when it makes MySQL the writable primary or an asynchronous replica.

The harness selects the action with `CHAOS_GR_EXIT_STATE_ACTION` (6aa6c59) and counts the primary reads, through vtgate, that the old primary answered after the fault, after another member became the primary of a majority view ("stale"), and after the topo primary changed. Runs 1–2 of each scenario used the binaries of 6aa6c59, the others those of 627f1f0 (the S9i fix; S9i itself is above).

| Scenario | Metric | READ_ONLY | OFFLINE_MODE |
|---|---|---|---|
| G3E (primary isolated, still reachable by vtgate) ×2 | reads answered by the old primary / stale | 76 / 6, 78 / 7 (last one 0.6s after the election) | 69 / 0, 69 / 0 |
| | gap | 7.71s, 7.74s | 7.85s, 7.83s |
| S3 (primary isolated) ×2 | stale reads; gap | 0, 0; 9.05s, 9.05s | 0, 0; 9.05s, 9.05s |
| S1 (primary mysqld killed) | gap | 7.48s (earlier run) | 7.88s |
| S2 (primary frozen) | gap | 9.05s (earlier run) | 9.05s |
| S7d (flapping, 11s isolated / 5s healed) ×4 | longest gap | 9.15s, 11.33s, 10.54s, 22.92s | 42.72s, 48.96s, 46.04s, 68.69s (until the writers stopped) |
| | total without an acked write | 41.5s, 42.1s, 38.7s, 50.7s | 62.5s, 58.0s, 55.1s, 77.7s |
| | stale reads | 0, 0, 1 (also after the topo primary changed), 0 | 0, 0, 0, 0 |

Every run: 0 acked writes lost, 0 violations. The earlier READ_ONLY S7d runs on 0407786 (see "S7d availability") had gaps of 9.1s, 31.9s, 51.6s and 9.8s.

**Read fencing.** Under READ_ONLY, an old primary that its group left behind keeps answering reads until vtgate follows the new primary term: in G3E, reads served by a member that was no longer in the group while another member was already the primary. Under OFFLINE_MODE, MySQL refuses the tablet's connections the moment the member leaves, before the majority elects a new primary: no stale read in any run.

**S7d.** Under OFFLINE_MODE, every run lost the group's majority at the second or third isolation; under READ_ONLY, 3 of 8 runs did. In S7d the member healed after 11s must rejoin before the next isolation 5s later, or the group of two left behind loses its majority when its primary is isolated, and writes stop until a bootstrap. MySQL finishes the member's leave (`unreachable_majority_timeout`) 3.1–4.0s after the heal; the tablet starts its join 0.01s after that. Whether the join is admitted before the next isolation depends on how early it starts: under READ_ONLY it started 1.37–1.97s before the isolation and 11 of 12 joins landed (the one that did not started 1.39s before it and ended in a group of one); under OFFLINE_MODE it started 1.18–1.45s before it and 1 of 5 landed, because the leave finished later (3.59–3.85s after the heal in S7d, against 3.07–3.67s under READ_ONLY). Across all scenarios the leave took 3.26–3.99s under OFFLINE_MODE (9 leaves) and 3.07–3.97s under READ_ONLY (20 leaves), so the shift is small, and it comes from MySQL: no Vitess step is on that path, and `offline_mode` never refused a recovery donor or a join in these runs. It still decides S7d, whose 5s heal window sits right at the edge.

**Decision: the default stays READ_ONLY.** OFFLINE_MODE fences reads on a member that left its group, which READ_ONLY does not, and costs nothing in the single-fault scenarios. But S7d's availability regressed in 4 of 4 runs (and 4 of 4 is unlikely by chance alone given 3 of 8 under READ_ONLY, p≈0.07), and the default must not trade availability for it. OFFLINE_MODE is supported (`--group-replication-exit-state-action=OFFLINE_MODE`, with the tablet clearing `offline_mode`), and recommended where stale reads from a partitioned old primary matter more than recovery under repeated partitions. Revisit when the leave after a heal is faster, or when the tablet can fence reads itself on a member that left its group.

Regression checks on the final binaries (627f1f0 and the documentation commit after it; the default READ_ONLY): S1 gap 7.65s, 0/2144 lost, 0 violations; S2 gap 9.05s, 0/3780 lost, the known 1-sample violation of the resumed old primary; S3 gap 9.04s, 0/3828 lost, 0 violations; G3E gap 7.60s, 0/4940 lost, 75 reads answered by the old primary, 5 of them stale, 0 violations. `TestGroupReplicationLifecycle` and `TestGroupReplicationOneVoterPerCell` pass.

## MySQL communication stack

Since this change Vitess always runs groups on `group_replication_communication_stack=MYSQL`: members connect through the MySQL port of their tablet records, as the replication user, whose credentials the tablet stores on the `group_replication_recovery` channel. The `--group-replication-port` flag and the `gr` port in the tablet record are gone; `--enable-group-replication` enables GR support, and `FullStatus` reports it. The replication user needs `GROUP_REPLICATION_STREAM` and `CONNECTION_ADMIN`, which `config/init_db.sql` grants and the tablet checks before a join. Every run above used XCom, on a separate port.

The chaos harness's partitions already cut the MySQL port, so they now cut the group's traffic with no port of its own. Results on the MySQL stack, GR mode, the default READ_ONLY:

| Scenario | Failover | Gap | Lost | Violations |
|---|---|---|---|---|
| S1 (primary mysqld killed) | 6.7s | 6.95s | 0/2217 | 0 |
| S2 (primary frozen) | 7.1s | 9.07s | 0/3734 | the known violation of the resumed old primary (2 samples, 200ms; the last XCom run: 1 sample) |
| S3 (primary isolated) | 7.7s | 9.05s | 0/3732 | 0 |
| S9i (primary's cell partitioned) | 6.9s | 9.05s | 0/3731 | 0 |
| G3E (primary isolated, still reachable by vtgate) | 6.9s | 7.10s | 0/4878 | 0; 69 reads answered by the old primary, 2 of them stale (XCom: 75 and 5) |

`TestGroupReplicationLifecycle` (migration from semi-sync and back, planned reparent, and a killed primary, which the group replaced in 7.1s) and `TestGroupReplicationOneVoterPerCell` pass. Failover and gaps match the XCom runs: the election is bounded by the same 5s failure detection, whichever stack carries the messages.

The exit state action comparison, repeated on the MySQL stack (binaries of 2b00fc5, runs interleaved; READ_ONLY's G3E, S3 and S1 are the runs above):

| Scenario | Metric | READ_ONLY | OFFLINE_MODE |
|---|---|---|---|
| S7d (flapping, 11s isolated / 5s healed) ×4 | longest gap | 68.58s, 68.91s, 68.88s (all three until the writers stopped), 46.92s | 31.18s, 46.18s, 9.54s, 38.76s |
| | total without an acked write | 77.6s, 78.0s, 77.9s, 56.0s | 60.5s, 55.2s, 39.0s, 47.8s |
| | group lost its majority at isolation | 2nd, 2nd and 4th, 2nd, 2nd | 4th, 2nd, never, 2nd |
| | acked writes lost | 0/1128, 0/1092, 0/1076, 0/3296 | 0/2601, 0/3408, 0/4665, 0/4060 |
| | stale reads | 0, 0, 0, 0 | 0, 0, 0, 0 |
| G3E (primary isolated, still reachable by vtgate) | reads answered by the old primary / stale; gap | 69 / 2; 7.10s | 68 / 0; 7.64s |
| S3 (primary isolated) | stale reads; gap | 0; 9.05s | 0; 9.06s |
| S1 (primary mysqld killed) | gap | 6.95s | 7.33s |

Every run: 0 violations, and all three members ONLINE at the end.

On XCom, OFFLINE_MODE lost S7d because MySQL finished the healed member's leave later (3.59–3.85s after the heal, against 3.07–3.67s), so its join started too close to the next isolation to land. On the MySQL stack the leave ends 3.12–3.79s after the heal under OFFLINE_MODE and 3.30–3.74s under READ_ONLY, and the tablet starts its join 0.02–0.04s later, 1.24–1.91s and 1.30–1.73s before the next isolation: OFFLINE_MODE no longer delays the leave. But fewer joins land: 5 of 12 up to each run's first loss of majority (OFFLINE_MODE 5 of 8, READ_ONLY 0 of 4), against 12 of 17 on XCom with similar leads, and the lead no longer decides it: joins started 1.90s and 1.91s before the isolation failed, one started 1.37s before landed. Every failed join logged `Failed to establish MySQL client connection` 4.3s, 5.4s, 7.5s and 10.6s after its START; 3 of the 5 that landed logged none. Joins that no isolation interrupted were admitted in 1.0–3.8s on both stacks. A failed join ended either alone in a new incarnation 13–17s after its START (4 runs), or, in READ_ONLY runs 1–3 only, in a view of the members that had already left: `No donor available`, ERROR, and a `STOP` that waited 60s for a view change (`timeout receiving a view change`); writes did not resume before the writers stopped.

**Decision: the default stays READ_ONLY.** S7d no longer separates the two actions: the failed joins were a race in MySQL that the exit state action does not take part in (see "Joins that a partition interrupts" below). With the 2s unreachable majority timeout chosen below, OFFLINE_MODE no longer avoids the stale primary reads either (G3E: 0, 3 and 4 per failover, against 6, 3 and 3 under READ_ONLY); only a 1s timeout made it do so, and 1s loses about half of the interrupted joins. OFFLINE_MODE's costs remain: it refuses vttablet's app, allprivs and filtered users on any member that leaves its group, even briefly, which stops replica reads and VReplication streams on it until it rejoins, and Vitess must clear the flag, which MySQL never does. Under READ_ONLY, replica reads from a member that left its group are bounded by the tablet's replication lag tracking, as for any replica; primary reads during the 0.6–2s before vtgate follows the new primary are not. OFFLINE_MODE stays supported.

### Joins that a partition interrupts

The S7d regression above is not caused by the communication stack. Rebuilt from 464e6a0 (XCom, `--group-replication-port`, with its own chaos test binary) and run in the same environment as HEAD, XCom loses S7d the same way: the group lost its majority at the second isolation in 4 of 4 XCom runs and 5 of 5 MySQL-stack READ_ONLY runs, with the same timings to 0.1s. Both symptoms are one MySQL race, decided by a fixed 1s timeout that Vitess sets. It is fixed by setting `group_replication_unreachable_majority_timeout` to 3s.

**How it was traced.** Every member ran with `loose-group_replication_communication_debug_options=GCS_DEBUG_ALL` and `log_error_verbosity=3` from an `EXTRA_MY_CNF` file: mysqlctl includes it in the generated my.cnf, and MySQL applies a `loose-` plugin option when vttablet installs the plugin at runtime. No harness or product change was needed. The `[GCS]` notes of the error log (`Connecting to`, `Connection to … failed`, `Node has not booted`, `Adding new node`, `installed new site definition`, `xcom_client_remove_node`) give XCom's steps with timestamps. `GCS_DEBUG_TRACE` has no timestamps except on state machine changes. A poll of `/proc/net/tcp` every 20ms showed the SYNs to the partitioned member (`ss`, `conntrack` and `tcpdump` are not installed).

**One heal cycle** (`/home/ubuntu/chaos-results-diag-ro3`, MySQL stack, READ_ONLY; zone2 joins, zone1 is isolated next, zone3 survives; times after the heal at 08:54:40.81):

| Time | Event | Log |
|---|---|---|
| +3.20 | MySQL finishes zone2's leave (started by `unreachable_majority_timeout` during the isolation). | mysql-zone2 `MY-011504` |
| +3.26 | The tablet starts the join; `add_node` is sent 0.23s later and decided in 0.1s. | mysql-zone2 `MY-013587` 08:54:44.07, `Sending add_node` 08:54:44.30 |
| +5.09 | zone1, the primary, isolated: 1.83s after the START. | events.txt |
| +6.58 | zone2 boots: a member's ping makes it request XCom's snapshot, 3.32s after its START. | `Node has not booted` 08:54:47.38 |
| +6.58 to +7.58 | zone2's XCom thread connects to zone1. The SYN is dropped, and the connection times out after 1s. | `Connecting to localhost:12115` 08:54:47.38, `Connection to localhost:12115 failed` 08:54:48.39 |
| +7.58 | zone2 connects to zone3 in 6ms. | `Connected to localhost:12121` 08:54:48.39 |
| +7.68 to +8.68, +9.78 to +10.78 | Next attempts to zone1, 0.1s and 1.1s after the previous failure, 1s each. | `Connection to localhost:12115 failed` 08:54:49.49, 08:54:51.59 |
| +9.68 | zone3 suspects zone1. zone2 is not in zone3's installed view yet, so zone3 has no majority; it asks XCom to expel zone1. | mysql-zone3 `MY-011496` 08:54:50.49, `xcom_client_remove_node` |
| +10.68 | `unreachable_majority_timeout` (1s): zone3 leaves the group. | mysql-zone3 `MY-011711` 08:54:51.49 |
| +10.78 | zone2's connection attempt returns, and XCom decides the expulsion of zone1 right after, 0.1s too late. | mysql-zone3 `installed new site definition` 08:54:51.59 |

The expulsion of the lost member needs the joiner's vote, and the joiner's XCom thread cannot vote while it waits in a connection attempt. XCom runs all its tasks on one thread, and `dial()` (`xcom_transport.cc`) connects with a 1000ms timeout through the network provider. The MySQL provider calls `mysql_real_connect` with that timeout (`gcs_mysql_network_provider.cc`), the XCom provider a `poll()` (`timed_connect_msec`): both block the thread for 1s when the SYN is dropped. `sender_task` retries after 0.1s, then 1.1s, 2.1s, 3.1s (`INITIAL_CONNECT_WAIT` 0.1s plus `CONNECT_WAIT_INCREASE` 1s per attempt). The survivor suspects the lost member 5s after the partition and leaves 1s later; with the joiner booted about 1.5s after the partition, that is 3.1s after the boot, just as the third attempt blocks the joiner for 1s. Of the 10 cycles lost with 1s in the runs below, 6 decided the expulsion 1.10s after the survivor's loss of majority, every time right when the joiner's attempt returned; 2 decided it in time (0.04s and 0.37s) but MySQL had not reported the restored majority when the timeout fired; in 2 the survivor did not even push it in time. In the cycles that were kept, MySQL reported the restored majority (`MY-011498`, or the new view) on the survivor's next one-second check, often exactly 1.00s after its loss of majority, in the same instant as the timeout. The older XCom runs won that coin flip in 11 of 12 READ_ONLY cycles, the earlier runs of 2b00fc5 in 5 of 12. The phase that decides it is set by XCom's one-second timers, so a small shift of timing between environments or builds flips the outcome. The connection attempts to the partitioned member happen on both stacks. Only the MySQL provider logs them (`MY-013780` at ERROR); XCom logs `Connection to … failed` at INFORMATION level.

**Symptom 1, the failed joins.** In 26 of the 33 traced cycles, on both stacks, the joiner booted 2.6–3.4s (mostly 3.3s) after its START, after the next isolation, whose lead is 1.2–1.9s; in the other 7, all second heal cycles, 1.1–1.6s after it. In a 3-member lab without Vitess or partitions, under write load, a member that stops and restarts Group Replication boots 0.5–1.9s after its `add_node`, also when it restarts right after its leave. What adds the 1–1.5s in S7d was not established; it is XCom's, on both stacks. The `Failed to establish MySQL client connection` errors 4.3, 5.4, 7.5 and 10.6s after the START are this boot (3.3s), plus the 1s connection timeout, and then the retries with their 0.1, 1.1 and 2.1s waits, each plus 1s. They are a symptom, not the cause: the joins that logged none booted before the next isolation. Seed order does not matter: the first seed accepted the `add_node` and the group decided it within 0.1s in every cycle, and the joiner then connects to every member of the configuration, in configuration order.

**Symptom 2, the dead view and the 60s `STOP`.** When the survivor leaves first, its leave (a removal of itself) is decided by the survivor and the joiner, which still form a majority of the 3-member XCom configuration. The joiner is then the only member left in the old group's configuration. Once the partition heals, GCS delivers the view of the old incarnation with the members that already left, removes them, and the joiner has no donor: `diag-ro1` zone1, 08:25:37.21, `Group membership changed to vm:11118, vm:11121, vm:11115 on view 17908430985243540:5`, `Members removed`, `No donor available to provide the certification information` (`MY-015084`), ERROR. MySQL's own leave after the error then waits for a view change that cannot come, for `VIEW_MODIFICATION_TIMEOUT`, hard-coded at 60s (`plugin_constants.h`, `leave_group_on_failure.cc`): `While leaving the group … timeout receiving a view change` at 08:26:37.21, 60.0s later. The tablet's `STOP GROUP_REPLICATION` at 08:25:38.24 waits for it and returns at 08:26:48.25. Meanwhile, the member's status queries do not return: the tablet kills them after 10s (`zone1-0000000100-vttablet-stderr.txt` from 08:25:41.96 on), and VTOrc cannot read this voter. It only detects `GroupNotBootstrapped` at 08:26:48.30, 0.05s after the leave and 37s after the last heal, which is why writes never resumed before the writers stopped. The same happens on XCom (`diag-xcom-ro4` zone1: ERROR at 09:02:03.31, timeout at 09:03:03.31), and in the older XCom OFFLINE_MODE runs 2 and 4, whose 49s and 69s gaps it explains. Vitess cannot shorten MySQL's wait; it can only avoid the race that leads to it.

**Fix.** Vitess raises `group_replication_unreachable_majority_timeout` from 1s (`mysql.GroupReplicationUnreachableMajorityTimeout`). This investigation used 3s; the sweep in "Unreachable majority timeout sweep" below found 2s as good and chose it. A longer timeout covers the joiner's 1s connection attempt, the 0.1s decision, and MySQL's next one-second check of the restored majority (1.65s after the loss of majority in every such cycle that the 3s timeout kept). It is a member setting, applied before each join, and it may differ between members, so mixed versions are compatible. `TestGroupReplicationUnreachableMajorityTimeoutOutlastsJoinerExpulsion` and `TestGroupReplicationCommands` fail with 1s. The runs in this subsection used 3s.

Cost: a partitioned member leaves 2s later. A partitioned primary cannot commit meanwhile, as before (MySQL blocks its transactions until a majority certifies them), but its clients wait 2s longer for the rollback, and it stays `super_read_only=OFF` 2s longer. In S3 (exploratory build), the old primary was `read_only=OFF` until +8.8s, its tablet still PRIMARY, while the new primary was writable from +7.1s, which the harness rightly does not count as two writable primaries, since the old one cannot commit. Under OFFLINE_MODE, MySQL sets `offline_mode` 2s later: G3E had 2 stale reads within 0.1s of the election (none with 1s), under READ_ONLY 8 (2–7). With 5s, G3E had 5 stale reads under OFFLINE_MODE, and S7d still lost its majority in 2 of 4 runs, so 3s it is.

What is left: in all 5 cycles that the 3s timeout lost, XCom expelled the lost member 1.10s after the survivor's loss of majority, but the survivor did not report a majority in the configuration of two that followed. It left 3s after the loss. In `diag-fix-om1` the joiner, alone, then formed a new incarnation after the heal (`Only one server alive`, view `17908465825544637:1`), which its tablet left 0.8s later (NEW-1). In all 5 the joiner had first connected to the survivor and then to the partitioned member; in the 4 kept cycles whose expulsion also came 1.10s late, it was the other way round. The XCom-level cause was not established. No dead view and no 60s `STOP` happened in any run with 3s.

Results (S7d, this environment; "lost" is the isolation at which the group lost its majority; 0 violations and 0 acknowledged writes lost in every run):

| S7d | Runs | Majority lost at | Longest gap | Total without an acked write | Dead view + 60s STOP |
|---|---|---|---|---|---|
| XCom (464e6a0), 1s, READ_ONLY | 4 | 2nd, 2nd and 4th, 2nd, 2nd | 36.9s, 36.0s, 32.6s, 69.2s* | 45.9s, 77.3s, 53.3s, 78.2s | 2 of 4 |
| MySQL stack (HEAD), 1s, READ_ONLY | 5 | 2nd in all 5 | 69.0s*, 54.5s, 68.8s*, 69.1s*, 69.0s* | 78.0s, 63.6s, 78.8s, 78.1s, 78.0s | 4 of 5 |
| MySQL stack (HEAD), 1s, OFFLINE_MODE | 1 | 3rd | 34.7s | 58.5s | 0 |
| 3s, READ_ONLY | 4 | 2nd, never, never, 2nd | 36.2s, 12.9s, 9.5s, 33.4s | 45.2s, 44.1s, 40.3s, 44.1s | 0 |
| 3s, OFFLINE_MODE | 4 | 2nd, never, 2nd, 2nd | 37.9s, 15.0s, 49.7s, 52.3s | 49.2s, 44.8s, 58.8s, 62.4s | 0 |
| 3s, exploratory build, READ_ONLY ×2, OFFLINE_MODE ×1 | 3 | never | 9.8s, 15.1s, 14.5s | 40.8s, 47.6s, 44.7s | 0 |

\* until the writers stopped. The 1s runs and the exploratory runs had GCS tracing on, the HEAD runs with 3s only the verbose error log. The exploratory build set the timeout from an environment variable; the code path is the same.

Across these runs, the 3s timeout kept 18 of the 23 heal cycles that a join followed, up to each run's first loss of majority (11 runs), against 1 of 11 with 1s (10 runs). On HEAD with 3s, the gaps were at most 52s and writes always resumed before the writers stopped: the losses no longer leave a member in a dead view, and VTOrc bootstraps the group after the heal.

Regression checks (3s, READ_ONLY): S1 gap 7.25s, 0/2237 lost, 0 violations; S3 gap 9.05s, 0/3678 lost, 0 violations; G3E (exploratory build) above, gaps 8.4s (READ_ONLY) and 8.5s (OFFLINE_MODE), 0 violations. `TestGroupReplicationLifecycle` and `TestGroupReplicationOneVoterPerCell` pass (one first attempt failed after 1.5s in `AddCellInfo`, before any Group Replication step, and passed when rerun).

## Unreachable majority timeout sweep

`group_replication_unreachable_majority_timeout` (umt) decides two things: how long a partitioned primary blocks its clients and stays writable, and whether a group survives the loss of a member while another one is joining (see "Joins that a partition interrupts"). Vitess sets it to 2s, after this sweep. Same binaries for every value (HEAD plus a test-only flag), 144 runs, all on MySQL 8.4.11; the harness's write probe (c861dbd) sends a transaction through vtgate every 500ms and waits up to 60s for it, and G12 (21628fe) restarts a secondary's mysqld, as in a rolling restart, then cuts off the primary's whole cell a set time after the joiner's `START GROUP_REPLICATION`, six cycles per run. 0 acked writes lost and 0 violations in every GR run.

**Cost, single faults** (3 runs per value; seconds after the fault):

| | G3E: probes on the old primary blocked until | G3E: old primary `super_read_only` OFF until | G3E: reads answered after the election, READ_ONLY / OFFLINE_MODE | S3: client-visible |
|---|---|---|---|---|
| semi-sync (2 runs) | 30.0 (vttablet timeout) | 46.2, after the heal | – | 18 probes failed after ≤10.0s |
| umt 1s | 5.8–6.3 | 6.4–6.8 | 13, 3, 9 / 0, 0, 0 | none failed: vtgate buffered them, committed on the new primary after 9.9–10.1s, at every umt |
| umt 2s | 6.9–7.3 | 7.4–7.8 | 6, 3, 3 / 4, 0, 3 | |
| umt 3s | 7.8–8.2 | 8.4–8.8 | 3, 1, 3 / 3, 3, 4 | |

Failover (6.4–7.2s to the group's election), the longest write gap (G3E 7.0–8.2s, S3 9.0–9.1s) and durability do not depend on umt. Probes routed to a partitioned GR primary all fail together, at the fault + 5s + umt + about 0.3s (errno 3100, or 1203 once blocked commits filled vttablet's transaction pool).

**Interrupted joins, G12** (READ_ONLY; majority kept / cycles; "in race" excludes cycles where the joiner was already in the view at the cut; a restarted joiner entered the view 1.3–1.45s after START):

| cut after START | umt 1s | umt 2s | umt 3s |
|---|---|---|---|
| 0.5s | 0/6 | 0/6 | 0/6 |
| 0.75s | 3/4 | 3/4 | 4/4 |
| 1.0s | 2/4 | 4/4 | 3/4 |
| 1.25s | 3/4 | 2/4 | 4/4 |
| 1.5s | 2/6 (0/4 in race) | 6/6 | 6/6 |
| 2.5s | 6/6 | 6/6 | 6/6 |
| in race, without 0.5s | 10/18 | 19/22 | 17/18 |

1s against 2s: p=0.04; 1s against 3s: p=0.018; 2s against 3s: p=0.61 (Fisher). A kept cycle cost 9.0–16.7s without writes, a lost one 27.9–47.8s; every lost cycle left the joiner in a one-member incarnation of its own, which Vitess did not follow. A cut 0.5s after START was lost at every umt: the joiner never entered a view before the heal.

**S7d** (READ_ONLY ×6 and OFFLINE_MODE ×4 per value): runs that lost the majority, 1s 9/10, 2s 2/10, 3s 2/10; interrupted joins kept, 1s 7/16, 2s 24/26, 3s 24/26 (1s against 2s p<0.01, 2s against 3s p=1). Longest gap of the runs that kept the majority 9.5–15.9s at 2s and 3s; of those that lost it 28.1–69.2s. "No donor available" dead views: 1s 4 runs, 2s 1, 3s 0.

**Decision: 2s.** It keeps as many interrupted joins as 3s, measurably more than 1s, and fences a partitioned primary 1s later than 1s rather than 2s. It costs the OFFLINE_MODE read fencing that only 1s gave, which the READ_ONLY default does not rely on. Evidence: `/home/ubuntu/chaos-umt/` (`summaries/` has the per-run tables).

## Replica reads with polling lag tracking

With the `READ_ONLY` exit state action, a member that leaves its group stays readable, so replica reads from it must be bounded by the tablet's replication lag tracking (design: "Replica reads and replication lag" in `doc/design-docs/GroupReplication.md`). In polling mode (`--enable-replication-reporter`) the poller only read `SHOW REPLICA STATUS FOR CHANNEL ''`, and a member has no default channel.

**Before** (44f9d49):
- Unit level: `poller.Status()` on a member (the server returns no row for `SHOW REPLICA STATUS FOR CHANNEL ''`) returned `no replication status (errno 100)`, which makes the tablet's health check go unhealthy.
- `TestGroupReplicationReplicaReadsWithPollingLag` with the 44f9d49 binaries: both secondaries logged `Going unhealthy due to replication error: no replication status (errno 100) (sqlstate HY000)`, and vtgate refused every `@replica` read for 90s with `no healthy tablet available for 'keyspace:"ks" shard:"0" tablet_type:REPLICA'`.
- The cluster framework passes `--enable-replication-reporter` to every tablet, so every GR chaos run above that did not set `CHAOS_VTTABLET_HEARTBEAT=1` had its secondaries NOT_SERVING. The harness only read from the primary, so nothing showed it.
- An applier-based lag alone does not fence a cut-off member. Raw MySQL 8.4.11, a group of two: with its primary frozen (SIGSTOP), the secondary stayed `ONLINE` in the same view (`17908682034375831:2`), with `COUNT_TRANSACTIONS_IN_QUEUE=0`, `COUNT_TRANSACTIONS_REMOTE_IN_APPLIER_QUEUE=0` and no worker applying. Only the peer's `UNREACHABLE` state showed that it received nothing.

**After.** The poller reads the member's group state with one query and takes the legitimacy verdict of the tablet's sync loop; a member that is not `ONLINE` with quorum in the shard's legitimate group reports the lag accumulated since it last was. `TestGroupReplicationReplicaReadsWithPollingLag` passes (both secondaries answer `@replica` reads), as do `TestGroupReplicationLifecycle` and `TestGroupReplicationOneVoterPerCell`.

G13 (new, harness only: `TestG13SecondaryCutOffFromGroupReplicaReads`) cuts off the secondary that answers replica reads from the other members, while vtgate, VTOrc and the topology servers still reach it, for 47s, and reads through vtgate `@replica` every 100ms. Each read returns the answering `server_uuid` and checks whether two acknowledged writes are visible: the last one acknowledged before the read, and the last one acknowledged at least 1s before it ("stale"). vtgate runs with `--discovery-low-replication-lag 5s` and `--min-number-serving-vttablets 1` (with the default of 2 and two secondaries, vtgate keeps a lagging secondary until the tablet's 2h unhealthy threshold), the tablets with `--health-check-interval 1s`; one run each:

| G13 | polling | heartbeat (`--heartbeat-interval 1s`) |
|---|---|---|
| cut-off member left its group | +7.3s | +7.9s |
| cut-off secondary's last replica read | +10.1s | +7.1s |
| its stale answers (first) | 50 of 56 (+1.1s) | 32 of 37 (+1.3s) |
| other secondary during the cut: reads answered, stale | 410, 0 | 441, 0 |
| failed replica reads | 0 | 0 |
| cut-off secondary back in replica reads after the heal | 13.4s (ONLINE after 11.4s) | 11.3s (ONLINE after 10.7s) |
| writes acked / lost | 8713 / 0 | 8733 / 0 |

With polling, the cut-off member is healthy for MySQL until Group Replication suspects its peers, about 5s after the cut (`its view of the group has no quorum (1 of 3 members reachable)` at +5.8s, the sync loop's verdict at +5.6s); its lag counts from its last healthy health check (+4.8s) and passed 5s at +9.8s. Heartbeat lag counts from the last heartbeat it applied, at the cut. Polling thus serves stale replica reads about 3s longer here, and up to the ~5s detection time in general.

The first polling run of G13 found that MySQL blocks the read of the group state while VTOrc's `GroupMemberNotOnline` recovery runs `START GROUP_REPLICATION` on the cut-off member, which VTOrc can reach: the reads timed out after the poller's 5s, the poller fell back to `no replication status`, and the tablet stopped serving, holding the query service's state lock for 5s on every health check. The poller now bounds the read to 1s, does not repeat it for 5s after a failure, and counts the member as not healthy meanwhile (`its group replication state cannot be read`, logged every 6s in the second run).

Regressions in polling mode (the harness default), same binaries: S1 failover 7.1s, gap 7.25s, 0/2240 lost, 0 violations; S3 failover 7.1s, gap 9.05s, 0/3728 lost, 0 violations. Evidence: `/home/ubuntu/chaos-results-lag-*`.

## Majority bootstrap analysis

When a group loses its majority, its members leave it, and VTOrc bootstraps it again only once **every** voter is reachable, on the voter whose executed and received GTID set contains every other voter's (`GroupNotBootstrapped`). In the G12 chaos scenario (below) that wait is most of the outage: the old primary's cell is cut off for 20s, and the shard has no primary until it is back. Bootstrapping from a reachable majority of the voters instead was analyzed and **rejected as unsafe**.

**What a commit acknowledgement means.** Raw MySQL 8.4.11, a group of two with Vitess's fixed settings (`paxos_single_leader=ON`, `member_expel_timeout=0`, `unreachable_majority_timeout=2`), `relay_log_recovery=ON`; the primary A's XCom traffic to B goes through a proxy that adds 20ms, then the proxy is killed and A→B is dropped, under write load on A (`/home/ubuntu/chaos-tl/analysis/lab4/`, `lab.sh`, `lab-results.txt`):

| Run | Result |
|---|---|
| L1, L1b, L4 | A had committed and acknowledged transactions that B, whose acceptance A needed, had neither received nor executed: 7 in each run (`:1433-1439`, `:1427-1433`, `:1427-1433`). |
| L1b, L4 | What B had received but not applied stays in its relay log after `STOP GROUP_REPLICATION` (`RECEIVED_TRANSACTION_SET` still readable), and a bootstrap of B applies it before B becomes writable (L4: 8s `super_read_only` while the backlog applied). |
| L2, L3 | A restart of B (`kill -9`, or a clean shutdown) with `relay_log_recovery=ON` discards the received backlog: `Received=""`, and a bootstrap of B never applies it. |
| L5 | Without the added delay, no transaction committed on A was missing from B (2 of 2 runs). |

A group acknowledges a commit once a majority has *accepted* it in XCom. An acceptor holds it in memory, and only receives it into its relay log once it learns the decision. While the group keeps its majority, XCom delivers it to every member. Once the group has lost its majority, and its members left, such a transaction exists only in the old primary's binlog. The remaining voters cannot tell: their received sets do not contain it.

**Verdict.** A bootstrap from a majority of the voters, without the old primary, can discard acknowledged transactions, and would leave them as errant transactions on the old primary; a restart of a voter can also discard what it had received. VTOrc keeps waiting for every voter (design: "What an acknowledged commit guarantees"). The availability cost is that of the slowest voter to come back; the fixes below only remove the time that Vitess added on top of it.

## Waits that Vitess added after a loss of majority (G12)

G12 (`TestG12VoterRejoinsWhilePrimaryCellIsolated`, `CHAOS_RACE_OFFSETS=500ms`, 6 cycles per run): a voter's mysqld is restarted and rejoins, and 0.5s into its join the primary's cell, its topology server included, is isolated for 20s. The group then always loses its majority, and the first write after the cut needs the heal, the members' MySQL leaves, VTOrc's bootstrap, a join and the sync loop's promotion. Per-cycle timelines come from the run logs (`/home/ubuntu/chaos-tl/analysis/cycles.py`, aggregated by `agg.py`; times from the cut).

**Before** (runs `/home/ubuntu/chaos-tl/G12-r1..r3`, 16 cycles that lost the majority and were bootstrapped), median first acknowledged write +40.6s, of which 3.6s (0–9.1s) was Vitess waiting for its own locks:

- **The tablet's action lock held across a topology write (9 of 16 cycles).** The old primary's sync loop demoted the tablet when MySQL lost its group, under the action lock, and waited, still under the lock, for the topology server to store the tablet record. The write, issued while the cell was cut off, returned when the step of the loop timed out (15s) or 7–11s after the heal (median 10.0s). VTOrc's `StartGroupReplication(bootstrap)` RPC on that tablet waited for the lock: its `STOP GROUP_REPLICATION` started the moment the record was published.
- **The shard lock held across a blocking join (r3, cycle 4).** VTOrc's `GroupMemberNotOnline` recovery started a join on the restarted voter right before the cut. The join RPC blocked behind the voter's own `START`, which had no group to join, and the recovery held the shard lock for 29.5s; the bootstrap, which needs it, started 7.7s late.

### Fixes

| Commit | Change |
|---|---|
| 7ecd4f4 | The sync loop's demotions (lost primary role, stale primary, foreign group) wait at most 1s for the topology under the action lock; the tablet runs with its new type right away, and its record is published in the background (`retryPublish`). A publish request wakes up the background publication (a promotion, VTOrc's `StaleTopoPrimary`), which otherwise waited `--publish-retry-interval` (30s). A promotion still writes the record first, with its term, under the state lock that the background publication takes to merge the running tablet: it cannot be undone. `StartGroupReplication` and its stop-serving check before a bootstrap wait at most 1s for the durability policy, the voters and the tablet records when the tablet read them before, and use what it read last. |
| f4f86ab | VTOrc's `GroupMemberNotOnline` still checks the legitimate group under the shard lock, then runs the join in the background, after the lock is released. One join per tablet and VTOrc at a time (`GroupJoinInFlight` skips the analysis meanwhile). A bootstrap in the meantime re-reads every voter under the lock and gives up while a member is active; a member whose join started is RECOVERING, which counts. |
| a760da7 | A PRIMARY tablet that does not serve for its replication group (fewer than a majority of the voters in its view, or a bootstrap of its MySQL) suppresses its heartbeat writes (below). |
| 5300a1b | Found while validating: a promotion could wait for a failed voter's status (2s) when the sync loop's background fetch had established the voter majority a moment earlier; it now checks first. |

Each change has a unit test that fails without it; the topology tests cut a memory topology off (its requests wait until the caller gives up or the partition heals).

### Results

Same harness and setup, binaries of these commits, 3 G12 runs (`/home/ubuntu/chaos-fix/lockwait/`; 17 cycles lost the majority and were bootstrapped; one more, r2 cycle 5, was not bootstrapped by VTOrc and is not counted, as before). Medians, ranges in parentheses, seconds:

| G12 segment (from the cut) | Before (16 cycles) | After (17 cycles) |
|---|---|---|
| A. heal | 20.1 | 20.2 |
| B. the bootstrapped member's MySQL leave, after the heal | 5.8 (5.3–17.4) | 5.6 (5.2–15.5) |
| C. **Vitess waiting** (bootstrap `START` minus max(heal, B)) | **3.6 (0.0–9.1)** | **0.0 (0.0–1.7)** |
| D. bootstrap to incarnation recorded | 1.7 (1.2–2.6) | 1.2 (1.1–1.3) |
| E. recorded to promotion (joins) | 4.7 (2.9–16.0) | 7.1 (2.6–42.5) |
| F. promotion to first write | 0.1 | 0.1 |
| **First acknowledged write** | **40.6 (31.3–48.4)** | **35.6 (29.6–69.5)** |
| Old primary's record published, after the heal | 10.0 (0.1–11.1) | 1.5 (0.3–3.9) |

The 1.7s left in C (r1 cycle 2) is the joiner's stray group, which blocked the bootstrap until it left at +27.4. Counting every voter's MySQL leave, not only the bootstrapped member's, C was 1.4s (−0.2–5.8) before and 0.0s (0.0–0.1) after. The bootstrap now starts as soon as MySQL lets it, so the joins (E) more often wait for the other voters' own leaves: 3.3s of E's median (0.3s before), against 3.2s (3.5s before) for the joins themselves. The outlier (r2 cycle 4, 42.5s) is MySQL: the remaining voter's join failed after 30s with `Timeout while waiting for the group communication engine to be ready`, and the restarted voter's `START` stayed blocked for a minute (errno 3663). 0 acknowledged writes lost and 0 violations in every run (10763, 26970 and 10681 acknowledged writes).

`StartGroupReplication` did not need its fallback in these runs: once the old primary's lock was free, its topology reads after the heal answered within 1s. A topology write retried after the heal also completed 0.3–3.9s after it, unlike the write issued during the cut (7–11s).

| Scenario | Before | After | Acked writes lost | Violations |
|---|---|---|---|---|
| S7d ×3: without an acknowledged write in total (longest gap) | 60.8s (31.7s), 57.4s (38.9s), 64.7s (22.5s, ongoing at stop) | 41.0s (11.6s), 68.3s (31.1s), 78.0s (68.9s, ongoing at stop) | 0/4474, 0/2040, 0/1061 | 0 |
| G9b (NEW-4: elected member's cell topo down) | gap 22.2s, 20.4s | gap 20.2s, shard primary moved at +20.2s | 0/3788 | 0 |
| S1 | gap 7.64s, 7.48s | gap 7.29s, new primary +7.0s | 0/2232 | 0 |
| S3 | gap 9.05s | gap 9.05s, new primary +7.25s | 0/3724 | 0 |

S7d stays dominated by MySQL (leaves and stray groups after each of the four isolations), and its runs vary as before. Two of its outages show issues that predate these commits:

- **r3: a bootstrap whose reply was lost** (fixed, see "Serving invariant, bootstrap intent and the migration's read-only moment"). VTOrc bootstrapped the group on the isolated primary 2s after a heal; the RPC returned after MySQL's leave (4.4s) and the bootstrap, by which time the next isolation had cut VTOrc off from the tablet. VTOrc never recorded the new incarnation, and the tablet trusted the group it had bootstrapped for `groupReplicationBootstrapGrace` (1 minute): no other bootstrap could start while it was active, and the tablet left it 60s later.
- **r2: the sync loop acted on a stale MySQL status** (fixed, same section). A step of the old primary's sync loop read MySQL's status while it was still the ONLINE primary of two members, before its group lost the majority, then waited on the topology of its isolated cell, and, after the heal, cleared the not-serving reason that a bootstrap RPC had set 16ms earlier. The bootstrap then made MySQL the writable primary of a group of one while the tablet served. The next isolation kept vtgate away until 1.3s before the next step of the loop stopped serving again: writes were acknowledged on that single voter for 1.3s, and existed only there until a second voter joined 4.7s later (none lost). The loop should read MySQL's status again before it makes a primary serve again.

### Heartbeats of a primary that does not serve

The query service of a PRIMARY tablet that does not serve still runs its replication tracker as a primary (`unservePrimary` calls `MakePrimary`), and with `--heartbeat-enable` it keeps writing a heartbeat every interval through the app user. In the fail-closed states of a Group Replication primary, MySQL is writable, so the heartbeats commit on a single voter. Checked in G11 with `CHAOS_VTTABLET_HEARTBEAT=1` (`--heartbeat-interval 1s`), keeping the data and reading P's binlog between its last application write (when it stopped serving, alone in its view after R1's graceful leave) and its death:

| G11 with heartbeats | Before (342bd0b) | After (a760da7) |
|---|---|---|
| P's transactions after it stopped serving | 12 heartbeats in 12s (`_vt.heartbeat`, one per second), on P only | none in 13s |
| Writes acknowledged again after P restarted | 9.4s | 10.5s |
| Acked writes lost, violations | 0/3528, 0 | 0/3600, 0 |

None was lost, since VTOrc waits for every voter and bootstraps the most advanced one, but they extended P's GTID set beyond the other voters', and would be errant if the group were ever bootstrapped elsewhere. A semi-sync primary that does not serve keeps writing heartbeats.

End-to-end, binaries of these commits: `TestGroupReplicationLifecycle` (new primary in topo 7.4s after the primary's mysqld was killed), `TestGroupReplicationOneVoterPerCell` and `TestGroupReplicationReplicaReadsWithPollingLag` pass.

## Serving invariant, bootstrap intent and the migration's read-only moment

This section closes the two S7d issues left open above (r2 and r3) and a flake of `TestGroupReplicationLifecycle`.

### Writes acknowledged by a single voter (S7d r2)

**How they are counted.** `single_voter.py` (in `/home/ubuntu/chaos-fix/serving2/`) reads a run's `events.txt`, where the observer logs every change of a tablet's state (sampled every 200ms: `gr=STATE/ROLE view=ONLINE/MEMBERS`), and every vttablet's query log. A write counts when a vttablet's query log shows an `insert into chaos_t` that MySQL executed (plan `Insert`, one row affected, no error; a write that the tablet refused before MySQL, for its tablet type for instance, is logged without a plan or rows) and that ended while the last observed state of that tablet was the ONLINE primary of a view with quorum and fewer than 2 ONLINE members, a minority of the 3 voters (a member without quorum cannot certify a commit, so a success it returns was certified by a majority before). The writes are grouped by single-voter interval, and an interval is a **decision** when the tablet committed nothing in the second before it began (it started or resumed serving while its view lacked the majority), a **shrink** when it did (it served legitimately and its view shrank under it, until the sync loop stopped serving). The observer's sampling makes the counts a lower bound.

Over the runs of the previous section (`/home/ubuntu/chaos-fix/lockwait/`): S7d r2 has one decision interval, 132 writes from 21:13:48.898 to 21:13:50.189 on zone2, alone in its view since 21:13:39.935; G11 (3 runs) and G12 r2 have shrink intervals of 24–52 writes over 0.2–0.5s after a voter left cleanly (G11's graceful leave, G12's restarted voter); the other runs have none.

**r2, from zone2's log.** VTOrc's bootstrap RPC arrived at 21:13:33.476 and stopped serving at 33.481. At 33.497 a run of the sync loop that had read MySQL's status before, when MySQL was the ONLINE primary of a view of two voters (`view_id=17908891737730644:4 online_members=2`), and had then waited for the topology of zone2's cut-off cell, cleared that reason ("the primary serves again"). The bootstrap's `START` ran at 39.655, after MySQL's leave of its old group, and made MySQL the writable primary of a group of one; the next isolation kept vtgate away until 48.628, and the tablet served writes until the next run of the loop stopped it at 50.196. The run did not take the action lock, which the bootstrap held, and did not read MySQL's status again.

**Fix (f782228).** A tablet starts or resumes serving as PRIMARY under a group replication policy with listed voters only if MySQL is the ONLINE primary, with quorum, of a view of the recorded incarnation with a majority of the voters ONLINE, as MySQL reports it under the action lock (design: "Serving invariant"):

- The sync loop stops serving on the status it read at the start of its run, without the lock, as before; it serves again only from `serveAgain`, which takes the action lock without waiting, reads MySQL's status after acquiring it, and decides against the shard record read before taking the lock (the lock is never held across a topology read).
- Every not-serving reason set is a new decision with a generation (`tmState.groupReplicationNotServing`, swapped atomically, so that no decision waits for the state lock that a background publication holds across a topology write); a decision to serve again only clears a reason set before the status it read.
- Every promotion goes through `changeTypeLocked`, which now makes the same decision: the tablet becomes PRIMARY but does not serve until the invariant holds (the loop's promotion, `PromoteReplica` for PRS, ERS and VTOrc, `InitPrimary`, `ReplicaWasPromoted`, `ChangeType`). `UndoDemotePrimary`, `DemotePrimary`'s revert and a primary that leaves its group serve again through `tmState`, which keeps the reason; a primary that leaves its group under a group replication policy with voters stays read-only.
- The incarnation must be the recorded one, not that of a group the tablet bootstrapped itself before it is recorded (see the bootstrap intent below).

Unit tests force each interleaving with the fake MySQL daemon, whose status reads and `START` the test observes and holds, and a cut-off memory topology: the r2 interleaving (`TestGroupReplicationSyncDoesNotServeOnStatusReadBeforeBootstrap`), a bootstrap still running while the stuck run resumes (`...DoesNotServeDuringBootstrap`), a bootstrap RPC that gave up while MySQL's `START` runs, the loop resuming before MySQL is the group primary (`...DoesNotServeBeforeBootstrappedMemberIsPrimary`), a promotion without the voter majority (and one with it, whose voters the tablet must first ask for their server_uuid), `UndoDemotePrimary` in an unrecorded incarnation, and a primary leaving its group. Each fails on 2ee8c64.

### A bootstrap whose reply was lost (S7d r3)

VTOrc bootstrapped the group on zone2 at 21:15:36.877. The tablet's `START` waited for MySQL's leave until 41.342 and the RPC completed on the tablet at 42.586, but the next isolation had cut VTOrc off: its recovery failed at 53.548 (`UNAVAILABLE`). The incarnation was never recorded. zone2 trusted its group for a minute and left it at 21:16:42.617; VTOrc bootstrapped zone1 at 21:16:46.882. About 64s without a group that Vitess could follow.

**Fix (09586e6).** VTOrc records a bootstrap intent in the shard record before the bootstrap (`Shard.group_replication_bootstrap_intent`: target, time, previous incarnation, token), and adopts the target's group when the RPC fails, or on a later pass (`GroupBootstrapNotRecorded`), under the shard lock, if the group's primary is the target, its incarnation differs from the intent's previous one, it has quorum, and it was created after the intent (design: "Bootstrap intent"). A recent intent fences a bootstrap on another tablet, and among voters with equal GTID sets the intent's target is chosen again. Incarnation writes are a compare-and-swap. Unit tests: a lost reply adopted right away and on a later pass, no adoption of a stray group of another voter, of the previous incarnation, of a group without quorum or created before the intent, the fence, the choice of the intent's target among equal voters, and the compare-and-swap conflicts (another incarnation recorded, the intent replaced).

**Found in chaos while validating it.** With the fence alone, killing the VTOrc that started a bootstrap (`/home/ubuntu/chaos-fix/serving/S7d-killorc`, binaries before the preference) cancelled the RPC on the tablet before MySQL's `START`; no group formed. The voters had equal GTID sets, and once the old primary's tablet had demoted itself the other VTOrcs chose another voter (the shard primary is preferred, then the lowest alias), which the intent fenced for its two minutes: 68.6s without writes, ongoing when the writers stopped, and the group bootstrapped 89s after the last heal. The intent's target is now chosen again among equal voters. The proto change is generated for `topodata.proto` only, purely additive in `topodata_vtproto.pb.go`, with a round-trip test.

### The migration's bootstrap and vtgate's buffer (`TestGroupReplicationLifecycle`)

About 1 run in 8 of `TestGroupReplicationLifecycle` failed one write during `MigrateReplicationMode`, after 30s, and then 13–14 writes during the PRS (`/home/user/vtlab/lifecycle-ab/head-8.log`, `/home/user/vtlab/lifecycle-ab2/base-7.log`; 2 of 24 runs on 2ee8c64 and its base). From the code:

- MySQL is `super_read_only` for a few milliseconds while Group Replication starts on the serving semi-sync primary, and refuses a write with errno 1290. `sqlerror.SQLError.VtRpcErrorCode` maps `EROptionPreventsStatement` to `CLUSTER_EVENT` (`go/mysql/sqlerror/sql_error.go`), which `TabletServer.convertAndLogError` returns to vtgate.
- `TabletGateway.withRetry` treats `CLUSTER_EVENT` as retryable: it marks the primary invalid for this request (`invalidTablets`), and its next iteration calls `buffer.WaitForFailoverEnd` with the error, which starts buffering: `KeyspaceEventWatcher.MarkShardNotServing` marks the shard not serving with `waitForReparent` set, since the error is a `CLUSTER_EVENT` that is not a resharding.
- The buffer ends on a keyspace event, which the watcher only emits once the shard serves again (`ensureConsistentLocked`). With `waitForReparent` set, a serving health check only counts if its primary term start time is newer than the last one (`onHealthCheck`); the same primary keeps serving with the same term, and sends no not-serving health check, which would clear `waitForReparent`. So the buffer only ends after `--buffer-max-failover-duration` (30s in the test), and every buffered write waits until then.
- The write that hit the refusal then fails anyway: its retry filters out the invalid primary (`getBalancerTablet`), finds no tablet, and returns the original error. The writes buffered by other requests are retried and succeed. The PRS that follows within `--buffer-min-time-between-failovers` of the end of that buffering is not buffered ("last failover which triggered buffering is too recent").

A second failure of the same moment showed while validating the fix for the first: a commit already under way when Group Replication starts is refused by its `before_commit` hook with errno 3100 (`Error on observer while running replication hook 'before_commit'`), which MySQL rolls back and vtgate fails right away, since it maps to `UNKNOWN` (1 of 25 runs, `/home/user/vtlab/fix-e2e/batch2/lifecycle-21.log`, with the read-only case already handled).

Making the tablet stop serving around the bootstrap would not help: a write that reaches the tablet in between fails with `CLUSTER_EVENT` too, and is never sent to the same tablet again; and if the not-serving health check reaches vtgate before the buffering starts, `waitForReparent` is set after it and the same primary's serving health check does not end the buffering either. Resuming with a new primary term would end it, at the cost of a reparent's side effects.

**Fix (deea0a6).** The tablet holds those writes instead. `StartGroupReplication(bootstrap)` on a PRIMARY tablet opens a window in the query service (`SetGroupReplicationBootstrapInProgress`) before MySQL starts Group Replication and closes it once MySQL is the writable primary of its group. A write that the tablet executes as a whole (autocommit DML, or a transaction of its own) and that MySQL refuses with errno 1290 or 3100 during the window, or up to 2s after it, waits for the window to end (at most 10s) and runs once more; MySQL refused it or rolled it back, so it had no effect. A statement of a client's transaction is not retried. Nothing changes in vtgate or for semi-sync. `TestWriteRefusedDuringGroupReplicationBootstrapIsRetried` (tabletserver) and `TestStartGroupReplicationBootstrapOnServingPrimaryHoldsReadOnlyRefusals` (tabletmanager) fail without it.

### Results

Binaries of deea0a6 (the three fixes), same harness and setup, runs one after the other (`/home/ubuntu/chaos-fix/serving2/`). Single-voter writes are counted as above; outages are the time without an acknowledged write in total, with the longest gap in parentheses.

| Scenario | Before (previous section) | After | Acked writes lost | Violations | Single-voter writes (decision / shrink) |
|---|---|---|---|---|---|
| S7d ×4 | 41.0s (11.6s), 68.3s (31.1s), 78.0s (68.9s, ongoing at stop); r2: 132 decision writes | 42.4s (11.3s), 40.6s (9.8s), 51.5s (30.7s), 46.1s (36.1s) | 0/4428, 0/4520, 0/3602, 0/4264 | 0 | 0 / 0 in every run |
| G12 ×3 (`CHAOS_RACE_OFFSETS=500ms`) | first write after the cut 35.6s (29.6–69.5) | 34.5s (31.5–38.5), see below | 0/14641, 0/14457, 0/14352 | 0 | 0 / 0 |
| G11 | gap 107.8s; shrink of 24–52 writes | gap 107.8s (P is down for 84s by design), writes back 10.1s after P restarted | 0/3632 | 0 | 0 / 72 (0.68s after R1's graceful shutdown, then P fails closed) |
| S1 | gap 7.29s | gap 7.84s | 0/2172 | 0 | 0 / 0 |
| S3 | gap 9.05s | gap 9.04s | 0/3812 | 0 | 0 / 0 |
| S7d, the bootstrapping VTOrc killed | 68.6s (ongoing at stop; fence alone, `serving/S7d-killorc`) | 67.2s (58.2s) | 0/2123 | 0 | 0 / 0 |

No decision write in any run: no tablet started or resumed serving while its view lacked the voter majority. The shrink writes of G11 are the case the invariant leaves to the sync loop (a voter leaves cleanly under a serving primary, which stops serving within `--group-replication-sync-interval`).

**G12 segments** (13 of 18 cycles lost the majority and were bootstrapped by VTOrc; medians, ranges, seconds from the cut): heal 20.1; the bootstrapped member's MySQL leave after the heal 5.6 (5.3–11.6); Vitess waiting 0.0 (0.0–0.1); bootstrap to incarnation recorded 1.2 (1.1–1.2); recorded to promotion 5.8 (3.4–8.6); promotion to first write 0.1 (0.0–0.8); first acknowledged write 34.5 (31.5–38.5); old primary's record published 1.7s after the heal (0.4–3.6). Every promotion now decides on a fresh status under the action lock (`changeTypeLocked`), which costs nothing measurable: segment F is unchanged. A control run on 2ee8c64's binaries right after (`G12-base-r1`, 4 of 6 cycles bootstrapped): first write 36.6 (32.5–41.7), F 0.1 (0.0–0.3), 0/19125 lost, 0 violations, no single-voter write; G12 does not produce the r2 interleaving.

**A lost reply, adopted (S7d r3).** VTOrc bootstrapped zone1 at 11:20:39.122; the next isolation cut VTOrc off, and the RPC failed at 11:20:55.940 after the heal (11:20:54.665). The same recovery adopted the group at 55.941 (the target was the ONLINE primary of a new incarnation created after the intent, with quorum), the tablet served at 57.858, and writes were acknowledged at 57.905. Before, the same sequence (S7d r3 of the previous section) cost about 64s.

**The bootstrapping VTOrc killed** (`kill_bootstrapping_orc.sh` kills the VTOrc that logs a bootstrap, here 33ms after it logged one on zone3 at 11:48:04.013). The tablet's RPC was cancelled while it stopped MySQL's failed membership, before `START`: no group formed, so there was nothing to adopt. The outage came from the dead VTOrc's shard lock: the other VTOrcs failed to lock the shard every second until 11:48:35.968, when its etcd lease expired, and bootstrapped zone3 again, the intent's target, at 11:48:36.016 (zone3 was also isolated twice in between); the incarnation was recorded at 11:48:37.184 and writes were acknowledged at 11:48:49.851, once two voters had joined. Two observations:

- **A join can form a group of one.** zone1's own sync loop had started a join (`START GROUP_REPLICATION`, not a bootstrap) at 11:47:50.531, which blocked during the isolations. At 11:48:09.500 MySQL completed it as the only member of a view of a new incarnation (`17909416894997945:1`), ONLINE PRIMARY and writable, on a tablet that was REPLICA. The loop saw an unrecorded incarnation at 11:48:10.515 and left it (`STOP GROUP_REPLICATION` completed at 11:48:15.232). The tablet never served as PRIMARY: vtgate's writes to it during those 5.7s were refused by the tablet before MySQL, and zone1 later joined zone3's group without extra transactions. VTOrc would not have adopted it: zone1 was not the intent's target. (`single_voter.py` first counted these 560 refused writes; it now counts only writes that MySQL executed.)
- **A killed VTOrc holds the shard lock until its lease expires** (about 30s with etcd here). This predates these commits and affects every recovery, not only bootstraps (see "Fence check" below, and "Operational notes" in the design doc).

**End to end**, binaries of deea0a6: `TestGroupReplicationLifecycle` passed 14 of 14 runs (`/home/user/vtlab/fix-e2e/lifecycle-*.log`), and 20 of 20 more with the same commit for the migration (`batch3/`): 0 of 34 failed, against 2 of 24 on 2ee8c64 and its base. In 2 of them the tablet retried a refused write during the migration's bootstrap, once with errno 1290 and once with errno 3100, and vtgate started buffering 3 times, as in every other run. With 8 more writer sessions through vtgate during the migration (`load/`, 5 runs), MySQL refused 3–9 writes per run with errno 1290 and the tablet retried them all, 0 failed. `TestGroupReplicationOneVoterPerCell` and `TestGroupReplicationReplicaReadsWithPollingLag` pass.

**Still open: the migration back.** In 25 of the 34 runs, and 16 of 24 on 2ee8c64 and its base, the migration back to semi-sync takes 38s instead of 12–16s: when the group shrinks to the primary, the primary stops serving (fewer than a majority of the voters, a rule that predates these commits) and serves again with the same primary term, and vtgate buffers until `--buffer-max-failover-duration`, the same mechanism as the bootstrap above, in the other direction. The test tolerates 5 failed writes; it failed once, in the fifth run with load, with 16 (`primary is not serving`), after the load had ended. The migration could clear the voters before the last secondary leaves, or end the not-serving moment with a new primary term; neither is in these commits.

## The migration's pauses and vtgate's buffer

This section replaces the tablet-side retry of deea0a6 with vtgate's buffering, and closes "Still open: the migration back" above. Design: "The migration's pauses and vtgate's buffer" in `doc/design-docs/GroupReplication.md`.

### Why the migration back buffered for 30s

The earlier explanation (the group shrinking to the primary stops it serving) does not hold: under the semi-sync policy that the migration back sets first, the voter-majority rule does not apply, and no run logged it. A run on 0070196's binaries (`/home/user/vtlab/gr-buffer/before/b1.log`, logs in `b1-logs/`) shows the actual sequence:

- 12:14:04.825, primary zone3-301 (`StopGroupReplication`): `PRIMARY: Serving -> PRIMARY: Not Serving` with term 12:13:45, then `STOP GROUP_REPLICATION`.
- 12:14:04.842, vtgate: `Starting buffering ... seen error: primary is not serving, there may be a reparent operation in progress`. This error is vtgate's own: the tablet gateway found no serving primary (vtgate had already processed the not-serving health check), and `ShouldStartBufferingForTarget` agreed. Starting the buffering calls `MarkShardNotServing(isReparentErr=true)`, which sets `waitForReparent` after the not-serving health check had cleared it.
- 12:14:08.425, primary: `PRIMARY: Not Serving -> PRIMARY: Serving`, same term. vtgate ignores this serving health check: with `waitForReparent` set, only a newer primary term ends the wait (`keyspace_events.go`, `onHealthCheck`).
- 12:14:34.843, vtgate: `Stopping buffering ... after: 30.0 seconds due to: stopping buffering because failover did not finish in time`.

So vtgate ends a same-term pause promptly only if a not-serving health check reaches its keyspace event watcher after the buffering started. That happened in the runs where a write reached the primary before vtgate saw it stop serving: the write's `CLUSTER_EVENT` started the buffering, the not-serving health check cleared `waitForReparent`, and the serving one ended the buffering; that write failed anyway, since vtgate does not send a request again to a tablet that refused it (`invalidTablets` in `withRetry`), which is why the test tolerated 5 failed writes. Which of the two came first was a race, lost in 25 of 34 runs. The bootstrap's 30s buffering (above) is the first case without the not-serving health check: the primary never stopped serving. `TestBufferingEndsWhenSamePrimaryServesAgain` (`go/vt/vtgate/buffer`) reproduces both orders against vtgate's buffer and keyspace event watcher: the second order only ends when the not-serving health check is sent again after the buffering started, and either order times out if a not-serving health check stops clearing `waitForReparent`.

### Fix

vtgate is unchanged. Both migration steps that make MySQL refuse commits under a serving primary are planned pauses (`pauseServingLocked`): the bootstrap forward, and the primary's leave of its group back (the step that switches it back to semi-sync durability). No other step refuses commits: in every run below, the MySQL error logs show `before_commit` failures only on the primary during its own `STOP GROUP_REPLICATION` (its heartbeat writes, inside the pause), and none during joins or the secondaries' leaves.

- The tablet first reports that it does not serve while it still serves (100ms, `--group-replication-pause-notice`), so vtgate buffers instead of sending writes that the tablet would refuse; then it stops serving and drains, MySQL changes, and the tablet serves again once MySQL is writable, through the serving invariant, with its primary term. The pause ends, and the tablet serves again, after `groupReplicationPauseResumeTimeout` (5s) whatever MySQL's state, and also when the change fails: MySQL is then still the semi-sync primary, and the RPC reports the error so that the migration can be run again. Nothing else makes the tablet serve during the pause.
- A PRIMARY that serves again with its term first broadcasts that it does not serve (`announceNotServingBeforeResuming` in the tabletserver's state manager), so the buffering ends on the next health check whichever order vtgate saw. This also ends the 30s buffering of `UndoDemotePrimary` and of a primary that the sync loop let serve again.
- In 2 of 16 runs with the pauses alone (`/home/user/vtlab/gr-buffer/v1/run5` and `run15`), the primary's sync loop still had the group replication policy cached (for up to 10s) when the group shrank to the primary at 12:44:45.921: it stopped serving for the lost voter majority, served again 1s later when its cache expired (vtgate's buffering ended then, thanks to the announcement), and the planned pause followed 2s later. vtgate buffers a shard at most once per `--buffer-min-time-between-failovers`, 1 minute by default, so the planned pause would have failed writes. Before it reports a lost majority, the loop now reads the policy again, along with the shard record it already read again (`voterMajorityLost`); `TestGroupReplicationSyncDoesNotStopServingUnderStalePolicy` fails without it.
- The retry of deea0a6 is removed: `read_only_window.go`, the hooks in `query_executor.go`, the window in `StartGroupReplication`, `SetGroupReplicationBootstrapInProgress`, and `sqlerror.ERRunHookError`, which nothing uses any more.

Unit tests, each failing without its fix: the pause's order (`TestBootstrapOnServingPrimaryPausesServing`, `TestPrimaryLeavingItsGroupPausesServing`), serving only once MySQL is writable (`TestPausedPrimaryServesOnlyOnceMySQLIsWritable`), failures during the pause (`TestPausedPrimaryResumesAfterFailedBootstrap`, `TestPausedPrimaryResumesWhenMySQLStaysReadOnly`), no serving during the pause on a state refresh (`TestPausedPrimaryDoesNotServeWhenItsStateIsRefreshed`), the serving invariant at the pause's end (`TestPausedPrimaryKeepsTheServingInvariant`), and the announcement (`TestPrimaryAnnouncesNotServingBeforeResuming`).

### Results

`TestGroupReplicationLifecycle` with `--enable-buffer`, one run after the other (`/home/user/vtlab/gr-buffer/`). "Longest write" is the longest time a write of the test's writer took during the migration step, buffered or not.

| | Before (0070196, deea0a6's retry) | Pauses only (`v1/`, 16 runs) | Final (`v2/`, 16 runs) |
|---|---|---|---|
| Runs passed | 34 of 34 (the test tolerated 5 failed writes back) | 16 of 16 | 16 of 16 |
| Failed writes, forward / back | 0 / up to 16 | 0 / 0 | 0 / 0 |
| Buffered until `--buffer-max-failover-duration` (30s), back | 25 of 34 runs (`b1`: 30.0s) | 0 | 0 |
| Longest write, forward | | 1.63–2.24s | 1.57–2.33s |
| Longest write, back | 30s in 25 of 34 runs | 4.64–4.89s | 4.65–4.91s |
| vtgate bufferings per migration, forward / back | | 1 / 1, and 1 / 2 in the 2 runs with the sync loop's pause | 1 / 1 in every run, all ended by the serving primary |
| Pause, forward / back | | 1.32–1.51s / 3.69–3.94s | 1.33–1.50s / 3.70–3.96s |

The pause forward is mostly MySQL's `START GROUP_REPLICATION` (about 1.2s), the pause back its `STOP GROUP_REPLICATION` (about 3.6s, the time the leave takes on every member, secondaries included). After the pause back, the first commit waits up to about 1s more for a semi-sync acknowledgement (0.96s in MySQL in `after/a1`): Group Replication's stop ends the binlog dump threads (the replicas log `Lost connection to MySQL server during query` when the primary's leave ends), and MySQL's semi-sync ack receiver only polls a reconnected replica from its next round. After the pause forward, the first commit takes 0.2–0.9s. The test bounds the longest write at 8s.

With 8 more writer sessions through vtgate during each migration (`load/`, 3 runs, `run_load.sh`), no write failed in either direction; before, MySQL refused 3–9 writes per run during the bootstrap and the tablet retried them. `TestGroupReplicationOneVoterPerCell` and `TestGroupReplicationReplicaReadsWithPollingLag` pass.

Chaos regression on the final binaries (`/home/ubuntu/chaos-fix/pause/`, `CHAOS_DURABILITY=group_replication_cross_cell`, one run each; the setup's migration to Group Replication pauses the primary too):

| Scenario | Previous section | Now | Acked writes lost | Violations | Single-voter writes (decision / shrink) |
|---|---|---|---|---|---|
| S1 | gap 7.84s | gap 7.63s | 0/2172 | 0 | 0 / 0 |
| S3 | gap 9.04s | gap 9.04s | 0/3768 | 0 | 0 / 0 |
| G12 (`CHAOS_RACE_OFFSETS=500ms`) | first write after the cut 34.5s (31.5–38.5) | 5 of 6 cycles lost the majority; longest outage per cycle 31.7–36.8s | 0/17791 | 0 | 0 / 0 |

## Fence check

The serving invariant (above) decides when a PRIMARY tablet serves; it does not make MySQL refuse writes. This section closes two windows in which MySQL took writes that the shard's group does not hold, and documents a third observation. Design: "Fence check" and "Operational notes" in `doc/design-docs/GroupReplication.md`.

### A join can form a writable group of one

**Before.** In the S7d run with the bootstrapping VTOrc killed (previous section), zone1's join completed at 11:48:09.500 as the only member of a new incarnation: ONLINE PRIMARY and writable on a REPLICA tablet for 5.73s. The sync loop, which waited for the join's action lock, noticed it 1s later; MySQL's `STOP GROUP_REPLICATION` then took 4.7s. G12 with `CHAOS_RACE_OFFSETS=500ms` forms such groups in every run, when the joiner's group loses its majority while it joins: 3–5 per run in the five previous runs (`serving2/G12-r1..r3`, `G12-base-r1`, `pause/G12`), each writable 4.56–4.85s until MySQL left it, 14.0–23.2s per run in total. Vitess never routed writes to them (the tablets were REPLICA, or did not serve); clients that write to MySQL directly could have committed transactions that the shard's group does not have.

**Fix.** The tablet's sync loop runs a fence check in a second goroutine. While a join or a bootstrap of the tablet's MySQL runs, for 30s after one ended, and while MySQL is fenced, it reads MySQL's view and `super_read_only` every 150ms with one query on in-memory tables, and decides on the shard record that the tablet read last, without the topology or the action lock. When MySQL is the primary of a view of another incarnation than the recorded one that lacks the voter majority, it sets `super_read_only` (1s `lock_wait_timeout`), and the sync loop leaves the group, fencing MySQL first if it gets there first. It never fences a member of the recorded group that may serve, a group the tablet is bootstrapping or has just bootstrapped and not recorded yet, nor the group of a live bootstrap intent's target, which VTOrc adopts. Only a decision under the action lock that the tablet may serve lifts the fence, and a mutex with two counters orders the fence with those decisions and with joins, bootstraps and leaves (design: "Ordering with the action lock").

**Found while validating it.** On the first version, the stray groups of G12 were fenced after 0.997–1.002s every time, at the moment MySQL logged `Plugin 'group_replication' has been started`. In the raw lab, during a `START GROUP_REPLICATION` that bootstraps a group (1.2–1.6s), `replication_group_members` lists the member as ONLINE PRIMARY with `super_read_only` off about 0.6s after the `START` began, but every read of `replication_group_member_stats`, where the view id is, waits until the `START` returns (up to 0.96s; the query without it answers in 13–63ms). While one of its starts runs, the check now reads without the view id and decides on the members alone: a join into the shard's group never makes the joiner its primary.

### A primary keeps committing after its group shrinks

**Before.** G11: after R2's expulsion and R1's graceful shutdown, P is the primary of a view of one with quorum, and commits on a single voter until the sync loop stops serving, within `--group-replication-sync-interval` (1s here). MySQL stays writable for clients that write to it directly. Counted by `shrink_writes.py` (below): 84 writes, the last one 0.81s after MySQL's view of one (`serving2/G11-r1`, the "72 writes within 0.68s" of `single_voter.py`), and 64, 52, 40 and 32 writes over 0.60, 0.50, 0.40 and 0.25s in the four runs before.

**Fix.** The fence check also watches a PRIMARY tablet every 150ms. Once it saw MySQL as the primary of a view with the voter majority in an incarnation, a view that lacks the voter majority, or a shard record that lists another incarnation, makes it set `super_read_only`, then the serving invariant's not-serving reason, so that vtgate buffers. The primary serves again, writable, once `serveAgain` decides under the action lock that the majority is back. When the view of a primary changes, the check makes the sync loop read the shard record and the policy again, so that the migration back to semi-sync, which stores the shard's policy before the first secondary leaves, is not fenced (the end-to-end migrations log no fence).

**Found while validating it.** The first G11 runs on the fix were not fenced: the check trusted the incarnation that the tablet had bootstrapped for `groupReplicationBootstrapGrace` (a minute) even once it was recorded, and the harness's migration bootstraps the group about 25s before the scenario's shrink; the sync loop alone stopped serving (4 and 12 single-voter writes, MySQL never read-only, `fence-v1-bootstrap-grace-bug/`). The tablet now trusts a group it bootstrapped only until its incarnation is recorded (`TestGroupReplicationFenceCheckFencesShrunkPrimaryAfterItsBootstrap`).

### Verified on MySQL 8.4.11

A raw lab of three instances (MySQL communication stack, outside Vitess; transcripts in `/home/user/vtlab/fence/sro-lab.txt`, `fence-query-lab.txt`, `start-block-lab*.txt`): the primary of a group accepts `SET GLOBAL super_read_only = ON` and then refuses commits with errno 1290, for the application user and for root; a secondary leaving and rejoining (recovering from the fenced primary), 15s in a view of two, 15s alone in a view of one and `STOP GROUP_REPLICATION` all keep it set, and the primary alone in its view still refuses commits; an election clears it (the bootstrap of a group of one by a member that was `super_read_only` before, and `group_replication_set_as_primary`); with a transaction open, the `SET` takes 19ms and the transaction's commit fails.

### How it is measured

- **Stray writable window** (`stray_window.py`): from a MySQL error log's `Setting super_read_only=OFF` (Group Replication's member action after an election), in the view it announced last, to the first of MySQL's `Setting super_read_only=ON`, `This member has left the group`, or the tablet's `MySQL is fenced with super_read_only`; an interval is stray when its incarnation was never recorded in the shard record. It reproduces the 5.73s of the run above.
- **Single-voter writes** (`shrink_writes.py`): the inserts that MySQL executed on the primary (vttablet query log: plan `Insert`, one row) and that ended after the primary's MySQL logged a view of one, until its next view; split into those that started before the view change (in flight, certified while the leave completed) and after it. `single_voter.py` (previous sections) relies on the observer's 200ms samples and counts nothing when it samples the view of one after the fence.
- G11 restarts the old primary's vttablet, which overwrites its log: the fence times of G11 come from copies taken every second.

### Results

Final binaries, runs one after the other (`/home/ubuntu/chaos-fix/fence/`, `CHAOS_DURABILITY=group_replication_cross_cell`; summaries in `/home/user/vtlab/fence/chaos-results.txt`). Outages are the time without an acknowledged write in total, with the longest gap in parentheses.

| Scenario | Before (latest previous runs) | After | Acked writes lost | Violations |
|---|---|---|---|---|
| Stray groups, G12 ×2 (`CHAOS_RACE_OFFSETS=500ms`) | 3–5 per run, each writable 4.56–4.85s, 14.0–23.2s per run | 6 and 6, each writable 0.009–0.136s, 0.515s and 0.394s per run; all fenced by the tablet, none left writable until MySQL's leave | 0/10307, 0/13509 | 0 |
| G12 outages (same runs) | 193.0s, longest per cycle 31.7–36.8s (`pause/G12`, 5 of 6 cycles lost the majority) | 193.1s and 205.0s, longest per cycle 29.7–33.8s and 30.7–40.9s (6 of 6 cycles lost the majority in each; the 40.9s cycle had a member in ERROR until +35.9s) | | |
| G11 ×2, single-voter writes after the shrink | 32–84 writes, the last 0.25–0.81s after the view of one; MySQL never read-only | 20 (4 in flight) and 4 writes, the last at +0.091s and +0.007s; MySQL fenced at +0.108s and +0.018s | 0/3580, 0/3536 | 0 |
| G11 ×2, outage | gap 107.8s (P is down for 84s by design) | 108.5s and 108.8s | | |
| S7d, the bootstrapping VTOrc killed (×3) | 67.2s (58.2s); a stray group writable 5.73s | r1, r2: no majority loss, so no bootstrap to kill: 40.2s (10.9s), 51.1s (19.2s). r3, the VTOrc killed 36ms after it started bootstrapping zone2: 73.1s (64.0s); zone1's stray group fenced after 0.147s | 0/4536, 0/3637, 0/1573 | 0 |
| S1 | gap 7.63s | 7.25s | 0/2252 | 0 |
| S3 | gap 9.04s | 9.04s | 0/3688 | 0 |

In the runs on the binaries in between (`fence-v2-view-blocks/`), with the view id read during starts, G11 had 12 single-voter writes in each run, the last at +0.111s and +0.094s (fenced at +0.111s in the second, from its log), and the stray groups of G12 were writable 0.997–1.002s each (6 in one run, 5.99s in total). `TestGroupReplicationLifecycle` (2 runs) and `TestGroupReplicationMigratesShardByShard` pass on the final binaries; the check logs that it watches each primary's group and never fences during the migrations either way.

**S7d r3 with the VTOrc killed.** zone1, the old primary, formed a group of its own at 02:13:14.301 while it joined; the check fenced it 0.147s later, the tablet demoted itself and left that group. VTOrc started bootstrapping zone2 at 02:13:30.827 and was killed 36ms later, after the RPC had reached the tablet: zone2's group formed, writable but not served, and is not fenced, being the live intent's target. The other VTOrcs failed to lock the shard 175 times until 02:14:04.797, when the dead VTOrc's lease expired, and adopted zone2's group; zone2 served at 02:14:04.815. The outage is the lease's, as in the run of the previous section, where the RPC was cancelled before the `START` and VTOrc bootstrapped again instead.

### A killed VTOrc holds the shard lock

Documentation only (design: "Operational notes"). In the killed-VTOrc S7d runs, the other VTOrcs failed to lock the shard every second until the dead VTOrc's etcd lease expired, 30–34s after the kill (`--topo-etcd-lease-ttl`, 30s by default). A VTOrc that exits cleanly releases its locks. A shorter TTL bounds that wait, but a live holder whose lease renewals stall for longer than the TTL loses the lock while it still acts.

### Still open

- A REPLICA tablet is only watched around its own joins: if the group's own election makes its MySQL the primary of a recorded view that lacks the voter majority, MySQL takes the writes of clients that write to it directly until the tablet is promoted (Vitess does not route there: the promotion does not serve).
- The check's read times out (2s, then the query is killed) while the tablet's own `STOP GROUP_REPLICATION` runs, since `replication_group_member_stats` does not answer then either: 1–4 times per end-to-end run. MySQL is `super_read_only` during a leave, so nothing is missed; the read could also leave out the view id during the tablet's stops.
- MySQL's own auto-rejoin (`--group-replication-autorejoin-tries`, 0 by default) more than 30s after the tablet's last join is not watched; the sync loop still leaves such a group within its interval.

## Bootstrap candidate and superseded bootstrap intents

The TLA+ model (`doc/design-docs/group_replication_tla`, design: "Model checking") found two problems in the bootstrap of a group that lost its majority, with every earlier fix on. Both are fixed in this branch; the design is in "The bootstrap candidate" and "Bootstrap intent" of `doc/design-docs/GroupReplication.md`.

### Bootstrap candidate

**The problem.** VTOrc ranked the voters by their executed and received GTID sets (`memberGTIDSet`), and broke ties by the lowest alias. A voter that held an acknowledged transaction only in the relay log of its `group_replication_applier` channel could win over the old primary, which held it in its binlog. A restart of that voter's mysqld before the bootstrap's `START` discards the relay log (`relay_log_recovery=ON`); the new group then lacks the write, and the old primary can never join it. Nothing checked the candidate again, neither VTOrc after its choice nor the tablet before the `START`. The model's trace (`relay_cand`, 13 states): s1, the primary, commits a write; s2 receives it without applying it; the group loses its majority and its members leave; VTOrc reads every voter and chooses s2 (equal sets, lowest alias); s2's host restarts; VTOrc records the intent and sends the RPC; s2 bootstraps a group without the write, and VTOrc records it. `TestBootstrapGroupReplicationPrefersTransactionsInTheBinlog` reproduces the choice on the Go code.

**What MySQL does with the relay log** (raw MySQL 8.4.11, three instances with Vitess's settings: MySQL communication stack, `paxos_single_leader=ON`, `member_expel_timeout=0`, `unreachable_majority_timeout=2`, `relay_log_recovery=ON`, `BEFORE_ON_PRIMARY_FAILOVER`; `/home/user/vtlab/fixes2/lab/relay.py`, transcripts in `/home/user/vtlab/fixes2/lab/results/`). m1 is the primary; m3 leaves the group; m2 holds `FLUSH TABLES WITH READ LOCK`, so that its applier cannot commit, while m1 commits rows 11–15 (GTIDs 6–10), which the group of two acknowledges; m2 then leaves (`STOP GROUP_REPLICATION`, which waits for the lock's release and applies one more transaction), and m1 leaves. m2 has executed 1–6 and received 1–10; m1 has executed 1–10; m3 has executed 1–5.

| Case | Result |
|---|---|
| (a) m2 **joins** a group bootstrapped on m3, which lacks 6–10 (`a.txt`) | At its `START`, m2's applier applies the relay log first (executed 1–6 → 1–10), then MySQL refuses the join: "This member has more executed transactions than those present in the group. Local transactions: 1–10 > Group transactions: 1–5", and the member leaves (errno 3092). The group does not diverge, but m2 can never join it, and holds transactions it lacks. |
| (a') m2 joins a group that holds them, bootstrapped on m1 (`a_has.txt`) | Joins; recovery skipped ("the joiner's gtid executed set ... already has all the transactions of the donor"). |
| (b) m2 **bootstraps** (`b.txt`) | It applies the relay log before it is ONLINE (executed 1–10); m1 and m3 join. |
| (c) with Group Replication stopped, `START REPLICA SQL_THREAD FOR CHANNEL 'group_replication_applier'` on m2 (`c.txt`, `c2.txt`) | Accepted, also with `super_read_only` on; the backlog is applied within 0.5s (1–6 → 1–10), and `STOP REPLICA SQL_THREAD` stops it (a second `STOP` is a no-op). `START REPLICA FOR CHANNEL 'group_replication_applier'` (the receiver too) is refused with errno 3139. After a `kill -9` and restart, m2's executed set stays 1–10 and its received set is empty: the transactions are in the binlog. m2 then bootstraps, and m1 and m3 join. A join of m2 into m1's group after the apply also succeeds. |
| (c') `START GROUP_REPLICATION` (bootstrap) while that applier thread still runs (`c3.txt`) | Succeeds: ONLINE PRIMARY, executed 1–10. |
| (d) `kill -9` and restart of m2, then m2 bootstraps (`d.txt`) | Received set empty, executed 1–6: the bootstrapped group lacks rows 12–15, and m1's join is refused (errno 3092, more executed transactions). This is the loss the model found. |

**Fix (47d5e41).** The rule, with why it is safe and live, is in the design ("The bootstrap candidate"). VTOrc still requires a voter whose executed and received set contains every voter's: case (a) shows that a voter left with any transaction the new group lacks can never join it. Among those voters it prefers the target of a live intent, then a voter that executed them all (the old primary in the trace above), then as before. The bootstrap RPC carries the union of every voter's executed and received sets (`StartGroupReplicationRequest.required_gtid_set`); right before MySQL's `START`, under the action lock, the tablet applies its relay log with the applier thread if its executed set lacks part of that set (case (c)), and refuses with `FAILED_PRECONDITION` unless the executed set then contains it. Requiring a voter that already executed every transaction was rejected: a transaction that the group decided while its primary crashed before committing it is in relay logs only, executed by no voter and never acknowledged, and such a rule never bootstraps the group (the model's `cand_strict`: stuck after 6 states).

### Superseded bootstrap intents

**The problem.** The model's `integrated` simulation found two groups bootstrapped from the same recorded incarnation (26 states); `stale_rpc` reproduces it exhaustively at small bounds (18 states, 2 VTOrcs, one lease expiry, one clean leave, no transaction): VTOrc o1 chooses s1 and records an intent; its lease expires; o2 chooses s1 too and replaces the intent; o2's RPC bootstraps s1, and o2 records the group; s1's MySQL leaves its group of one; o1's RPC, which waited for s1's action lock, bootstraps s1 again; o2, finding every voter out of a group and no live intent, bootstraps s2. The compare-and-swap refuses to record s1's second group, which stays read-only and unserved; s1 is out of the shard's group until it leaves it, up to the one-minute grace.

**Fix (8a5fdb3).** The RPC carries the intent's token and the incarnation it was recorded for; the tablet refuses with `FAILED_PRECONDITION` if the shard record no longer holds them, when it gets the action lock, before each `STOP` of a `START` in progress, and right before MySQL's `START`, each read bounded by `groupReplicationTopoReadTimeout` (1s), and bootstraps without the check if the topology does not answer (`stale_rpc_timeout` shows that this fallback can still reach the state, 18 states). With the check on, TLC found a second path (`dup_record`, 22 states): o1's bootstrap reply arrived after o2 had adopted o1's group and recorded a new intent for the group's next bootstrap; o1's write of the same incarnation cleared that newer intent while its bootstrap still ran, so o1 could bootstrap s2 next to it, and the running bootstrap's group could no longer be adopted. A write that finds its incarnation recorded already now clears only an intent for an earlier incarnation. With both fixes, `stale_rpc_fixed` (same bounds) passes exhaustively, and the `integrated` simulation finds nothing.

### Tests

Each fails without its change (checked by mutation: the binlog preference removed, the required set or the token not sent, the tablet's check, the relay log's application, the check before a `STOP` or at the RPC's start removed, the check failing closed on a topology that does not answer, the late write clearing any intent, the applier left running): `TestBootstrapGroupReplicationPrefersTransactionsInTheBinlog` and `TestBootstrapGroupReplicationRequiresEveryTransaction` (VTOrc's choice, the required set, the token and the expected incarnation in the RPC), `TestBootstrapAppliesRelayLogBeforeStart`, `TestBootstrapRefusesWithoutRequiredTransactions`, `TestBootstrapRefusesWhenRelayLogIsNotApplied`, `TestStartGroupReplicationRejectsInvalidRequiredSet`, `TestBootstrapRefusesSupersededIntent` (also: a PRIMARY tablet keeps serving), `TestBootstrapOfSupersededIntentDoesNotStopNewerStart`, `TestBootstrapIntentCheckDoesNotWaitForCutOffTopo`, `TestApplyGroupReplicationRelayLog` (`mysqlctl`, against a fake server) and `TestRecordGroupReplicationBootstrapKeepsNewerIntent`. They pass 20 times in a row under `-race`. The `tmrpctest` round trip carries the new request fields through gRPC and the generated marshalling code.

### Results

Binaries of 8a5fdb3, one host, runs one after the other (`/home/ubuntu/chaos-fixes2/`, `CHAOS_DURABILITY=group_replication_cross_cell`). "Before" is the latest runs in "Audit: only Vitess makes MySQL writable" (design doc). "Unavailable" sums the gaps of 1s or more without an acknowledged write.

| Scenario | Before | After | Acked writes lost | Violations |
|---|---|---|---|---|
| G12, a voter rejoins while the primary's cell is isolated (`CHAOS_RACE_OFFSETS=500ms`, 6 cycles) | 2 runs: unavailable 217.3s and 189.9s, longest gap 40.9s and 43.0s | 1 run: unavailable 145.9s; 3 cycles lost the majority (outage 36.8s, 32.9s, 35.9s each) and 3 kept it (14.9s, 13.2s, 11.1s). VTOrc bootstrapped 3 times, each RPC with the required set and the intent's token; none refused, none needed the relay log applied. The `START` came 20ms, 4.0s and 3.1s after the RPC: the latter two waited for MySQL's `STOP` of a member in ERROR, as before; the checks took milliseconds | 0/20141 | 0 |
| G11, a graceful leave then the primary dies | 2 runs: longest gap 104.6s, 108.7s | 1 run: 105.4s. 27 writes acknowledged while only R1's relay log and P held them, none lost: VTOrc bootstrapped P, which executed them all | 0/3563 | 0 |
| S7d, flapping primary | 2 runs: unavailable 43.5s and 46.6s, longest gap 13.2s and 14.7s | 2 runs: 44.2s and 60.0s, longest gap 35.2s and 51.0s | 0/4316, 0/2840 | 0 |

**S7d.** In both runs the group lost its majority after the second isolation, as in 2 of 10 runs with the 2s unreachable majority timeout ("Unreachable majority timeout sweep"): the isolated primary's group had two members, the third one rejoining after its own isolation. The voter that held the most transactions was then isolated again 5s after the heal, while the members were still leaving their group in ERROR, and VTOrc, which waits for every voter, bootstrapped 1.7s (r1) and 2.5s (r2) after the next heal; the longest gaps are those two isolations. The earlier runs of the same scenario had gaps of 11.6–68.9s ("Waits that Vitess added after a loss of majority"). Neither run shows a refused bootstrap. Each run's report notes a second MySQL with `read_only=OFF` for 0.2–0.6s: the isolated old primary, which could not commit without its majority, between the new primary's promotion and its own `unreachable_majority_timeout`, which made it `super_read_only` 8.0s after the isolation, as designed; no write was committed on it after the new primary's promotion.

End to end, on the same binaries: `TestGroupReplicationLifecycle` (new primary in the topology 6.8s after the primary's mysqld was killed) and `TestGroupReplicationMigratesShardByShard` pass.

### Still open

- A restart of the candidate's mysqld after VTOrc chose it makes the tablet refuse the bootstrap; if the restart cost the candidate transactions that another voter holds, VTOrc then chooses that voter, but the intent written for the first one fences a bootstrap on any other tablet for its two minutes. Fixed when the refusal is definitive: see "Withdrawing the intent of a refused bootstrap".
- If the relay log's received set lists transactions that the applier cannot apply, the candidate refuses every bootstrap, and VTOrc keeps choosing it; the shard then needs an operator. Not seen in the lab or in chaos.
- The token check is skipped when the tablet's topology does not answer within a second (`stale_rpc_timeout` in the model).

## Withdrawing the intent of a refused bootstrap

### The delay

A live bootstrap intent fences a bootstrap on any other voter for two minutes (`GroupReplicationBootstrapIntentFence`), so that the group of a bootstrap whose reply was lost can still be adopted. Since "Bootstrap candidate" (47d5e41), the candidate's tablet refuses a bootstrap when MySQL lacks a transaction that the request requires, as after a restart of mysqld that discarded its relay log. That refusal is correct, but the intent still named the candidate: VTOrc's next pass chose the voter that held the lost transactions, and the fence refused it until the intent expired. The TLA+ model shows the stuck state (`refusal_stuck`, below); `TestGroupReplicationWithdrawsRefusedBootstrapIntent` reproduces it on MySQL 8.4 (see "Results").

### Fix (b8c4855, model 1641439)

The tablet reports a refusal as **definitive** when it proves that the request did not, and never will, start MySQL's bootstrap, and VTOrc then withdraws its intent at once. The design is in "Bootstrap intent" of `doc/design-docs/GroupReplication.md`.

**Which refusals qualify.**

| Refusal | Definitive | Why |
|---|---|---|
| MySQL lacks a transaction of `required_gtid_set`, decided under the action lock, with MySQL in no group and no `START GROUP_REPLICATION` in `performance_schema.processlist` (also after the RPC stopped a `START` that was in progress, once MySQL accepted the `STOP`) | yes | Every `START` of the tablet runs under the action lock, which the RPC holds until it returns; MySQL starts none on its own (`group_replication_start_on_boot` and auto-rejoin are off); VTOrc sends one RPC per intent. |
| The same, while a `START` runs, or when the status read fails | no | A `START` that an earlier RPC issued before its client gave up can still form a group of one. |
| A superseded intent or another incarnation (token check) | no | The intent is not the caller's any more; the compare-and-swap would be a no-op anyway. |
| MySQL is already an active member | no | It may be the group of an earlier bootstrap of the same intent, which VTOrc adopts. |
| `UNAVAILABLE` after waiting 10s for a `START` in progress, configuration or topology errors, a timeout, a lost reply, a transport error, a canceled context | no | The request, or an earlier one, may still start a group. |

**The signal.** A new optional request field, `StartGroupReplicationRequest.report_definitive_refusal`, asks the tablet to report a definitive refusal in a new response field, `StartGroupReplicationResponse.definitive_refusal`, instead of as an error: gRPC returns one or the other. The tablet manager returns it as a typed error (`tmclient.GroupBootstrapRefusedError`, code `FAILED_PRECONDITION`), `grpctmserver` turns it into the response field only when the request asked for it, and `grpctmclient` turns the field back into the typed error. No error string is matched. A tablet that does not know the request field refuses with an error, and a VTOrc that does not set it gets one: both keep the intent, as before.

**The withdrawal** (`reparentutil.WithdrawGroupReplicationBootstrapIntent`) runs under the shard lock that the recovery holds, re-checked first as for every other intent write, and is a compare-and-swap: it removes the intent only while the shard record holds the same token, for the incarnation it was recorded for. A newer intent, which another VTOrc wrote after this one's lease expired, and an incarnation recorded since, stay as they are; the write is then a no-op. A stalled VTOrc whose lease expired fails the lock check and writes nothing; one whose lease expires between the check and the write can only remove its own intent, after its own target refused definitively.

**After a withdrawal, the refused target cannot bootstrap from that intent.** Its tablet refused under the action lock, so the RPC did not start MySQL; no `START` ran at the refusal, and none can start without the action lock. VTOrc sent that token in one RPC only (the gRPC client does not retry). An RPC of an older intent that still waits for the tablet's lock is refused by the token check, since the shard record no longer holds its token, unless the tablet's topology does not answer within a second, which skips the check as before (`stale_rpc_timeout`). Withdrawing does not change that exposure: an RPC that skips the check could already bootstrap next to the live intent's own bootstrap.

### The model

`GRSafety.tla` models the definitive refusal (reply -2, only on the GTID check with no `START` running), the withdrawal with its compare-and-swap (`WITHDRAW_ON_REFUSAL`), and two unsafe variants that validate the conditions (README, "Withdrawing a refused intent"):

| Configuration | What | Outcome |
|---|---|---|
| `withdraw_any` | VTOrc withdraws after any error or timeout (`WITHDRAW_ANY_FAILURE`) | `NoDualBootstrap` violated, 14 states: the RPC times out while MySQL's bootstrap of s1 runs; VTOrc withdraws, bootstraps s2, and s1's bootstrap completes |
| `withdraw_starting` | the tablet reports its `UNAVAILABLE` after waiting for a `START` as definitive (`DEFINITIVE_WHILE_STARTING`) | `NoDualBootstrap` violated, 19 states: a bootstrap `START` of an earlier RPC still runs on s2 when the next RPC gives up waiting for it; VTOrc withdraws, bootstraps s1, and s2's `START` completes |
| `refusal_stuck` | stuck-state check, without the withdrawal | deadlock, 14 states: the candidate restarts and refuses; the voter that holds the transaction is fenced until the intent expires |
| `refusal_withdraw` | as `refusal_stuck`, with the withdrawal, every invariant | no stuck state, no violation: 5.4M states, exhaustive |
| `withdraw_orcs` | two VTOrcs, lease expiry, two crashes (a definitive refusal is reachable), every invariant | no violation, 23.9M states, exhaustive (22 minutes) |

`current`, `tablet`, `orcs`, `orcs_stall`, `stale_rec`, `stale_rpc_fixed`, `s7d_r3_adopt` and the `integrated` simulation pass with the withdrawal on (README, "Results"). The model also had an artifact that weakened `NoDualBootstrap`: the provenance of a bootstrap `START` (the recorded incarnation it started from) was overwritten by a later RPC that waited for it, and cleared when that RPC gave up; it is now kept for as long as the `START` runs.

### Tests

Each fails without its change (checked by mutation: the start-in-progress check removed, the refusal never marked, the token or active-member refusals marked definitive; VTOrc not withdrawing, withdrawing on any error, not asking for the report; the compare-and-swap without the token, without the incarnation, without the lock check; `grpctmserver` not converting or ignoring the request field, `grpctmclient` not converting): `TestBootstrapRefusalForMissingTransactionsIsDefinitive`, `TestBootstrapRefusalIsNotDefinitiveWhileStartInProgress`, `TestBootstrapGroupReplicationWithdrawsIntentOfDefinitiveRefusal` (a definitive refusal, a plain refusal, a timeout, a transport error, a canceled context; the next pass bootstraps the voter that holds the lost transaction only after a definitive refusal), `TestWithdrawGroupReplicationBootstrapIntent`, and the `tmrpctest` round trip. They pass 20 times in a row under `-race` (the `tmrpctest` suite in 20 separate runs: its fake keeps package-level flags that fail any `-count` above 1, before this change too).

`TestGroupReplicationWithdrawsRefusedBootstrapIntent` (end to end, MySQL 8.4.11, three voters, VTOrc): the primary commits 100 rows while the candidate's applier is blocked (`FLUSH TABLES WITH READ LOCK`), and the candidate leaves its group with them in its relay log; an intent of an earlier pass names the candidate; the group loses its last member. VTOrc chooses the candidate again and sends the bootstrap, which waits for the candidate's action lock (`SleepTablet`, 25s) while its mysqld is killed and restarted by `mysqld_safe` (`relay_log_recovery` discards the backlog). The tablet then refuses.

### Results

**End to end** (`TestGroupReplicationWithdrawsRefusedBootstrapIntent`, one run each):

| Binaries | VTOrc's intent → bootstrap of the primary recorded | Refusal → bootstrap |
|---|---|---|
| Before (8a5fdb3) | 121.2s | 99.8s: the intent fenced the primary until it expired |
| After (b8c4855) | 24.2s (of which 21s is the test's hold of the candidate's lock) | 1.7s: VTOrc withdrew the intent and bootstrapped the primary on its next pass, 0.4s after the refusal |

`TestGroupReplicationLifecycle` (new primary in the topology 6.6s after the primary's mysqld was killed) and `TestGroupReplicationMigratesShardByShard` pass.

**Chaos** (binaries of b8c4855, one host, runs one after the other, `/home/ubuntu/chaos-fixes3/`, `CHAOS_DURABILITY=group_replication_cross_cell`). "Before" is the latest runs in "Bootstrap candidate and superseded bootstrap intents".

| Scenario | Before | After | Acked writes lost | Violations |
|---|---|---|---|---|
| G12, a voter rejoins while the primary's cell is isolated (`CHAOS_RACE_OFFSETS=500ms`, 6 cycles) | 1 run: unavailable 145.9s; 3 cycles lost the majority (outage 36.8s, 32.9s, 35.9s each) | 1 run: unavailable 186.4s, longest gap 43.6s; 4 cycles lost the majority (40.8s, 35.9s, 35.4s, 43.6s) and 2 kept it. VTOrc bootstrapped 4 times; no bootstrap was refused, so nothing was withdrawn | 0/14753 | 0 |
| S7d, flapping primary | 2 runs: unavailable 44.2s and 60.0s, longest gap 35.2s and 51.0s | 1 run: unavailable 42.6s (4 outages, 9.0–15.1s), longest gap 15.1s; the group kept its majority, no bootstrap | 0/4392 | 0 |

Neither scenario produces a definitive refusal: in both, the candidate holds every transaction in its binlog. The runs check that the change costs nothing elsewhere; the outage of a cycle that loses the majority is as before (35–44s, dominated by MySQL's leaves in ERROR), and G12's total grows with the number of such cycles, which the race offset leaves to chance (3 of 6 before, 4 of 6 now).

### Still open

- **A bootstrap RPC that fails without a definitive refusal still fences the other voters for two minutes.** The most likely form of the delay is a candidate whose mysqld is down, or restarting, when the RPC arrives: the tablet's first MySQL read fails, and VTOrc cannot tell that failure from a lost reply. Likewise when the candidate restarts after an RPC that failed: VTOrc no longer chooses it (it lacks the transactions), so no RPC is ever refused. The model shows that state (`WaitsForExpiry` accepts it as the designed wait). A follow-up could send the bootstrap again to the target of a live intent that is reachable but no longer a candidate, with a fresh intent for the same target: its tablet refuses definitively, since it lacks the required set, and the next pass chooses the right voter. It is safe by the tablet's checks, but changes the candidate rule, and is not done here. **Fixed** without a fresh intent and without changing the candidate rule: see "Re-probing the target of a stale bootstrap intent".
- **A candidate whose applier cannot apply its relay log** applied it for up to `groupReplicationRelayLogApplyTimeout` (30s), the same as VTOrc's RPC timeout (`--wait-replicas-timeout`, 30s): VTOrc timed out first, and the intent stayed. **Fixed:** the apply now ends `groupReplicationRefusalReserve` (5s) before the RPC's deadline, which leaves time to stop the applier, read MySQL's status again and return the definitive refusal (`TestBootstrapRefusalReachesCallerWhenRelayLogApplyStalls` fails without it: the refusal returned only once the caller's deadline had passed). The end-to-end test avoids the apply: the restart discards the relay log, and the refusal is immediate.
- The token check is skipped when the tablet's topology does not answer within a second (`stale_rpc_timeout`), as before.

## Re-probing the target of a stale bootstrap intent

### The delay

The withdrawal (previous section) lifts the fence of an intent only when its own bootstrap RPC is refused definitively. When that RPC fails otherwise (it times out, or reaches the tablet while mysqld is down and fails on its first MySQL read), VTOrc keeps the intent, as it must: it cannot tell that failure from a lost reply. If the target's mysqld then restarts, `relay_log_recovery` discards the transactions the target held only in its relay log. Once it answers again, the target is reachable, in no group and runs no `START`, but VTOrc no longer chooses it: another voter holds those transactions. Nothing ever refused the intent's bootstrap, so the intent fenced the bootstrap of that voter until it expired, two minutes after VTOrc recorded it. The TLA+ model's stuck-state check shows it (`reprobe_stuck`, below); `TestGroupReplicationReprobesStaleBootstrapIntent` reproduces it on MySQL 8.4: 122.3s from the intent to the bootstrap (see "Results").

### Fix (d849c86, model 24e0d2f)

When a live intent names a voter other than the candidate VTOrc chose (`staleGroupBootstrapIntentTarget`), VTOrc first sends that intent's own bootstrap to it again (`runGroupBootstrap`), at most once per pass, if all of these hold on that pass:

| Condition | Why |
|---|---|
| The target's tablet answered `FullStatus` | Every voter must answer for a bootstrap anyway. |
| Its MySQL is not an active member (ONLINE, RECOVERING) | A member refuses the bootstrap without a definitive refusal, and a group it formed is `GroupBootstrapNotRecorded`'s to adopt. (`GroupNotBootstrapped` does not run while any member is active.) |
| No `START GROUP_REPLICATION` runs on it | That `START` may still form a group of one; the intent keeps fencing, also once VTOrc's 10s grace for a `START` passed. The tablet would wait for it and refuse definitively only after MySQL left what it formed, but VTOrc does not interrupt it. |
| It is still a voter with a tablet record | A tablet that is not a voter takes no part in the bootstrap. |
| The intent has a token | The tablet's token check and the withdrawal's compare-and-swap need it; an intent of a component without tokens keeps fencing until it expires. |

The request carries the intent's token, the incarnation it was recorded for, the required set computed on this pass, and `report_definitive_refusal`. VTOrc does not write the intent: its time, and the fence, are not extended. The tablet needs no change. Its outcomes:

| Outcome | VTOrc |
|---|---|
| Definitive refusal (MySQL lacks a required transaction, no `START` runs) | Withdraws the intent (compare-and-swap on the token, under the shard lock it re-checks), reads the shard record and every voter again, and bootstraps the candidate in the same pass: the read takes milliseconds, and the next pass would add about a second. |
| The target bootstraps (MySQL executed every required transaction, as the tablet checks right before `START`) | Records the group as the intent's (compare-and-swap), and makes the other voters join it, as after the intent's first RPC. |
| Any other failure | Tries the adoption, and keeps the intent, as before. |

### Why it is safe

- **A target that bootstraps** holds every transaction that any voter executed or received, in its binlog: the tablet checks the required set under its action lock, right before MySQL's `START`. It is as safe as a bootstrap of the candidate. The required set is needed: the model's `reprobe_noreq` variant, which sends the re-probe without it, loses an acknowledged write (below).
- **The definitive refusal still proves that no bootstrap starts from the intent,** although VTOrc sent the intent's token twice. The refusal proves, under the action lock, that the re-probe started nothing and that no `START` runs. The first RPC failed before VTOrc sent the re-probe. If its handler still waits for the action lock, it gets it after the refusal: once the intent is withdrawn, its token check refuses it (unless the tablet's topology does not answer, the known `stale_rpc_timeout` exposure, which the re-probe does not change); before that, its own required set refuses it: computed on an earlier pass, it holds what the target lacks now, since the union of the voters' executed and received sets only shrinks while no group runs.
- **Adoption.** A target in a group is never re-probed; `GroupBootstrapNotRecorded` adopts its group.
- **Two VTOrcs, and a VTOrc that resumes after its lease expired.** The re-probe runs under the shard lock, and every write after it, the withdrawal and the candidate's intent, re-checks the lock and is a compare-and-swap. A re-probe of an intent that another VTOrc wrote is the same RPC as that VTOrc's own. A stalled VTOrc that sends a re-probe after another VTOrc withdrew the intent gets the token check's refusal, which is not definitive; one whose lease expired cannot withdraw.
- **The stale-intent token check** on the tablet is what refuses a re-probe of an intent that was superseded meanwhile; that refusal is not definitive, and VTOrc keeps whatever the shard record holds.

### The model

`GRSafety.tla` adds the re-probe to `OBegin` (`REPROBE_STALE_INTENT`), a reply matched to the RPC VTOrc waits for (a re-probe reuses the VTOrc and the token, as gRPC calls do not share replies), a re-probe budget (`MaxProbe`), and a checking mode, `EXPIRY_WAIT_OK`, that accepts the wait for an intent's expiry as before. The code bootstraps the candidate in the same pass; the model ends the recovery after the withdrawal, which includes the code's continuation (README, "Re-probing a stale intent's target").

| Configuration | What | Outcome |
|---|---|---|
| `reprobe_stuck` | stuck-state check, without the re-probe, the wait not accepted | deadlock, 13 states: VTOrc chooses s2; s2's mysqld restarts without the transaction; the intent's RPC times out; only s3 holds the transaction, and the intent for s2 fences it until it expires |
| `reprobe` | as `reprobe_stuck`, with the re-probe, every invariant | no stuck state, no violation: 5.37M states, exhaustive |
| `reprobe_noreq` | unsafe variant: the re-probe without the required set (`REPROBE_NO_REQ`, two transactions) | `NoLostAck` violated, 17 states: s1 commits transaction 1; the group decides 2 while s1 crashes; s2 receives both; VTOrc chooses s2, s2 restarts without them, the RPC times out; the next pass re-probes s2, which bootstraps without 1 |
| `withdraw_orcs` | two VTOrcs, lease expiry, two crashes, with the re-probe, every invariant | no violation, 24.0M states, exhaustive (23 minutes; 23.9M without the re-probe) |

`current`, `tablet`, `orcs`, `orcs_stall`, `stale_rec`, `stale_rpc_fixed`, `s7d_r3_adopt` (now without accepting the wait for an intent's expiry) and `s7d_r2_ma_off` pass with the re-probe on, with the same state counts as before: the re-probe needs a second crash. The `tablet_tx2`, `integrated` and `integrated_core` simulations find nothing, and every validation configuration still finds its bug (README, "Results").

### Tests

Each fails without its change (checked by mutation, `/home/user/vtlab/fixes4/mut/vtorc.txt`: no re-probe; the next pass instead of the same pass after the withdrawal; the intent rewritten for the re-probe; the intent withdrawn after any failure of the re-probe; each condition of the table removed; a target's bootstrap not recorded; the re-probe without the required set; without the token): `TestBootstrapGroupReplicationReprobesStaleIntentTarget` (a definitive refusal, then the candidate's bootstrap in the same pass; a timeout, a transport error and a refusal that is not definitive keep the intent unchanged, its time included; a target that bootstraps is recorded; at each re-probe, the request carries the intent's token and incarnation and the current required set, and the shard record holds the intent unchanged), `TestStaleGroupBootstrapIntentTarget` (each condition), `TestBootstrapGroupReplicationFencedByIntent` (a `START` in progress on the intent's target, past VTOrc's grace, keeps the fence, without a re-probe), and `TestBootstrapGroupReplicationWithdrawsIntentOfDefinitiveRefusal`, whose second pass now re-probes the candidate after a failure that kept the intent, and bootstraps the voter that holds the lost transaction. They pass 20 times in a row under `-race`.

`TestGroupReplicationReprobesStaleBootstrapIntent` (end to end, MySQL 8.4.11, three voters, VTOrc) shares the setup of `TestGroupReplicationWithdrawsRefusedBootstrapIntent`: the candidate holds the primary's last 100 transactions in its relay log only, and an intent of an earlier pass names it. VTOrc's bootstrap RPC waits for the candidate's action lock (`SleepTablet`, 10s) while its mysqld is killed, and `mysqld_safe` is stopped (`SIGSTOP`) so that it does not restart it: the RPC fails on its first MySQL read, and VTOrc keeps the intent. `mysqld_safe` then resumes and restarts mysqld, which discards the relay log.

### Results

**End to end** (`TestGroupReplicationReprobesStaleBootstrapIntent`, one run each; "before" is 5521e0c, "after" d849c86):

| Binaries | Candidate's mysqld back → bootstrap of the primary recorded | VTOrc's intent → bootstrap recorded |
|---|---|---|
| Before (5521e0c) | 110.1s: the intent fenced the primary until it expired | 122.3s |
| After (d849c86) | 3.0s: on the first pass that read every voter, VTOrc re-probed the candidate, which refused definitively within 60ms; VTOrc withdrew the intent and bootstrapped the primary in the same pass (recorded 1.2s later) | 16.2s (of which 13.3s until mysqld was back) |

`TestGroupReplicationWithdrawsRefusedBootstrapIntent` (1.7s after the candidate's lock was released, 24.3s after the intent), `TestGroupReplicationLifecycle` (new primary in the topology 7.4s after the primary's mysqld was killed) and `TestGroupReplicationMigratesShardByShard` pass on the same binaries.

**Chaos** (binaries of d849c86, one host, one run each, one after the other, `CHAOS_DURABILITY=group_replication_cross_cell`; the TLC runs were paused meanwhile). "Before" is the latest runs in "Withdrawing the intent of a refused bootstrap".

| Scenario | Before | After | Acked writes lost | Violations |
|---|---|---|---|---|
| G12, a voter rejoins while the primary's cell is isolated (`CHAOS_RACE_OFFSETS=500ms`, 6 cycles) | 1 run: unavailable 186.4s, longest gap 43.6s; 4 cycles lost the majority (40.8s, 35.9s, 35.4s, 43.6s) | 1 run: unavailable 159.0s (16 outages), longest gap 48.8s; 3 cycles lost the majority (32.7s, 36.9s, 48.8s) and 3 kept it. VTOrc bootstrapped 3 times; no bootstrap was refused, and no intent was stale, so nothing was re-probed | 0/16974 | 0 |
| S7d, flapping primary | 1 run: unavailable 42.6s, longest gap 15.1s | 1 run: unavailable 45.2s (5 outages, 2.7–14.6s), longest gap 14.6s; the group kept its majority, no bootstrap | 0/4079 | 0 |

Neither scenario makes an intent stale: in both, the candidate holds every transaction in its binlog, and every bootstrap RPC succeeded. The runs check that the change costs nothing elsewhere; the outages of the cycles that lose the majority are as before (33–49s, dominated by MySQL's leaves in ERROR, which lasted up to 30s, in the fourth cycle).

### Still open

- A re-probe that fails without a definitive refusal keeps the intent, like the first RPC; VTOrc re-probes on each pass while the target answers. A target whose tablet holds its action lock, or does not answer `StartGroupReplication`, makes each pass wait up to the RPC's timeout (`--wait-replicas-timeout`, 30s) under the shard lock, until the intent expires.
- While the target does not answer, or runs a `START`, the intent fences the other voters until it expires, as before.
- The token check is skipped when the tablet's topology does not answer within a second (`stale_rpc_timeout`), as before.

## Chaos sweep (f528e9a)

Every chaos scenario that applies to Group Replication, run again on the binaries of f528e9a (MySQL 8.4.11, the MySQL communication stack, `READ_ONLY`, the 2s unreachable majority timeout, VTOrc failover grace 30s), `CHAOS_DURABILITY=group_replication_cross_cell`, with semi-sync `cross_cell` baselines in the same environment. The runs share one 4-core host with two other workloads and take turns on a CPU lock, so no two chaos runs overlap. G12 runs with `CHAOS_RACE_OFFSETS=500ms`. Not run: S11, S11k, S11b, S11c, S12*, S13* (see "Not tested").

Runs: two per GR scenario (three for the detection scenarios); the plan was three, but the third round was replaced by a round on the binaries of the fixes below, once they are merged ("After the fixes (pending)"). Raw results: `/home/ubuntu/chaos-sweep/<run>/` (report, events, gzipped logs); per-run summaries `/home/user/vtlab/sweep/results.jsonl`, built by `analyze.py`; the table by `table.py`.

Columns: "without an acked write" is the sum of the outages of at least 1s, "longest gap" the longest interval without an acknowledged write (both min / median / max over the runs); "runs that lost the majority" counts runs with an interval of at least 10s in which no tablet was ONLINE in a view of at least two members; "bootstraps" counts VTOrc's bootstrap RPCs; refusals, withdrawals, re-probes and adoptions count VTOrc's log lines for each (see "Withdrawing the intent of a refused bootstrap" and "Re-probing the target of a stale bootstrap intent"). "Earlier" is the latest longest gap reported above for the scenario.

### Results

0 acknowledged writes lost in all 73 GR runs (334,075 acknowledged writes; 64 sweep runs and the 9 detection runs below). Every GR run converged, with all three members ONLINE.

| Scenario | Runs | Without an acked write, min / median / max (s) | Longest gap, min / median / max (s) | New primary in topo (s) | Lost / acked | Violations per run | Runs that lost the majority | Bootstraps per run | Refusals / withdrawals / re-probes / adoptions | Earlier longest gap (s) |
|---|---|---|---|---|---|---|---|---|---|---|
| D1 | 3 | 7.2 / 7.4 / 7.8 | 7.24 / 7.40 / 7.76 | 7.2 / 7.5 / 7.8 | 0/9580 | 0, 0, 0 | 0 | 0, 0, 0 | 0 / 0 / 0 / 0 | – |
| D1h | 3 | 7.3 / 7.4 / 8.0 | 7.33 / 7.36 / 8.00 | 7.2 / 7.5 / 8.0 | 0/12485 | 0, 0, 0 | 0 | 0, 0, 0 | 0 / 0 / 0 / 0 | – |
| G1x | 2 | 7.7 / 7.7 / 7.8 | 7.65 / 7.72 / 7.79 | 7.0 / 7.4 / 7.8 | 0/6391 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | 7.5 |
| G3D | 2 | 0.0 / 0.0 / 0.0 | 0.06 / 0.06 / 0.06 | – | 0/4000 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | – |
| G3E | 2 | 7.8 / 8.4 / 8.9 | 7.82 / 7.83 / 7.83 | 7.5 / 7.5 / 7.5 | 0/9143 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | 8.5 |
| G9b | 2 | 21.6 / 25.3 / 29.1 | 21.61 / 23.42 / 25.23 | 21.7 / 23.2 / 24.7 | 0/7208 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | 22.2 |
| G9h | 3 | 19.0 / 23.5 / 26.4 | 18.96 / 23.52 / 26.36 | 19.0 / 23.5 / 26.5 | 0/12331 | 0, 0, 0 | 0 | 0, 0, 0 | 0 / 0 / 0 / 0 | – |
| G11 | 2 | 105.6 / 107.1 / 108.6 | 105.56 / 107.06 / 108.57 | – | 0/7184 | 0, 0 | 2 | 1, 1 | 0 / 0 / 0 / 0 | 108.9 |
| G11k | 2 | 111.6 / 113.1 / 114.6 | 111.64 / 113.14 / 114.64 | – | 0/6996 | 0, 0 | 2 | 1, 1 | 0 / 0 / 0 / 0 | 115.6 |
| G11s | 2 | 112.1 / 112.2 / 112.3 | 112.13 / 112.20 / 112.28 | – | 0/7203 | 0, 0 | 2 | 1, 1 | 0 / 0 / 0 / 0 | – |
| G12 | 2 | 120.6 / 198.2 / 275.8 | 33.80 / 53.00 / 72.20 | 8.6 / 58.7 / 108.7 | 0/37977 | 0, 0 | 2 | 4, 2 | 0 / 0 / 0 / 0 | 48.8 |
| G13 | 2 | 0.0 / 0.0 / 0.0 | 0.53 / 0.64 / 0.74 | – | 0/17102 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | – |
| S1 | 2 | 7.0 / 7.2 / 7.5 | 6.97 / 7.24 / 7.52 | 6.8 / 7.0 / 7.2 | 0/4464 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | 7.9 |
| S1b | 2 | 6.9 / 7.1 / 7.3 | 6.94 / 7.13 / 7.33 | 6.8 / 7.0 / 7.2 | 0/3776 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | – |
| S2 | 2 | 9.0 / 9.1 / 9.1 | 9.04 / 9.05 / 9.06 | 6.5 / 6.6 / 6.8 | 0/7332 | 1, 1 | 0 | 0, 0 | 0 / 0 / 0 / 0 | 9.1 |
| S3 | 2 | 9.0 / 9.0 / 9.1 | 9.04 / 9.04 / 9.05 | 7.5 / 7.6 / 7.8 | 0/7469 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | 9.1 |
| S3hb | 2 | n/a (no writes) | n/a | 6.5 / 6.9 / 7.2 | 0/978 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | – |
| S4 | 2 | 7.4 / 7.4 / 7.5 | 7.37 / 7.41 / 7.45 | 7.2 / 7.2 / 7.2 | 0/8768 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | 6.8 |
| S5 | 2 | 225.5 / 225.5 / 225.6 | 225.49 / 225.53 / 225.57 | 225.0 / 225.1 / 225.2 | 0/4750 | 0, 0 | 2 | 1, 1 | 0 / 0 / 0 / 0 | 225.0 |
| S5b | 2 | 225.6 / 225.6 / 225.6 | 225.55 / 225.59 / 225.64 | 225.2 | 0/4776 | 0, 0 | 2 | 1, 1 | 0 / 0 / 0 / 0 | 225.0 |
| S6 | 2 | 0.0 / 0.0 / 0.0 | 0.06 / 0.09 / 0.12 | – | 0/11004 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | – |
| S6b | 2 | 0.0 / 0.0 / 0.0 | 0.06 / 0.07 / 0.08 | – | 0/11008 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | – |
| S7 | 2 | 114.8 / 115.6 / 116.5 | 106.92 / 111.70 / 116.48 | 7.5 / 7.8 / 8.0 | 0/4456 | 0, 0 | 2 | 1, 1 | 0 / 0 / 0 / 0 | – |
| S7b | 2 | 114.5 / 115.7 / 117.0 | 114.48 / 115.75 / 117.01 | 7.2 / 7.5 / 7.8 | 0/4364 | 0, 0 | 2 | 1, 1 | 0 / 0 / 0 / 0 | – |
| S7c | 2 | 21.1 / 28.0 / 34.9 | 9.22 / 10.28 / 11.33 | 24.0 / 34.9 / 45.8 | 0/9940 | 0, 1 | 0 | 0, 0 | 0 / 0 / 0 / 0 | – |
| S7d | 2 | 41.2 / 49.8 / 58.5 | 13.36 / 31.40 / 49.44 | 7.8 / 7.9 / 8.0 | 0/7550 | 0, 0 | 1 | 1, 0 | 0 / 0 / 0 / 1 | 69.2 |
| S8 | 2 | 7.4 / 7.4 / 7.4 | 7.41 / 7.42 / 7.43 | 7.5 / 7.5 / 7.5 | 0/17203 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | 7.7 |
| S8b | 2 | 7.1 / 7.4 / 7.7 | 7.09 / 7.37 / 7.65 | 6.9 / 7.2 / 7.5 | 0/8340 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | 7.7 |
| S9 | 2 | 7.7 / 7.7 / 7.7 | 7.65 / 7.69 / 7.73 | 7.5 / 7.6 / 7.8 | 0/8754 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | 7.7 |
| S9b | 2 | 25.6 / 27.3 / 29.0 | 25.56 / 27.26 / 28.96 | 25.8 / 27.4 / 29.0 | 0/7334 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | 23.1 |
| S9i | 2 | 9.0 / 10.8 / 12.5 | 9.04 / 9.04 / 9.05 | 7.5 / 7.8 / 8.0 | 0/7341 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | 9.1 |
| S10 | 2 | 7.5 / 7.6 / 7.8 | 7.45 / 7.61 / 7.76 | 7.5 / 7.6 / 7.8 | 0/18736 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | 7.7 |
| V1 | 2 | 0.0 / 0.0 / 0.0 | 0.83 / 0.86 / 0.90 | – | 0/17756 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | – |
| V2 | 2 | 7.7 / 10.1 / 12.6 | 7.28 / 7.47 / 7.67 | 6.9 / 7.1 / 7.4 | 0/12033 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | 7.6 |
| V3 | 2 | 7.2 / 7.4 / 7.6 | 7.20 / 7.40 / 7.60 | 7.4 / 7.5 / 7.6 | 0/8343 | 0, 0 | 0 | 0, 0 | 0 / 0 / 0 / 0 | 7.4 |
| semi-sync D1 | 3 | 1.6 / 1.6 / 3.3 | 1.60 / 1.60 / 3.32 | 1.8 / 1.8 / 3.5 | 0/9930 | 0, 0, 0 | – | – | 0 / 0 / 0 / 0 | – |
| semi-sync D1h | 3 | 90.2 / 90.3 / 90.3 | 90.20 / 90.28 / 90.34 | 90.2 / 90.5 / 90.5 | 0/12640 | 0, 0, 0 | – | – | 0 / 0 / 0 / 0 | – |
| semi-sync G3D | 1 | 2.2 | 2.24 | 2.2 | 0/7878 | 1 | – | – | 0 / 0 / 0 / 0 | – |
| semi-sync G3E | 1 | 12.0 | 12.04 | 11.8 | 0/4952 | 5 | – | – | 0 / 0 / 0 / 0 | – |
| semi-sync S1 | 1 | 1.6 | 1.60 | 1.8 | 0/2216 | 0 | – | – | 0 / 0 / 0 / 0 | – |
| semi-sync S2 | 1 | 12.1 | 12.05 | 11.2 | 0/3928 | 1 | – | – | 0 / 0 / 0 / 0 | – |
| semi-sync S3 | 1 | 11.2 | 11.24 | 11.5 | 0/4008 | 2 | – | – | 0 / 0 / 0 / 0 | – |
| semi-sync S4 | 1 | 73.2 | 73.15 | 70.8 | 0/3760 | 2 | – | – | 0 / 0 / 0 / 0 | – |
| semi-sync S5 | 1 | 120.3 | 120.35 | 120.5 | 0/3832 | 0 | – | – | 0 / 0 / 0 / 0 | – |
| semi-sync S5b | 1 | 238.5 | 238.49 | – | 0/1100 | 0 | – | – | 0 / 0 / 0 / 0 | – |
| semi-sync S7d | 1 | 45.7 | 12.01 | 11.2 | 0/4358 | 4 | – | – | 0 / 0 / 0 / 0 | – |
| semi-sync S9 | 1 | 187.7 | 186.65 | 186.8 | 0/2916 | 5 | – | – | 0 / 0 / 0 / 0 | – |
| semi-sync S9b | 1 | 221.5 | 221.50 | – | 0/2684 | 0 | – | – | 0 / 0 / 0 / 0 | – |
| semi-sync S9i | 1 | 133.6 | 133.60 | 130.8 | 0/2660 | 1 | – | – | 0 / 0 / 0 / 0 | – |
| semi-sync S10 | 1 | 11.7 | 11.68 | 11.8 | 0/3192 | 0 | – | – | 0 / 0 / 0 / 0 | – |
| semi-sync V2 | 1 | 93.4 | 93.41 | 90.9 | 0/3184 | 5 | – | – | 0 / 0 / 0 / 0 | – |

The semi-sync rows are baselines in the same environment (one run each; D1 and D1h three; S5's first run failed in the harness's setup, `CreateShard` racing with a vttablet, and was run again).

**Against the audit's numbers** (the "Earlier" column; no run exceeded 1.5× the earlier maximum except G12 r1, below):

- **Single failovers** (S1, S1b, S3, S4, S8, S8b, S9, S9i, S10, V2, V3, G1x, G3E): longest gap 6.9–9.1s, new primary in the topology 6.8–8.0s after the fault, as before. The total without an acknowledged write is larger than the gap in three runs, S9i r1 (12.5s), V2 r1 (12.6s) and G3E r1 (8.9s), from later outages of 1.1–3.6s (V2 r1: client writes failing with `invalid connection` 6s after the new primary served); they were not investigated further.
- **No failover** (S6, S6b, V1, G3D, G13): no outage of 1s or more; G3D's stale `SetReplicationSource` is still refused (cb582df).
- **The group cannot elect** (S5, S5b: the primary and an acker down; S7, S7b: two primaries killed in a row; G11, G11s, G11k: a voter leaves, then the primary dies): every run lost the majority, VTOrc bootstrapped once, after every voter was back, on the voter holding every transaction; the outages are the scenarios' own (225s, 115–117s, 106–115s) and match the audit.
- **Flapping** (S7c, S7d): S7c 21.1s and 34.9s without an acknowledged write (gaps 9.2s and 11.3s); S7d 58.5s and 41.2s (gaps 49.4s and 13.4s); r1 lost the majority once and was bootstrapped, r2 kept it. Within the audit's range (S7d gaps 9.1–69.2s).
- **NEW-4 moves** (S9b, G9b): 25.6s and 29.0s, 25.2s and 21.6s, against 20.2–23.1s before. The move waits for VTOrc to detect `GroupPrimaryNotInTopo`, which a hung or dead cell topology delays (see "VTOrc detection while another cell's topology server hangs").
- **G12** (6 cycles each): r1 275.8s without an acknowledged write, longest gap 72.2s; r2 120.6s and 33.8s. Earlier: 159–186s and 43.6–48.8s. r1 is 1.5× the earlier maximum; its three longest outages are SW-2 below. In r2, 2 cycles lost the majority (33.8s, 31.5s) and 4 kept it.
- **Bootstrap intents**: no bootstrap was refused, no intent was withdrawn, and nothing was re-probed in any run. VTOrc adopted a group once (S7d r1): its bootstrap RPC reached the old primary just before the next isolation, which cut off that tablet's cell; the tablet bootstrapped without checking the intent, since its topology did not answer (the known `stale_rpc_timeout` exposure, "Still open" above; the intent was the current one), and VTOrc, cut off from it, adopted the group 11s later, after the heal.

**Violations.** Three runs reported one violation each, and none lost or committed a write on a deposed primary:

| Run | Violation | Classification |
|---|---|---|
| S2 r1 | 2 samples (33ms) of two writable PRIMARY tablets, right after the frozen old primary resumed | The known window ("S2 violation" above): its stale view still shows itself ONLINE primary of 3, its tablet steps down 0.06–0.14s later, and the commits it resumes are rolled back once MySQL learns it was expelled (`errno 3100`, "unable to be certified and will now rollback"). |
| S2 r2 | 2 samples (25ms), same | Same window. It recurred in 2 of 2 runs (and once in the semi-sync baseline, 198ms). |
| S7c r2 | 27 samples (5.2s) of two writable PRIMARY tablets | Same mechanism, longer. The isolated primary zone1 was healed after 5.02s; the other members had already suspected it, and expelled it 3.3s after the heal (19:21:57.98) although they saw it reachable again. zone1 never received that view: the next isolation started 0.77s later, and it kept `read_only=OFF`, its tablet PRIMARY and a view of 3 ONLINE members until the following heal, when MySQL put it in ERROR and rolled back its blocked commits (19:22:04.04). The new primary was writable from 19:21:58.97. 5 primary reads answered by zone1 after the election, none after the topology change. |

The semi-sync baselines reproduce the audit's semi-sync findings in this environment, with 0 acknowledged writes lost: S3 11.0s of two writable primaries and the old primary left `super_read_only=OFF`; S4 73.2s down and errant GTIDs; S7d 4 violations; S9 186.7s down, S9i 133.6s and errant GTIDs; S9b no failover, also within the 90s after etcd returned (221.5s without an acknowledged write; the audit saw one 58s after); V2 93.4s down with 3.0s of two writable primaries; G3D the old primary left `super_read_only=OFF`; G3E 21.0s of two writable primaries and an old primary that never reconverged; D1h no failover until the hung topology answered. S2 shows the same short window as GR (198ms). S5 failed over 0.4s after the hung replica resumed (120.3s); S5b did not fail over, also after the replica's isolation healed, until the writers stopped (238.5s).

### Found by the sweep

Both were found by the sweep and are fixed on `gr-fixes5` (pending merge); the numbers above are from f528e9a, before those fixes.

**SW-1. VTOrc configures asynchronous replication on a voter whose join is in progress (G13 r1 and r2).** When a voter is not an active member, VTOrc's `GroupMemberNotOnline` makes it join the group. While a `START GROUP_REPLICATION` runs on it, that analysis does not match (`GroupStartInProgress`), and while VTOrc's own join runs, its recovery is skipped (`GroupJoinInFlight`). The next analysis for the tablet then matches: `NotConnectedToPrimary`, and `ReplicationStopped` after it, which only require that MySQL is not an active member (`analysis_problem.go`). Their recovery, `fixReplica` (`topology_recovery.go`), has no Group Replication guard: it sets the tablet read-only and calls `SetReplicationSource` with the shard primary, which runs `CHANGE REPLICATION SOURCE TO` on the default channel and `START REPLICA`.

In G13 r1 (`/home/ubuntu/chaos-sweep/gr-G13-r1/`), the secondary zone2 was cut off from the other members, left its group, and VTOrc's join of 13:54:54 failed after its 30s deadline, while MySQL's `START` kept running. All three VTOrcs then ran `FixReplica` on zone2 (13:55:24, 13:55:36, 13:55:37), followed by `ReplicationStopped` recoveries (13:55:38, 13:55:39). MySQL configured the default channel and refused to start its threads (`MY-011537`/`MY-011539`: "Can't start replica IO THREAD of channel '' when group replication is running with single-primary mode and the primary member is not known"). zone2 was ONLINE again at 13:55:43, but it answered no replica read until the writers stopped at 13:56:02, 19.7s later; in the earlier G13 run it was back 2s after it was ONLINE. A completed join removes the default channel (`finishGroupJoinLocked`), but this join completed in MySQL after the RPC had failed, so nothing removed it, and the tablet's lag poller only measures a member through the group when MySQL has no default channel (`poller.Status`): it reported the lag of an asynchronous replica whose threads were stopped, growing since its last measurement, above vtgate's `--discovery-low-replication-lag` (5s). Not lost writes or a second primary: MySQL refuses the channel's threads while Group Replication runs. But the member stays out of replica reads, and, after the tablet's unhealthy threshold, unhealthy.

The recovery runs in most scenarios that take a voter out of its group (`/home/user/vtlab/sweep/fixscan.py`, after each scenario's start): `FixReplica` on a voter in 50 of the 73 GR runs of the sweep, 111 times (G12 r1 43, G13 r1 6, G13 r2 7, S7d r2 6), mostly once, while the old primary rejoined. Usually its RPCs waited for the tablet's action lock, which the tablet's own join held, and reached the tablet once MySQL was RECOVERING or ONLINE, and the tablet left the replication source alone ("MySQL is an active group replication member, not changing its replication source"); 6 failed when the tablet's `SetReadOnly` timed out (in S1 r1, after waiting 15s for the action lock, under the shard lock). The default channel was configured in 14 runs (60 times, G12 r1 37), and left configured at the end in both G13 runs: in G13 r2, the cut-off secondary zone3 (configured at 21:35:34) answered no replica read after the heal either, against 289 for the other secondary. The tablet's own step-down from a stale primary (`shard_sync.go`, "Another tablet ... has won primary election", then `SetReplicationSource`) configures the default channel of a voter the same way (S7c r2 zone1 at 19:22:05; S2); there, the join that followed removed it.

**SW-2. VTOrc's join after a bootstrap tries a voter that is stuck in its own join first (G12 r1).** After VTOrc bootstraps a group, it makes the other voters join it right away, through `StartGroupReplication` (d6a5d08). That RPC joins with the tablet's seeds sorted by address (`groupReplicationSeeds`): the tablet only moves the members it saw active to the front (`preferSeeds`) after its own check before a join (`legitimateGroupActiveElsewhere`), which the RPC path does not run. In G12, the voter restarted at the start of each cycle has a `START` that was cut off by the isolation and stays blocked until MySQL gives up on its communication engine (90s). When that voter sorts first, the other voter's join waits for it, and MySQL fails the join after 30s (`[GCS] Timeout while waiting for the group communication engine to be ready!`), then joins through the next seed about 6s later.

In G12 r1 (`/home/ubuntu/chaos-sweep/gr-G12-r1/`), 4 of 6 cycles lost the majority, and VTOrc bootstrapped zone3 (port 18921, sorted last) every time. Each of the 4 joins after a bootstrap whose first seed was the voter stuck in its own join timed out after 30s (zone1 13:40:54 → 13:41:24, online 13:41:32; zone2 13:42:19 → 13:42:49, online 13:42:56; zone1 13:44:08 → 13:44:38, online 13:44:45; zone2 13:46:01 → 13:46:31, online 13:46:37); every join whose first seed was the group's member took 2–4s (`g12seeds.py`). In cycles 4–6 that join was the one that restored the majority: their outages were 68.9s, 68.8s and 72.2s, against 33–49s in the earlier G12 runs; in cycle 3 the restarted voter joined first, and the outage was 39.8s. The audit's earlier G12 outlier (lockwait r2 cycle 4, 42.5s, "Timeout while waiting for the group communication engine to be ready") is the same delay. Which voter is bootstrapped, and so whether the stuck voter sorts first, varies between runs.

### After the fixes (pending)

The third round planned on f528e9a was replaced by a round on the fixed binaries, to run once `gr-fixes5` (its fixes 1–7, SW-1 and SW-2 among them) is merged: one run of every GR scenario above, a second run of G12, G13, S7c, S7d, S9b, G9b and G11/G11s/G11k, and the three detection scenarios, compared with these rounds through the same scripts. What each fix must show: G13 with no `FixReplica` on a voter and no default channel configured (`fixscan.py`), and the cut-off secondary back in replica reads once ONLINE; G12 with no join whose first seed is a voter with a `START` in progress, and no `Timeout while waiting for the group communication engine to be ready` (`g12seeds.py`); everything else within the ranges above, with 0 acknowledged writes lost.

## Buffering during an unplanned failover

`TestUnplannedFailoverTimes` (`go/test/endtoend/reparent/grouprepl`, opt-in with `VT_UNPLANNED_FAILOVER_TRIALS`) now measures what clients see during an unplanned failover, with vtgate's buffering on (`--enable-buffer`, the harness default) or off (`VT_UNPLANNED_FAILOVER_BUFFER=on|off|both`). A client sends an insert through vtgate every 50ms, each on its own connection from a pool and in its own goroutine, so a write stuck on a failed primary does not hold back the next ones, and gives up on a write after 20s. The test kills the primary's host (`mysqld_safe`, `mysqld` and `vttablet`, `SIGKILL`) or freezes its `mysqld` (`SIGSTOP`, vttablet alive), and counts, from 1s before the failure until 5s after the first acknowledged write: the writes that failed, those that timed out, vtgate's buffered requests and bufferings (`BufferRequestsBuffered`, `BufferStarts`), the longest acknowledged write, and the time until the first write sent after the failure was acknowledged. Every acknowledged write is checked on the new primary. Semi-sync is `cross_cell` with VTOrc polling every 1s; Group Replication is `group_replication_cross_cell` after `MigrateReplicationMode`. Binaries of f528e9a, one host, MySQL 8.4.11, every trial run alone (`-keep-data=false`).

| Failure | Policy | vtgate buffer | Trials | New primary in topo | First acknowledged write | Failed writes (timed out after 20s) / sent | Buffered requests (bufferings) | Longest acknowledged write | Acknowledged writes lost |
|---|---|---|---|---|---|---|---|---|---|
| Host crash | semi-sync | on | 3 | 2.5–3.4s | 2.5–3.3s | 0 / 170–187 | 49–65 (1) | 2.46–3.26s | 0 of 1083 |
| Host crash | semi-sync | off | 3 | 3.3–3.4s | 3.3–3.4s | 65–66 (0) / 186–187 | 0 | 0.01–0.02s | 0 of 903 |
| Host crash | Group Replication | on | 4 | 6.6–7.4s | 6.8–7.6s | 0 / 258–273 | 131–147 (1) | 6.81–7.61s | 0 of 1781 |
| Host crash | Group Replication | off | 3 | 6.6–7.0s | 7.0–7.4s | 132–140 (0) / 260–270 | 0 | 0.02–0.72s | 0 of 925 |
| mysqld frozen | semi-sync | on | 3 | 11.4–11.8s | 11.4–11.8s | 228–234 (1) / 348–356 | 0 | 0.02–0.05s | 0 of 902 |
| mysqld frozen | semi-sync | off | 3 | 11.3–12.3s | 11.3–12.3s | 226–246 (1) / 346–366 | 0 | 0.02–0.47s | 0 of 900 |
| mysqld frozen | Group Replication | on | 3 | 6.8–7.0s | 7.1–7.6s | 135–140 (1) / 261–273 | 0 | 0.31–0.71s | 0 of 927 |
| mysqld frozen | Group Replication | off | 4 | 6.5–7.1s | 6.8–7.9s | 130–141 (1–5) / 256–278 | 0 | 0.01–0.81s | 0 of 1230 |

**Host crash.** vtgate buffers the whole failover in both modes, once per failover, and no write fails: vtgate's health check loses the primary's vttablet, its next write fails inside vtgate with `primary is not serving, there may be a reparent operation in progress` (`CLUSTER_EVENT`), which starts the buffering, and the new primary's promotion ends it ("Stopping buffering ... after 2.4–3.2s" with semi-sync, "after 6.5–7.3s" with Group Replication, "due to: a primary promotion has been detected"). Group Replication buffers about twice as many writes, for about twice as long: its failure detection takes 5s (`member_expel_timeout=0` plus the 5s suspicion), where VTOrc detects a host that refuses connections on its next poll. The buffered writes are the longest acknowledged writes, 6.8–7.6s (default `--buffer-window` 30s, `--buffer-max-failover-duration` 30s). Without the buffer the same writes fail at once with `no healthy tablet available for 'keyspace:"ks" shard:"0" tablet_type:PRIMARY'`.

**mysqld frozen: neither mode buffers.** The frozen primary's vttablet is alive and keeps reporting a serving PRIMARY to vtgate's health check, so vtgate keeps sending writes to it. They do not fail with a buffering error: they block in MySQL until vttablet gives up (`DeadlineExceeded`, errno 1317, vttablet's query timeout), and once the blocked transactions fill vttablet's transaction pool, the next ones fail at once with `ResourceExhausted ... transaction pool connection limit exceeded` (errno 1203); the client's own 20s timeout ends one or a few writes per trial. vtgate starts buffering only on an error that signals a failover (`buffer.CausedByFailover`: a `CLUSTER_EVENT` error such as `primary is not serving`), which none of these is. When the new primary's tablet reports PRIMARY with a newer term, vtgate routes to it directly: nothing was buffered, so there is nothing to drain. This is the same with semi-sync, whose failover takes 11.3–12.3s here (VTOrc must time out on the frozen MySQL first) against 6.5–7.1s for Group Replication, so Group Replication fails fewer writes (130–141 against 226–246) only because it fails over sooner.

So Group Replication does buffer, as much as semi-sync, when the primary's vttablet goes away. A frozen MySQL behind a live vttablet is not buffered in either mode; making it buffered would need the old primary's vttablet to stop serving (it cannot read its frozen MySQL; the sync loop's status reads time out after 10s, by when the group has elected another member) or vtgate to buffer on the timeout errors, which is not specific to Group Replication.

The 0.3–0.8s longest acknowledged writes of Group Replication without the buffer and in the freeze trials are writes on the new primary in the seconds after the election, which was not investigated further.

## VTOrc detection while another cell's topology server hangs

The NEW-4 runs above showed VTOrc detecting `GroupPrimaryNotInTopo` 9s after it discovered the elected member while a cell's etcd was down. Three new scenarios (`go/test/endtoend/vtorc/chaos/detection_test.go`) measure the detection with every topology server healthy and while one hangs: `SIGSTOP`, so that requests to it wait instead of failing, which is what a hung or partitioned server does. Each kills the primary's `mysqld` and `mysqld_safe`; the hang starts 10s before and ends 90s after the kill, or once the new primary is in the topology.

- **D1**: every topology server healthy.
- **D1h**: the etcd of a replica's cell hangs. In GR mode, it is the cell of the member that the group does not elect, so the new primary's own promotion does not depend on it; the semi-sync run hangs the same replica's cell.
- **G9h** (GR only): the etcd of the elected member's cell hangs, as in G9b, where it was killed.

The times come from the VTOrc logs (`/home/user/vtlab/sweep/det.py`): the first analysis of a primary failure (`DeadPrimary*`), the first `GroupPrimaryNotInTopo`, the first recovery step, and the failed per-cell tablet loads. Binaries of f528e9a, 3 trials each; seconds after the kill.

| Scenario | Policy | New primary in topo | First primary-failure analysis (earliest VTOrc) | Analysis passes | Recovery |
|---|---|---|---|---|---|
| D1 | semi-sync | 1.75, 3.5, 1.75 | +1.4, +1.1, +1.4 | – (recovered on the first pass) | ERS at +1.4, +3.1, +1.4 |
| D1 | GR | 7.5, 7.75, 7.25 | +2.5, +2.8, +2.3 | every 1.0s | none needed: the group elected at +6.7, the tablet promoted itself |
| D1h | semi-sync | **90.5, 90.2, 90.5** (once etcd resumed) | +6.4, +3.4, +6.4 (other VTOrcs up to +18.4) | 3–24s apart | ERS from +18.4–21.4, blocked until etcd resumed |
| D1h | GR | 7.2, 8.0, 7.5 | +4.2, +6.2, +5.5 | 0.1–2.9s apart | none needed |
| G9h | GR | 26.5, 19.0, 23.5 | +3.5, +5.2, +3.5; `GroupPrimaryNotInTopo` at +21.5, +14.2, +18.5 (7.5–14.8s after the election) | 3–15s apart | move at +25.6, +18.2, +22.6 |

0 acknowledged writes lost and 0 violations in every run.

**Cause of the delay: the topology refresh runs inside VTOrc's main loop.** `ContinuousDiscovery` (`go/vt/vtorc/logic/vtorc.go`) selects over the health tick (instance discovery), the recovery tick (analysis and recovery) and the tablet topology tick, and runs the topology refresh (`refreshAllInformation`) synchronously, with a deadline of `--topo-information-refresh-duration` (3s in the end-to-end cluster; the default is 15s). While a cell's topology server hangs, every refresh lasts the whole deadline (`Failed to load tablets from cell zoneN: context deadline exceeded`), and its tick is ready again when it returns, so Go's `select` picks at random between it and the other ticks: analysis passes come every 3s, with runs of several refreshes in a row. In G9h r1, vtorc-zone3 ran no analysis from +3.5s to +15.5s (4 refreshes) and discovered the elected member at +0.5s and then only at +15.5s. With a healthy topology the passes come every 1s. With the default 15s, each blocked refresh would cost up to 15s (not measured).

**Semi-sync: the ERS waits for the hung cell.** In all three semi-sync D1h runs, no VTOrc completed the failover until the harness resumed etcd 90s after the kill, and the new primary was in the topology 0.2–0.5s after it. Each attempt (r1, vtorc-zone3): `DeadPrimary` at +6.4s; the recovery's "Force refreshing all shard tablets" took 15s (the hung cell's `GetShardReplication` until the deadline, after which the reads of the other cells failed on the same expired context); the ERS then started at +21.4s and blocked in `GetTabletMapForShard` (`emergency_reparenter.go:290`, the recovery's context, no per-cell deadline) until etcd resumed; that VTOrc's lease had expired by then (`Error running ERS - node doesn't exist: lease`), and the ERS of another VTOrc, which started later, completed right after etcd resumed. This is the audit's §3B (a failover blocked by an unreachable cell's topology), reproduced with the hung topology server of a replica's cell.

**Group Replication does not wait for it.** The group elects without the topology (+6.7s) and the elected member's tablet promotes itself, so D1h's failover is unchanged (7.2–8.0s against 7.25–7.75s); VTOrc's slower detection does not matter. When the hung cell is the elected member's (G9h), its tablet cannot write its record, and VTOrc must move the group primary (NEW-4): the move waits for the detection of `GroupPrimaryNotInTopo`, which the starved analysis passes delay to 7.5–14.8s after the election, and runs 4.0–4.1s after it (the refresh of the cells that answer and the per-cell read, 2s each, wait for the hung cell's deadline). G9h's failover took 19.0–26.5s; G9b, with the etcd killed, took 21.6s and 25.2s in this sweep (20.2–22.2s before).

Not changed here: running the topology refresh outside the main loop (or giving it a per-cell deadline shorter than the tick) would remove the starvation in both modes, and a per-cell deadline in the semi-sync recovery's refresh and in the ERS's tablet map read would let the semi-sync failover proceed without the hung cell.

## Not tested

- S11/S11b/S11c and S12/S13 as written: they use `SOURCE_DELAY`, `fixReplica` and the default channel, which GR members do not have. G11/G11k/G11s replace them.
- Builtin backups (same graceful leave as G11, for the whole backup), 5-member groups, clock skew, and `IncapacitatedPrimary` with a lossy link.
- The MySQL-level cause of NEW-1 and NEW-5 (MySQL 8.4.11 only).

## Reproducing

```
go/test/endtoend/vtorc/chaos/chaos_run.sh -test.run '^TestS7dFlappingPrimaryLong$' -test.v -test.timeout 30m   # semi-sync
CHAOS_DURABILITY=group_replication_cross_cell go/test/endtoend/vtorc/chaos/chaos_run.sh -test.run '^TestG11GracefulLeaveThenPrimaryDies$' -test.v -test.timeout 30m
CHAOS_RACE_OFFSETS=500ms CHAOS_DURABILITY=group_replication_cross_cell go/test/endtoend/vtorc/chaos/chaos_run.sh -test.run '^TestG12VoterRejoinsWhilePrimaryCellIsolated$' -test.v -test.timeout 60m
CHAOS_DURABILITY=group_replication_cross_cell go/test/endtoend/vtorc/chaos/chaos_run.sh -test.run '^TestG13SecondaryCutOffFromGroupReplicaReads$' -test.v -test.timeout 30m   # polling
CHAOS_VTTABLET_HEARTBEAT=1 CHAOS_DURABILITY=group_replication_cross_cell go/test/endtoend/vtorc/chaos/chaos_run.sh -test.run '^TestG13SecondaryCutOffFromGroupReplicaReads$' -test.v -test.timeout 30m
CHAOS_DURABILITY=group_replication_cross_cell go/test/endtoend/vtorc/chaos/chaos_run.sh -test.run '^TestG9hPrimaryDiesWhileElectedMembersCellTopoHangs$' -test.v -test.timeout 30m   # also TestD1KillPrimaryDetection, TestD1hKillPrimaryWhileOtherCellTopoHangs
VT_UNPLANNED_FAILOVER_TRIALS=3 VT_UNPLANNED_FAILOVER_BUFFER=both go test ./go/test/endtoend/reparent/grouprepl -run TestUnplannedFailoverTimes -timeout 4h -args -keep-data=false
```

`chaos_run.sh` must run as root; it builds the test binary, drops to `RUN_USER` (default `ubuntu`) with `CAP_NET_ADMIN`, and deletes the run's VTDATAROOT afterwards. `CHAOS_TABLET_EXTRA_ARGS` adds vttablet flags. Reports go to `/home/$RUN_USER/chaos-results/<scenario>/`.

## Outside Group Replication: config reads race with config writes

Found while checking a race that `go test -race` reports in `TestGetDetectionAnalysisGroupReplication` (`go/vt/vtorc/inst`). The bug is the same on `main` (checked against e5e0091). This branch does not change `go/viperutil`, and does not fix it: it belongs in its own upstream PR, since every Vitess component reads its dynamic configuration through this code.

**What races.** `go/viperutil/internal/sync/sync.go` guards each key with its own `sync.RWMutex`, and the live `viper.Viper` with one shared `sync.Mutex`, `v.m` (line 43):

- `Set` (line 76) takes the key's lock, then `v.m`, and writes the live viper.
- The getter that `AdaptGetter` returns (line 326) takes only its own key's read lock, then reads the live viper (`getter(v.live)(key)`, line 342).
- `loadFromDisk` (line 275), which applies a changed config file, takes only `v.m`, and replaces `v.live` with a new viper.
- `AllSettings` (line 268) and `WriteConfig` (line 231) take `v.m`; `WriteConfig` takes every key's lock first.

Viper keeps every key's override in one map. A `Set` of one key therefore writes the map while a getter of another key reads it, each under a different key lock. And a reload replaces `v.live` while any getter reads it, under no common lock.

**Where it shows.** VTOrc's `inst` package starts a goroutine from `init()` (`go/vt/vtorc/inst/analysis_dao.go:47`) that waits until the configuration is loaded, which the package's tests mark in their own `init()`, and then reads `RecoveryPollDuration`. A test that sets a dynamic value first, here `config.SetGroupReplicationVoterReplacementGracePeriod` when `TestGetDetectionAnalysisGroupReplication` runs alone, writes the map at the same time. Run with `-race -count=3 -run TestGetDetectionAnalysisGroupReplication`, it fails on the base of this branch as well.

**Production impact.** The getters race with `loadFromDisk`, which runs when a watched config file changes (`--config-file` with dynamic values), in every binary that registers dynamic `viperutil` values. In VTOrc, only tests call the dynamic setters (`config.Set*`); the race that matters in production is the one with a reload. A concurrent map read and write in Go can abort the process (`fatal error: concurrent map read and map write`).

**Proposed fix.**

1. Make `v.m` a `sync.RWMutex`.
2. Have the adapted getter take `v.m.RLock()` after its key's read lock, and read `v.live` under it.
3. Leave `Set`, `WriteConfig`, `AllSettings` and `loadFromDisk` taking `v.m` for writing.

Every path that takes both locks takes the key lock first and `v.m` second, so this adds no lock-order inversion. A read lock on every config read costs little: readers only exclude `Set`, a reload and a config write.

**Test.** A unit test in `go/viperutil/internal/sync` that, under `-race`, runs a getter of one key concurrently with `Set` of another key, and with `loadFromDisk`. It fails on `main` with the race detector's report.
