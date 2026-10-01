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
- `TestStopGroupReplicationRestoresReadWriteOnPrimary` is flaky on this branch (2/40 runs without any change); `TestWaitForDBAGrants` fails in this environment.
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

**Open: the default stays READ_ONLY for now.** S7d, the only reason to keep it, no longer favours it (the group lost its majority in 3 of 4 OFFLINE_MODE runs and 4 of 4 READ_ONLY runs, both decided by MySQL's joins; 4 runs per mode do not separate them), and OFFLINE_MODE still fences reads on a member that left its group at no cost in G3E, S3 and S1. But S7d regressed under both actions against XCom (READ_ONLY lost its majority in 4 of 4 runs here, 3 of 8 on XCom), and the failed joins and the 60s `STOP` are not understood yet; the default is decided once they are.

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
