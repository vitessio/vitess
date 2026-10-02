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
- **A killed VTOrc holds the shard lock until its lease expires** (about 30s with etcd here). This predates these commits and affects every recovery, not only bootstraps.

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
```

`chaos_run.sh` must run as root; it builds the test binary, drops to `RUN_USER` (default `ubuntu`) with `CAP_NET_ADMIN`, and deletes the run's VTDATAROOT afterwards. `CHAOS_TABLET_EXTRA_ARGS` adds vttablet flags. Reports go to `/home/$RUN_USER/chaos-results/<scenario>/`.
