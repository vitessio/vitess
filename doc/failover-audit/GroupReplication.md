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

Not fixed (design changes): NEW-1 to NEW-6.

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
