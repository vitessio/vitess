# MySQL Group Replication as a first-class replication mode

Status: prototype / RFC. Related: [#18648](https://github.com/vitessio/vitess/issues/18648) (Uber's "consensus" RFC), [#13300](https://github.com/vitessio/vitess/issues/13300) (removal of the experimental VTGR).

## Goals

- A shard can replicate with MySQL Group Replication (GR) in single-primary mode instead of asynchronous replication plus semi-sync, selected with a **durability policy**, just like `semi_sync` or `cross_cell` today.
- Every existing operational tool keeps working with the same semantics: `PlannedReparentShard`, `EmergencyReparentShard`, VTOrc, backups/restores, `vtctldclient GetFullStatus`, vtgate buffering. No second orchestrator.
- An existing semi-sync shard can be converted to GR **online**, one tablet at a time, with durability preserved at every step, and converted back.
- GR's guarantees are used, not re-implemented: the group fences a partitioned primary, elects a new primary without an external agent, and never lets a member diverge.

## Non-goals

- Multi-primary mode. Vitess routes writes to a single primary; multi-primary adds certification conflicts for no gain.
- InnoDB Cluster / MySQL Shell / MySQL Router. Vitess already provides routing (vtgate) and topology (topo).
- More than 9 voting members per shard (a MySQL limit). Additional tablets replicate asynchronously from the group.

## Why VTGR failed, and what is different

VTGR was a separate daemon per tablet host that talked MySQL protocol, duplicated VTOrc's control loop, turned every replication RPC into a silent no-op (the `MysqlGR` flavor), and left PRS/ERS semantics undefined. Nobody ran it in production.

This design instead makes GR a **replication mode inside the existing control plane**:

| Concern | Where it lives | Mechanism |
|---|---|---|
| Choose GR | keyspace durability policy | `group_replication`, `group_replication_cross_cell` |
| MySQL primitives | `go/mysql`, `mysqlctl.MysqlDaemon` | `GroupReplicationStatus`, `ConfigureGroupReplication`, `StartGroupReplication`, `StopGroupReplication`, `SetGroupReplicationPrimary` |
| Observability | `FullStatus.group_replication_status` | VTOrc, `GetFullStatus` |
| Membership lifecycle | vttablet (tabletmanager) | tablet derives its GR config from topo; RPCs `StartGroupReplication`/`StopGroupReplication` |
| Topology follows the group | vttablet reconcile loop + VTOrc | tablet promotes/demotes its own record when its group role changes |
| Planned switchover | PRS | same orchestration; tablet implements `PromoteReplica` with `group_replication_set_as_primary()` |
| Unplanned failover | the group itself + ERS/VTOrc | group elects; Vitess reconciles topo; ERS handles loss of quorum only when explicitly asked |
| Migration | `vtctldclient MigrateReplicationMode` (reparentutil) | ordered, idempotent, shard-locked steps |

## Semi-sync vs Group Replication (summary)

See the research notes linked from the PR for sources; the points that shape the design:

- **Durability.** Lossless semi-sync (AFTER_SYNC) returns to the client after *one* eligible replica has written the event to its relay log. GR returns after a *majority* of the group agreed on the order of the transaction and certified it. Neither waits for the transaction to be applied on another server. With one member per cell in three cells, both cost roughly one round trip to the nearest other cell; `cross_cell` semi-sync and a 3-cell group have similar commit latency.
- **Fencing.** Semi-sync has no membership: a partitioned primary keeps its binlog, and after an ERS its unacknowledged transactions become errant GTIDs; Vitess must fence it (`super_read_only`, tablet type, errant GTID detection). A GR primary that loses its majority cannot commit; its transactions block and are never certified, so it cannot diverge. `group_replication_unreachable_majority_timeout` bounds how long it blocks.
- **Election.** Semi-sync needs an external agent (VTOrc) to promote. GR elects on its own after a fixed 5s detection period plus the expulsion delay. On MySQL 8.4.11 that delay measured 1s with `group_replication_member_expel_timeout=0`, but 16s for any value from 1 to 10s. It chooses by lowest version, then highest `group_replication_member_weight`, then lowest `server_uuid`. Vitess maps promotion rules onto member weights so that the group's choice matches Vitess's preferences.
- **Loss of majority.** Semi-sync keeps running as long as one eligible acker remains. GR stops accepting writes; only `group_replication_force_members` (an operator decision, split-brain risk) unblocks it. This is the main availability trade-off.
- **Behaviour changes users see.** Every table needs a primary key; transactions above `group_replication_transaction_size_limit` (~143MB) are rolled back; one slow secondary throttles the primary through flow control; GTIDs use the group UUID instead of the server UUID.

## Architecture

### Durability policy and replication mode

`policy.Durabler` stays unchanged, so out-of-tree policies keep compiling. A policy can additionally implement:

```go
type ReplicationModer interface { ReplicationMode() ReplicationMode }       // Async | GroupReplication
type GroupReplicationDurabler interface {
    Durabler; ReplicationModer
    IsGroupMember(*topodatapb.Tablet) bool      // may be a voting member (vs always an async replica)
    MemberWeight(*topodatapb.Tablet) int        // group_replication_member_weight
    RequiresCrossCellMajority() bool
    MaxVotersPerCell() int                      // 0 = no limit other than 9 members
}
```

Two policies are registered:

- `group_replication`: PRIMARY and REPLICA tablets are eligible voters, up to 9; other types replicate asynchronously from the group's primary. `SemiSyncAckers()` is 0.
- `group_replication_cross_cell`: the same, but with **one voter per cell**, so no cell can hold a majority of the voting members. Every durable transaction then exists in two cells, which is the GR equivalent of `cross_cell`. The other PRIMARY/REPLICA tablets of a cell replicate asynchronously. VTOrc reports a cell majority as a violation.

Which eligible tablets actually vote is recorded per shard; see "Voters" below.

### Principle: the observed MySQL state decides the behaviour, the policy decides the goal

Every tablet reports whether its MySQL is an active member of a group (`ONLINE`/`RECOVERING`). Tablet RPCs dispatch on that observed state, not on the policy:

- On an active member, replication RPCs have GR meaning. `SetReplicationSource` never touches the default channel; `PromoteReplica` calls `group_replication_set_as_primary`.
- On a non-member, they keep their asynchronous meaning, even if the keyspace policy says GR. Those are the async replicas of the group, or tablets that are not converted yet.

The policy only defines the target: who should be a member, with which weight, and whether semi-sync is still required. This makes the half-migrated states of an online conversion well defined.

Corollary: **an active group with at least two ONLINE members supersedes semi-sync.** On such a primary neither the tablet nor VTOrc demands semi-sync, whatever the policy says. This rule is what lets a keyspace be converted shard by shard while its policy is still `semi_sync`.

### Voters

A group should be small: every commit waits for a majority of the voting members, and flow control follows the slowest of them. One member per cell already puts every durable commit in two cells, so more members per cell add cost without adding durability. A shard therefore has a fixed set of **voters**, the tablets that are voting members of its group. Every other PRIMARY, REPLICA or RDONLY tablet replicates asynchronously from the primary, as all Vitess replicas do today.

- **Where.** The voters are stored in the shard record, `Shard.group_replication_voters` (a list of tablet aliases), so vttablet, VTOrc, PRS, ERS and the migration all agree on them. Only listed tablets join the group, and only listed tablets rejoin it on their own; a tablet that is not listed stays, or becomes, an asynchronous replica. Under a group replication policy an **empty list means "not selected yet"**: the components then fall back to the policy (every eligible tablet counts as a voter) until one of the writers below fills it in.
- **Who writes it**, always under the shard lock:
  - `MigrateReplicationMode`, before it bootstraps the group, and when converting back, after the last member left the group, it clears the list.
  - `PlannedReparentShard`, on the initial promotion of a shard that never had a primary, before `InitPrimary` bootstraps the group.
  - VTOrc, when it replaces a voter (below).
- **Selection** (`policy.SelectVoters(durability, current, groupPrimary, candidates)`). It changes the current voters as little as possible:
  1. The group's primary always keeps its seat; removing it would force a failover.
  2. A current voter is kept while it is eligible (`IsGroupMember`) and has not failed, within the limit of its cell.
  3. Active members come next, so that a tablet already in the group is preferred to one that would have to join.
  4. Free seats go to eligible, non-failed tablets, by promotion rule, then lowest alias.

  Every cell is limited to `MaxVotersPerCell()` voters (1 for `group_replication_cross_cell`), and the group to 9. The result is deterministic, so every writer computes the same list from the same inputs.
- **Replacement.** A voter that is unreachable for longer than `--group-replication-voter-replacement-grace-period` (VTOrc flag, default 1m) is considered failed. VTOrc's analysis `GroupVotersOutOfDate` then selects the voters again, with the current list and the group's primary, and stores the new list; the new voter's tablet joins the group, and the old one becomes an asynchronous replica when it comes back. A voter that is only briefly unreachable, for example while it restarts, keeps its seat.
- **Promotion.** Only a voter can become the primary. Swapping a non-voter in for a voter as part of a planned reparent is future work.

### Group configuration, derived from topo

Apart from the voters, nothing new is stored in topo:

- **Group name.** A UUIDv5 of `keyspace/shard` in a fixed Vitess namespace, the same on every tablet of the shard. After a reshard the new shards get new groups automatically.
- **Local address.** `<mysql hostname>:<--group-replication-port>`. The tablet publishes the port in its tablet record as `port_map["gr"]`. The default is off; the flag enables GR support on the tablet.
- **Seeds.** The `gr` addresses of the other voters of the shard, read from topo each time the tablet (re)joins.
- **Credentials.** Distributed recovery authenticates as the existing replication user (`START GROUP_REPLICATION USER=…, PASSWORD=…`, never stored), with `group_replication_recovery_get_public_key=ON` or TLS.
- **Tunables** (vttablet flags, applied with `SET GLOBAL` before each start):
  - `consistency`, default `BEFORE_ON_PRIMARY_FAILOVER`: a new primary applies its backlog before it serves.
  - `exit_state_action`, default `READ_ONLY`.
  - `unreachable_majority_timeout`, default 1s: a primary cut off from its majority errors out instead of blocking forever.
  - `autorejoin_tries`.
  - `group_replication_start_on_boot` is persisted OFF. vttablet decides when to join, as it does for async replication (`skip_replica_start`).
- **Fixed settings.** Vitess always applies these two, because its design depends on them. They are not flags.
  - `member_expel_timeout=0`. The group replaces a failed primary about 7s after it fails. On MySQL 8.4.11, any value from 1 to 10s delays the expulsion, and so the election, by about 16s. The tablet rejoins expelled members on its own, so an expulsion caused by a short stall is cheap.
  - `paxos_single_leader=ON`. The primary is the group's only consensus leader, so a slow or failed secondary does not delay commits. MySQL applies it when a group is bootstrapped and refuses a joiner whose setting differs from the group's (verified on 8.4.11), so it must be the same on every member. A group that was not bootstrapped by Vitess with this setting cannot be joined; it has to be restarted from scratch first.

  These settings, together with the `unreachable_majority_timeout` default of 1s, match Uber's RFC (#18648).
- **Plugin.** It is loaded with `INSTALL PLUGIN` when needed. That survives restarts, needs no mysqld restart, and is not binlogged. Loading it through my.cnf (`plugin-load-add=group_replication.so`) is also supported.

### Tablet (vttablet) behaviour

- **Startup** (`initializeReplication`). If the tablet is a voter (listed in the shard record):
  1. Configure GR and `START GROUP_REPLICATION` (join, never bootstrap).
  2. If the join fails because no member is reachable, retry in the background. Bootstrapping is never done from startup: two tablets could each create a group.
  3. `checkPrimaryShip` does not trust a stale PRIMARY record. The tablet starts as REPLICA, and the reconcile loop makes it PRIMARY if its MySQL is the group primary.

  Tablets that are not voters replicate asynchronously from the shard primary, as today.
- **Reconcile loop.** Every `--group-replication-sync-interval` (default 1s) the tablet reads its GR status:
  - If the member is ONLINE, PRIMARY and has quorum, and the tablet is not PRIMARY: promote the tablet record (`ChangeTabletType(PRIMARY)`). This yields a new `PrimaryTermStartTime`; the shard record follows through `shardSyncLoop`, and vtgate follows the new term.
  - If the tablet is PRIMARY but the member is not the group primary, or has lost quorum: demote the record to REPLICA and stop serving. vtgate buffers or fails fast exactly as for a PRS.
  - If the tablet is a voter but its member is `OFFLINE` or `ERROR`, and it is not in a backup or restore: rejoin with backoff. GR itself refuses a member with extra transactions.

  This is how an unplanned failover elected by the group reaches Vitess within about a second, without VTOrc.
- **RPCs on an active member:**

  | RPC | GR behaviour |
  |---|---|
  | `InitPrimary` | Bootstrap the group on this tablet (shard must have no active member; caller holds the shard lock), then as today. |
  | `PromoteReplica` | `group_replication_set_as_primary(own uuid)`; wait for role PRIMARY and `super_read_only=OFF`; change type. No `RESET REPLICA ALL`. |
  | `DemotePrimary` | Stop serving and set `super_read_only`, as today; skip the semi-sync steps. |
  | `UndoDemotePrimary` | Only if the member is still the group primary. |
  | `SetReplicationSource` | No-op for the default channel; wait for the reparent journal or position; fix the tablet type. |
  | `StartReplication` / `StopReplication` | Join or leave the group. |
  | `SetReadWrite` (`read_only=OFF`) | **Refused unless the member is the group primary.** A GR secondary with `super_read_only=OFF` accepts writes and replicates them to the group; this was verified in the lab. |
  | `StartGroupReplication(bootstrap)` / `StopGroupReplication` (new) | Explicit membership control, used by VTOrc and the migration. `StartGroupReplication` first stops the async channel and afterwards clears it (`RESET REPLICA ALL`): GR refuses to start while the channel runs, and a leftover channel can be resurrected later. |
- **Semi-sync.** `fixSemiSync` treats an active group with at least two ONLINE members as sufficient durability. It then disables semi-sync and does not open the semi-sync monitor.

### PlannedReparentShard

The orchestration is unchanged. The GR meaning comes from the tablet RPCs:

1. Preflight: the new primary must be a listed voter and an ONLINE member of the same group as the current primary; otherwise PRS fails with `FAILED_PRECONDITION` and names the voters. A tablet that is not a voter cannot be promoted while the group is active; swapping it in for a voter is future work. Without `--new-primary`, the election only considers the voters.
2. `DemotePrimary(old)` stops serving, so vtgate buffers.
3. `WaitForPosition(new, pos)`.
4. `PromoteReplica(new)` runs `group_replication_set_as_primary`. GR moves `super_read_only` and waits for in-flight transactions.
5. `SetReplicationSource` on the other members is a journal wait. The async replicas are re-pointed as today.
6. `PopulateReparentJournal(new)`.

The initial promotion of a shard that never had a primary selects the voters, with the primary-elect among them, and stores them in the shard record under the shard lock before `InitPrimary` bootstraps the group.

### EmergencyReparentShard

The group fails over by itself when a majority survives. ERS in GR mode must uphold the rules in `EmergencyReparentShard.md`: certainty, time-bound stages, shard lock re-checks, reparent journal, and no errant GTIDs.

1. Lock the shard and collect `FullStatus` from all tablets, time-bound.
2. If a reachable member reports that it is ONLINE, PRIMARY and has quorum, the group has already elected. Promote that tablet in topo (`PromoteReplica`, which is only a type change there) and write the reparent journal. If `--new-primary` names another member, follow with a PRS-style switch. Tablets that are not voters are re-pointed as async replicas; voters that are not active are left to rejoin the group.
3. If no reachable member has quorum, ERS fails, unless the operator passes `--group-replication-force-quorum` (not implemented in the prototype). That choice needs a human, like `--allow-split-brain-promotion`: it would pick the member with the most advanced *received* GTID set and use `group_replication_force_members`.

### VTOrc

VTOrc keeps its single loop. Discovery reads `FullStatus.group_replication_status`, and analysis gains GR codes:

| Analysis | Condition | Recovery |
|---|---|---|
| `GroupPrimaryNotInTopo` | A member is the ONLINE group primary with quorum, but its tablet is not the topo primary (for example, the tablet-local loop failed). | `PromoteReplica` on that tablet (type change) plus journal. |
| `GroupMemberNotOnline` | The tablet is a voter, and it is OFFLINE or ERROR. | `StartGroupReplication(bootstrap=false)`. |
| `GroupNotBootstrapped` | GR policy, no member of the shard is active, and all voters are reachable. | `InitPrimary` (bootstrap) on the tablet with the most advanced GTID set, under the shard lock. |
| `GroupVotersOutOfDate` | The voters `SelectVoters` would choose differ from the shard record, for example because a voter has been unreachable for longer than `--group-replication-voter-replacement-grace-period` (default 1m). | Select the voters again (`SelectVoters` with the current list and the group's primary) and store them under the shard lock. |
| `GroupQuorumLost` | Members are reachable, but none has quorum. | None; alert (ERS with an explicit force flag is the operator path). |
| `GroupCellMajority` | `group_replication_cross_cell`, and one cell holds a majority of the ONLINE members. | None; alert. |

For tablets that are active members, the async analyses are suppressed:
- `ReplicationStopped`
- `NotConnectedToPrimary`
- `ConnectedToWrongPrimary`
- `ReplicaMisconfigured`
- `PrimaryHasPrimary`
- the semi-sync `*MustBeSet` / `*MustNotBeSet` codes
- `PrimaryIsReadOnly` while the member is not the group primary

`DeadPrimary` in a shard whose members are active does not run ERS right away: the group elects on its own. VTOrc waits for `--group-replication-failover-grace-period` (default 30s) for `GroupPrimaryNotInTopo` to appear, and only then falls back to the ERS path described above.

### Other components

- **Lag.** Heartbeat-based lag (`--heartbeat-enable`) works unchanged. The polling tracker must not rely on the default channel on GR members.
- **Status queries.** The MySQL flavors now read `SHOW REPLICA STATUS FOR CHANNEL ''` and filter `replication_connection_configuration` on `CHANNEL_NAME = ''`. On a GR member the plugin's own channels (`group_replication_applier`, `group_replication_recovery`) show up in those tables and used to break parsing (`query returned 2 rows`).
- **Backups.**
  - Online engines (xtrabackup, mysqlshell, clone) work on a member as is.
  - The builtin engine stops mysqld. The tablet leaves the group for the duration and rejoins afterwards through the reconcile loop.
  - A tablet restored from a backup joins with incremental recovery as long as the donors still have the binlogs, otherwise with clone.
- **Errant GTIDs.** Transactions of the group carry the group name as their UUID, which is identical on every member. VTOrc's errant GTID detection must compare against the group primary's executed set, including the group UUID. GR refuses to admit a member that has extra transactions anyway.
- **`sql_log_bin=0` writes** (`ExecuteFetchAsDBA --disable-binlogs`, TableGC purge, `ApplySchemaChange` without replication) create silent divergence under GR exactly as under async replication. No new risk, but worth documenting.
- **Schema requirements.** A preflight refuses the conversion when a user table lacks a primary key (or non-null unique key) or is not InnoDB. After conversion, `sql_require_primary_key=ON` is recommended. All `_vt` sidecar tables already have primary keys.

## Migration: semi-sync → Group Replication, online

`vtctldclient MigrateReplicationMode --durability-policy group_replication_cross_cell <keyspace>[/<shard>]` runs the steps below for each shard, under the shard lock. Every step is idempotent, so the command can be re-run after a failure; `--dry-run` prints the plan. The keyspace durability policy is switched only after every shard is converted.

The steps were validated on MySQL 8.4.11 with a continuous write load; bootstrapping and joining caused **0 failed writes**.

1. **Preflight.**
   - All tablets reachable; GTID mode ON; ROW binlog format.
   - MySQL ≥ 8.0.27; 8.4 is recommended because of its defaults (`BEFORE_ON_PRIMARY_FAILOVER`, `OFFLINE_MODE`, certification GC).
   - The voters are selected (`SelectVoters` for the target policy, with the current primary as the group's primary and any voters already in the shard record as the current list, so a re-run keeps them). There must be at least 3 and at most 9; under `group_replication_cross_cell`, which allows one voter per cell, that means eligible tablets in at least 3 cells. The error names the cells found.
   - Every user table has a primary key and is InnoDB.
   - Every voter has a `gr` port.
2. **Store the voters** in the shard record.
3. **Bootstrap on the current primary** (`StartGroupReplication(bootstrap=true)`). Writes continue. Semi-sync stays enabled, and the async replicas keep acknowledging.
4. **Join the other voters one by one** (`StartGroupReplication`). Each one stops its async channel, recovers incrementally from a donor, and becomes ONLINE. Cross-cell voters join first, so the group gets a cross-cell majority as early as possible.
5. **Disable semi-sync on the primary as soon as the group has two ONLINE members,** and before the last semi-sync acker leaves its async channel. With Vitess's infinite semi-sync timeout, losing the last acker blocks every commit; the lab reproduced this. Ackers that are not voters keep acknowledging until this point, so they count as remaining ackers while the voters join. From this point, the group's majority provides durability. The "group supersedes semi-sync" rule makes the tablet and VTOrc agree.
6. Every other tablet becomes, or stays, an async replica of the primary, with semi-sync off. A tablet that is an active member but not a voter leaves the group first.
7. After all shards are converted, set the keyspace durability policy. VTOrc then manages membership.

### Rollback: Group Replication → semi-sync

1. Set the keyspace durability policy back to the semi-sync policy. Nothing changes immediately, because the active groups supersede semi-sync.
2. For each secondary: `StopGroupReplication`, then `SetReplicationSource(primary, semiSync=true)`. The primary enables semi-sync as soon as it has an eligible acker, and before its group shrinks below two ONLINE members.
3. On the primary: stop serving (vtgate buffers), `StopGroupReplication`, clear `super_read_only`, and serve again. GR sets `super_read_only` when it stops. The lab measured about 4s of rejected writes without buffering; with the PRS-style buffering this becomes a short stall.
4. Clear the voters in the shard record once the last member left the group. A later conversion selects them afresh.

## Recommended topology

- Three cells, with one voting member per cell (a group of 3) under `group_replication_cross_cell`. Use five members as 2-2-1 to survive two failures.
- Additional REPLICA tablets and all RDONLY tablets replicate asynchronously from the group primary, for read scaling. Under `group_replication_cross_cell` this follows from the one-voter-per-cell rule (see "Voters"); a second REPLICA in a cell is a ready replacement when that cell's voter fails.
- `group_replication_paxos_single_leader=ON` and `group_replication_member_expel_timeout=0` are always applied (see Fixed settings).
- Tune flow control so that one slow member does not throttle the primary.
- Async replicas replicate from the primary, as all Vitess replicas do today. Replicating from the local voter would save cross-cell bandwidth, at the cost of lag and repointing when that voter changes; MySQL's `SOURCE_CONNECTION_AUTO_FAILOVER` would handle the repointing.

## Validation

**Lab, raw MySQL 8.4.11:**
- Bootstrapping a group on a live semi-sync primary and joining its replicas caused 0 failed writes.
- `START GROUP_REPLICATION` is refused while the async channel runs.
- A secondary with `super_read_only=OFF` accepts writes and replicates them to the group.
- Semi-sync with Vitess's infinite timeout blocks all commits once the last acker leaves.
- Stopping GR on the primary rejects commits for about 3–4s.

**End-to-end** (`go/test/endtoend/reparent/grouprepl`: 3 cells, 1 REPLICA per cell plus 1 RDONLY, VTOrc and a buffering vtgate, continuous writes through vtgate):

| Phase | Result |
|---|---|
| `MigrateReplicationMode` cross_cell → group_replication_cross_cell | 6s, 0 failed writes |
| `PlannedReparentShard` (group_replication_set_as_primary) | 0.6s, 0 failed writes |
| `kill -9` of the primary's mysqld | New primary in topo 6.6s after the kill (21.6s with MySQL's default `member_expel_timeout` of 5s). The new primary's tablet promoted itself within 5ms of the election, writes resumed, and the old primary rejoined as a secondary. |
| `MigrateReplicationMode` back to cross_cell | No data loss; vtgate buffered while the primary left its group |

## Prototype scope

Implemented in this branch:
- Durability policies and the replication-mode interface.
- GR status in `FullStatus`, and the MySQL/mysqlctl primitives.
- Status queries that read only the default channel.
- The `StartGroupReplication`/`StopGroupReplication` RPCs.
- The tablet reconcile loop and GR-aware tablet RPCs.
- PRS and ERS support.
- VTOrc analyses and recoveries.
- `MigrateReplicationMode` in both directions.
- An end-to-end test on MySQL 8.4.

Known gaps, found while reviewing and testing the prototype:
- ERS and PRS choose their path from the keyspace policy. A shard that has been converted while the keyspace policy is still async (the policy switches after the last shard) takes the async ERS path.
- During the migration back, the primary re-enables semi-sync only after its group drops below two ONLINE members. That leaves up to one sync interval with neither group nor semi-sync durability. Enabling semi-sync while the group is still active would close this window; it is harmless when an acker is attached.
- A voter that changes to a non-eligible type (RDONLY, DRAINED) does not leave the group until VTOrc replaces it in the voter list.
- PRS cannot promote a tablet that is not a voter; it fails and names the voters. Swapping a non-voter in for a voter as part of the reparent is future work.
- Failover time is bounded below by GR's fixed 5s failure detection.
- The member weight is applied only when a member joins.

Follow-ups:
- PRS to a tablet that is not a voter, by swapping it into the voter list first.
- ERS with forced quorum (`group_replication_force_members`).
- Builtin-backup integration.
- The MySQL communication stack (`communication_stack=MYSQL`, the default from 26.7), which removes the separate port.
- vtadmin and operator support.
- Flow-control defaults.
- A multi-shard migration test.
