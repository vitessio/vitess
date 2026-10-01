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

### The legitimate group

Group Replication decides which member is the primary, and Vitess follows it. But Vitess must only follow the shard's **legitimate** group, and must not create the conditions for a new one. The chaos tests found that a member that had just left its group, and was made to join again while the other members were leaving too, ended up alone in a new group: MySQL installed a view of a new *incarnation* (the part of the view id before the `:`) that contained only this member, which was ONLINE, PRIMARY and had quorum in its view of one. The tablet promoted itself, and every transaction that only the other members held was lost (`doc/failover-audit/GroupReplication.md`, NEW-1).

- **Incarnation.** `Shard.group_replication_incarnation` records the incarnation of the shard's group. MySQL keeps it for as long as the group exists, and every bootstrap creates a new one. Whoever bootstraps a group records it under the shard lock, right after the bootstrap: `MigrateReplicationMode`, the initial promotion of `PlannedReparentShard` (and `InitShardPrimary`), and VTOrc's `BootstrapGroupReplication`. The reverse migration clears it with the voters. An empty value means unknown (a group bootstrapped before incarnations were recorded); only the voter rule below applies then.
- **Legitimate primary** (`policy.LegitimateGroup`). A member is the primary that Vitess follows when it is the ONLINE primary of a group with quorum in its own view, its view belongs to the recorded incarnation (if one is recorded), and a **majority of the listed voters are ONLINE in its view**. Voters are found in a view by the `server_uuid` of their MySQL (from `FullStatus`) or by the MySQL address of their tablet record (`MEMBER_HOST`/`MEMBER_PORT`; MySQL reports its own hostname, which need not match the tablet record). With no voter listed, MySQL's own view quorum applies, as before.
- **Unreachable cells.** The tablet records only give the voters' MySQL addresses. They are read cell by cell, each cell with its own 2s deadline (`topo.GetTabletMapForShardWithCellTimeout`): the topology server of a cut-off cell does not answer until the caller gives up, and a single deadline for all cells made the reads of the other cells fail too. A voter without a tablet record is still found by its known `server_uuid`. The reconcile loop reuses the tablet records it read within 30s as long as they, with the known `server_uuid`s, identify every voter, so the promotion of a member elected while the old primary's cell is partitioned does not wait for that cell (S9i chaos scenario: 139s without a primary before, 9.0s after). VTOrc's `PromoteGroupPrimary` and its join check read the tablets the same way and accept a partial result.
- **Where it applies.** The tablet's reconcile loop only promotes a legitimate primary. VTOrc's `GroupPrimaryNotInTopo`, its `PromoteGroupPrimary` recovery, its notion of the group's primary for voter selection (`GroupVotersOutOfDate`), ERS's search for the group's quorum, and the PRS preflight all use the same rule.
- **Foreign incarnation.** A member that is active in a group of another incarnation than the recorded one does not hold the shard's acknowledged transactions. Its tablet makes it leave that group (`STOP GROUP_REPLICATION`; MySQL stays `super_read_only`) and demotes itself if it was PRIMARY. It then rejoins the legitimate group like any voter that is out of it (see "Joining"): its own rejoins are not suspended, since the group may need this voter to get a majority, and so a primary tablet, back. A tablet that bootstrapped a group itself trusts its incarnation for a minute (`groupReplicationBootstrapGrace`), until the component that asked for the bootstrap has recorded it; a mismatch with the cached shard record is always confirmed with a fresh read.
- **Joining.** A tablet (at startup and in its reconcile loop) and VTOrc (`GroupMemberNotOnline`) only start a join while another tablet of the shard reports an active member of the legitimate group with quorum in its view. The tablet asks its peers' `FullStatus` with a 2s timeout, the shard primary first; VTOrc asks every tablet of the shard, returns as soon as the answers settle the question, and waits at most 2s for the others, so that the join starts right after a current check (a check that waited 9s for an isolated tablet started a join into a group that had lost its majority meanwhile). VTOrc's recovery does not need a shard primary tablet: a group that was just bootstrapped, or lost the majority of its voters, only gets one once enough voters have joined it. After a bootstrap, VTOrc makes the other voters join the new group right away. A `START GROUP_REPLICATION` with no group to join cannot join anything: it blocks until MySQL's join timeout, and MySQL refuses a bootstrap on the same member meanwhile (`errno 3724`), which starved VTOrc's bootstrap (NEW-2 of the audit). Such joins are also where members formed groups of their own. MySQL keeps running a `START` whose client gave up, so the reconcile loop waits up to a minute for the joins it starts, and any join or bootstrap first stops a `START` still in progress. A joining member contacts first the peers that were just seen active in the legitimate group (`group_replication_group_seeds` order).
- **Group shrink fails closed.** MySQL's view quorum counts only the members still in the view: after the other voters left the group (an expulsion, a clean shutdown, `unreachable_majority_timeout`), a primary alone in its view has quorum and keeps committing, and those commits exist on one voter only. Under a group replication policy with listed voters, a PRIMARY tablet whose group view holds fewer than a majority of the voters stops serving (reason `replication group lost the majority of its voters`), keeps its type so that vtgate buffers, leaves MySQL alone, and serves again once the majority is back. This is the same trade-off as a semi-sync primary without an acker: writes block rather than become durable on a single server. A PRIMARY tablet also stops serving, for the same reason, before its MySQL bootstraps a group: the new group has one member and MySQL makes it writable right away. While the tablet's MySQL is not a group primary (for example during that bootstrap), the reconcile loop leaves the not-serving state as it is, and `UndoDemotePrimary` is refused unless MySQL is the primary of the legitimate group. It does not apply during a migration (the policy is not a group replication policy yet), when the group grows from one member while the primary keeps serving with semi-sync.

### Group configuration, derived from topo

Apart from the voters and the group incarnation, nothing new is stored in topo:

- **Group name.** A UUIDv5 of `keyspace/shard` in a fixed Vitess namespace, the same on every tablet of the shard. After a reshard the new shards get new groups automatically.
- **Communication stack.** Always `group_replication_communication_stack=MYSQL`: members connect to each other through MySQL's own port, authenticated as the replication user and secured like any other MySQL client connection. There is no separate group communication port, allowlist or XCom TLS configuration. MySQL deprecates the XCom stack in 9.7.2 and makes `MYSQL` the default in 26.7. Every member of a group must use the same stack; a group bootstrapped with XCom cannot be joined, and has to be restarted from scratch.
- **Enabling.** `--enable-group-replication` (default off) enables GR support on the tablet. `FullStatus` reports it (`group_replication_enabled`), which the migration preflight checks.
- **Local address.** The tablet record's MySQL address, `<mysql hostname>:<mysql port>`: the address other tablets replicate from.
- **Seeds.** The MySQL addresses of the other tablets of the shard, read from topo each time the tablet (re)joins. Non-voters are harmless seeds: a joiner only needs one reachable member.
- **Credentials.** The MySQL stack authenticates the connections between members, as well as distributed recovery, with the credentials stored on the `group_replication_recovery` channel; credentials given to `START GROUP_REPLICATION` would only be used for recovery. The tablet stores the existing replication user's credentials there (`CHANGE REPLICATION SOURCE TO … FOR CHANNEL 'group_replication_recovery'`) before each join, as it does on the default channel for asynchronous replication, with `group_replication_recovery_get_public_key=ON` or TLS. The replication user needs `GROUP_REPLICATION_STREAM` for these connections and `CONNECTION_ADMIN`, without which MySQL expels a member placed in `offline_mode`. `config/init_db.sql` grants both to `vt_repl`; existing deployments grant them on the shard primary before the migration. The tablet checks `mysql.global_grants` before it configures a join and refuses with `FAILED_PRECONDITION`, naming the `GRANT` to run, rather than letting MySQL refuse the connections and the join fail only after its timeout.
- **Tunables** (vttablet flags, applied with `SET GLOBAL` before each start):
  - `consistency`, default `BEFORE_ON_PRIMARY_FAILOVER`: a new primary applies its backlog before it serves.
  - `exit_state_action`, default `READ_ONLY`. `OFFLINE_MODE` (MySQL 8.4's default) is supported: MySQL then also sets `offline_mode` on a member that leaves its group involuntarily, which refuses vttablet's app, allprivs and filtered users (not the replication user, which has `CONNECTION_ADMIN`), so a partitioned old primary stops answering reads when the member leaves its group. MySQL never clears it; the tablet does, once the member is ONLINE in the legitimate group with a majority of the voters in its view, when it bootstraps the group, and when it makes MySQL the writable primary or an asynchronous replica. It is not the default: it also refuses VReplication's filtered user and any other user without `CONNECTION_ADMIN`, so a member that is only briefly out of its group stops serving and breaks its streams until it rejoins, and Vitess must clear a flag that MySQL never clears. With `READ_ONLY`, Vitess fences a member that left its group the way it fences any replica: MySQL is `super_read_only`, and replica reads are bounded by the tablet's replication lag tracking, with heartbeats or by polling (see "Replica reads and replication lag"). Primary reads are not: vtgate keeps sending them to the old primary until it follows the new one, about 0.6–2s after the group's election (1–13 reads answered after the election per failover in the G3E chaos scenario, at this test's read rate). With the 2s unreachable majority timeout below, OFFLINE_MODE does not avoid them either (0–4 per failover); it only did with 1s. The chaos tests do not separate the two actions on availability (see "Exit state action" and "Unreachable majority timeout sweep" in `doc/failover-audit/GroupReplication.md`).
  - `autorejoin_tries`, default 0. The tablet rejoins an expelled member itself, once the shard's legitimate group is active on another tablet. MySQL's own auto-rejoin does not check that: in the chaos tests an attempt blocked the member for about a minute (its status queries hung, and every change was refused) and could end in a group of its own.
  - `group_replication_start_on_boot` is persisted OFF. vttablet decides when to join, as it does for async replication (`skip_replica_start`).
- **Fixed settings.** Vitess always applies these three, because its design depends on them. They are not flags.
  - `member_expel_timeout=0`. The group replaces a failed primary about 7s after it fails. On MySQL 8.4.11, any value from 1 to 10s delays the expulsion, and so the election, by about 16s. The tablet rejoins expelled members on its own, so an expulsion caused by a short stall is cheap.
  - `paxos_single_leader=ON`. The primary is the group's only consensus leader, so a slow or failed secondary does not delay commits. MySQL applies it when a group is bootstrapped and refuses a joiner whose setting differs from the group's (verified on 8.4.11), so it must be the same on every member. A group that was not bootstrapped by Vitess with this setting cannot be joined; it has to be restarted from scratch first.

  - `unreachable_majority_timeout=2`. A member cut off from the majority of its group cannot commit: MySQL blocks its transactions until a majority certifies them. After 2s it rolls them back, leaves the group and becomes `super_read_only` (and `offline_mode` under `OFFLINE_MODE`), without Vitess acting. With 0 it stays in its minority forever, its commits blocked. The value is a race with the group's own repair: when a group of two loses a member while a third one is joining, the remaining member finds itself without a majority 5s later (the joiner is not in its view yet), and the group survives only if it expels the lost member, with the joiner's vote, before the timeout. That expulsion takes up to about 2s: the joiner's group communication thread blocks for 1s at a time trying to connect to the lost member, and MySQL notices the restored majority on its next one-second check. In the chaos tests, a voter rejoining after a restart while the primary's cell was cut off kept the group's majority in 10 of 18 races with 1s, 19 of 22 with 2s and 17 of 18 with 3s, and the flapping-partition scenario S7d lost its majority in 9 of 10 runs with 1s, 2 of 10 with 2s and 2 of 10 with 3s; a lost race costs 28–78s without writes, and with 1s the joiner sometimes ended alone in the old group's configuration, in ERROR for a minute. 2s and 3s cannot be told apart, so Vitess uses 2s. The cost is fencing that is 1s later than with 1s: a partitioned primary's clients wait about 7s, rather than 6s, for their commits to fail (a semi-sync primary blocks them 10–30s), and it becomes `super_read_only` 1s later. The election does not depend on it (see "Unreachable majority timeout sweep" in `doc/failover-audit/GroupReplication.md`). It does not replace the Vitess checks: a stale member alone in a new incarnation, or a primary whose group shrank through clean leaves, never sees an unreachable majority, and MySQL stays readable after it leaves (see Legitimate group and Group shrink).

  These settings matched Uber's RFC (#18648) until Vitess raised the unreachable majority timeout from 1s to 2s after the chaos results above.
- **Plugin.** It is loaded with `INSTALL PLUGIN` when needed. That survives restarts, needs no mysqld restart, and is not binlogged. Loading it through my.cnf (`plugin-load-add=group_replication.so`) is also supported.

### Tablet (vttablet) behaviour

- **Startup** (`initializeReplication`). If the tablet is a voter (listed in the shard record):
  1. Configure GR and `START GROUP_REPLICATION` (join, never bootstrap).
  2. If the join fails because no member is reachable, retry in the background. Bootstrapping is never done from startup: two tablets could each create a group.
  3. `checkPrimaryShip` does not trust a stale PRIMARY record. The tablet starts as REPLICA, and the reconcile loop makes it PRIMARY if its MySQL is the group primary.

  Tablets that are not voters replicate asynchronously from the shard primary, as today.
- **Reconcile loop.** Every `--group-replication-sync-interval` (default 1s) the tablet reads its GR status:
  - If the member is the legitimate primary (see "The legitimate group"), and the tablet is not PRIMARY: promote the tablet record (`ChangeTabletType(PRIMARY)`). This yields a new `PrimaryTermStartTime`; the shard record follows through `shardSyncLoop`, and vtgate follows the new term.
  - If the member is active in a group of another incarnation than the recorded one: leave that group. It rejoins the legitimate group like any other voter (below).
  - If the tablet is PRIMARY but the member is not the group primary, or has lost quorum: demote the record to REPLICA and stop serving. vtgate buffers or fails fast exactly as for a PRS.
  - If the tablet is PRIMARY and its group view holds fewer than a majority of the listed voters: stop serving, keep the type, serve again once the majority is back (group replication policy only).
  - If the tablet is PRIMARY, a listed voter under a group replication policy, and its member is not in any group (`OFFLINE`, for example after mysqld restarted): demote the record to REPLICA without touching MySQL, so that vtgate stops routing to it and the next step rejoins it.
  - If the tablet is a voter but its member is `OFFLINE` or `ERROR`, and it is not in a backup or restore: rejoin with backoff, but only while another tablet reports an active member of the legitimate group. GR itself refuses a member with extra transactions.

  - On every run, it tells the query service whether MySQL is ONLINE in the shard's legitimate group, for replication lag polling (see "Replica reads and replication lag").

  This is how an unplanned failover elected by the group reaches Vitess within about a second, without VTOrc.
- **RPCs on an active member:**

  | RPC | GR behaviour |
  |---|---|
  | `InitPrimary` | Bootstrap the group on this tablet (shard must have no active member; caller holds the shard lock), then as today. |
  | `PromoteReplica` | `group_replication_set_as_primary(own uuid)`; wait for role PRIMARY and `super_read_only=OFF`; change type. No `RESET REPLICA ALL`. |
  | `DemotePrimary` | Stop serving and set `super_read_only`, as today; skip the semi-sync steps. |
  | `UndoDemotePrimary` | Only if the member is still the group primary; under a group replication policy with listed voters, only on the primary of the legitimate group (recorded incarnation, majority of the voters ONLINE). |
  | `SetReplicationSource` | No-op for the default channel; wait for the reparent journal or position; fix the tablet type. Refused on the group primary, whose request can only be stale, except for a reparent's request (with a reparent journal entry): PRS repoints the old primary while the primary-elect switches the group's primary, so it waits until the group moved the primary away. |
  | `StartReplication` / `StopReplication` | Join or leave the group. |
  | `SetReadWrite` (`read_only=OFF`) | **Refused unless the member is the group primary.** A GR secondary with `super_read_only=OFF` accepts writes and replicates them to the group; this was verified in the lab. |
  | `StartGroupReplication(bootstrap)` / `StopGroupReplication` (new) | Explicit membership control, used by VTOrc and the migration. `StartGroupReplication` first stops the async channel and afterwards clears it (`RESET REPLICA ALL`): GR refuses to start while the channel runs, and a leftover channel can be resurrected later. A bootstrap suspends the tablet's rejoins while it runs, and stops a `START GROUP_REPLICATION` still in progress (`errno 3724`, or a member RECOVERING without any ONLINE member) first. On a PRIMARY tablet under a group replication policy with several voters, it first stops serving (see "Group shrink fails closed"). |
- **Semi-sync.** `fixSemiSync` treats an active group with at least two ONLINE members as sufficient durability. It then disables semi-sync and does not open the semi-sync monitor.

### PlannedReparentShard

The orchestration is unchanged. The GR meaning comes from the tablet RPCs:

1. Preflight: the new primary must be a listed voter and an ONLINE member of the same group, and incarnation, as the current primary, and that group must be the shard's legitimate group (the recorded incarnation, a majority of the voters ONLINE); otherwise PRS fails with `FAILED_PRECONDITION` and names the voters. A tablet that is not a voter cannot be promoted while the group is active; swapping it in for a voter is future work. Without `--new-primary`, the election only considers the voters.
2. `DemotePrimary(old)` stops serving, so vtgate buffers.
3. `WaitForPosition(new, pos)`.
4. `PromoteReplica(new)` runs `group_replication_set_as_primary`. GR moves `super_read_only` and waits for in-flight transactions.
5. `SetReplicationSource` on the other members is a journal wait. The async replicas are re-pointed as today.
6. `PopulateReparentJournal(new)`.

The initial promotion of a shard that never had a primary selects the voters, with the primary-elect among them, and stores them in the shard record under the shard lock before `InitPrimary` bootstraps the group. Once `InitPrimary` returns, it records the new group's incarnation.

### EmergencyReparentShard

The group fails over by itself when a majority survives. ERS in GR mode must uphold the rules in `EmergencyReparentShard.md`: certainty, time-bound stages, shard lock re-checks, reparent journal, and no errant GTIDs.

1. Lock the shard and collect `FullStatus` from all tablets, time-bound.
2. If a reachable member reports that it is ONLINE, PRIMARY and has quorum, in the shard's legitimate group (recorded incarnation, majority of the voters ONLINE in its view), the group has already elected. Promote that tablet in topo (`PromoteReplica`, which is only a type change there) and write the reparent journal. If `--new-primary` names another member, follow with a PRS-style switch. Tablets that are not voters are re-pointed as async replicas; voters that are not active are left to rejoin the group.
3. If no reachable member has quorum, ERS fails, unless the operator passes `--group-replication-force-quorum` (not implemented in the prototype). That choice needs a human, like `--allow-split-brain-promotion`: it would pick the member with the most advanced *received* GTID set and use `group_replication_force_members`.

### VTOrc

VTOrc keeps its single loop. Discovery reads `FullStatus.group_replication_status`, and analysis gains GR codes:

| Analysis | Condition | Recovery |
|---|---|---|
| `GroupPrimaryNotInTopo` | A member is the legitimate primary of the shard's group, but its tablet is not the topo primary (for example, the tablet-local loop failed). | Confirm with fresh statuses, then `PromoteReplica` on that tablet (type change) plus journal. If the topology server of that tablet's cell does not answer, move the group primary instead (see below). |
| `GroupMemberNotOnline` | The tablet is a voter, it is OFFLINE or ERROR, and another tablet is an active member of the legitimate group with quorum. | Confirm with fresh statuses (at most 2s), then `StartGroupReplication(bootstrap=false)`. Runs whether or not the shard has a primary tablet. |
| `GroupNotBootstrapped` | GR policy, no member of the shard is active, and all voters are reachable. | `StartGroupReplication(bootstrap=true)` on the voter with the most advanced GTID set, under the shard lock, then record the new group's incarnation, and make the other voters join it (in the background). |
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

`StaleTopoPrimary` (a PRIMARY tablet with an older term than the shard's primary) only fixes the tablet type of a listed voter under a group replication policy: the voter rejoins its group, and configuring the default channel on it would start asynchronous replication next to the group (NEW-3 of the audit). Other tablets are still made asynchronous replicas.

A voter that `GroupMemberNotOnline` applies to is not analyzed for `ClusterHasNoPrimary` or `PrimaryTabletDeleted`, which outrank it: a group that lost the majority of its voters has no primary that a reparent could follow, and only gets one once its voters rejoin. The failovers are still analyzed on the group's members.

**A group primary whose cell's topology server is down.** The group's election ignores the topology. A tablet writes its own record, in its cell's topology server, before it becomes PRIMARY, and VTOrc's `PromoteGroupPrimary` goes through the tablet: while that server does not answer, neither can promote the member the group elected, and the shard has no primary (NEW-4 of the audit: 104.6s in the G9b chaos scenario, 169.6s in S9b). `PromoteGroupPrimary` reads the shard's tablets cell by cell (`topo.GetTabletMapAndFailedCellsForShard`, 2s per cell), and when the group primary's cell is among the cells that did not answer, it makes another member the group primary instead:

- Under the shard lock, it re-reads the status of the tablets of the cells that answered. The members of the shard's legitimate group among them (recorded incarnation, a majority of the voters ONLINE in their view) must agree that the analyzed member is their primary. The group primary's tablet must not be PRIMARY in the shard record, nor in its own state if it answers within 2s; the move does not otherwise depend on that tablet.
- The target is an ONLINE voter in that view, in a cell that answered, that the durability policy allows to be promoted (as for a planned reparent): the highest member weight, then the lowest alias. VTOrc calls `PromoteReplica` on it, which runs `group_replication_set_as_primary` and makes its tablet PRIMARY, and writes the reparent journal. No RPC is added.
- It waits until the group primary has not been the topology primary for 5s (`groupPrimaryMoveGracePeriod`): a tablet promotes itself within one sync interval, the per-cell read already waited 2s for the cell, and a short unavailability of a topology server (an etcd leader election) does not move the primary. A member whose cell answers is never moved away from.
- A member that the group primary was moved away from is neither moved to nor moved away from again for a minute, so the primary does not go back and forth. Without an eligible target, or within that minute, VTOrc logs why and skips the recovery of that tablet for 10s (`GroupPrimaryMoveBackoff`) instead of failing it on every poll.
- The tablet that could not be promoted may still write its record later, if its topology server comes back during its attempt: its term is taken after it checked that its MySQL is the group primary, so it is older than the new primary's; its own state stays REPLICA, since making MySQL writable is refused on a group secondary; and `StaleTopoPrimary` fixes the record. A tablet that did become PRIMARY before the move demotes itself through the reconcile loop.

The GR recoveries of a single tablet also refresh the shard's tablet records cell by cell, keeping the records of the cells that do not answer, instead of waiting the whole remote operation timeout for them. G9b: the shard primary changed 22.2s and 20.4s after the primary died (104.6s before); S9b 23.3s when the group elected the member of the cut-off cell (169.6s before).

`DeadPrimary` in a shard whose members are active does not run ERS right away: the group elects on its own. VTOrc waits for `--group-replication-failover-grace-period` (default 30s) for `GroupPrimaryNotInTopo` to appear, and only then falls back to the ERS path described above.

### Replica reads and replication lag

A member that leaves its group under the `READ_ONLY` exit state action keeps answering reads. Its tablet stays a serving REPLICA, and replica reads from it are bounded by the tablet's replication lag tracking, like those of any replica: the tablet reports its lag to vtgate, vtgate stops routing replica reads to a tablet whose lag exceeds `--discovery-low-replication-lag` (unless fewer than `--min-number-serving-vttablets` tablets are below it), and the tablet stops serving once its lag exceeds `--unhealthy-threshold`. Both lag tracking modes work on the members of a group.

- **Heartbeat** (`--heartbeat-enable`), unchanged. The primary writes a heartbeat row every `--heartbeat-interval`; the group replicates it like any transaction; a secondary's lag is the age of the last heartbeat it applied. A member that no longer receives the shard's transactions (cut off, out of its group, alone in a group of its own) no longer applies heartbeats, and its lag grows from the moment it stopped receiving them.
- **Polling** (`--enable-replication-reporter`). The poller used to read only `SHOW REPLICA STATUS FOR CHANNEL ''`. A member has no default channel (the tablet clears it when MySQL joins a group), so the poller reported `no replication status` and every secondary of a group stopped serving, partitioned or not. The poller now understands Group Replication (`go/vt/vttablet/tabletserver/repltracker/poller.go`):
  - **Which path.** It still reads `SHOW REPLICA STATUS FOR CHANNEL ''` first. Only if MySQL has no default channel does it read the group state, and it only uses it if the Group Replication plugin is active. A tablet without Group Replication runs the same query as before and gets the same lag or error; so does an asynchronous replica of a group, or a tablet that is being converted to Group Replication, which still replicates through its default channel even if the plugin is loaded. Reading the group state first would make the plugin, rather than the channel that actually carries the tablet's transactions, decide where the lag comes from.
  - **One query** (`readGroupReplicationApplierStatus` in `go/mysql/group_replication_lag.go`): whether the plugin is active; the member's own state; how many members its view has, and how many of them it can reach; its view id; how many transactions wait for certification or in the applier queue (`replication_group_member_stats`); and the age of the oldest transaction the applier works on: `UNIX_TIMESTAMP(NOW(6))` minus the smallest original commit timestamp of the `group_replication_applier` channel's workers (`APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP`, one row per parallel worker) and coordinator (`PROCESSING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP`). It reads only in-memory `performance_schema` tables and `information_schema.PLUGINS`.
  - **Lag of a healthy member**: the age of the oldest transaction the applier works on, the first transaction the member has not applied; 0 when no transaction waits. On a GR member, MySQL sets a transaction's original commit timestamp on the member that ran it (the primary) when the transaction is ready to commit, before certification, and every member keeps it. When transactions wait but none has reached a worker or the coordinator yet (an instant: 1 of 300 reads under write load on MySQL 8.4.11), the poller reads again; if that does not tell either, it estimates the lag like an unknown `Seconds_Behind_Source`. An idle group has no lag at all, where heartbeats report up to one heartbeat interval.
  - **Healthy** means: MySQL reports the member `ONLINE`, reaching a majority of its view (both read in the same query, on every health check), and the tablet manager's group replication sync loop found it `ONLINE` in the shard's legitimate group (the recorded incarnation, quorum, a majority of the listed voters `ONLINE` in its view) less than 10s ago, in the incarnation that MySQL reports now.
  - **Out-of-group rule.** Any other member (`OFFLINE`, `ERROR`, `RECOVERING`, `ONLINE` without quorum, `ONLINE` in a group without a majority of the voters or of another incarnation) does not receive the shard's transactions, and its idle applier would report no lag forever. It is reported like a replica whose replication stopped: the time since it was last found healthy plus the lag measured then, so the thresholds above take it out of replica reads. A member that was never found healthy reports an error (`UNAVAILABLE`), as a replica that never replicated does, and does not serve. A member whose group state cannot be read counts as not healthy: MySQL blocks reads of these tables while a `START` or `STOP GROUP_REPLICATION` runs on it, for example VTOrc's attempts to make a cut-off member join. The read is bounded to 1s and not repeated for 5s after a failure, because the health check holds the query service's state lock, which queries wait for.
  - **Layering.** MySQL's own view tells whether the member is in a group and can reach its majority, but not whether that group is the shard's legitimate group: a group that shrank to one member, or a stray incarnation that a failed join formed, has quorum in its own view. That takes the shard record (incarnation, voters) and the voters' `server_uuid`s, which the tablet manager's sync loop already reads every second to decide whether to promote, leave a foreign group or rejoin. The sync loop therefore passes its verdict, and the view it is about, to the query service on every run (`tabletserver.Controller.SetGroupReplicationVerdict`, in-process; no RPC or proto change), and the poller combines it with what MySQL reports at the time of each health check. The poller does not trust a verdict about another incarnation than MySQL's current one, so a stray group that formed since the last run of the loop is not healthy; a verdict that the loop stopped renewing (a stuck or stopped loop) is not trusted after 10s. A member that left its group, or lost its majority, is noticed by the next health check, without waiting for the loop. The verdict counts the voters in the view by their known `server_uuid` or by the MySQL address of their tablet record, without asking the voters for their `server_uuid`s on the way (the loop does that in the background), so that the loop never waits for a cut-off voter.
- **Clock skew.** Both modes compare a timestamp taken on the primary with the secondary's clock: heartbeats with the clocks of the two vttablets, polling with those of the two mysqlds (the original commit timestamp against the secondary's `NOW(6)`). A skew shifts the reported lag by the skew; a secondary whose clock is ahead reports more lag, and one whose clock is behind reports less, down to 0 (the poller does not report negative lag). Keep the clocks synchronized. The time since a member was last healthy is measured with the tablet's own clock and does not depend on skew.
- **Heartbeat or polling.** In the G13 chaos scenario (a secondary cut off from the other members, still reachable by vtgate; `--discovery-low-replication-lag 5s`, `--min-number-serving-vttablets 1`, `--health-check-interval 1s`), the cut-off secondary answered its last replica read 7.1s after the cut with heartbeats and 10.1s after it with polling; all its answers in between were stale. Heartbeat lag grows from the moment heartbeats stop arriving. Polling only knows that a member is cut off once MySQL does: Group Replication suspects an unreachable member after about 5s (not configurable), and the member leaves its group 2s later. The lag of a cut-off member is thus underestimated by up to about 5s with polling, and stale replica reads last that much longer; with the default `--discovery-low-replication-lag` of 30s, about 35s rather than 30s. Polling needs no writes on the primary and costs one query per health check per tablet; heartbeats write a row on the primary every interval, which every replica applies. Heartbeats are preferable where replica reads must be fresh; polling is supported.
- **Thresholds.** `group_replication_cross_cell` with three cells has two secondaries per shard, and vtgate's default `--min-number-serving-vttablets` of 2 keeps both in replica reads whatever their lag, up to `--discovery-high-replication-lag-minimum-serving` (2h): a cut-off secondary then serves stale reads until its tablet's `--unhealthy-threshold` (2h by default). Lower `--min-number-serving-vttablets` to 1, or `--unhealthy-threshold`, to take it out sooner. vtgate only routes replica reads to the REPLICA tablets of its own cell or cell alias, and prefers those of its own cell; with one voter per cell, a vtgate's cell may hold only the primary.

### Other components

- **Lag.** Both heartbeat and polling lag tracking work on the members of a group; see "Replica reads and replication lag".
- **Status queries.** The MySQL flavors now read `SHOW REPLICA STATUS FOR CHANNEL ''` and filter `replication_connection_configuration` on `CHANNEL_NAME = ''`. On a GR member the plugin's own channels (`group_replication_applier`, `group_replication_recovery`) show up in those tables and used to break parsing (`query returned 2 rows`).
- **Group Replication status.** It is read from `performance_schema.replication_group_members`, `replication_group_member_stats` (view id), `replication_connection_status` (received set) and the member's variables, including `group_replication_paxos_single_leader`. It does not read `replication_group_communication_information`: on a member expelled after a freeze, that query never returned and held a lock that the member's own rejoin needed, which wedged the member in ERROR (NEW-5 of the audit). The reads run with the caller's context, and the tablet bounds every read by 10s, so that a status query cannot hold the action lock indefinitely.
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
   - MySQL ≥ 8.0.27, the first version with the MySQL communication stack; 8.4 is recommended because of its defaults (`BEFORE_ON_PRIMARY_FAILOVER`, `OFFLINE_MODE`, certification GC).
   - The voters are selected (`SelectVoters` for the target policy, with the current primary as the group's primary and any voters already in the shard record as the current list, so a re-run keeps them). There must be at least 3 and at most 9; under `group_replication_cross_cell`, which allows one voter per cell, that means eligible tablets in at least 3 cells. The error names the cells found.
   - Every user table has a primary key and is InnoDB.
   - Every voter runs vttablet with `--enable-group-replication`, and its tablet record has a MySQL port.
   - The replication user's privileges are not read here, since vtctld does not know the user: each tablet checks them before it configures its join, the primary first, before the bootstrap.
2. **Store the voters** in the shard record.
3. **Bootstrap on the current primary** (`StartGroupReplication(bootstrap=true)`), and **record the group's incarnation** in the shard record. Writes continue. Semi-sync stays enabled, and the async replicas keep acknowledging.
4. **Join the other voters one by one** (`StartGroupReplication`). Each one stops its async channel, recovers incrementally from a donor, and becomes ONLINE. Cross-cell voters join first, so the group gets a cross-cell majority as early as possible.
5. **Disable semi-sync on the primary as soon as the group has two ONLINE members,** and before the last semi-sync acker leaves its async channel. With Vitess's infinite semi-sync timeout, losing the last acker blocks every commit; the lab reproduced this. Ackers that are not voters keep acknowledging until this point, so they count as remaining ackers while the voters join. From this point, the group's majority provides durability. The "group supersedes semi-sync" rule makes the tablet and VTOrc agree.
6. Every other tablet becomes, or stays, an async replica of the primary, with semi-sync off. A tablet that is an active member but not a voter leaves the group first.
7. After all shards are converted, set the keyspace durability policy. VTOrc then manages membership.

### Rollback: Group Replication → semi-sync

1. Set the keyspace durability policy back to the semi-sync policy. Nothing changes immediately, because the active groups supersede semi-sync.
2. For each secondary: `StopGroupReplication`, then `SetReplicationSource(primary, semiSync=true)`. The primary enables semi-sync as soon as it has an eligible acker, and before its group shrinks below two ONLINE members.
3. On the primary: stop serving (vtgate buffers), `StopGroupReplication`, clear `super_read_only`, and serve again. GR sets `super_read_only` when it stops. The lab measured about 4s of rejected writes without buffering; with the PRS-style buffering this becomes a short stall.
4. Clear the voters and the group incarnation in the shard record once the last member left the group. A later conversion selects them afresh and records the incarnation of its new group.

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
| One voter per cell (`TestGroupReplicationOneVoterPerCell`, two REPLICA tablets in one cell) | Only one of them votes; the other replicates asynchronously. When the voter's host dies, VTOrc makes the other tablet the cell's voter after the grace period, with 0 failed writes |
| Replica reads with polling lag tracking (`TestGroupReplicationReplicaReadsWithPollingLag`, `--enable-replication-reporter`, a cell alias over the three cells) | Both secondaries serve `@replica` reads through vtgate. Before the poller understood Group Replication, both reported `no replication status` and stopped serving: `no healthy tablet available` for every replica read |

**Unplanned failover, semi-sync vs Group Replication** (`TestUnplannedFailoverTimes`, opt-in with `VT_UNPLANNED_FAILOVER_TRIALS`; 3 trials each, one host, so no network latency between cells):

| Failure | Mode | New primary in topo | First write after the failure acknowledged | Acknowledged writes lost |
|---|---|---|---|---|
| Host crash (mysqld_safe, mysqld and vttablet killed) | semi-sync `cross_cell`, VTOrc poll 5s or 1s | 3.3s | 3.2–3.3s | 0 |
| Host crash | `group_replication_cross_cell` | 6.4–6.6s | 7.0–7.3s | 0 |
| mysqld frozen (SIGSTOP), vttablet alive | semi-sync `cross_cell`, VTOrc poll 5s or 1s | 11.3s | 11.3–12.3s | 0 |
| mysqld frozen | `group_replication_cross_cell` | 6.2–6.7s | 7.2–8.1s | 0 |

A dead host refuses connections, so VTOrc detects it on its next check and ERS takes about a second. Group Replication cannot beat its fixed 5s failure detection. A frozen MySQL is detected by timeouts on the Vitess side; the group's detection does not depend on how the member fails.

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
- A join that is in progress when the group it joins loses its majority ends, in MySQL 8.4.11, with the joiner alone in a new group incarnation 9–20s later, or blocks the member for up to a minute (a `STOP` fails meanwhile with errno 3663). Vitess never follows such a group and its tablet leaves it (4–6s), but the member then needs another join. After a total loss of the majority, members can stay in ERROR, or not answer status queries, for 5–27s while their communication engine exits, and the group is only bootstrapped again once every voter is readable. Under repeated partitions (the S7d chaos scenario) these MySQL delays, not Vitess, bound availability: see "S7d availability" in `doc/failover-audit/GroupReplication.md`. `unreachable_majority_timeout=0`, which avoids the leaves, did not help there and leaves an isolated primary writable. A join that a partition interrupts survives only if the group expels the partitioned member, with the joiner's vote, before the remaining member's `unreachable_majority_timeout`; the joiner's XCom thread blocks 1s at a time connecting to the partitioned member. With 2s the group kept its majority in 19 of 22 such races and in 24 of 26 S7d joins; in the others the remaining member did not see its majority restored, left, and the joiner formed a group of its own, which Vitess does not follow. When the remaining member leaves first, the joiner can end in a view of members that already left (`No donor available`, ERROR), and MySQL's leave then waits for a hard-coded 60s, during which the member's status queries do not return and VTOrc cannot bootstrap; with 2s this happened in 1 of 10 S7d runs. A primary lost within about 0.5s of a voter starting its join loses the majority whatever the timeout: the joiner has not booted yet (0 of 18 races kept at 1s, 2s and 3s).
- The member weight is applied only when a member joins.
- VTOrc moves a group primary out of a cell whose topology server is down only when its own vantage point cannot reach that server, and only after it has observed the member as the group primary through the member's vttablet (the analysis needs a valid check of it). Under an asymmetric partition, VTOrcs of different cells may disagree about which cells answer; the shard lock serializes their moves and each one re-reads the group under the lock, but the one-minute hold against moving back is per VTOrc. A group primary whose vttablet does not answer either is left to the ERS fallback after `--group-replication-failover-grace-period`.
- While a cell's topology server hangs, VTOrc's main loop is blocked by each topology refresh for `--topo-information-refresh-duration`, so discovery and analysis run at that period (3s in the chaos tests) rather than every second. In G9b this put the detection of `GroupPrimaryNotInTopo` 10s after the election; the move itself took 5s.

Follow-ups:
- PRS to a tablet that is not a voter, by swapping it into the voter list first.
- ERS with forced quorum (`group_replication_force_members`).
- Builtin-backup integration.
- vtadmin and operator support.
- Flow-control defaults.
- A multi-shard migration test.
