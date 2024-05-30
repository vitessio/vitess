## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
  - **[Breaking Changes](#breaking-changes)**
    - [Local examples now use etcd v3 storage and API](#local-examples-etcd-v3)
  - **[New command line flags and behavior](#new-flag)**
    - [VTOrc flag `--allow-emergency-reparent`](#new-flag-toggle-ers)
    - [VTOrc flag `--change-tablets-with-errant-gtid-to-drained`](#new-flag-errant-gtid-convert)
    - [ERS sub flag `--wait-for-all-tablets`](#new-ers-subflag)
    - [VTGate flag `--grpc-send-session-in-streaming`](#new-vtgate-streaming-sesion)
  - **[Experimental Foreign Key Support](#foreign-keys)**
  - **[VTAdmin](#vtadmin)**
    - [Updated to node v18.16.0](#update-node)
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
    - [Legacy Client Binaries](#legacy-client-binaries)
    - [Deprecated Flags](#deprecated-flags)
    - [Deprecated Stats](#deprecated-stats)
    - [Deleted Flags](#deleted-flags)
    - [Deleted `V3` planner](#deleted-v3)
    - [Deleted `k8stopo`](#deleted-k8stopo)
    - [Deleted `vtgr`](#deleted-vtgr)
    - [Deleted `query_analyzer`](#deleted-query_analyzer)
  - **[New Stats](#new-stats)**
    - [VTGate Vindex unknown parameters](#vtgate-vindex-unknown-parameters)
    - [VTBackup stat `Phase`](#vtbackup-stat-phase)
    - [VTBackup stat `PhaseStatus`](#vtbackup-stat-phase-status)
    - [Backup and restore metrics for AWS S3](#backup-restore-metrics-aws-s3)
    - [VTCtld and VTOrc reparenting stats](#vtctld-and-vtorc-reparenting-stats)
  - **[VTTablet](#vttablet)**
    - [VTTablet: New ResetSequences RPC](#vttablet-new-rpc-reset-sequences)
  - **[Docker](#docker)**
    - [Debian: Bookworm added and made default](#debian-bookworm)
    - [Debian: Buster removed](#debian-buster)
  - **[Durability Policies](#durability-policies)**
    - [New Durability Policies](#new-durability-policies)

## <a id="major-changes"/>Major Changes

### <a id="breaking-changes"/>Breaking Changes

#### <a id="local-examples-etcd-v3"/>Local examples now use etcd v3 storage and API
In previous releases the [local examples](https://github.com/vitessio/vitess/tree/main/examples/local) were
explicitly using etcd v2 storage (`etcd --enable-v2=true`) and API (`ETCDCTL_API=2`) mode. We have now
removed this legacy etcd usage and instead use the new (default) etcd v3 storage and API. Please see
[PR #13791](https://github.com/vitessio/vitess/pull/13791) for details. If you are using the local
examples in any sort of long-term non-testing capacity, then you will need to explicitly use the v2 storage
and API mode or [migrate your existing data from v2 to v3](https://etcd.io/docs/v3.5/tutorials/how-to-migrate/).

### <a id="new-flag"/>New command line flags and behavior

#### <a id="new-flag-toggle-ers"/>VTOrc flag `--allow-emergency-reparent`

VTOrc has a new flag `--allow-emergency-reparent` that specifies whether VTOrc is allowed to run emergency
failover operations. Users that want VTOrc to fix replication issues, but don't want it to run any failovers
should use this flag. This flag defaults to `true` which corresponds to the default behavior from prior releases.

#### <a id="new-flag-errant-gtid-convert"/>VTOrc flag `--change-tablets-with-errant-gtid-to-drained`

VTOrc has a new flag `--change-tablets-with-errant-gtid-to-drained` that allows users to choose whether VTOrc should change the
tablet type of tablets with errant GTIDs to `DRAINED`. By default, this flag is disabled.

This feature allows users to configure VTOrc such that any tablet that encounters errant GTIDs is automatically taken out of the
serving graph. These tablets can then be inspected for what the errant GTIDs are, and once fixed, they can rejoin the cluster.

#### <a id="new-ers-subflag"/>ERS sub flag `--wait-for-all-tablets`

vtctldclient command `EmergencyReparentShard` has a new sub-flag `--wait-for-all-tablets` that makes `EmergencyReparentShard` wait
for a response from all the tablets. Originally `EmergencyReparentShard` was meant only to be run when a primary tablet is unreachable.
We have realized now that there are cases when replication is broken but all tablets are reachable. In these cases, it is advisable to
call `EmergencyReparentShard` with `--wait-for-all-tablets` so that it does not ignore any of the tablets.

#### <a id="new-vtgate-streaming-sesion"/>VTGate GRPC stream execute session flag `--grpc-send-session-in-streaming`

This flag enables transaction support on VTGate's `StreamExecute` gRPC API.
When this is enabled, `StreamExecute`  will return the session in the last packet of the response.
Users should enable this flag only after client code has been changed to expect such a packet.

It is disabled by default.

### <a id="foreign-keys"/>Experimental Foreign Key Support

A new optional field `foreignKeyMode` has been added to the VSchema. This field can be provided for each keyspace. The VTGate flag `--foreign_key_mode` has been deprecated in favor of this field.

There are 3 foreign key modes now supported in Vitess -
1. `unmanaged` -
   This mode represents the default behavior in Vitess, where it does not manage foreign key column references. Users are responsible for configuring foreign keys in MySQL in such a way that related rows, as determined by foreign keys, reside within the same shard.
2. `managed` [EXPERIMENTAL] -
   In this experimental mode, Vitess is fully aware of foreign key relationships and actively tracks foreign key constraints using the schema tracker. VTGate will handle DML operations with foreign keys and correctly cascade updates and deletes. 
   It will also verify `restrict` constraints and validate the existence of parent rows before inserting child rows.
   This ensures that all child operations are logged in binary logs, unlike the InnoDB implementation of foreign keys.
   This allows the usage of VReplication workflows with foreign keys.
   Implementation details are documented in the [RFC for foreign keys](https://github.com/vitessio/vitess/issues/12967).
3. `disallow` -
   In this mode Vitess explicitly disallows any DDL statements that try to create a foreign key constraint. This mode is equivalent to running VTGate with the flag `--foreign_key_mode=disallow`.

In addition to query support, there is a new flag to `MoveTables` called `--atomic-copy` which should be used to import data into Vitess from databases which have foreign keys defined in the schema.

#### Upgrade process

After upgrading from v17 to v18, users should specify the correct foreign key mode for all their keyspaces in the VSchema using the new property.
Once this change has taken effect, the deprecated flag `--foreign_key_mode` can be dropped from all VTGates. Note that this is only required if running in `disallow` mode.
No action is needed to use `unmanaged` mode.

### <a id="vtadmin"/>VTAdmin

#### <a id="updated-node"/>vtadmin-web updated to node v18.16.0 (LTS)

Building `vtadmin-web` now requires node >= v18.16.0 (LTS). Breaking changes from v16 to v18 are listed
in https://nodejs.org/en/blog/release/v18, but none apply to VTAdmin. Full details on node v18.16.0 are listed
on https://nodejs.org/en/blog/release/v18.16.0.

### <a id="deprecations-and-deletions"/>Deprecations and Deletions

#### <a id="legacy-client-binaries"/>Legacy Client Binaries

`vtctldclient` is our new modern *Vitess controller daemon* (`vtctld`) *client* – which you will use to perform commands
and take actions in your Vitess clusters. It is [replacing the legacy `vtctl`/`vtctlclient` binaries](https://vitess.io/docs/18.0/reference/vtctldclient-transition/overview/).
Some of the benefits are:

- [Dedicated RPCs for each command](https://github.com/vitessio/vitess/blob/release-18.0/proto/vtctlservice.proto#L32-L353)
that are used between `vtctldclient` and `vtctld` – this offers clean separation of commands and makes it easier to
develop new features without impacting other commands. This also presents an [API that other clients (both Vitess and
3rd party) can use to interface with Vitess](https://vitess.io/blog/2023-04-17-vtctldserver-api/).
- Use of modern frameworks: [`pFlag`](https://github.com/spf13/pflag#readme), [`Cobra`](https://cobra.dev), and [`Viper`](https://github.com/spf13/viper#readme).
This makes development easier while also offering a better UX. For example, this offers a way to use
[configuration files](https://vitess.io/docs/18.0/reference/viper/config-files/) with support for
[dynamic configuration](https://vitess.io/docs/18.0/reference/viper/dynamic-values/) ([see also](https://github.com/vitessio/vitess/blob/release-18.0/doc/viper/viper.md)).
- The [reference documentation](https://vitess.io/docs/18.0/reference/programs/vtctldclient/) is now built through code. This
removes a burden from developers while helping users by ensuring the docs are always correct and up-to-date.

In Vitess 18 we have completed migrating all client commands to `vtctldclient` – the last ones being the [OnlineDDL](https://github.com/vitessio/vitess/issues/13712)
and [VReplication](https://github.com/vitessio/vitess/issues/12152) commands. With this work now completed, the
legacy `vtctl`/`vtctlclient` binaries are now fully deprecated and we plan to remove them in Vitess 19. You should
[begin your transition](https://vitess.io/docs/18.0/reference/vtctldclient-transition/) before upgrading to Vitess 18.

#### <a id="deprecated-flags"/>Deprecated Command Line Flags

Throttler related `vttablet` flags:

- `--throttle_threshold` is deprecated and will be removed in `v19`
- `--throttle_metrics_query` is deprecated and will be removed in `v19`
- `--throttle_metrics_threshold` is deprecated and will be removed in `v19`
- `--throttle_check_as_check_self` is deprecated and will be removed in `v19`
- `--throttler-config-via-topo` is deprecated after assumed `true` in `v17`. It will be removed in a future version.

Cache related `vttablet` flags:

- `--queryserver-config-query-cache-lfu` is deprecated and will be removed in `v19`. The query cache always uses a LFU implementation now.
- `--queryserver-config-query-cache-size` is deprecated and will be removed in `v19`. This option only applied to LRU caches, which are now unsupported.

Buffering related `vtgate` flags:

- `--buffer_implementation` is deprecated and will be removed in `v19`

Cache related `vtgate` flags:

- `--gate_query_cache_lfu` is deprecated and will be removed in `v19`. The query cache always uses a LFU implementation now.
- `--gate_query_cache_size` is deprecated and will be removed in `v19`. This option only applied to LRU caches, which are now unsupported.

VTGate flags:

- `--schema_change_signal_user` is deprecated and will be removed in `v19`
- `--foreign_key_mode` is deprecated and will be removed in `v19`. For more detail read the [foreign keys](#foreign-keys) section.

VDiff v1:

[VDiff v2 was added in Vitess 15](https://vitess.io/blog/2022-11-22-vdiff-v2/) and marked as GA in 16.
The [legacy v1 client command](https://vitess.io/docs/18.0/reference/vreplication/vdiffv1/) is now deprecated in Vitess 18 and will be **removed** in Vitess 19.
Please switch all of your usage to the [new VDiff client](https://vitess.io/docs/18.0/reference/vreplication/vdiff/) command ASAP.


#### <a id="deprecated-stats"/>Deprecated Stats

The following `EmergencyReparentShard` stats are deprecated in `v18` and will be removed in `v19`:
- `ers_counter`
- `ers_success_counter`
- `ers_failure_counter`

These metrics are replaced by [new reparenting stats introduced in `v18`](#vtctld-and-vtorc-reparenting-stats).

VTBackup stat `DurationByPhase` is deprecated. Use the binary-valued `Phase` stat instead.

#### <a id="deleted-flags"/>Deleted Command Line Flags

Flags in `vtcombo`:
- `--vtctld_addr`

Flags in `vtctldclient ApplySchema`:
- `--skip-preflight`

Flags in `vtctl ApplySchema`:
- `--skip_preflight`

Flags in `vtgate`:
- `--vtctld_addr`

Flags in `vttablet`:
- `--vtctld_addr`
- `--use_super_read_only`
- `--disable-replication-manager`
- `--init_populate_metadata`
- `--queryserver-config-pool-prefill-parallelism`
- `--queryserver-config-stream-pool-prefill-parallelism`
- `--queryserver-config-transaction-pool-prefill-parallelism`
- `--queryserver-config-schema-change-signal-interval`
- `--enable-lag-throttler`

Flags in `vtctld`:
- `--vtctld_show_topology_crud`
- `--durability_policy`

Flags in `vtorc`:
- `--lock-shard-timeout`
- `--orc_web_dir`

#### <a id="deleted-v3"/>Deleted `v3` planner

The `Gen4` planner has been the default planner since Vitess 14. The `v3` planner was deprecated in Vitess 15 and has been removed in Vitess 18.

#### <a id="deleted-k8stopo"/>Deleted `k8stopo`

`k8stopo` was deprecated in Vitess 17, see https://github.com/vitessio/vitess/issues/13298. It has now been removed.

#### <a id="deleted-vtgr"/>Deleted `vtgr`

`vtgr` was deprecated in Vitess 17, see https://github.com/vitessio/vitess/issues/13300. It has now been removed.

#### <a id="deleted-query_analyzer"/>Deleted `query_analyzer`

The undocumented `query_analyzer` binary has been removed in Vitess 18, see https://github.com/vitessio/vitess/issues/14054.

### <a id="new-stats"/>New stats

#### <a id="vtgate-vindex-unknown-parameters"/>VTGate Vindex unknown parameters

The VTGate stat `VindexUnknownParameters` gauges unknown Vindex parameters found in the latest VSchema pulled from the topology.

#### <a id="vtbackup-stat-phase"/>VTBackup `Phase` stat

In v17, the `vtbackup` stat `DurationByPhase` stat was added to measure the time spent by `vtbackup` in each phase. This stat turned out to be awkward to use in production, and has been replaced in v18 by a binary-valued `Phase` stat.

`Phase` reports a 1 (active) or a 0 (inactive) for each of the following phases:

 * `CatchupReplication`
 * `InitialBackup`
 * `RestoreLastBackup`
 * `TakeNewBackup`

To calculate how long `vtbackup` has spent in a given phase, sum the 1-valued data points over time and multiply by the data collection or reporting interval. For example, in Prometheus:

```
sum_over_time(vtbackup_phase{phase="TakeNewBackup"}) * <interval>
```
#### <a id="vtbackup-stat-phase-status"/>VTBackup `PhaseStatus` stat

`PhaseStatus` reports a 1 (active) or a 0 (inactive) for each of the following phases and statuses:

 * `CatchupReplication` phase has statuses `Stalled` and `Stopped`.
    * `Stalled` is set to `1` when replication stops advancing.
    * `Stopped` is set to `1` when replication stops before `vtbackup` catches up with the primary.

#### <a id="backup-restore-metrics-aws-s3"/>Backup and restore metrics for AWS S3

Requests to AWS S3 are instrumented in backup and restore metrics. For example:

```
vtbackup_backup_count{component="BackupStorage",implementation="S3",operation="AWS:Request:Send"} 823
vtbackup_backup_duration_nanoseconds{component="BackupStorage",implementation="S3",operation="AWS:Request:Send"} 1.33632421437e+11
vtbackup_restore_count{component="BackupStorage",implementation="S3",operation="AWS:Request:Send"} 165
vtbackup_restore_count{component="BackupStorage",implementation="S3",operation="AWS:Request:Send"} 165
```

#### <a id="vtctld-and-vtorc-reparenting-stats"/>VTCtld and VTOrc reparenting stats

New VTCtld and VTOrc stats were added to measure frequency of reparents by keyspace/shard:
- `emergency_reparent_counts` - Number of times `EmergencyReparentShard` has been run. It is further subdivided by the keyspace, shard and the result of the operation.
- `planned_reparent_counts` - Number of times `PlannedReparentShard` has been run. It is further subdivided by the keyspace, shard and the result of the operation.

Also, the `reparent_shard_operation_timings` stat was added to provide per-operation timings of reparent operations.

### <a id="vttablet"/>VTTablet

#### <a id="vttablet-new-rpc-reset-sequences"/>New ResetSequences rpc

A new VTTablet RPC `ResetSequences` has been added, which is being used by `MoveTables` and `Migrate` for workflows
where a `sequence` table is being moved (https://github.com/vitessio/vitess/pull/13238). This has an impact on the
Vitess upgrade process from an earlier version if you need to use such a workflow before the entire cluster is upgraded.

Any MoveTables or Migrate workflow that moves a sequence table should only be run after all vitess components have been
upgraded, and no upgrade should be done while such a workflow is in progress.

#### <a id="vttablet-tx-throttler-dry-run"/>New Dry-run/monitoring-only mode for the transaction throttler

A new CLI flag `--tx-throttler-dry-run` to set the Transaction Throttler to monitoring-only/dry-run mode has been added.
If the transaction throttler is enabled with `--enable-tx-throttler` and the new dry-run flag is also specified, the
tablet will not actually throttle any transactions; however, it will increase the counters for transactions throttled
(`vttablet_transaction_throttler_throttled`). This allows users to deploy the transaction throttler in production and
gain observability on how much throttling would take place, without actually throttling any requests.

### <a id="docker"/>Docker Builds

#### <a id="debian-bookworm"/>Bookworm added and made default

Bookworm was released on 2023-06-10, and will be the new default base container for Docker builds.
Bullseye images will still be built and available as long as the OS build is current, tagged with the `-bullseye` suffix.

#### <a id="debian-buster"/>Buster removed

Buster LTS supports will stop in June 2024, and Vitess 18 will be supported through October 2024.
To prevent supporting a deprecated buster build for several months after June 2024, we are preemptively
removing Vitess support for Buster.

### <a id="durability-policies"/>Durability Policies

#### <a id="new-durability-policies"/>New Durability Policies

Two new built-in durability policies have been added in Vitess 18: `semi_sync_with_rdonly_ack` and `cross_cell_with_rdonly_ack`.
These policies are similar to `semi_sync` and `cross_cell` respectively, the only difference is that `rdonly` tablets can also send semi-sync ACKs.