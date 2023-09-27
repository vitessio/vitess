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
  - **[VTAdmin](#vtadmin)**
    - [Updated to node v18.16.0](#update-node)
  - **[Deprecations and Deletions](#deprecations-and-deletions)**
    - [Deprecated Flags](#deprecated-flags)
    - [Deleted `V3` planner](#deleted-v3)
    - [Deleted `k8stopo`](#deleted-k8stopo)
    - [Deleted `vtgr`](#deleted-vtgr)
    - [Deleted `query_analyzer`](#deleted-query_analyzer)
    - [Deprecated VTBackup stat `DurationByPhase`](#deprecated-vtbackup-stat-duration-by-phase)
    - [Deprecated VDiff v1](#deprecated-vdiff-v1)
  - **[New stats](#new-stats)**
    - [VTGate Vindex unknown parameters](#vtgate-vindex-unknown-parameters)
    - [VTBackup stat `Phase`](#vtbackup-stat-phase)
    - [VTBackup stat `PhaseStatus`](#vtbackup-stat-phase-status)
    - [Backup and restore metrics for AWS S3](#backup-restore-metrics-aws-s3)
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
[PR #13791](https://github.com/vitessio/vitess/pull/13791) for additional info. If you are using the local
examples in any sort of long-term non-testing capacity, then you will need to explicitly use the v2 storage
and API mode or [migrate your existing data from v2 to v3](https://etcd.io/docs/v3.5/tutorials/how-to-migrate/).

### <a id="new-flag"/>New command line flags and behavior

#### <a id="new-flag-toggle-ers"/>VTOrc flag `--allow-emergency-reparent`

VTOrc has a new flag `--allow-emergency-reparent` that allows the users to toggle the ability of VTOrc to run emergency
reparent operations. The users that want VTOrc to fix the replication issues, but don't want it to run any reparents
should start using this flag. By default, VTOrc will be able to run `EmergencyReparentShard`. The users must specify the
flag to `false` to change the behaviour.

#### <a id="new-flag-errant-gtid-convert"/>VTOrc flag `--change-tablets-with-errant-gtid-to-drained`

VTOrc has a new flag `--change-tablets-with-errant-gtid-to-drained` that allows users to choose whether VTOrc should change the
tablet type of tablets with errant GTIDs to `DRAINED`. By default, the flag is false.

This feature allows users to configure VTOrc such that any tablet that encounters errant GTIDs is automatically taken out of the
serving graph. These tablets can then be inspected for what the errant GTIDs are, and once fixed, they can rejoin the cluster.

#### <a id="new-ers-subflag"/>ERS sub flag `--wait-for-all-tablets`

Running `EmergencyReparentShard` from the vtctldclient has a new sub-flag `--wait-for-all-tablets` that makes `EmergencyReparentShard` wait 
for a response from all the tablets. Originally `EmergencyReparentShard` was meant only to be run when a primary tablet is unreachable.
We have realized now that there are cases when the replication is broken but all the tablets are reachable. In these cases, it is advisable to 
call `EmergencyReparentShard` with `--wait-for-all-tablets` so that it doesn't ignore one of the tablets.

#### <a id="new-vtgate-streaming-sesion"/>VTGate GRPC stream execute session flag `--grpc-send-session-in-streaming`

This flag enables transaction support on `StreamExecute` api.
One enabled, VTGate `StreamExecute` grpc api will send session as the last packet in the response.
The client should enable it only when they have made the required changes to expect such a packet.

It is disabled by default.

### <a id="vtadmin"/>VTAdmin

#### <a id="updated-node"/>vtadmin-web updated to node v18.16.0 (LTS)

Building vtadmin-web now requires node >= v18.16.0 (LTS). Breaking changes from v16 to v18 are listed
in https://nodejs.org/en/blog/release/v18.0.0, but none apply to VTAdmin. Full details on v18.16.0 are listed
here https://nodejs.org/en/blog/release/v18.16.0.

### <a id="deprecations-and-deletions"/>Deprecations and Deletions

#### <a id="deprecated-flags"/>Deprecated Command Line Flags

Throttler related `vttablet` flags:

- `--enable-lag-throttler` is now removed after being deprecated in `v17.0`
- `--throttle_threshold` is deprecated and will be removed in `v19.0`
- `--throttle_metrics_query` is deprecated and will be removed in `v19.0`
- `--throttle_metrics_threshold` is deprecated and will be removed in `v19.0`
- `--throttle_check_as_check_self` is deprecated and will be removed in `v19.0`
- `--throttler-config-via-topo` is deprecated after assumed `true` in `v17.0`. It will be removed in a future version.

Cache related `vttablet` flags:

- `--queryserver-config-query-cache-lfu` is deprecated and will be removed in `v19.0`. The query cache always uses a LFU implementation now.
- `--queryserver-config-query-cache-size` is deprecated and will be removed in `v19.0`. This option only applied to LRU caches, which are now unsupported.

Buffering related `vtgate` flags:

- `--buffer_implementation` is deprecated and will be removed in `v19.0`

Cache related `vtgate` flags:

- `--gate_query_cache_lfu` is deprecated and will be removed in `v19.0`. The query cache always uses a LFU implementation now.
- `--gate_query_cache_size` is deprecated and will be removed in `v19.0`. This option only applied to LRU caches, which are now unsupported.

VTGate flag:

- `--schema_change_signal_user` is deprecated and will be removed in `v19.0`

#### <a id="deleted-v3"/>Deleted `v3` planner

The `Gen4` planner has been the default planner since Vitess 14. The `v3` planner was deprecated in Vitess 15 and has now been removed in this release.

#### <a id="deleted-k8stopo"/>Deleted `k8stopo`

The `k8stopo` has been deprecated in Vitess 17, also see https://github.com/vitessio/vitess/issues/13298. With Vitess 18
the `k8stopo` has been removed.

#### <a id="deleted-vtgr"/>Deleted `vtgr`

The `vtgr` has been deprecated in Vitess 17, also see https://github.com/vitessio/vitess/issues/13300. With Vitess 18 `vtgr` has been removed.

#### <a id="deleted-query_analyzer"/>Deleted `query_analyzer`

The undocumented `query_analyzer` binary has been removed in Vitess 18, see https://github.com/vitessio/vitess/issues/14054.

#### <a id="deprecated-vdiff-v1"/>Deprecated VDiff v1

[VDiff v2 was added in Vitess 15.0](https://vitess.io/blog/2022-11-22-vdiff-v2/) and marked as GA in 16.0. The [legacy v1 client command](https://vitess.io/docs/18.0/reference/vreplication/vdiffv1/) is now deprecated in Vitess 18.0 and will be **removed** in 19.0. Please switch all of your usage to the [new VDiff client](https://vitess.io/docs/18.0/reference/vreplication/vdiff/) command ASAP.

#### <a id="deprecated-vtbackup-stat-duration-by-phase"/>Deprecated VTbackup stat `DurationByPhase`

VTBackup stat `DurationByPhase` is deprecated. Use the binary-valued `Phase` stat instead.

### <a id="new-stats"/>New stats

#### <a id="vtgate-vindex-unknown-parameters"/>VTGate Vindex unknown parameters

The VTGate stat `VindexUnknownParameters` gauges unknown Vindex parameters found in the latest VSchema pulled from the topology.

#### <a id="vtbackup-stat-phase"/>VTBackup `Phase` stat

In v17, the `vtbackup` stat `DurationByPhase` stat was added measuring the time spent by `vtbackup` in each phase. This stat turned out to be awkward to use in production, and has been replaced in v18 by a binary-valued `Phase` stat.

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

### <a id="vttablet"/>VTTablet

#### <a id="vttablet-new-rpc-reset-sequences"/>New ResetSequences rpc

A new vttablet RPC `ResetSequences` has been added, which is being used by `MoveTables` and `Migrate` for workflows
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

### <a id="docker"/>Docker

#### <a id="debian-bookworm"/>Bookworm added and made default

Bookworm was released on 2023-06-10, and will be the new default base container for Docker builds.
Bullseye images will still be built and available as long as the OS build is current, tagged with the `-bullseye` suffix.

#### <a id="debian-buster"/>Buster removed

Buster LTS supports will stop in June 2024, and Vitess v18.0 will be supported through October 2024. 
To prevent supporting a deprecated buster build for several months after June 2024, we are preemptively
removing Vitess support.

### <a id="durability-policies"/>Durability Policies

#### <a id="new-durability-policies"/>New Durability Policies

2 new inbuilt durability policies have been added to Vitess in this release namely `semi_sync_with_rdonly_ack` and `cross_cell_with_rdonly_ack`. These policies are exactly like `semi_sync` and `cross_cell` respectively, and differ just in the part where the rdonly tablets can also send semi-sync ACKs. 
