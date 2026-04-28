# Release of Vitess v25.0.0

## Summary

### Table of Contents

- **[Major Changes](#major-changes)**
    - **[New Support](#new-support)**
    - **[Breaking Changes](#breaking-changes)**
    - **[VTOrc](#major-changes-vtorc)**
        - [Gossip Protocol for Primary Tablet Failure Detection](#vtorc-gossip-protocol)
- **[Minor Changes](#minor-changes)**
    - **[VReplication](#minor-changes-vreplication)**
        - [Default data protection for `_reverse` workflow cancel/complete](#vreplication-reverse-workflow-data-protection)

## <a id="major-changes"/>Major Changes</a>

### <a id="new-support"/>New Support</a>

### <a id="breaking-changes"/>Breaking Changes</a>

### <a id="major-changes-vtorc"/>VTOrc</a>

#### <a id="vtorc-gossip-protocol"/>Gossip Protocol for Primary Tablet Failure Detection</a>

VTOrc can now detect and recover from primary vttablet process failures even when MySQL is still running. Previously, VTOrc only detected MySQL replication issues, leaving shards unavailable when the vttablet process died until an operator intervened.

This feature uses a gossip protocol where tablets periodically exchange liveness information. When a primary vttablet stops gossiping (because the process died), peers detect the stale timestamp and mark it as down. VTOrc then triggers an Emergency Reparent Shard (ERS) after confirming quorum agreement among replicas.

**Enabling Gossip**

Enable gossip at the keyspace level using `vtctldclient`:

```bash
vtctldclient UpdateGossipConfig --enable \
  --ping-interval=1s \
  --max-update-age=5s \
  --phi-threshold=4 \
  commerce
```

Configuration is stored in the topology and propagated via `SrvKeyspace`, so changes take effect immediately without restarting tablets or VTOrc.

**Configuration Parameters**

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--ping-interval` | How often tablets exchange gossip messages | 1s |
| `--max-update-age` | How long before a silent node is considered stale | 5s |
| `--phi-threshold` | Phi-accrual failure detector threshold (higher = more tolerant of delays) | 4 |

**Requirements**

Tablets must have the gossip gRPC service enabled. Add `grpc-gossip` to the `--service-map` flag:

```bash
vttablet --service-map grpc-queryservice,grpc-tabletmanager,grpc-gossip ...
```

**Safety Guarantees**

- **Quorum requirement**: ERS only triggers when a strict majority of non-primary replicas confirm the primary is down.
- **Small-shard safety**: Shards with 2 or fewer replicas require VTOrc's own health check to corroborate the gossip verdict before triggering ERS.
- **Partition tolerance**: VTOrc suppresses gossip-based ERS when it suspects it may be partitioned from the cluster (cannot reach more than half of the primaries it monitors).

**New Analysis Type**

VTOrc introduces a new analysis type: `PrimaryTabletUnreachableByQuorum`. This appears in the `/api/detection-analysis` output when gossip indicates the primary tablet is down with quorum confirmation.

**Debug Endpoints**

Both vttablet and VTOrc expose a `/debug/gossip` endpoint showing current gossip state, member status, and configuration.

See [#19686](https://github.com/vitessio/vitess/pull/19686) for implementation details.

## <a id="minor-changes"/>Minor Changes</a>

#### <a id="vreplication-reverse-workflow-data-protection"/>Default data protection for `_reverse` workflow cancel/complete</a>

When calling `cancel` or `complete` on an auto-generated `_reverse` workflow without explicitly providing `--keep-data=false`, the system now defaults to keeping data and returns a warning. This prevents accidental deletion of production tables on the original source side, where the `_reverse` workflow's target is actually your production keyspace.

**Behavior change:**

| Workflow type | `--keep-data` flag | Effective `keep_data` | Warning emitted |
|--------------|-------------------|----------------------|-----------------|
| Normal       | omitted           | `false`              | No              |
| `_reverse`   | omitted           | `true`               | **Yes** |
| `_reverse`   | `--keep-data=false` | `false`            | No              |

The `--keep-data` flag help text has been updated to note this default explicitly. This change applies to MoveTables, Reshard, and other VReplication workflow types that use the shared cancel/complete paths.

See [#19906](https://github.com/vitessio/vitess/pull/19906) for details.
