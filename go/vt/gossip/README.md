# Gossip

Package gossip provides a minimal membership and liveness layer for Vitess
tablets and VTOrc. It enables quorum-based failure detection: when enough
peers agree that a PRIMARY vttablet is unreachable, VTOrc can trigger
Emergency Reparent Shard (ERS) without relying solely on its own view.

## How it works

Each participating process (vttablet or VTOrc) runs a gossip agent that
periodically exchanges state with a randomly chosen peer via gRPC push-pull
RPCs. The agent maintains:

- **Member list** with keyspace/shard/tablet-alias metadata.
- **Per-peer phi-accrual failure detector** that tracks heartbeat intervals
  and computes a suspicion score.
- **Liveness state** per member: `Alive`, `Suspect`, `Down`, or `Unknown`.

When a vttablet process dies, it stops participating in gossip exchanges.
Other tablets' detectors notice the silence: phi rises past the configured
threshold (marking the node `Suspect`), and once the last update exceeds
`MaxUpdateAge` the node transitions to `Down`. Never-observed seeds remain
`Unknown` until their first exchange, preventing false positives during
startup.

State merges use timestamp-based last-writer-wins. A living node
continuously refreshes its own timestamp on every gossip round, so peers
always have fresh evidence of liveness. Future timestamps from clock-skewed
peers are clamped to local time. On equal timestamps, `Alive` takes
precedence over `Down` to prevent late-joining observers from latching onto
stale verdicts.

## Configuration

Gossip is configured per-keyspace via the topo `Keyspace` record and
propagated to `SrvKeyspace`. Per-process setup:

- **vttablet**: the gossip gRPC service reuses the existing vttablet
  gRPC server and inherits the tablet-manager TLS flags. It registers
  only when `grpc-gossip` is in `--service-map` (the example scripts
  under `examples/common/scripts/vttablet-up.sh` include it).
- **VTOrc**: set `--gossip-listen-addr` so VTOrc stands up its own
  gossip gRPC listener.

Use `vtctldclient` to manage it:

```sh
# Enable gossip with default tuning.
vtctldclient UpdateGossipConfig --enable commerce

# Customize tuning parameters.
vtctldclient UpdateGossipConfig \
  --enable \
  --phi-threshold=4 \
  --ping-interval=1s \
  --max-update-age=5s \
  commerce

# Disable gossip.
vtctldclient UpdateGossipConfig --disable commerce
```

Changes propagate live to running tablets and VTOrc via SrvKeyspace
watchers. No restart is required.

| Parameter | Default | Description |
|---|---|---|
| `--phi-threshold` | 4 | Phi-accrual suspicion threshold. |
| `--ping-interval` | 1s | How often each node exchanges state with a random peer. |
| `--max-update-age` | 5s | How long a stale last-update must be before marking a peer `Down`. |

## Seed discovery

Seeds are discovered automatically from topo:

- **vttablet** queries `GetTabletMapForShard` for other tablets in its shard.
- **VTOrc** queries its tablet DB for all known tablets' gRPC addresses.

No static seed list is needed.

## Quorum analysis

VTOrc merges gossip state into its `CheckAndRecover` loop. For each shard:

1. If the PRIMARY's gossip status is `Down`, **and**
2. A strict majority of non-primary replicas are `Alive` (with at least
   2 alive observers),

then VTOrc produces a `PrimaryTabletUnreachableByQuorum` analysis, which
triggers ERS via the existing `recoverDeadPrimaryFunc` path.

### Corroboration from VTOrc's own view

VTOrc augments the gossip verdict with a `VTOrcView` built from its
per-primary health checks:

- **Small-shard safety**: for shards with ≤2 total replicas, strict
  majority equals unanimous — too easy for a correlated failure (e.g. a
  cross-cell partition) to produce a false positive. These shards
  additionally require VTOrc's own health check to report the primary
  as unreachable.
- **Exact-tie breaker**: for larger shards, VTOrc's view breaks exact
  ties (even replica count, exactly half alive). Errors and unknowns
  abstain.
- **Partition heuristic**: if VTOrc cannot reach more than half of the
  primaries it knows about, its view is most likely about VTOrc's own
  connectivity rather than the primaries. In that case the corroborating
  signal is suppressed so VTOrc won't trigger ERS based on its own
  partition.

## Debug endpoint

Both vttablet and VTOrc serve `/debug/gossip` returning a JSON snapshot:

```json
{
  "node_id": "zone1-0000000100",
  "bind_addr": "host1:15999",
  "epoch": 42,
  "members": [
    {"id": "zone1-0000000100", "addr": "host1:15999", "meta": {"keyspace": "commerce", "shard": "0"}},
    {"id": "zone1-0000000101", "addr": "host2:15999", "meta": {"keyspace": "commerce", "shard": "0"}}
  ],
  "states": {
    "zone1-0000000100": {"status": "alive", "phi": 0.1, "last_update": "2026-03-22T10:00:00.000Z"},
    "zone1-0000000101": {"status": "down", "phi": 8.3, "last_update": "2026-03-22T09:59:50.000Z"}
  }
}
```
