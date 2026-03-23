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
propagated to `SrvKeyspace`. No per-process flags are needed (except
`--gossip-listen-addr` for VTOrc). Use `vtctldclient` to manage it:

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
2. A strict majority of non-primary replicas are `Alive` (minimum 2),

then VTOrc produces a `PrimaryTabletUnreachableByQuorum` analysis, which
triggers ERS via the existing `recoverDeadPrimaryFunc` path.

On an exact tie (even replica count, exactly half alive), VTOrc's own
health check acts as a tiebreaker. Errors and unknowns abstain rather than
affirm.

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
