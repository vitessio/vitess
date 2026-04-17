# Release of Vitess v25.0.0
## Summary

### Table of Contents

- **[Minor Changes](#minor-changes)**
    - **[VTGate](#minor-changes-vtgate)**
        - [Priority-weighted warming reads concurrency](#vtgate-warming-reads-weighted-semaphore)

## <a id="minor-changes"/>Minor Changes</a>

### <a id="minor-changes-vtgate"/>VTGate</a>

#### <a id="vtgate-warming-reads-weighted-semaphore"/>Priority-weighted warming reads concurrency</a>

The `--warming-reads-concurrency` flag now uses a weighted semaphore instead of a buffered channel. Lower-priority queries (higher `PRIORITY` value) consume more semaphore capacity, making them the first to be shed under contention. This keeps warming read slots available for the most important queries. Default-priority queries (priority 0) behave the same as before.

Two new stats counters are exported alongside the existing `ReplicaWarmingReadsMirrored` counter: `ReplicaWarmingReadsDropped` (keyed by keyspace — warming reads shed due to concurrency limits or invalid priority) and `ReplicaWarmingReadsErrors` (keyed by keyspace and gRPC error code — warming reads that failed during execution).
