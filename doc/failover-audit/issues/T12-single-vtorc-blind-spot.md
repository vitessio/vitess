<!-- Draft for the deployment's VTOrc placement, not for vitessio/vitess. Not filed. -->
# Remove the single-VTOrc failover blind spot

**Tracker task:** T12
**Owner area:** deployment: VTOrc placement

## Problem

A cluster that runs a single VTOrc instance, in the first of its three cells, has a failover blind spot. If that cell is lost or partitioned, nothing can fail over the shard, whatever happens to the primary. When the VTOrc shares the primary's cell (one in three clusters, and it changes with every failover), a single AZ outage takes the primary and its only failover agent together.

## Evidence

- E2E, `semisync-1vtorc` with the VTOrc in the primary's cell: S9 (cell killed) no failover for 136s, S9i (cell partitioned) 133s, S8 (VTOrc cut from topo) 134s, V4 66s; all bounded only by when the test restored the cell.
- `semisync-1vtorc` with the VTOrc in another cell: S8b (VTOrc cut from topo) no failover for 131s; V2 (VTOrc with primary, both cut from replicas) 95s.
- `semisync-3vtorc` (3 VTOrcs): the same scenarios fail over in 3–30s.

## Proposed change

Preferred: run VTOrc in at least two cells. Interim: keep the VTOrc out of the primary's cell.

## Steps

1. Decide between: a standby VTOrc in another cell for every such cluster (three VTOrcs, one per cell), or keeping one VTOrc but never in the primary's cell (move it after failovers).
2. Weigh the lock contention of multiple VTOrcs (T5, T7, T15) and land those fixes first if going to three.
3. Re-run the chaos V-scenarios and S8/S9/S9i with the new placement.

Full context: `doc/failover-audit/SemiSyncFailover.md`.
