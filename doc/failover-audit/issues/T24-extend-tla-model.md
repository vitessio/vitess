<!-- Draft for follow-up work on the TLA+ model, not for vitessio/vitess. Not filed. -->
# Extend the semi-sync TLA+ model: PRS, RDONLY tablets, topo outages, clock skew

**Tracker task:** T24
**Owner area:** Failover validation (TLA+ model)

## Problem

The model leaves out PlannedReparentShard and `IncapacitatedPrimary` (which also call `DemotePrimary` and `SetReplicationSource`), RDONLY tablets and other durability policies, topo outages and cell-local topos, and wall-clock primary terms: it orders primaries with an integer term, so it assumes no clock skew.

## Evidence

Model and its limits: `doc/design-docs/semi_sync_tla/README.md`, section "What is not modeled".

## Proposed change

See steps.

## Steps

1. Add PRS steps and the PRS-then-ERS fallback.
2. Add a topo that can be unavailable or partitioned per cell.
3. Model `PrimaryTermStartTime` as per-host clock readings with bounded skew, and compare it where the code does (`shard_sync`, tablet startup, vtgate, VTOrc's `shardPrimary`).
4. Re-run `current_fixed` and `orcs2_no_expiry`.

Full context: `doc/failover-audit/SemiSyncFailover.md`.
