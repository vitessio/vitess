<!-- Draft for follow-up work on the TLA+ model, not for vitessio/vitess. Not filed. -->
# Extend the semi-sync TLA+ model: PRS's other paths, RDONLY tablets, topo outages, clock skew

**Tracker task:** T24
**Owner area:** Failover validation (TLA+ model)

## Problem

The model now covers PlannedReparentShard's graceful promotion (configurations `prs_*`, findings B9 and B10). It still leaves out PRS's initial and potential promotions, `IncapacitatedPrimary` and the other VTOrc recoveries that call PRS, RDONLY tablets and other durability policies, topo outages and cell-local topos, and wall-clock primary terms: it orders primaries with an integer term, so it assumes no clock skew.

## Evidence

Model and its limits: `doc/design-docs/semi_sync_tla/README.md`, section "What is not modeled".

## Proposed change

See steps.

## Steps

1. Done: PRS's graceful promotion. Remaining: its initial and potential promotions, and the PRS-then-ERS fallback.
2. Add a topo that can be unavailable or partitioned per cell.
3. Model `PrimaryTermStartTime` as per-host clock readings with bounded skew, and compare it where the code does (`shard_sync`, tablet startup, vtgate, VTOrc's `shardPrimary`).
4. Re-run `current_fixed` and `orcs2_no_expiry`.

Full context: `doc/failover-audit/SemiSyncFailover.md`.
