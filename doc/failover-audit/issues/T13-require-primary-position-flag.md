<!-- Draft for the deployment's VTOrc flags, not for vitessio/vitess. Not filed. -->
# Evaluate VTOrc's --emergency-reparent-require-primary-position

**Tracker task:** T13
**Owner area:** deployment: VTOrc flags

## Problem

Recent Vitess added an opt-in VTOrc flag, `--emergency-reparent-require-primary-position` (default false). With it, ERS refuses unless some candidate has received the primary GTID set that VTOrc last polled. It turns some acknowledged-write losses (relay log discard, T1/T6) into refused failovers, but only for commits VTOrc saw before its last poll, and not after a VTOrc restart.

## Evidence

- Flag added by commits `d071e50`, `4689ec4`, `317423f` (Vitess `main`).
- S11/S11k lose 500 writes without it (both binaries).

## Proposed change

Enable it if it closes S11 in practice without adding refusals to the ordinary failure scenarios; it is a complement to T1, not a replacement.

## Steps

1. Run the chaos S11, S11k and S13-T3 scenarios with the flag on (add it to `OrcExtraArgs` in `profile.go`).
2. Check how often ERS refuses in the ordinary scenarios (S1–S10): a refusal is an outage.
3. Decide; if adopted, add it to the flags the deployment starts VTOrc with.

Full context: `doc/failover-audit/SemiSyncFailover.md`.
