<!-- Draft for follow-up failover validation, not for vitessio/vitess. Not filed. -->
# Confirm the chaos profiles' assumptions against the deployment

**Tracker task:** T22
**Owner area:** Failover validation

## Problem

The `semisync-*` chaos profiles assume two things about the deployment they stand for:
- its my.cnf leaves `relay_log_recovery=1` (the Vitess default);
- the cells' topo survives the loss of one availability zone (the profile puts all cell topos on the global etcd, which no fault kills).

If the deployment runs one etcd per cell in that cell's availability zone, the cell-outage scenarios (S9, S9i, S9b) are much worse: the earlier audit measured 133–155s without failover.

## Evidence

Profile definition: `go/test/endtoend/vtorc/chaos/profile.go`.

## Proposed change

Update the profiles to match the deployment.

## Steps

1. Check the tablets' mysqld variables (`SHOW GLOBAL VARIABLES`) on a running cluster.
2. Check where the global and cell topo servers run, and which availability zones they depend on.
3. Adjust `go/test/endtoend/vtorc/chaos/profile.go` (`SharedCellTopo`, `ExtraMyCnf`) and re-run the affected scenarios.

Full context: `doc/failover-audit/SemiSyncFailover.md`.
