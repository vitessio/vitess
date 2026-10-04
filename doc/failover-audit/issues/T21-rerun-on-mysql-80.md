<!-- Draft for follow-up failover validation, not for vitessio/vitess. Not filed. -->
# Re-run the chaos matrix on MySQL 8.0

**Tracker task:** T21
**Owner area:** Failover validation

## Problem

Many semi-sync deployments still run MySQL 8.0; all chaos runs here used 8.4.6 (the earlier audit used 8.0.46). Semi-sync and relay-log behaviour should match, but the fixes (notably T20's `WAIT_FOR_EXECUTED_GTID_SET` path and T6's receiver-only CHANGE) need verification on 8.0.

## Evidence

All results in `doc/failover-audit/SemiSyncFailover.md` are on MySQL 8.4.6.

## Proposed change

Run the existing harness; no code change expected.

## Steps

1. Install a MySQL 8.0 release (`VT_MYSQL_ROOT`), use `config/mycnf/mysql8026.cnf`.
2. Run `chaos_matrix.sh` for `semisync-3vtorc`, `semisync-1vtorc` (colo and remote) with the binaries of `main` and of the branch.
3. Compare with the 8.4 tables in the report.

Full context: `doc/failover-audit/SemiSyncFailover.md`.
