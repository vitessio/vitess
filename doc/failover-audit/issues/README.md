# Issue drafts

This directory has one draft per task of the [Semi-sync Failover Fixes](https://claude.ai/artifact/GDwmhFXv7utfKr2o6egyYB) tracker. The findings behind them are in `../SemiSyncFailover.md`. **None of these drafts has been filed.**

There are two kinds of draft:
- **`vitessio/vitess`.** These follow the repository's Bug Report template (`.github/ISSUE_TEMPLATE/bug_report.yml`). Sections: Overview (with a proposed fix and how to test it), Reproduction Steps, Binary Version, Environment, Log Fragments.
- **Deployment and follow-up.** Configuration changes for the deployment under test (mysqld settings, VTOrc placement and flags), and further validation work on the harness and the model. They are not Vitess bugs.

| Task | Priority | Draft | Destination | Status |
|---|---|---|---|---|
| T1 | P0 | [Stop replicas discarding ACKed transactions on restart](T1-relay-log-recovery-config.md) | deployment (mysqld config) | open, after T27 |
| T2 | P0 | [Replica restart during ERS repoints to the old primary](T2-replica-restart-repoints-during-ers.md) | vitessio/vitess | open |
| T3 | P0 | [ERS's repoints outlive the shard lock](T3-ers-detached-repoints-outlive-lock.md) | vitessio/vitess | open |
| T4 | P0 | [fixReplica repoints to a replaced primary](T4-fixreplica-stale-primary-view.md) | vitessio/vitess | open |
| T5 | P0 | [Lock lease expiry, and fixPrimary undoing a demotion](T5-lock-lease-expiry-and-fixprimary.md) | vitessio/vitess | open |
| T6 | P0 | [Repointing discards ACKed relay log events](T6-repoint-discards-relay-log.md) | vitessio/vitess | open |
| T7 | P1 | [Replica repairs starve the PrimarySemiSyncBlocked failover](T7-fixreplica-starves-semisync-blocked-ers.md) | vitessio/vitess | open |
| T8 | P1 | [A cancelled DemotePrimary reverts to serving](T8-cancelled-demote-reverts-to-serving.md) | vitessio/vitess | open |
| T9 | P1 | [ReplicaIsWritable does not set super_read_only](T9-replicaiswritable-super-read-only.md) | vitessio/vitess | open |
| T10 | P1 | [ERS on a stale shard record](T10-ers-stale-shard-record.md) | vitessio/vitess | open |
| T11 | P1 | [An aborted ERS leaves replicas repointed](T11-ers-abort-leaves-repointed-replicas.md) | vitessio/vitess | open |
| T12 | P1 | [Single-VTOrc blind spot](T12-single-vtorc-blind-spot.md) | deployment (VTOrc placement) | open |
| T13 | P1 | [Evaluate --emergency-reparent-require-primary-position](T13-require-primary-position-flag.md) | deployment (VTOrc flags) | open |
| T14 | P2 | [UnreachablePrimary restarts a healthy replica](T14-unreachableprimary-restarts-healthy-replica.md) | vitessio/vitess | open |
| T15 | P2 | [Repair retries and lock contention](T15-vtorc-retries-and-lock-contention.md) | vitessio/vitess | open |
| T16 | P2 | [vtgate forgets the highest primary term](T16-vtgate-forgets-highest-term.md) | vitessio/vitess | open |
| T17 | in review | [SetReplicationSource on a PRIMARY acknowledges unreplicated commits](T17-setreplicationsource-on-primary-acks.md) | vitessio/vitess | fixed on the branch, PR to open |
| T18 | in review | [ERS's IO-only stop skips a retrying receiver](T18-ers-io-stop-skips-retrying-receiver.md) | vitessio/vitess | fixed on the branch, PR to open |
| T19 | in review | [A drained tablet keeps sending ACKs](T19-drained-tablet-keeps-acking.md) | vitessio/vitess | fixed on the branch, PR to open |
| T20 | in review | [PromoteReplica discards received transactions](T20-promote-discards-received-transactions.md) | vitessio/vitess | fixed on the branch, PR to open |
| T21 | P2 | [Re-run the chaos matrix on MySQL 8.0](T21-rerun-on-mysql-80.md) | follow-up | open |
| T22 | P2 | [Confirm the profile's assumptions](T22-confirm-profile-assumptions.md) | follow-up | open |
| T23 | P2 | [Fix the remaining harness artifacts](T23-harness-artifacts.md) | follow-up | partly fixed |
| T24 | P2 | [Extend the TLA+ model](T24-extend-tla-model.md) | follow-up | open |
| T25 | P1 | [PlannedReparentShard outlives its shard lock](T25-prs-outlives-shard-lock.md) | vitessio/vitess | open |
| T26 | P2 | [PlannedReparentShard leaves the primary demoted when DemotePrimary errors](T26-prs-demote-error-leaves-primary-demoted.md) | vitessio/vitess | open |
| T27 | in review | [EmergencyReparentShard never starts a candidate's stopped applier](T27-ers-does-not-start-stopped-applier.md) | vitessio/vitess | fixed on the branch, PR to open |
