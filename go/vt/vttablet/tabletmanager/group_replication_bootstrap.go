/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tabletmanager

import (
	"context"
	"log/slog"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// groupReplicationRelayLogApplyTimeout bounds how long a bootstrap waits, under the action lock,
// for MySQL to apply the transactions that it received from its last group and had not applied
// (see checkGroupBootstrapLocked). The caller's context may end it earlier.
var groupReplicationRelayLogApplyTimeout = 30 * time.Second

// groupReplicationRefusalReserve is the part of its caller's deadline that a bootstrap keeps after
// applying the relay log: for stopping the applier thread, reading MySQL's executed set and status
// again, and returning its refusal. VTOrc withdraws the bootstrap intent only on a definitive refusal
// that reaches it (see refuseGroupBootstrapLocked); a refusal that arrives after the caller gave up
// leaves the intent to fence every other voter until it expires.
var groupReplicationRefusalReserve = 5 * time.Second

// relayLogApplyTimeout returns how long a bootstrap may wait for MySQL to apply its relay log:
// groupReplicationRelayLogApplyTimeout, or less, so that groupReplicationRefusalReserve of ctx's
// deadline remains. It is not positive when less than the reserve remains.
func relayLogApplyTimeout(ctx context.Context) time.Duration {
	timeout := groupReplicationRelayLogApplyTimeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = min(timeout, time.Until(deadline)-groupReplicationRefusalReserve)
	}
	return timeout
}

// groupBootstrapChecks are what the caller of a bootstrap asks the tablet to verify under the
// action lock, right before MySQL's START GROUP_REPLICATION (see StartGroupReplicationRequest).
// VTOrc chose the voter to bootstrap from a read of every voter's status; MySQL may have changed
// since, while the RPC waited for the action lock.
type groupBootstrapChecks struct {
	// requiredGTIDSet must be in MySQL's executed GTID set: the transactions that the caller found
	// executed or received on any voter, which include every acknowledged transaction.
	requiredGTIDSet replication.Mysql56GTIDSet
	// intentToken, if set, is the token of the bootstrap intent that the caller recorded in the
	// shard record for this bootstrap, for the incarnation expectedIncarnation (see
	// checkGroupBootstrapIntentLocked).
	intentToken         string
	expectedIncarnation string
}

// newGroupBootstrapChecks returns the checks that req asks for, or nil when it asks for none.
func newGroupBootstrapChecks(req *tabletmanagerdatapb.StartGroupReplicationRequest) (*groupBootstrapChecks, error) {
	if !req.GetBootstrap() || (req.GetRequiredGtidSet() == "" && req.GetBootstrapIntentToken() == "") {
		return nil, nil
	}
	checks := &groupBootstrapChecks{
		intentToken:         req.GetBootstrapIntentToken(),
		expectedIncarnation: req.GetExpectedIncarnation(),
	}
	if req.GetRequiredGtidSet() != "" {
		required, err := replication.ParseMysql56GTIDSet(req.GetRequiredGtidSet())
		if err != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid required GTID set %q: %v", req.GetRequiredGtidSet(), err)
		}
		checks.requiredGTIDSet = required
	}
	return checks, nil
}

// checkGroupBootstrapIntentLocked refuses a bootstrap, with FAILED_PRECONDITION, whose bootstrap
// intent the shard record no longer holds, or that was recorded for another incarnation than the
// shard record lists now. It runs under the action lock: when the RPC gets it, before it stops a
// START GROUP_REPLICATION in progress, and right before MySQL's START.
//
// VTOrc records the intent under the shard lock before it sends the RPC, and the intent fences a
// bootstrap on another tablet. But an RPC can wait for the tablet's action lock for a long time:
// meanwhile the VTOrc that sent it may have lost its shard lock, and another VTOrc may have replaced
// the intent, bootstrapped this tablet, recorded the group, and found the group gone again and
// bootstrapped another voter (the TLA+ model's integrated simulation; doc/design-docs/
// group_replication_tla). The stale RPC would then bootstrap a second group from the same recorded
// incarnation. The compare-and-swap refuses to record that group, which stays read-only and is
// never served, but the tablet's MySQL is out of the shard's group until it leaves it. Checking
// before a STOP also keeps a stale RPC from making MySQL leave the group of a newer bootstrap that
// is still to be adopted.
//
// The read of the shard record waits at most groupReplicationTopoReadTimeout, like the other reads
// of a bootstrap: VTOrc may reach a tablet whose topology server does not answer. If it does not
// answer in time, the tablet bootstraps without the check, as before it: the check protects the
// availability of the shard's group, while a bootstrap that the topology keeps from running would
// cost it.
func (tm *TabletManager) checkGroupBootstrapIntentLocked(ctx context.Context, checks *groupBootstrapChecks) error {
	if checks == nil || checks.intentToken == "" {
		return nil
	}
	tablet := tm.Tablet()
	readCtx, cancel := context.WithTimeout(ctx, groupReplicationTopoReadTimeout)
	defer cancel()
	si, err := tm.TopoServer.GetShard(readCtx, tablet.Keyspace, tablet.Shard)
	if err != nil {
		if ctx.Err() != nil {
			return vterrors.Wrapf(err, "failed to read the shard record of %s/%s", tablet.Keyspace, tablet.Shard)
		}
		log.Warn("Group replication: the topology did not answer in time, bootstrapping without checking the bootstrap intent",
			slog.String("intent_token", checks.intentToken), slog.Any("error", err))
		return nil
	}
	if token := si.GetGroupReplicationBootstrapIntent().GetToken(); token != checks.intentToken {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"refusing to bootstrap the replication group: the shard record of %s/%s holds the bootstrap intent %q, not %q, for which the bootstrap was requested",
			tablet.Keyspace, tablet.Shard, token, checks.intentToken)
	}
	if incarnation := si.GetGroupReplicationIncarnation(); incarnation != checks.expectedIncarnation {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
			"refusing to bootstrap the replication group: the shard record of %s/%s lists the incarnation %q, not %q, for which the bootstrap was requested",
			tablet.Keyspace, tablet.Shard, incarnation, checks.expectedIncarnation)
	}
	return nil
}

// checkGroupBootstrapLocked refuses a bootstrap, with FAILED_PRECONDITION, whose intent was
// superseded (see checkGroupBootstrapIntentLocked), or when MySQL lacks something that its caller
// requires. It runs under the action lock, right before MySQL's START.
//
// The required GTID set must be in MySQL's binlog: an acknowledged transaction was committed by the
// primary that acknowledged it, so it is in the executed set of a voter, and the caller found every
// such set. MySQL may hold some of them only in the relay log of its group_replication_applier
// channel: transactions it received from its last group and had not applied when it left. A
// bootstrap applies them first, but a restart of mysqld with relay_log_recovery=ON discards them
// (doc/failover-audit/GroupReplication.md, "Bootstrap candidate"). If the relay log still holds
// what is missing, the tablet applies it first, without the group, so that the bootstrap starts
// from a binlog that holds every required transaction, which no restart can lose. Otherwise, a
// restart discarded them since the caller read MySQL's status: the bootstrap would lose them, and
// the caller must choose again. That refusal is definitive (see refuseGroupBootstrapLocked) when no
// START GROUP_REPLICATION runs on MySQL.
func (tm *TabletManager) checkGroupBootstrapLocked(ctx context.Context, checks *groupBootstrapChecks) error {
	if err := tm.checkGroupBootstrapIntentLocked(ctx, checks); err != nil {
		return err
	}
	if checks == nil || len(checks.requiredGTIDSet) == 0 {
		return nil
	}
	required := checks.requiredGTIDSet
	executed, err := tm.executedGTIDSet(ctx)
	if err != nil {
		return err
	}
	if executed.Contains(required) {
		return nil
	}
	status, err := tm.groupReplicationStatus(ctx)
	if err != nil {
		return err
	}
	received := replication.Mysql56GTIDSet{}
	if set := status.GetReceivedTransactionSet(); set != "" {
		if received, err = replication.ParseMysql56GTIDSet(set); err != nil {
			return vterrors.Wrapf(err, "failed to parse the received transaction set %q", set)
		}
	}
	if executed.Union(received).Contains(required) {
		if timeout := relayLogApplyTimeout(ctx); timeout > 0 {
			log.Info("Applying the transactions that MySQL received from its last group before bootstrapping a new one",
				slog.String("executed", executed.String()), slog.String("received", received.String()), slog.String("required", required.String()),
				slog.Duration("timeout", timeout))
			applyCtx, cancel := context.WithTimeout(ctx, timeout)
			applyErr := tm.MysqlDaemon.ApplyGroupReplicationRelayLog(applyCtx, required)
			cancel()
			if applyErr != nil {
				log.Warn("Failed to apply the relay log before bootstrapping the group", slog.Any("error", applyErr))
			}
		} else {
			log.Warn("Not applying the relay log before bootstrapping the group: too little of the caller's deadline remains to return a refusal",
				slog.Duration("reserve", groupReplicationRefusalReserve))
		}
		if executed, err = tm.executedGTIDSet(ctx); err != nil {
			return err
		}
		if executed.Contains(required) {
			return nil
		}
	}
	return tm.refuseGroupBootstrapLocked(ctx, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
		"refusing to bootstrap the replication group: MySQL has not executed %s, which its caller found on a voter (executed %s, received %s); a restart of mysqld may have discarded its relay log since",
		required.Difference(executed), executed, received))
}

// refuseGroupBootstrapLocked returns refusal, the refusal of a bootstrap because MySQL lacks a
// transaction that the request requires, as a definitive refusal (tmclient.GroupBootstrapRefusedError)
// if MySQL is not an active member and no START GROUP_REPLICATION runs on it, as a fresh read of its
// status under the action lock finds. It returns refusal as it is otherwise, or if the read fails.
//
// A definitive refusal tells the caller that the request did not start a group and never will, so
// that it may withdraw the bootstrap intent it recorded for it (see
// reparentutil.WithdrawGroupReplicationBootstrapIntent): the intent then no longer fences a bootstrap
// on another voter, which holds the transactions that this MySQL lost. This holds only while no START
// runs: MySQL keeps running a START whose client gave up, an earlier bootstrap or join of the tablet,
// and such a START can still form a group of one; withdrawing its intent would then let the caller
// bootstrap a second group, which VTOrc could no longer adopt. Every START of the tablet runs under the
// action lock, which this RPC holds until it returns, and MySQL starts none on its own
// (group_replication_start_on_boot and auto-rejoin are off; see Mysqld.StartGroupReplication), so
// none can start after the read either.
func (tm *TabletManager) refuseGroupBootstrapLocked(ctx context.Context, refusal error) error {
	status, err := tm.groupReplicationStatus(ctx)
	if err != nil {
		log.Warn("Failed to read the group replication status before refusing the bootstrap, the refusal is not definitive", slog.Any("error", err))
		return refusal
	}
	if status.GetStartInProgress() || mysql.IsGroupMemberActive(status) {
		log.Warn("A START GROUP_REPLICATION runs on MySQL, or MySQL is in a group: the refusal of the bootstrap is not definitive",
			slog.Bool("start_in_progress", status.GetStartInProgress()), slog.String("member_state", status.GetMemberState()))
		return refusal
	}
	return tmclient.NewGroupBootstrapRefusedError(refusal)
}

// executedGTIDSet returns MySQL's executed GTID set.
func (tm *TabletManager) executedGTIDSet(ctx context.Context) (replication.Mysql56GTIDSet, error) {
	pos, err := tm.MysqlDaemon.PrimaryPosition(ctx)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to read the executed GTID set")
	}
	if pos.GTIDSet == nil {
		return replication.Mysql56GTIDSet{}, nil
	}
	executed, ok := pos.GTIDSet.(replication.Mysql56GTIDSet)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "the executed GTID set %v is not a MySQL 5.6 GTID set", pos.GTIDSet)
	}
	return executed, nil
}
