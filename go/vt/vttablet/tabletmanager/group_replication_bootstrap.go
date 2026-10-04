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

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// groupReplicationRelayLogApplyTimeout bounds how long a bootstrap waits, under the action lock,
// for MySQL to apply the transactions that it received from its last group and had not applied
// (see checkGroupBootstrapLocked). The caller's context may end it earlier.
var groupReplicationRelayLogApplyTimeout = 30 * time.Second

// groupBootstrapChecks are what the caller of a bootstrap asks the tablet to verify under the
// action lock, right before MySQL's START GROUP_REPLICATION (see StartGroupReplicationRequest).
// VTOrc chose the voter to bootstrap from a read of every voter's status; MySQL may have changed
// since, while the RPC waited for the action lock.
type groupBootstrapChecks struct {
	// requiredGTIDSet must be in MySQL's executed GTID set: the transactions that the caller found
	// executed or received on any voter, which include every acknowledged transaction.
	requiredGTIDSet replication.Mysql56GTIDSet
}

// newGroupBootstrapChecks returns the checks that req asks for, or nil when it asks for none.
func newGroupBootstrapChecks(req *tabletmanagerdatapb.StartGroupReplicationRequest) (*groupBootstrapChecks, error) {
	if !req.GetBootstrap() || req.GetRequiredGtidSet() == "" {
		return nil, nil
	}
	required, err := replication.ParseMysql56GTIDSet(req.GetRequiredGtidSet())
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid required GTID set %q: %v", req.GetRequiredGtidSet(), err)
	}
	return &groupBootstrapChecks{requiredGTIDSet: required}, nil
}

// checkGroupBootstrapLocked refuses a bootstrap, with FAILED_PRECONDITION, when MySQL lacks
// something that its caller requires. It runs under the action lock, right before MySQL's START.
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
// the caller must choose again.
func (tm *TabletManager) checkGroupBootstrapLocked(ctx context.Context, checks *groupBootstrapChecks) error {
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
		log.Info("Applying the transactions that MySQL received from its last group before bootstrapping a new one",
			slog.String("executed", executed.String()), slog.String("received", received.String()), slog.String("required", required.String()))
		applyCtx, cancel := context.WithTimeout(ctx, groupReplicationRelayLogApplyTimeout)
		applyErr := tm.MysqlDaemon.ApplyGroupReplicationRelayLog(applyCtx, required)
		cancel()
		if applyErr != nil {
			log.Warn("Failed to apply the relay log before bootstrapping the group", slog.Any("error", applyErr))
		}
		if executed, err = tm.executedGTIDSet(ctx); err != nil {
			return err
		}
		if executed.Contains(required) {
			return nil
		}
	}
	return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION,
		"refusing to bootstrap the replication group: MySQL has not executed %s, which its caller found on a voter (executed %s, received %s); a restart of mysqld may have discarded its relay log since",
		required.Difference(executed), executed, received)
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
