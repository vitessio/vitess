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

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	// repairHeartbeatRequest holds the status and requested heartbeat interval.
	repairHeartbeatRequest struct {
		// status is the replication status read by the caller.
		status replication.ReplicationStatus

		// interval is the requested heartbeat interval in seconds, used in warnings.
		interval float64
	}

	// repairHeartbeatResponse holds the repair decision and whether replication was stopped.
	repairHeartbeatResponse struct {
		// changeSource is true when the full CHANGE REPLICATION SOURCE TO is safe to run.
		changeSource bool

		// stopped means this call stopped replication, not that it was already stopped.
		stopped bool
	}
)

// repairHeartbeat stops replication if needed and returns whether a heartbeat-only
// change is safe, or an error if the check fails. It does not change the interval
// or restart replication.
func (tm *TabletManager) repairHeartbeat(ctx context.Context, req *repairHeartbeatRequest) (*repairHeartbeatResponse, error) {
	resp := &repairHeartbeatResponse{changeSource: true}

	// Stop both threads so received and applied positions cannot advance during the check.
	if req.status.IOState != replication.ReplicationStateStopped || req.status.SQLState != replication.ReplicationStateStopped {
		if err := tm.MysqlDaemon.StopReplication(ctx, tm.hookExtraEnv()); err != nil {
			return nil, err
		}
		resp.stopped = true
	}

	// Read fresh positions because replication could have advanced since the caller's read.
	stoppedStatus, err := tm.MysqlDaemon.ReplicationStatus(ctx)
	if err != nil {
		if restartErr := tm.restartAfterFailedRepair(ctx, resp.stopped); restartErr != nil {
			return nil, vterrors.Wrapf(restartErr, "read replication status after stop: %v", err)
		}

		return nil, vterrors.Wrap(err, "read replication status after stop")
	}

	// Reject running threads because their positions are not stable for the safety check.
	if stoppedStatus.IOState != replication.ReplicationStateStopped || stoppedStatus.SQLState != replication.ReplicationStateStopped {
		err := vterrors.New(vtrpc.Code_FAILED_PRECONDITION, "replication threads must be stopped before heartbeat repair")
		if restartErr := tm.restartAfterFailedRepair(ctx, resp.stopped); restartErr != nil {
			return nil, vterrors.Wrapf(restartErr, "%v", err)
		}

		return nil, err
	}

	// Skip the change if received transactions remain unapplied. CHANGE REPLICATION
	// SOURCE TO deletes relay logs when both threads are stopped unless relay-log
	// coordinates are specified. Under semi-sync, these logs can hold acknowledged
	// transactions that must survive a source failure.
	if !stoppedStatus.Position.AtLeast(stoppedStatus.RelayLogPosition) {
		log.Warn("Skipping heartbeat repair to avoid deleting received but unapplied transactions from the relay log",
			slog.String("tablet", topoproto.TabletAliasString(tm.Tablet().Alias)),
			slog.String("source_host", req.status.SourceHost),
			slog.Int("source_port", int(req.status.SourcePort)),
			slog.Float64("heartbeat_interval", req.interval),
		)

		resp.changeSource = false
	}

	return resp, nil
}

// restartAfterFailedRepair starts replication again when the repair stopped it.
func (tm *TabletManager) restartAfterFailedRepair(ctx context.Context, stopped bool) error {
	if !stopped {
		return nil
	}

	if err := tm.MysqlDaemon.StartReplication(ctx, tm.hookExtraEnv()); err != nil {
		return vterrors.Wrap(err, "restart replication after failed heartbeat repair")
	}

	return nil
}
