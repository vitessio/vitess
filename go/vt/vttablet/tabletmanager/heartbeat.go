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

		// interval is the requested heartbeat interval in seconds.
		interval float64

		// shouldBeReplicating is whether the caller wants replication running when the RPC returns.
		shouldBeReplicating bool
	}

	// repairHeartbeatResponse holds what the repair did and what the caller may still do.
	repairHeartbeatResponse struct {
		// repaired is true when the heartbeat was changed with only the IO thread stopped.
		repaired bool

		// changeSource is true when the full CHANGE REPLICATION SOURCE TO is safe to run.
		changeSource bool

		// stopped means this call stopped replication, not that it was already stopped.
		stopped bool
	}
)

// repairHeartbeat changes the heartbeat interval when the flavor can do it with
// only the IO thread stopped. Otherwise it stops replication if needed and returns
// whether a full source change is safe, or an error if the check fails.
func (tm *TabletManager) repairHeartbeat(ctx context.Context, req *repairHeartbeatRequest) (*repairHeartbeatResponse, error) {
	resp := &repairHeartbeatResponse{}
	status := req.status

	// Try the IO-only change first. MySQL keeps the relay log while the applier runs,
	// so nothing is at risk. An applier that stopped without an error is started for
	// that if the caller wants replication running anyway.
	started := false
	if status.SQLHealthy() || (status.LastSQLError == "" && req.shouldBeReplicating) {
		if !status.SQLHealthy() {
			if err := tm.MysqlDaemon.StartReplication(ctx, tm.hookExtraEnv()); err != nil {
				return nil, err
			}
			started = true
		}

		err := tm.MysqlDaemon.SetReplicationHeartbeat(ctx, req.interval)
		if err == nil {
			resp.repaired = true
			return resp, nil
		}
		// Only an unsupported flavor falls through to the full change.
		if vterrors.Code(err) != vtrpc.Code_UNIMPLEMENTED {
			return nil, err
		}
	}

	// Stop both threads so received and applied positions cannot advance during the check.
	resp.changeSource = true
	if started || status.IOState != replication.ReplicationStateStopped || status.SQLState != replication.ReplicationStateStopped {
		if err := tm.MysqlDaemon.StopReplication(ctx, tm.hookExtraEnv()); err != nil {
			return nil, err
		}
		resp.stopped = true
	}

	// Read fresh positions because replication could have advanced since the caller's read.
	stoppedStatus, err := tm.MysqlDaemon.ReplicationStatus(ctx)
	if err != nil {
		return nil, vterrors.Wrap(err, "read replication status after stop")
	}

	// Reject running threads because their positions are not stable for the safety check.
	if stoppedStatus.IOState != replication.ReplicationStateStopped || stoppedStatus.SQLState != replication.ReplicationStateStopped {
		return nil, vterrors.New(vtrpc.Code_FAILED_PRECONDITION, "replication threads must be stopped before heartbeat repair")
	}

	// Skip the change if received transactions remain unapplied, or if the applier
	// stopped on an error and so never will apply them. CHANGE REPLICATION SOURCE TO
	// deletes relay logs when both threads are stopped, and under semi-sync those logs
	// can hold acknowledged transactions that must survive a source failure.
	if status.LastSQLError != "" || !stoppedStatus.Position.AtLeast(stoppedStatus.RelayLogPosition) {
		log.Warn("Skipping heartbeat repair to avoid deleting received but unapplied transactions from the relay log",
			slog.String("tablet", topoproto.TabletAliasString(tm.Tablet().Alias)),
			slog.String("source_host", status.SourceHost),
			slog.Int("source_port", int(status.SourcePort)),
			slog.Float64("heartbeat_interval", req.interval),
		)

		resp.changeSource = false
	}

	return resp, nil
}
