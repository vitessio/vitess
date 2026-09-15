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
	// heartbeatRepairRequest is a heartbeat change on a replica whose source
	// host and port already match.
	heartbeatRepairRequest struct {
		// status is the replication status from before any thread was touched.
		status replication.ReplicationStatus

		// interval is the heartbeat to set, in seconds.
		interval float64
	}

	// heartbeatRepairResponse is what repairHeartbeat did and what is left for
	// the caller.
	heartbeatRepairResponse struct {
		// changeSource is true when the relay log is empty and the full
		// CHANGE REPLICATION SOURCE TO is safe to run.
		changeSource bool

		// stopped is true when both threads were stopped for the check.
		stopped bool
	}
)

// repairHeartbeat decides whether the heartbeat can be changed with a full
// CHANGE REPLICATION SOURCE TO. That statement deletes the relay log when both
// threads are stopped, and under semi-sync the relay log may be the only copy
// of acknowledged transactions. So we stop both threads, re-read the status so
// the IO thread cannot add to the relay log under us, and only allow the
// change when everything received has been applied. Otherwise the heartbeat
// stays wrong until a later attempt finds the relay log empty, which is fine:
// a wrong heartbeat only matters when the primary is quiet, and that is when
// the applier catches up.
func (tm *TabletManager) repairHeartbeat(ctx context.Context, req *heartbeatRepairRequest) (*heartbeatRepairResponse, error) {
	resp := &heartbeatRepairResponse{changeSource: true}

	// Stop anything still moving, including an IO thread stuck in Connecting.
	if req.status.IOState != replication.ReplicationStateStopped || req.status.SQLState != replication.ReplicationStateStopped {
		if err := tm.MysqlDaemon.StopReplication(ctx, tm.hookExtraEnv()); err != nil {
			return nil, err
		}
		resp.stopped = true
	}

	// Re-read now that the relay log cannot change.
	stoppedStatus, err := tm.MysqlDaemon.ReplicationStatus(ctx)
	if err != nil {
		return nil, vterrors.Wrap(err, "read replication status after stop")
	}

	if stoppedStatus.IOState != replication.ReplicationStateStopped || stoppedStatus.SQLState != replication.ReplicationStateStopped {
		return nil, vterrors.New(vtrpc.Code_FAILED_PRECONDITION, "replication threads must be stopped before heartbeat repair")
	}

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
