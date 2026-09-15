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
	// heartbeatRepairRequest asks for a new heartbeat on the current source.
	heartbeatRepairRequest struct {
		// status is from before any thread was touched.
		status replication.ReplicationStatus

		// interval is the heartbeat in seconds.
		interval float64
	}

	// heartbeatRepairResponse is what repairHeartbeat did.
	heartbeatRepairResponse struct {
		// changeSource means the relay log is empty and a full CHANGE is safe.
		changeSource bool

		// stopped means both threads were stopped.
		stopped bool
	}
)

// repairHeartbeat decides whether a full CHANGE can run without losing the
// relay log. It stops both threads and allows it only on an empty relay log.
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
