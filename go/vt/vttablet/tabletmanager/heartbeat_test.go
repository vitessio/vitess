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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/mysqlctl/mock"
	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// TestRepairHeartbeat checks call order and the relay-log safety decision.
func TestRepairHeartbeat(t *testing.T) {
	unimplemented := vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported")
	applierStopped := vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "applier is not running")
	for _, tt := range []struct {
		name         string
		before       string
		executed     string
		received     string
		ioState      replication.ReplicationState
		sqlState     replication.ReplicationState
		afterIO      replication.ReplicationState
		afterSQL     replication.ReplicationState
		applierError string
		shouldRun    bool
		calls        []string
		startError   error
		repairError  error
		stopError    error
		statusError  error
		restartError error
		cancelBefore bool
		wantError    string
		wantRepaired bool
		wantChange   bool
		wantStopped  bool
	}{
		// Try the IO-only change first whenever the applier runs or can be started.
		{name: "io_only_running", calls: []string{"heartbeat"}, before: "1-9", ioState: replication.ReplicationStateRunning, sqlState: replication.ReplicationStateRunning, wantRepaired: true},
		{name: "io_only_starts_applier", calls: []string{"start", "heartbeat"}, before: "1-9", ioState: replication.ReplicationStateRunning, shouldRun: true, wantRepaired: true},
		{name: "io_only_not_started_when_not_wanted", calls: []string{"stop", "status"}, before: "1-10", executed: "1-10", received: "1-10", ioState: replication.ReplicationStateRunning, wantChange: true, wantStopped: true},
		{name: "io_only_skipped_on_applier_error", calls: []string{"stop", "status"}, before: "1-9", executed: "1-9", received: "1-10", ioState: replication.ReplicationStateRunning, shouldRun: true, applierError: "applier failed", wantStopped: true},
		{name: "io_only_error", calls: []string{"heartbeat"}, before: "1-9", sqlState: replication.ReplicationStateRunning, repairError: errors.New("heartbeat unavailable"), wantError: "heartbeat unavailable"},
		{name: "io_only_start_error", calls: []string{"start"}, before: "1-9", shouldRun: true, startError: errors.New("start unavailable"), wantError: "start unavailable"},
		{name: "io_only_unsupported_after_start", calls: []string{"start", "heartbeat", "stop", "status"}, before: "1-9", executed: "1-10", received: "1-10", shouldRun: true, repairError: unimplemented, wantChange: true, wantStopped: true},

		// Fall back when the daemon found the applier stopped after it stopped the IO thread.
		{name: "io_only_applier_stopped_late", calls: []string{"heartbeat", "stop", "status"}, before: "1-9", executed: "1-9", received: "1-10", sqlState: replication.ReplicationStateRunning, repairError: applierStopped, wantStopped: true},

		// Check fresh positions to decide whether the full change is safe.
		{name: "drained_relay_log", calls: []string{"heartbeat", "stop", "status"}, repairError: unimplemented, before: "1-9", executed: "1-10", received: "1-10", ioState: replication.ReplicationStateRunning, sqlState: replication.ReplicationStateRunning, wantChange: true, wantStopped: true},
		{name: "applied_superset", calls: []string{"heartbeat", "stop", "status"}, repairError: unimplemented, before: "1-9", executed: "1-11", received: "1-10", ioState: replication.ReplicationStateRunning, sqlState: replication.ReplicationStateRunning, wantChange: true, wantStopped: true},
		{name: "unapplied_relay_log", calls: []string{"heartbeat", "stop", "status"}, repairError: unimplemented, before: "1-10", executed: "1-10", received: "1-11", ioState: replication.ReplicationStateRunning, sqlState: replication.ReplicationStateRunning, wantStopped: true},
		{name: "already_stopped", calls: []string{"status"}, before: "1-10", executed: "1-10", received: "1-10", wantChange: true},
		{name: "unapplied_already_stopped", calls: []string{"status"}, before: "1-9", executed: "1-9", received: "1-10"},
		{name: "applier_error", calls: []string{"stop", "status"}, before: "1-9", executed: "1-9", received: "1-10", ioState: replication.ReplicationStateRunning, applierError: "applier failed", wantStopped: true},

		// Stop active threads before reading their positions.
		{name: "io_connecting", calls: []string{"stop", "status"}, before: "1-10", executed: "1-10", received: "1-10", ioState: replication.ReplicationStateConnecting, wantChange: true, wantStopped: true},
		{name: "sql_running", calls: []string{"heartbeat", "stop", "status"}, repairError: unimplemented, before: "1-10", executed: "1-10", received: "1-10", sqlState: replication.ReplicationStateRunning, wantChange: true, wantStopped: true},

		// Restart after a failed check only if the repair stopped replication.
		{name: "stop_failure", calls: []string{"stop"}, before: "1-10", ioState: replication.ReplicationStateRunning, stopError: errors.New("stop unavailable"), wantError: "stop unavailable"},
		{name: "status_read_failure", calls: []string{"stop", "status", "restart"}, before: "1-10", ioState: replication.ReplicationStateRunning, statusError: errors.New("status unavailable"), wantError: "read replication status after stop: status unavailable"},
		{name: "io_still_running", calls: []string{"stop", "status", "restart"}, before: "1-10", ioState: replication.ReplicationStateRunning, afterIO: replication.ReplicationStateRunning, wantError: "replication threads must be stopped before heartbeat repair"},
		{name: "sql_still_running", calls: []string{"heartbeat", "stop", "status", "restart"}, repairError: unimplemented, before: "1-10", sqlState: replication.ReplicationStateRunning, afterSQL: replication.ReplicationStateRunning, wantError: "replication threads must be stopped before heartbeat repair"},
		{name: "status_read_failure_restart_fails", calls: []string{"stop", "status", "restart"}, before: "1-10", ioState: replication.ReplicationStateRunning, statusError: errors.New("status unavailable"), restartError: errors.New("start unavailable"), wantError: "read replication status after stop: status unavailable: restart replication after failed heartbeat repair: start unavailable"},
		{name: "status_read_failure_ctx_cancelled", calls: []string{"stop", "status", "restart"}, before: "1-10", ioState: replication.ReplicationStateRunning, statusError: context.Canceled, cancelBefore: true, wantError: "read replication status after stop: context canceled"},
		{name: "already_stopped_status_fails", calls: []string{"status"}, before: "1-10", statusError: errors.New("status unavailable"), wantError: "read replication status after stop: status unavailable"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, state := range []*replication.ReplicationState{&tt.ioState, &tt.sqlState, &tt.afterIO, &tt.afterSQL} {
				if *state == replication.ReplicationStateUnknown {
					*state = replication.ReplicationStateStopped
				}
			}

			ctx, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			daemon := mock.NewMockMysqlDaemon(gomock.NewController(t))
			tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), daemon, nil)
			req := &repairHeartbeatRequest{
				status: replication.ReplicationStatus{
					IOState:          tt.ioState,
					SQLState:         tt.sqlState,
					Position:         replication.MustParsePosition("MySQL56", serverUUID+":"+tt.before),
					RelayLogPosition: replication.MustParsePosition("MySQL56", serverUUID+":1-10"),
					LastSQLError:     tt.applierError,
					SourceHost:       "mysql-primary",
					SourcePort:       3306,
				},
				interval:            30,
				shouldBeReplicating: tt.shouldRun,
			}
			status := replication.ReplicationStatus{
				IOState:      tt.afterIO,
				SQLState:     tt.afterSQL,
				LastSQLError: tt.applierError,
			}
			if tt.executed != "" {
				status.Position = replication.MustParsePosition("MySQL56", serverUUID+":"+tt.executed)
				status.RelayLogPosition = replication.MustParsePosition("MySQL56", serverUUID+":"+tt.received)
			}

			var calls []any
			for _, call := range tt.calls {
				switch call {
				case "start":
					calls = append(calls, daemon.EXPECT().StartReplication(ctx, tm.hookExtraEnv()).Return(tt.startError))
				case "restart":
					calls = append(calls, daemon.EXPECT().StartReplication(gomock.Cond(func(c context.Context) bool { return c.Err() == nil }), tm.hookExtraEnv()).Return(tt.restartError))
				case "heartbeat":
					calls = append(calls, daemon.EXPECT().SetReplicationHeartbeat(ctx, 30.0).Return(tt.repairError))
				case "stop":
					calls = append(calls, daemon.EXPECT().StopReplication(ctx, tm.hookExtraEnv()).Return(tt.stopError))
				case "status":
					calls = append(calls, daemon.EXPECT().ReplicationStatus(ctx).DoAndReturn(func(context.Context) (replication.ReplicationStatus, error) {
						// The caller's deadline may pass during the check. The restart must not use it.
						if tt.cancelBefore {
							cancel()
						}
						return status, tt.statusError
					}))
				default:
					t.Fatalf("unknown daemon call %q", call)
				}
			}
			gomock.InOrder(calls...)

			resp, err := tm.repairHeartbeat(ctx, req)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				assert.Nil(t, resp)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, tt.wantRepaired, resp.repaired)
			assert.Equal(t, tt.wantChange, resp.changeSource)
			assert.Equal(t, tt.wantStopped, resp.stopped)
		})
	}
}
