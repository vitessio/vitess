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
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/mysqlctl/mock"
)

// TestRepairHeartbeat checks call order and the relay-log safety decision.
func TestRepairHeartbeat(t *testing.T) {
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
		calls        []string
		stopError    error
		statusError  error
		restartError error
		wantError    string
		wantChange   bool
		wantStopped  bool
	}{
		// Check fresh positions to decide whether the change is safe.
		{name: "drained_relay_log", calls: []string{"stop", "status"}, before: "1-9", executed: "1-10", received: "1-10", ioState: replication.ReplicationStateRunning, wantChange: true, wantStopped: true},
		{name: "applied_superset", calls: []string{"stop", "status"}, before: "1-9", executed: "1-11", received: "1-10", ioState: replication.ReplicationStateRunning, wantChange: true, wantStopped: true},
		{name: "unapplied_relay_log", calls: []string{"stop", "status"}, before: "1-10", executed: "1-10", received: "1-11", ioState: replication.ReplicationStateRunning, wantStopped: true},
		{name: "already_stopped", calls: []string{"status"}, before: "1-10", executed: "1-10", received: "1-10", wantChange: true},
		{name: "unapplied_already_stopped", calls: []string{"status"}, before: "1-9", executed: "1-9", received: "1-10"},
		{name: "applier_error", calls: []string{"stop", "status"}, before: "1-9", executed: "1-9", received: "1-10", ioState: replication.ReplicationStateRunning, applierError: "applier failed", wantStopped: true},

		// Stop active threads before reading their positions.
		{name: "io_connecting", calls: []string{"stop", "status"}, before: "1-10", executed: "1-10", received: "1-10", ioState: replication.ReplicationStateConnecting, wantChange: true, wantStopped: true},
		{name: "sql_running", calls: []string{"stop", "status"}, before: "1-10", executed: "1-10", received: "1-10", sqlState: replication.ReplicationStateRunning, wantChange: true, wantStopped: true},

		// Restart after a failed check only if the repair stopped replication.
		{name: "stop_failure", calls: []string{"stop"}, before: "1-10", ioState: replication.ReplicationStateRunning, stopError: errors.New("stop unavailable"), wantError: "stop unavailable"},
		{name: "status_read_failure", calls: []string{"stop", "status", "start"}, before: "1-10", ioState: replication.ReplicationStateRunning, statusError: errors.New("status unavailable"), wantError: "read replication status after stop: status unavailable"},
		{name: "io_still_running", calls: []string{"stop", "status", "start"}, before: "1-10", ioState: replication.ReplicationStateRunning, afterIO: replication.ReplicationStateRunning, wantError: "replication threads must be stopped before heartbeat repair"},
		{name: "sql_still_running", calls: []string{"stop", "status", "start"}, before: "1-10", sqlState: replication.ReplicationStateRunning, afterSQL: replication.ReplicationStateRunning, wantError: "replication threads must be stopped before heartbeat repair"},
		{name: "status_read_failure_restart_fails", calls: []string{"stop", "status", "start"}, before: "1-10", ioState: replication.ReplicationStateRunning, statusError: errors.New("status unavailable"), restartError: errors.New("start unavailable"), wantError: "read replication status after stop: status unavailable: restart replication after failed heartbeat repair: start unavailable"},
		{name: "already_stopped_status_fails", calls: []string{"status"}, before: "1-10", statusError: errors.New("status unavailable"), wantError: "read replication status after stop: status unavailable"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, state := range []*replication.ReplicationState{&tt.ioState, &tt.sqlState, &tt.afterIO, &tt.afterSQL} {
				if *state == replication.ReplicationStateUnknown {
					*state = replication.ReplicationStateStopped
				}
			}

			ctx := t.Context()
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
				interval: 30,
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
				case "stop":
					calls = append(calls, daemon.EXPECT().StopReplication(ctx, tm.hookExtraEnv()).Return(tt.stopError))
				case "status":
					calls = append(calls, daemon.EXPECT().ReplicationStatus(ctx).Return(status, tt.statusError))
				case "start":
					calls = append(calls, daemon.EXPECT().StartReplication(ctx, tm.hookExtraEnv()).Return(tt.restartError))
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
			assert.Equal(t, tt.wantChange, resp.changeSource)
			assert.Equal(t, tt.wantStopped, resp.stopped)
		})
	}
}
