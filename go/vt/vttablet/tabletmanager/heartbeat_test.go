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

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type (
	// stoppedStatusDaemon changes status after a stop to test the heartbeat decision.
	stoppedStatusDaemon struct {
		// FakeMysqlDaemon checks the exact replication statement sequence.
		*mysqlctl.FakeMysqlDaemon
		// t reports status reads made before replication stops.
		t *testing.T
		// reads counts status snapshots.
		reads int
		// executed is the executed set returned after the stop.
		executed replication.Position
		// received is the received set returned after the stop.
		received replication.Position
		// statusError fails the status read after the stop.
		statusError error
		// connecting keeps an unhealthy IO thread active without a planned stop.
		connecting bool
	}
)

// ReplicationStatus returns an IO-only first snapshot and changed sets after the stop.
func (d *stoppedStatusDaemon) ReplicationStatus(ctx context.Context) (replication.ReplicationStatus, error) {
	d.reads++
	status, err := d.FakeMysqlDaemon.ReplicationStatus(ctx)
	if d.reads == 1 {
		status.IOState = replication.ReplicationStateRunning
		status.SQLState = replication.ReplicationStateStopped
		if d.connecting {
			status.IOState = replication.ReplicationStateConnecting
			status.LastIOError = "connection refused"
		}
		return status, err
	}

	assert.False(d.t, d.Replicating, "status must be read after STOP REPLICA")
	status.Position = d.executed
	status.RelayLogPosition = d.received
	return status, d.statusError
}

func TestSetReplicationSourceHeartbeatStoppedStatus(t *testing.T) {
	for _, tt := range []struct {
		name        string
		before      string
		executed    string
		received    string
		change      bool
		stopError   bool
		statusError bool
		connecting  bool
	}{
		{name: "received_after_first_read", before: "1-10", executed: "1-10", received: "1-11"},
		{name: "applied_before_stop", before: "1-9", executed: "1-10", received: "1-10", change: true},
		{name: "stop_error", before: "1-10", stopError: true},
		{name: "status_error", before: "1-10", statusError: true},
		{name: "io_reconnecting", before: "1-10", executed: "1-10", received: "1-10", change: true, connecting: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			ts := memorytopo.NewServer(ctx, "cell1")
			t.Cleanup(ts.Close)
			_, err := ts.GetOrCreateShard(ctx, "ks", "0")
			require.NoError(t, err)

			parent := newTestTablet(t, 200, "ks", "0", nil)
			parent.Type = topodatapb.TabletType_PRIMARY
			parent.MysqlHostname = "mysql-primary"
			parent.MysqlPort = 3306
			require.NoError(t, ts.CreateTablet(ctx, parent))

			daemon := &stoppedStatusDaemon{FakeMysqlDaemon: newTestMysqlDaemon(t, 1), t: t, connecting: tt.connecting}
			daemon.CurrentSourceHost = parent.MysqlHostname
			daemon.CurrentSourcePort = parent.MysqlPort
			daemon.Replicating = true
			daemon.CurrentPrimaryPosition = replication.MustParsePosition("MySQL56", serverUUID+":"+tt.before)
			daemon.CurrentRelayLogPosition = replication.MustParsePosition("MySQL56", serverUUID+":1-10")
			daemon.ReplicationConfigurationResult = &replicationdatapb.Configuration{HeartbeatInterval: 15}
			daemon.SetReplicationSourceInputs = []string{"mysql-primary:3306"}
			daemon.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA"}
			switch {
			case tt.stopError:
				daemon.StopReplicationError = errors.New("stop unavailable")
				daemon.ExpectedExecuteSuperQueryList = nil
			case tt.statusError:
				daemon.statusError = errors.New("status unavailable")
			default:
				daemon.executed = replication.MustParsePosition("MySQL56", serverUUID+":"+tt.executed)
				daemon.received = replication.MustParsePosition("MySQL56", serverUUID+":"+tt.received)
				if tt.change {
					daemon.ExpectedExecuteSuperQueryList = append(daemon.ExpectedExecuteSuperQueryList, "FAKE SET SOURCE")
				}
				// A replica that was only reconnecting was not replicating, so nothing restarts it.
				if !tt.connecting {
					daemon.ExpectedExecuteSuperQueryList = append(daemon.ExpectedExecuteSuperQueryList, "START REPLICA")
				}
			}

			tm := newTestReplicationTM(newTestTablet(t, 100, "ks", "0", nil), daemon, ts)
			tm.tmc = newFakeTMClient()
			err = tm.SetReplicationSource(ctx, parent.Alias, 0, "", false, false, 30)
			switch {
			case tt.connecting:
				require.NoError(t, err)
				assert.False(t, daemon.Replicating)
				assert.Equal(t, 2, daemon.reads)
			case tt.stopError:
				require.ErrorContains(t, err, "stop unavailable")
				assert.Equal(t, 1, daemon.reads)
			case tt.statusError:
				require.ErrorContains(t, err, "status unavailable")
				assert.False(t, daemon.Replicating)
				assert.Equal(t, 2, daemon.reads)
			default:
				require.NoError(t, err)
				assert.True(t, daemon.Replicating)
				assert.Equal(t, 2, daemon.reads)
			}
			assert.NoError(t, daemon.CheckSuperQueryList())
		})
	}
}
