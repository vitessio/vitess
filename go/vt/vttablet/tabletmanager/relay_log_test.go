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
	"bytes"
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/reparenttestutil"
	"vitess.io/vitess/go/vt/vterrors"

	replicationdatapb "vitess.io/vitess/go/vt/proto/replicationdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const (
	relayLogTestPrimaryHost = "mysql-primary"
	relayLogTestPrimaryPort = 3306
	// relayLogTestPrimaryPosition is the executed position of the primary the replica is
	// pointed at. The replica positions below use the primary's own server UUID, the only
	// UUID for which the errant GTID check lets a replica be ahead of its new primary.
	relayLogTestPrimaryPosition = "16b1039f-22b6-11ed-b765-0a43f95f28a3:1-220"
	relayLogTestServerUUID      = "16b1039f-22b6-11ed-b765-0a43f95f28a3"
	// relayLogTestOtherServerUUID is the server UUID of another server, such as a former primary.
	relayLogTestOtherServerUUID = "8bc65c84-3fe4-11ed-a912-257f0fcdd6c9"
	// relayLogTestOtherServerTransactions are transactions of the other server that the primary
	// lacks.
	relayLogTestOtherServerTransactions = relayLogTestOtherServerUUID + ":1-5"
)

// primaryStatusTMClient is a fakeTMClient whose PrimaryStatus reports a configurable position.
type primaryStatusTMClient struct {
	*fakeTMClient
	position string
}

func (c *primaryStatusTMClient) PrimaryStatus(ctx context.Context, tablet *topodatapb.Tablet) (*replicationdatapb.PrimaryStatus, error) {
	return &replicationdatapb.PrimaryStatus{
		Position:   "MySQL56/" + c.position,
		ServerUuid: relayLogTestServerUUID,
	}, nil
}

func mustParseMysql56Position(t *testing.T, gtids string) replication.Position {
	t.Helper()
	pos, err := replication.ParsePosition(replication.Mysql56FlavorID, gtids)
	require.NoError(t, err)
	return pos
}

// relayLogTestSource returns the primary as a replication source that has executed gtids.
func relayLogTestSource(t *testing.T, gtids string) replicationSource {
	t.Helper()
	uuid, err := replication.ParseSID(relayLogTestServerUUID)
	require.NoError(t, err)
	return replicationSource{
		position: mustParseMysql56Position(t, gtids),
		uuid:     uuid,
	}
}

type relayLogTestEnv struct {
	tm     *TabletManager
	mysqld *mysqlctl.FakeMysqlDaemon
	tmc    *primaryStatusTMClient
	parent *topodatapb.TabletAlias
}

// newRelayLogTestEnv sets up a replica that has executed 1-200 and received 1-210 of the
// primary's transactions, replicating with auto-positioning from sourceHost:sourcePort.
// Transactions 201-210 are in its relay log only; the primary has executed primaryPosition.
func newRelayLogTestEnv(t *testing.T, sourceHost string, sourcePort int32, primaryPosition string) *relayLogTestEnv {
	t.Helper()
	ctx := t.Context()
	ts := memorytopo.NewServer(ctx, "cell1")
	t.Cleanup(ts.Close)

	_, err := ts.GetOrCreateShard(ctx, "ks", "0")
	require.NoError(t, err)
	tablet := newTestTablet(t, 100, "ks", "0", nil)
	require.NoError(t, ts.CreateTablet(ctx, tablet))
	parent := &topodatapb.Tablet{
		Alias:         &topodatapb.TabletAlias{Cell: "cell1", Uid: 200},
		Keyspace:      "ks",
		Shard:         "0",
		Type:          topodatapb.TabletType_PRIMARY,
		MysqlHostname: relayLogTestPrimaryHost,
		MysqlPort:     relayLogTestPrimaryPort,
	}
	require.NoError(t, ts.CreateTablet(ctx, parent))

	fmd := newTestMysqlDaemon(t, 1)
	fmd.Replicating = true
	fmd.AutoPosition = true
	fmd.CurrentSourceHost = sourceHost
	fmd.CurrentSourcePort = sourcePort
	fmd.CurrentPrimaryPosition = mustParseMysql56Position(t, relayLogTestServerUUID+":1-200")
	fmd.CurrentRelayLogPosition = mustParseMysql56Position(t, relayLogTestServerUUID+":1-210")
	fmd.SetReplicationSourceInputs = []string{relayLogTestPrimaryHost + ":3306"}

	tm := newTestReplicationTM(tablet, fmd, ts)
	tmc := &primaryStatusTMClient{fakeTMClient: newFakeTMClient(), position: primaryPosition}
	tm.tmc = tmc
	return &relayLogTestEnv{tm: tm, mysqld: fmd, tmc: tmc, parent: parent.Alias}
}

// setReplicationSource calls the SetReplicationSource RPC like VTOrc's fixReplica does
// (forcing replication to start), with the given heartbeat interval.
func (env *relayLogTestEnv) setReplicationSource(t *testing.T, heartbeatInterval float64) error {
	return env.tm.SetReplicationSource(t.Context(), env.parent, 0, "", true, false, heartbeatInterval)
}

// receiveOtherServerTransactions adds relayLogTestOtherServerTransactions, which the primary
// lacks, to the replica's relay log.
func (env *relayLogTestEnv) receiveOtherServerTransactions(t *testing.T) {
	t.Helper()
	env.mysqld.CurrentRelayLogPosition = mustParseMysql56Position(t, relayLogTestServerUUID+":1-210,"+relayLogTestOtherServerTransactions)
}

// assertRelayLogKept asserts that the replica still has the unapplied transactions 201-210.
func (env *relayLogTestEnv) assertRelayLogKept(t *testing.T) {
	t.Helper()
	assert.Equal(t, relayLogTestServerUUID+":1-210", env.mysqld.CurrentRelayLogPosition.GTIDSet.String(), "the relay log must be kept")
}

// lockedBuffer is a bytes.Buffer that is safe for concurrent writes.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// captureLogs collects what is logged until the test ends.
func captureLogs(t *testing.T) *lockedBuffer {
	logs := &lockedBuffer{}
	old := log.SwapLogger(slog.New(slog.NewTextHandler(logs, nil)))
	t.Cleanup(func() { log.SwapLogger(old) })
	return logs
}

func setReplicationPreserveRelayLogs(t *testing.T, enabled bool) {
	old := replicationPreserveRelayLogs
	replicationPreserveRelayLogs = enabled
	t.Cleanup(func() { replicationPreserveRelayLogs = old })
}

// TestRepointSameSourcePreservesRelayLog covers VTOrc's fixReplica, which repoints a replica to
// the primary it already replicates from, with a heartbeat interval. Only the receiver is
// stopped and reconfigured, so the applier keeps the relay log.
func TestRepointSameSourcePreservesRelayLog(t *testing.T) {
	env := newRelayLogTestEnv(t, relayLogTestPrimaryHost, relayLogTestPrimaryPort, relayLogTestPrimaryPosition)
	env.mysqld.ExpectedExecuteSuperQueryList = []string{
		"STOP REPLICA IO_THREAD",
		"FAKE SET SOURCE RECEIVER",
		"START REPLICA",
	}

	require.NoError(t, env.setReplicationSource(t, 15))
	require.NoError(t, env.mysqld.CheckSuperQueryList())
	env.assertRelayLogKept(t)
}

// TestRepointDifferentSourcePreservesRelayLog covers a repoint to a new primary, as done by
// reparents.
func TestRepointDifferentSourcePreservesRelayLog(t *testing.T) {
	env := newRelayLogTestEnv(t, "mysql-old-primary", 3305, relayLogTestPrimaryPosition)
	env.mysqld.ExpectedExecuteSuperQueryList = []string{
		"STOP REPLICA IO_THREAD",
		"FAKE SET SOURCE RECEIVER",
		"START REPLICA",
	}

	require.NoError(t, env.setReplicationSource(t, 0))
	require.NoError(t, env.mysqld.CheckSuperQueryList())
	assert.Equal(t, relayLogTestPrimaryHost, env.mysqld.CurrentSourceHost)
	assert.EqualValues(t, relayLogTestPrimaryPort, env.mysqld.CurrentSourcePort)
	env.assertRelayLogKept(t)
}

// TestRepointStartsStoppedApplierToPreserveRelayLog checks that a stopped applier is started
// before the change, so the relay log survives.
func TestRepointStartsStoppedApplierToPreserveRelayLog(t *testing.T) {
	env := newRelayLogTestEnv(t, "mysql-old-primary", 3305, relayLogTestPrimaryPosition)
	env.mysqld.Replicating = false
	env.mysqld.ExpectedExecuteSuperQueryList = []string{
		"STOP REPLICA IO_THREAD",
		"START REPLICA SQL_THREAD",
		"FAKE SET SOURCE RECEIVER",
		"START REPLICA",
	}

	require.NoError(t, env.setReplicationSource(t, 0))
	require.NoError(t, env.mysqld.CheckSuperQueryList())
	env.assertRelayLogKept(t)
}

// TestRepointDiscardsRelayLogWhenApplierCannotRun checks that when the applier cannot run and the
// replica is pointed at a different source, the repoint falls back to the full reconfiguration,
// as before: it discards the relay log, whose transactions the new primary has, and logs them.
// The receiver is started again if replication is to run.
func TestRepointDiscardsRelayLogWhenApplierCannotRun(t *testing.T) {
	testCases := []struct {
		name string
		// setup prepares the replica's stopped applier.
		setup func(env *relayLogTestEnv)
		// forceStart makes SetReplicationSource start replication afterwards.
		forceStart bool
		expected   []string
	}{
		{
			// Starting the applier would apply what an operator may have stopped it for.
			name:       "replication is to stay stopped",
			setup:      func(env *relayLogTestEnv) {},
			forceStart: false,
			expected:   []string{"STOP REPLICA IO_THREAD", "STOP REPLICA", "FAKE SET SOURCE"},
		},
		{
			name: "applier does not start",
			setup: func(env *relayLogTestEnv) {
				env.mysqld.StartSQLThreadError = vterrors.New(vtrpcpb.Code_UNKNOWN, "applier cannot start")
			},
			forceStart: true,
			expected:   []string{"STOP REPLICA IO_THREAD", "START REPLICA SQL_THREAD", "STOP REPLICA", "FAKE SET SOURCE", "START REPLICA"},
		},
		{
			// For example, on the error it stopped on before.
			name:       "applier stops again",
			setup:      func(env *relayLogTestEnv) { env.mysqld.SQLThreadStopsOnStart = true },
			forceStart: true,
			expected:   []string{"STOP REPLICA IO_THREAD", "START REPLICA SQL_THREAD", "STOP REPLICA", "FAKE SET SOURCE", "START REPLICA"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureLogs(t)
			env := newRelayLogTestEnv(t, "mysql-old-primary", 3305, relayLogTestPrimaryPosition)
			env.mysqld.Replicating = false
			tc.setup(env)
			env.mysqld.ExpectedExecuteSuperQueryList = tc.expected

			require.NoError(t, env.tm.SetReplicationSource(t.Context(), env.parent, 0, "", tc.forceStart, false, 0))
			require.NoError(t, env.mysqld.CheckSuperQueryList())
			assert.Equal(t, relayLogTestPrimaryHost, env.mysqld.CurrentSourceHost)
			assert.Equal(t, env.mysqld.CurrentPrimaryPosition, env.mysqld.CurrentRelayLogPosition, "the relay log must be discarded")
			assert.Contains(t, logs.String(), "discarding the relay log to change the replication source: the replication applier cannot run")
			assert.Contains(t, logs.String(), relayLogTestServerUUID+":201-210")
		})
	}
}

// TestRepointWithStoppedApplierAndNothingUnappliedUsesFullReconfiguration checks that a stopped
// applier with nothing left to apply is not started: there is no relay log to keep, and the full
// reconfiguration also resets the applier.
func TestRepointWithStoppedApplierAndNothingUnappliedUsesFullReconfiguration(t *testing.T) {
	env := newRelayLogTestEnv(t, "mysql-old-primary", 3305, relayLogTestPrimaryPosition)
	env.mysqld.Replicating = false
	env.mysqld.CurrentRelayLogPosition = env.mysqld.CurrentPrimaryPosition
	env.mysqld.ExpectedExecuteSuperQueryList = []string{
		"STOP REPLICA IO_THREAD",
		"STOP REPLICA",
		"FAKE SET SOURCE",
		"START REPLICA",
	}

	require.NoError(t, env.setReplicationSource(t, 0))
	require.NoError(t, env.mysqld.CheckSuperQueryList())
}

// TestRepointKeepsRelayLogWhenApplierCannotRunAndSourceUnchanged covers VTOrc's repair of a
// replica whose applier cannot run: it repoints to the primary the replica already replicates
// from, with a heartbeat interval. Any change of the replication source would discard the relay
// log, so the receiver is restarted on it instead. The relay log holds transactions the primary
// has not committed yet, or committed after its position was read: the primary has executed only
// 1-205 of the 1-210 the replica received.
func TestRepointKeepsRelayLogWhenApplierCannotRunAndSourceUnchanged(t *testing.T) {
	testCases := []struct {
		name string
		// setup prepares the replica's stopped applier.
		setup func(env *relayLogTestEnv)
		// forceStart makes SetReplicationSource start replication afterwards.
		forceStart bool
		expected   []string
	}{
		{
			name:       "replication is to stay stopped",
			setup:      func(env *relayLogTestEnv) {},
			forceStart: false,
			expected:   []string{"STOP REPLICA IO_THREAD"},
		},
		{
			name: "applier does not start",
			setup: func(env *relayLogTestEnv) {
				env.mysqld.StartSQLThreadError = vterrors.New(vtrpcpb.Code_UNKNOWN, "applier cannot start")
			},
			forceStart: true,
			expected:   []string{"STOP REPLICA IO_THREAD", "START REPLICA SQL_THREAD", "START REPLICA"},
		},
		{
			name:       "applier stops again",
			setup:      func(env *relayLogTestEnv) { env.mysqld.SQLThreadStopsOnStart = true },
			forceStart: true,
			expected:   []string{"STOP REPLICA IO_THREAD", "START REPLICA SQL_THREAD", "START REPLICA"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env := newRelayLogTestEnv(t, relayLogTestPrimaryHost, relayLogTestPrimaryPort, relayLogTestServerUUID+":1-205")
			env.mysqld.Replicating = false
			tc.setup(env)
			env.mysqld.ExpectedExecuteSuperQueryList = tc.expected

			require.NoError(t, env.tm.SetReplicationSource(t.Context(), env.parent, 0, "", tc.forceStart, false, 15))
			require.NoError(t, env.mysqld.CheckSuperQueryList())
			env.assertRelayLogKept(t)
		})
	}
}

// TestRepointLogsRelayLogDiscardedByReceiverChange checks the applier stopping right before the
// receiver-only change: MySQL then discards the relay log, which the repoint cannot prevent, but
// logs.
func TestRepointLogsRelayLogDiscardedByReceiverChange(t *testing.T) {
	logs := captureLogs(t)
	env := newRelayLogTestEnv(t, "mysql-old-primary", 3305, relayLogTestPrimaryPosition)
	env.mysqld.ExpectedExecuteSuperQueryList = []string{
		"STOP REPLICA IO_THREAD",
		"FAKE SET SOURCE RECEIVER",
		"START REPLICA",
	}
	env.mysqld.ExecuteSuperQueryListCallback = func() {
		if env.mysqld.ExpectedExecuteSuperQueryCurrent == 1 {
			// The applier stops right before the receiver-only change.
			env.mysqld.Replicating = false
		}
	}

	require.NoError(t, env.setReplicationSource(t, 0))
	require.NoError(t, env.mysqld.CheckSuperQueryList())
	assert.Equal(t, env.mysqld.CurrentPrimaryPosition, env.mysqld.CurrentRelayLogPosition, "the relay log must be discarded")
	assert.Contains(t, logs.String(), "the replication applier stopped before the replication receiver was changed, which discarded the relay log")
	assert.Contains(t, logs.String(), relayLogTestServerUUID+":201-210")
}

// TestRepointRefusesTransactionsReceivedWhileStoppingReceiver checks that the relay log is
// checked once the receiver has stopped: transactions of the old primary that the new primary
// lacks, written to the relay log while the receiver was being stopped, are refused, and left
// unapplied, whether the applier keeps running or stops; and refused as well
// when the applier already applied them.
func TestRepointRefusesTransactionsReceivedWhileStoppingReceiver(t *testing.T) {
	testCases := []struct {
		name string
		// setup prepares the replica's applier.
		setup func(env *relayLogTestEnv)
		// onStop runs while the receiver is being stopped.
		onStop   func(env *relayLogTestEnv)
		expected []string
	}{
		{
			name:     "applier keeps running",
			setup:    func(env *relayLogTestEnv) {},
			onStop:   func(env *relayLogTestEnv) {},
			expected: []string{"STOP REPLICA IO_THREAD", "STOP REPLICA"},
		},
		{
			name:     "applier stops",
			setup:    func(env *relayLogTestEnv) {},
			onStop:   func(env *relayLogTestEnv) { env.mysqld.Replicating = false },
			expected: []string{"STOP REPLICA IO_THREAD"},
		},
		{
			// The applier applies them before the status is read.
			name:  "applier applies them",
			setup: func(env *relayLogTestEnv) {},
			onStop: func(env *relayLogTestEnv) {
				env.mysqld.CurrentPrimaryPosition = mustParseMysql56Position(t, relayLogTestServerUUID+":1-200,"+relayLogTestOtherServerTransactions)
			},
			expected: []string{"STOP REPLICA IO_THREAD", "STOP REPLICA"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			env := newRelayLogTestEnv(t, "mysql-old-primary", 3305, relayLogTestPrimaryPosition)
			tc.setup(env)
			env.mysqld.ExpectedExecuteSuperQueryList = tc.expected
			stopped := false
			env.mysqld.ExecuteSuperQueryListCallback = func() {
				if !stopped {
					stopped = true
					tc.onStop(env)
					env.receiveOtherServerTransactions(t)
				}
			}

			err := env.setReplicationSource(t, 0)
			require.Error(t, err)
			assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
			require.ErrorContains(t, err, "would introduce errant GTIDs")
			require.ErrorContains(t, err, relayLogTestOtherServerTransactions)
			require.NoError(t, env.mysqld.CheckSuperQueryList())
			assert.False(t, env.mysqld.Replicating, "the applier must not apply them")
			assert.Equal(t, "mysql-old-primary", env.mysqld.CurrentSourceHost)
			assert.Equal(t, relayLogTestServerUUID+":1-210,"+relayLogTestOtherServerTransactions, env.mysqld.CurrentRelayLogPosition.GTIDSet.String(), "the relay log must be kept")
		})
	}
}

// TestRepointReportsFailedApplierStopOnRefusal checks that a refused repoint reports it when the
// applier, which could still apply the transactions the new source lacks, cannot be stopped.
func TestRepointReportsFailedApplierStopOnRefusal(t *testing.T) {
	env := newRelayLogTestEnv(t, "mysql-old-primary", 3305, relayLogTestPrimaryPosition)
	env.mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA IO_THREAD"}
	env.mysqld.StopReplicationError = vterrors.New(vtrpcpb.Code_DEADLINE_EXCEEDED, "stop timed out")
	stopped := false
	env.mysqld.ExecuteSuperQueryListCallback = func() {
		if !stopped {
			stopped = true
			env.receiveOtherServerTransactions(t)
		}
	}

	err := env.setReplicationSource(t, 0)
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	require.ErrorContains(t, err, relayLogTestOtherServerTransactions)
	require.ErrorContains(t, err, "stopping the replication applier to keep the unapplied ones unapplied failed: stop timed out")
	require.NoError(t, env.mysqld.CheckSuperQueryList())
}

// TestRepointReconfiguresRecoverableReceiverChangeError checks that a recoverable replication
// metadata error from the receiver-only change falls back to the full reconfiguration, which
// resets the broken metadata.
func TestRepointReconfiguresRecoverableReceiverChangeError(t *testing.T) {
	env := newRelayLogTestEnv(t, "mysql-old-primary", 3305, relayLogTestPrimaryPosition)
	env.mysqld.SetReplicationSourceReceiverError = recoverableReplicationInitError()
	env.mysqld.ExpectedExecuteSuperQueryList = []string{
		"STOP REPLICA IO_THREAD",
		"STOP REPLICA",
		"FAKE SET SOURCE",
		"START REPLICA",
	}

	require.NoError(t, env.setReplicationSource(t, 0))
	require.NoError(t, env.mysqld.CheckSuperQueryList())
}

// TestRepointReportsReceiverChangeError checks that a failed receiver-only change that the full
// reconfiguration cannot repair is returned with its context: the receiver is stopped.
func TestRepointReportsReceiverChangeError(t *testing.T) {
	env := newRelayLogTestEnv(t, "mysql-old-primary", 3305, relayLogTestPrimaryPosition)
	env.mysqld.SetReplicationSourceReceiverError = vterrors.New(vtrpcpb.Code_UNKNOWN, "access denied")
	env.mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA IO_THREAD"}

	err := env.setReplicationSource(t, 0)
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_UNKNOWN, vterrors.Code(err))
	require.ErrorContains(t, err, "failed to change the replication receiver; the replication receiver is stopped: access denied")
	require.NoError(t, env.mysqld.CheckSuperQueryList())
	env.assertRelayLogKept(t)
}

// TestRepointFirstTimeSetupUsesFullReconfiguration checks that a tablet without replication
// configured (e.g. a demoted primary) is set up with the full command: there is no relay log.
func TestRepointFirstTimeSetupUsesFullReconfiguration(t *testing.T) {
	env := newRelayLogTestEnv(t, "", 0, relayLogTestPrimaryPosition)
	env.mysqld.Replicating = false
	env.mysqld.ReplicationStatusError = mysql.ErrNotReplica
	env.mysqld.ExpectedExecuteSuperQueryList = []string{
		"FAKE SET SOURCE",
		"START REPLICA",
	}

	require.NoError(t, env.setReplicationSource(t, 0))
	require.NoError(t, env.mysqld.CheckSuperQueryList())
}

// TestRepointWithoutAutoPositionUsesFullReconfiguration checks that replication that is not
// configured with auto-positioning, which Vitess always enables, keeps the previous repoint.
func TestRepointWithoutAutoPositionUsesFullReconfiguration(t *testing.T) {
	env := newRelayLogTestEnv(t, "mysql-old-primary", 3305, relayLogTestPrimaryPosition)
	env.mysqld.AutoPosition = false
	env.mysqld.ExpectedExecuteSuperQueryList = []string{
		"STOP REPLICA",
		"FAKE SET SOURCE",
		"START REPLICA",
	}

	require.NoError(t, env.setReplicationSource(t, 0))
	require.NoError(t, env.mysqld.CheckSuperQueryList())
}

// TestRepointKillSwitchUsesFullReconfiguration checks that --replication-preserve-relay-logs=false
// restores the previous behavior.
func TestRepointKillSwitchUsesFullReconfiguration(t *testing.T) {
	setReplicationPreserveRelayLogs(t, false)
	// Even relay log transactions that the new primary lacks are discarded, as tablet startup
	// did before.
	env := newRelayLogTestEnv(t, "mysql-old-primary", 3305, relayLogTestPrimaryPosition)
	env.receiveOtherServerTransactions(t)
	env.mysqld.ExpectedExecuteSuperQueryList = []string{
		"STOP REPLICA",
		"FAKE SET SOURCE",
		"START REPLICA",
	}

	err := env.tm.repointReplication(t.Context(), relayLogTestPrimaryHost, relayLogTestPrimaryPort, 15,
		relayLogTestSource(t, relayLogTestPrimaryPosition), true, true)
	require.NoError(t, err)
	require.NoError(t, env.mysqld.CheckSuperQueryList())
}

// TestRepointUnsupportedFlavorUsesFullReconfiguration checks that flavors without the
// receiver-only command (MySQL before 8.0.26) keep the previous repoint.
func TestRepointUnsupportedFlavorUsesFullReconfiguration(t *testing.T) {
	env := newRelayLogTestEnv(t, relayLogTestPrimaryHost, relayLogTestPrimaryPort, relayLogTestPrimaryPosition)
	env.mysqld.ReplicationSourceReceiverChangeUnsupported = true
	env.mysqld.ExpectedExecuteSuperQueryList = []string{
		"STOP REPLICA",
		"FAKE SET SOURCE",
		"START REPLICA",
	}

	require.NoError(t, env.setReplicationSource(t, 15))
	require.NoError(t, env.mysqld.CheckSuperQueryList())
}

// TestTabletStartupRefusesRelayLogTransactionsTheSourceLacks covers a replica that restarts after
// a reparent it missed: its relay log still holds transactions of the old primary that the new
// primary lacks. Applying them would introduce errant GTIDs, and discarding them would lose them,
// so the repoint is refused, and the applier stopped. Unlike SetReplicationSource, tablet startup checks only the
// executed GTIDs for errant ones, so the repoint must refuse itself.
func TestTabletStartupRefusesRelayLogTransactionsTheSourceLacks(t *testing.T) {
	ctx := t.Context()
	env := newRelayLogTestEnv(t, "mysql-old-primary", 3305, relayLogTestPrimaryPosition)
	reparenttestutil.SetKeyspaceDurability(ctx, t, env.tm.TopoServer, "ks", policy.DurabilityNone)
	_, err := env.tm.TopoServer.UpdateShardFields(ctx, "ks", "0", func(si *topo.ShardInfo) error {
		si.PrimaryAlias = env.parent
		return nil
	})
	require.NoError(t, err)
	env.receiveOtherServerTransactions(t)

	env.mysqld.ExpectedExecuteSuperQueryList = []string{"STOP REPLICA IO_THREAD", "STOP REPLICA"}

	_, err = env.tm.initializeReplication(ctx, topodatapb.TabletType_REPLICA)
	require.Error(t, err)
	assert.Equal(t, vtrpcpb.Code_FAILED_PRECONDITION, vterrors.Code(err))
	require.ErrorContains(t, err, relayLogTestOtherServerTransactions)
	require.NoError(t, env.mysqld.CheckSuperQueryList(), "only the receiver and the applier may be stopped")
	assert.Equal(t, "mysql-old-primary", env.mysqld.CurrentSourceHost)
	assert.Equal(t, relayLogTestServerUUID+":1-210,"+relayLogTestOtherServerTransactions, env.mysqld.CurrentRelayLogPosition.GTIDSet.String(), "the relay log must be kept")
}
