/*
Copyright 2025 The Vitess Authors.

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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/reparenttestutil"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"
)

func TestShutdownTimeout(t *testing.T) {
	cases := []struct {
		name     string
		timeout  *vttime.Duration
		expected time.Duration
	}{
		{
			name:     "no timeout",
			timeout:  nil,
			expected: mysqlShutdownTimeout,
		},
		{
			name:     "timeout is set",
			timeout:  &vttime.Duration{Seconds: 1},
			expected: 1 * time.Second,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := logutil.NewMemoryLogger()
			timeout := shutdownTimeout(l, tc.timeout)
			assert.Equal(t, tc.expected, timeout)
		})
	}
}

// TestBackupRestoresTypeWhenChangeToBackupFails checks that a backup that fails
// after publishing the BACKUP type restores the tablet's original type and
// serving state.
func TestBackupRestoresTypeWhenChangeToBackupFails(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	ts := memorytopo.NewServer(ctx, "cell1")
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	t.Cleanup(tm.Stop)
	tm.Cnf = &mysqlctl.Mycnf{}
	fake := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fake.DB().SetNeverFail(true)

	const engineName = "fake-drain"
	mysqlctl.BackupRestoreEngineMap[engineName] = &mysqlctl.FakeBackupEngine{ShouldDrainForBackupReturn: true}
	t.Cleanup(func() { delete(mysqlctl.BackupRestoreEngineMap, engineName) })

	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	require.True(t, qsc.IsServing())
	injectedErr := errors.New("injected replication status failure")
	fake.ReplicationStatusError = injectedErr

	err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{BackupEngine: new(engineName)})
	require.ErrorContains(t, err, injectedErr.Error())
	assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
	ti, err := ts.GetTablet(ctx, tm.Tablet().Alias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_REPLICA, ti.Type)
	assert.True(t, qsc.IsServing())
}

// TestBackupRestoresPrimarySemiSync checks that a primary whose change to BACKUP
// fails turns source-side semi-sync back on before it serves again.
func TestBackupRestoresPrimarySemiSync(t *testing.T) {
	ctx, tm, fake, qsc, _ := newBackupTest(t, topodatapb.TabletType_PRIMARY)
	injectedErr := errors.New("injected replication status failure")
	fake.ReplicationStatusError = injectedErr
	servingWhenEnabled := true
	fake.SetSemiSyncEnabledFunc = func(primary, replica bool) error {
		if primary {
			servingWhenEnabled = qsc.IsServing()
		}
		return nil
	}

	err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{AllowPrimary: true, BackupEngine: new(primaryDrainEngine)})
	require.ErrorContains(t, err, injectedErr.Error())
	assert.False(t, servingWhenEnabled, "semi-sync was enabled after serving resumed")
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	ti, err := tm.TopoServer.GetTablet(ctx, tm.Tablet().Alias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, ti.Type)
	assert.True(t, qsc.IsServing())
	assert.True(t, fake.SemiSyncPrimaryEnabled)
}

// TestBackupKeepsPrimaryDrainedWhenSemiSyncRestoreFails checks that a primary
// that cannot turn semi-sync back on stays in BACKUP without serving, and that
// Backup reports the failure without hiding an earlier error.
func TestBackupKeepsPrimaryDrainedWhenSemiSyncRestoreFails(t *testing.T) {
	restoreErr := errors.New("injected semi-sync enable failure")
	statusErr := errors.New("injected replication status failure")
	tests := []struct {
		name      string
		statusErr error
		wantErr   string
		backedUp  bool
	}{
		{name: "after a successful backup", wantErr: restoreErr.Error(), backedUp: true},
		{name: "after the change to BACKUP failed", statusErr: statusErr, wantErr: statusErr.Error()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, tm, fake, qsc, _ := newBackupTest(t, topodatapb.TabletType_PRIMARY)
			handle := useFakeBackupStorage(t)
			fake.ReplicationStatusError = tt.statusErr
			fake.SetSemiSyncEnabledFunc = func(primary, replica bool) error {
				if primary {
					return restoreErr
				}
				return nil
			}

			err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{AllowPrimary: true, BackupEngine: new(primaryDrainEngine)})
			require.ErrorContains(t, err, tt.wantErr)
			if tt.backedUp {
				assert.Len(t, handle.EndBackupCalls, 1)
				assert.Empty(t, handle.AbortBackupCalls)
			} else {
				require.NotErrorIs(t, err, restoreErr)
			}
			assert.Equal(t, topodatapb.TabletType_BACKUP, tm.Tablet().Type)
			ti, err := tm.TopoServer.GetTablet(ctx, tm.Tablet().Alias)
			require.NoError(t, err)
			assert.Equal(t, topodatapb.TabletType_BACKUP, ti.Type)
			assert.False(t, qsc.IsServing())
			assert.False(t, fake.SemiSyncPrimaryEnabled)
		})
	}
}

// useFakeBackupStorage makes backups write to a fake storage and returns the
// handle of the backup.
func useFakeBackupStorage(t *testing.T) *mysqlctl.FakeBackupHandle {
	const storageName = "fake-primary-backup"
	handle := &mysqlctl.FakeBackupHandle{NameV: "backup"}
	storage := &mysqlctl.FakeBackupStorage{StartBackupReturn: mysqlctl.FakeBackupStorageStartBackupReturn{BackupHandle: handle}}
	storage.WithParamsReturn = storage
	previous := backupstorage.BackupStorageImplementation
	backupstorage.BackupStorageMap[storageName] = storage
	backupstorage.BackupStorageImplementation = storageName
	t.Cleanup(func() {
		backupstorage.BackupStorageImplementation = previous
		delete(backupstorage.BackupStorageMap, storageName)
	})
	return handle
}

const primaryDrainEngine = "fake-primary-drain"

// newBackupTest returns a serving tablet of the given type, PRIMARY or REPLICA,
// with the semi-sync settings of that type, in a keyspace whose durability
// policy requires semi-sync, and a draining backup engine.
func newBackupTest(t *testing.T, tabletType topodatapb.TabletType) (context.Context, *TabletManager, *mysqlctl.FakeMysqlDaemon, *tabletservermock.Controller, *memorytopo.Factory) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	ts, factory := memorytopo.NewServerAndFactory(ctx, "cell1")
	t.Cleanup(ts.Close)
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	t.Cleanup(tm.Stop)
	tm.Cnf = &mysqlctl.Mycnf{}
	fake := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fake.DB().SetNeverFail(true)
	reparenttestutil.SetKeyspaceDurability(ctx, t, ts, "ks", policy.DurabilitySemiSync)

	if tabletType == topodatapb.TabletType_PRIMARY {
		require.NoError(t, tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_PRIMARY, DBActionNone))
		ti, err := ts.GetTablet(ctx, tm.Tablet().Alias)
		require.NoError(t, err)
		require.NoError(t, checkShardRecord(ctx, t, ts, ti.Alias, ti.PrimaryTermStartTime))
	}
	require.Equal(t, tabletType, tm.Tablet().Type)
	require.NoError(t, fake.SetSemiSyncEnabled(ctx, tabletType == topodatapb.TabletType_PRIMARY, true))
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	require.True(t, qsc.IsServing())

	mysqlctl.BackupRestoreEngineMap[primaryDrainEngine] = &mysqlctl.FakeBackupEngine{
		ShouldDrainForBackupReturn: true,
		ExecuteBackupReturn:        mysqlctl.FakeBackupEngineExecuteBackupReturn{Res: mysqlctl.BackupUsable},
	}
	t.Cleanup(func() { delete(mysqlctl.BackupRestoreEngineMap, primaryDrainEngine) })
	return ctx, tm, fake, qsc, factory
}

// TestBackupRefusesPrimaryWhoseDurabilityPolicyCannotBeRead checks that a
// primary backup that cannot read or resolve the durability policy is refused
// before the tablet changes type.
func TestBackupRefusesPrimaryWhoseDurabilityPolicyCannotBeRead(t *testing.T) {
	topoErr := errors.New("injected keyspace read failure")
	for _, tc := range []struct {
		name    string
		fail    func(t *testing.T, ctx context.Context, tm *TabletManager, factory *memorytopo.Factory)
		wantErr string
	}{
		{
			name: "keyspace read fails",
			fail: func(t *testing.T, ctx context.Context, tm *TabletManager, factory *memorytopo.Factory) {
				factory.AddOperationError(memorytopo.Get, "keyspaces/ks/Keyspace$", topoErr)
			},
			wantErr: topoErr.Error(),
		},
		{
			name: "unknown policy",
			fail: func(t *testing.T, ctx context.Context, tm *TabletManager, factory *memorytopo.Factory) {
				reparenttestutil.SetKeyspaceDurability(ctx, t, tm.TopoServer, "ks", "unknown-policy")
			},
			wantErr: "unknown-policy",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, tm, fake, qsc, factory := newBackupTest(t, topodatapb.TabletType_PRIMARY)
			tc.fail(t, ctx, tm, factory)
			engine := mysqlctl.BackupRestoreEngineMap[primaryDrainEngine].(*mysqlctl.FakeBackupEngine)

			err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{AllowPrimary: true, BackupEngine: new(primaryDrainEngine)})
			require.ErrorContains(t, err, tc.wantErr)
			assert.Empty(t, engine.ExecuteBackupCalls)
			assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
			ti, err := tm.TopoServer.GetTablet(ctx, tm.Tablet().Alias)
			require.NoError(t, err)
			assert.Equal(t, topodatapb.TabletType_PRIMARY, ti.Type)
			assert.True(t, qsc.IsServing())
			assert.True(t, fake.SemiSyncPrimaryEnabled)
		})
	}
}

// duringBackupEngine runs a function while the backup runs.
type duringBackupEngine struct {
	*mysqlctl.FakeBackupEngine
	during func()
}

func (e *duringBackupEngine) ExecuteBackup(ctx context.Context, params mysqlctl.BackupParams, bh backupstorage.BackupHandle) (mysqlctl.BackupResult, error) {
	e.during()
	return e.FakeBackupEngine.ExecuteBackup(ctx, params, bh)
}

// TestBackupRestoresPrimarySemiSyncFromPolicyAtStart checks that a primary's
// semi-sync is restored as the durability policy required when the backup
// started. VTOrc reconciles a policy that changed during the backup.
func TestBackupRestoresPrimarySemiSyncFromPolicyAtStart(t *testing.T) {
	for _, tc := range []struct {
		name         string
		atStart      string
		duringBackup string
		wantSemiSync bool
	}{
		{name: "semi_sync, then none", atStart: policy.DurabilitySemiSync, duringBackup: policy.DurabilityNone, wantSemiSync: true},
		{name: "none, then semi_sync", atStart: policy.DurabilityNone, duringBackup: policy.DurabilitySemiSync},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, tm, fake, qsc, _ := newBackupTest(t, topodatapb.TabletType_PRIMARY)
			reparenttestutil.SetKeyspaceDurability(ctx, t, tm.TopoServer, "ks", tc.atStart)
			require.NoError(t, fake.SetSemiSyncEnabled(ctx, tc.wantSemiSync, tc.wantSemiSync))
			useFakeBackupStorage(t)
			mysqlctl.BackupRestoreEngineMap[primaryDrainEngine] = &duringBackupEngine{
				FakeBackupEngine: mysqlctl.BackupRestoreEngineMap[primaryDrainEngine].(*mysqlctl.FakeBackupEngine),
				during: func() {
					reparenttestutil.SetKeyspaceDurability(ctx, t, tm.TopoServer, "ks", tc.duringBackup)
				},
			}

			err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{AllowPrimary: true, BackupEngine: new(primaryDrainEngine)})
			require.NoError(t, err)
			assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
			assert.True(t, qsc.IsServing())
			assert.Equal(t, tc.wantSemiSync, fake.SemiSyncPrimaryEnabled)
		})
	}
}

// TestBackupLeavesReplicaSourceSemiSyncOff checks that a replica's backup
// doesn't turn on source-side semi-sync, and doesn't depend on the durability
// policy being readable when it starts.
func TestBackupLeavesReplicaSourceSemiSyncOff(t *testing.T) {
	for _, tc := range []struct {
		name     string
		atStart  string
		readFail bool
	}{
		{name: "semi_sync", atStart: policy.DurabilitySemiSync},
		{name: "none", atStart: policy.DurabilityNone},
		{name: "keyspace read fails", atStart: policy.DurabilitySemiSync, readFail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, tm, fake, qsc, factory := newBackupTest(t, topodatapb.TabletType_REPLICA)
			reparenttestutil.SetKeyspaceDurability(ctx, t, tm.TopoServer, "ks", tc.atStart)
			handle := useFakeBackupStorage(t)
			if tc.readFail {
				factory.AddOperationError(memorytopo.Get, "keyspaces/ks/Keyspace$", errors.New("injected keyspace read failure"))
			}

			err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{BackupEngine: new(primaryDrainEngine)})
			require.NoError(t, err)
			assert.Len(t, handle.EndBackupCalls, 1)
			assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
			assert.True(t, qsc.IsServing())
			assert.False(t, fake.SemiSyncPrimaryEnabled)
		})
	}
}
