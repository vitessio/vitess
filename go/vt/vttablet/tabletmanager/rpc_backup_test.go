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
	ctx, tm, fake, qsc := newPrimaryBackupTest(t)
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
			ctx, tm, fake, qsc := newPrimaryBackupTest(t)
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

// newPrimaryBackupTest returns a serving primary with semi-sync enabled, in a
// keyspace whose durability policy requires it, and a draining backup engine.
func newPrimaryBackupTest(t *testing.T) (context.Context, *TabletManager, *mysqlctl.FakeMysqlDaemon, *tabletservermock.Controller) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	ts := memorytopo.NewServer(ctx, "cell1")
	t.Cleanup(ts.Close)
	tm := newTestTM(t, ts, 1, "ks", "0", nil)
	t.Cleanup(tm.Stop)
	tm.Cnf = &mysqlctl.Mycnf{}
	fake := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
	fake.DB().SetNeverFail(true)
	reparenttestutil.SetKeyspaceDurability(ctx, t, ts, "ks", policy.DurabilitySemiSync)

	require.NoError(t, tm.tmState.ChangeTabletType(ctx, topodatapb.TabletType_PRIMARY, DBActionNone))
	ti, err := ts.GetTablet(ctx, tm.Tablet().Alias)
	require.NoError(t, err)
	require.NoError(t, checkShardRecord(ctx, t, ts, ti.Alias, ti.PrimaryTermStartTime))
	require.NoError(t, fake.SetSemiSyncEnabled(ctx, true, true))
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	require.True(t, qsc.IsServing())

	mysqlctl.BackupRestoreEngineMap[primaryDrainEngine] = &mysqlctl.FakeBackupEngine{
		ShouldDrainForBackupReturn: true,
		ExecuteBackupReturn:        mysqlctl.FakeBackupEngineExecuteBackupReturn{Res: mysqlctl.BackupUsable},
	}
	t.Cleanup(func() { delete(mysqlctl.BackupRestoreEngineMap, primaryDrainEngine) })
	return ctx, tm, fake, qsc
}
