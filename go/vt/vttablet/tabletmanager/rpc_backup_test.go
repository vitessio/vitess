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

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/policy"
	"vitess.io/vitess/go/vt/vtctl/reparentutil/reparenttestutil"
	"vitess.io/vitess/go/vt/vterrors"
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

	_, err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{BackupEngine: new(engineName)})
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

	_, err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{AllowPrimary: true, BackupEngine: new(primaryDrainEngine)})
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

			_, err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{AllowPrimary: true, BackupEngine: new(primaryDrainEngine)})
			require.ErrorContains(t, err, tt.wantErr)
			if tt.backedUp {
				require.ErrorContains(t, err, "backup completed, but")
				assert.Len(t, handle.EndBackupCalls, 1)
				assert.Empty(t, handle.AbortBackupCalls)
			} else {
				assert.NotContains(t, err.Error(), "backup completed")
				require.NotErrorIs(t, err, restoreErr)
			}
			assert.Equal(t, topodatapb.TabletType_BACKUP, tm.Tablet().Type)
			ti, err := tm.TopoServer.GetTablet(ctx, tm.Tablet().Alias)
			require.NoError(t, err)
			assert.Equal(t, topodatapb.TabletType_BACKUP, ti.Type)
			assert.False(t, qsc.IsServing())
			assert.False(t, fake.SemiSyncPrimaryEnabled)
			assert.False(t, tm.SemiSyncMonitor.IsOpen())
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
	if tabletType == topodatapb.TabletType_PRIMARY {
		tm.SemiSyncMonitor.Open()
	}
	t.Cleanup(tm.SemiSyncMonitor.Close)
	qsc := tm.QueryServiceControl.(*tabletservermock.Controller)
	require.True(t, qsc.IsServing())

	mysqlctl.BackupRestoreEngineMap[primaryDrainEngine] = &mysqlctl.FakeBackupEngine{
		ShouldDrainForBackupReturn: true,
		ExecuteBackupReturn:        mysqlctl.FakeBackupEngineExecuteBackupReturn{Res: mysqlctl.BackupUsable},
	}
	t.Cleanup(func() { delete(mysqlctl.BackupRestoreEngineMap, primaryDrainEngine) })
	return ctx, tm, fake, qsc, factory
}

// TestBackupRefusesPrimaryWithInvalidDurabilityConfiguration checks that a
// primary backup that cannot resolve or apply its durability policy is refused
// before the tablet changes type.
func TestBackupRefusesPrimaryWithInvalidDurabilityConfiguration(t *testing.T) {
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
		{
			name: "semi-sync plugin check fails",
			fail: func(t *testing.T, ctx context.Context, tm *TabletManager, factory *memorytopo.Factory) {
				tm.MysqlDaemon = &backupPluginCheckDaemon{FakeMysqlDaemon: tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon), err: errors.New("injected plugin check failure")}
			},
			wantErr: "injected plugin check failure",
		},
		{
			name: "required semi-sync plugin is absent",
			fail: func(t *testing.T, ctx context.Context, tm *TabletManager, factory *memorytopo.Factory) {
				fake := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
				require.NoError(t, fake.SetSemiSyncEnabled(ctx, false, false))
				tm.MysqlDaemon = &backupPluginCheckDaemon{FakeMysqlDaemon: fake, plugin: mysql.SemiSyncTypeOff}
			},
			wantErr: "VT09013",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, tm, fake, qsc, factory := newBackupTest(t, topodatapb.TabletType_PRIMARY)
			tc.fail(t, ctx, tm, factory)
			wasSemiSync := fake.SemiSyncPrimaryEnabled
			engine := mysqlctl.BackupRestoreEngineMap[primaryDrainEngine].(*mysqlctl.FakeBackupEngine)

			_, err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{AllowPrimary: true, BackupEngine: new(primaryDrainEngine)})
			require.ErrorContains(t, err, tc.wantErr)
			assert.Empty(t, engine.ExecuteBackupCalls)
			assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
			ti, err := tm.TopoServer.GetTablet(ctx, tm.Tablet().Alias)
			require.NoError(t, err)
			assert.Equal(t, topodatapb.TabletType_PRIMARY, ti.Type)
			assert.True(t, qsc.IsServing())
			assert.Equal(t, wasSemiSync, fake.SemiSyncPrimaryEnabled)
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

			_, err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{AllowPrimary: true, BackupEngine: new(primaryDrainEngine)})
			require.NoError(t, err)
			assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
			assert.True(t, qsc.IsServing())
			assert.Equal(t, tc.wantSemiSync, fake.SemiSyncPrimaryEnabled)
			assert.Equal(t, tc.wantSemiSync, tm.SemiSyncMonitor.IsOpen())
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

			_, err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{BackupEngine: new(primaryDrainEngine)})
			require.NoError(t, err)
			assert.Len(t, handle.EndBackupCalls, 1)
			assert.Equal(t, topodatapb.TabletType_REPLICA, tm.Tablet().Type)
			assert.True(t, qsc.IsServing())
			assert.False(t, fake.SemiSyncPrimaryEnabled)
		})
	}
}

// ctxSemiSyncMysqlDaemon fails SetSemiSyncEnabled once its context is done, as
// a real MySQL connection does.
type ctxSemiSyncMysqlDaemon struct {
	*mysqlctl.FakeMysqlDaemon
}

func (d *ctxSemiSyncMysqlDaemon) SetSemiSyncEnabled(ctx context.Context, primary, replica bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return d.FakeMysqlDaemon.SetSemiSyncEnabled(ctx, primary, replica)
}

// TestBackupRestoresPrimarySemiSyncWhenCanceled checks that a primary whose
// backup is canceled after it enters BACKUP still turns semi-sync back on and
// serves as PRIMARY again.
func TestBackupRestoresPrimarySemiSyncWhenCanceled(t *testing.T) {
	ctx, tm, fake, qsc, _ := newBackupTest(t, topodatapb.TabletType_PRIMARY)
	tm.MysqlDaemon = &ctxSemiSyncMysqlDaemon{FakeMysqlDaemon: fake}
	useFakeBackupStorage(t)
	backupCtx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	mysqlctl.BackupRestoreEngineMap[primaryDrainEngine] = &duringBackupEngine{
		FakeBackupEngine: mysqlctl.BackupRestoreEngineMap[primaryDrainEngine].(*mysqlctl.FakeBackupEngine),
		during: func() {
			assert.False(t, fake.SemiSyncPrimaryEnabled)
			cancel()
		},
	}

	_, err := tm.Backup(backupCtx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{AllowPrimary: true, BackupEngine: new(primaryDrainEngine)})
	// The fake engine completes the backup despite the cancellation.
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	ti, err := tm.TopoServer.GetTablet(ctx, tm.Tablet().Alias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, ti.Type)
	assert.True(t, qsc.IsServing())
	assert.True(t, fake.SemiSyncPrimaryEnabled)
}

// stalledSemiSyncMysqlDaemon doesn't answer a primary's semi-sync enable until
// its context ends, like a mysqld that stopped answering. With completes set,
// the enable then succeeds, like a query that completes while it is killed.
type stalledSemiSyncMysqlDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	completes bool
}

func (d *stalledSemiSyncMysqlDaemon) SetSemiSyncEnabled(ctx context.Context, primary, replica bool) error {
	if primary {
		if _, ok := ctx.Deadline(); !ok {
			return errors.New("semi-sync enable without a deadline would never return")
		}
		<-ctx.Done()
		if !d.completes {
			return ctx.Err()
		}
	}
	return d.FakeMysqlDaemon.SetSemiSyncEnabled(context.WithoutCancel(ctx), primary, replica)
}

// useShortRemoteOperationTimeout shortens the restore budget. Call it before
// the tablet starts, so the budget is reset after the tablet stops.
func useShortRemoteOperationTimeout(t *testing.T) {
	previous := topo.RemoteOperationTimeout
	topo.RemoteOperationTimeout = 100 * time.Millisecond
	t.Cleanup(func() { topo.RemoteOperationTimeout = previous })
}

// TestBackupReleasesActionLockWhenPrimaryRestoreHangs checks that a primary
// whose restore after a backup doesn't complete stays in BACKUP without
// serving, reports the failure, and releases the action lock.
func TestBackupReleasesActionLockWhenPrimaryRestoreHangs(t *testing.T) {
	for _, tc := range []struct {
		name string
		// hang makes a restore step stop answering, once the backup runs.
		hang         func(tm *TabletManager, fake *mysqlctl.FakeMysqlDaemon, factory *memorytopo.Factory)
		wantErr      string
		wantSemiSync bool
	}{
		{
			name: "semi-sync enable",
			hang: func(tm *TabletManager, fake *mysqlctl.FakeMysqlDaemon, factory *memorytopo.Factory) {
				tm.MysqlDaemon = &stalledSemiSyncMysqlDaemon{FakeMysqlDaemon: fake}
			},
			wantErr: context.DeadlineExceeded.Error(),
		},
		{
			// The topo fails every call, so the PRIMARY write never commits; this
			// doesn't cover a write that committed but couldn't be confirmed.
			name: "type change after semi-sync enable",
			hang: func(tm *TabletManager, fake *mysqlctl.FakeMysqlDaemon, factory *memorytopo.Factory) {
				factory.SetError(errors.New("injected topo failure"))
			},
			wantErr:      "updating tablet_type",
			wantSemiSync: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useShortRemoteOperationTimeout(t)
			ctx, tm, fake, qsc, factory := newBackupTest(t, topodatapb.TabletType_PRIMARY)
			// Lets a restore that never gives up finish, so the tablet can stop.
			t.Cleanup(func() { factory.SetError(nil) })
			handle := useFakeBackupStorage(t)
			mysqlctl.BackupRestoreEngineMap[primaryDrainEngine] = &duringBackupEngine{
				FakeBackupEngine: mysqlctl.BackupRestoreEngineMap[primaryDrainEngine].(*mysqlctl.FakeBackupEngine),
				during:           func() { tc.hang(tm, fake, factory) },
			}

			returned := make(chan error, 1)
			go func() {
				_, err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{AllowPrimary: true, BackupEngine: new(primaryDrainEngine)})
				returned <- err
			}()
			var err error
			require.Eventually(t, func() bool {
				select {
				case err = <-returned:
					return true
				default:
					return false
				}
			}, 30*time.Second, 10*time.Millisecond, "Backup did not return")
			require.ErrorContains(t, err, tc.wantErr)
			require.ErrorContains(t, err, "backup completed, but")
			assert.Equal(t, vtrpcpb.Code_DEADLINE_EXCEEDED, vterrors.Code(err))
			assert.Len(t, handle.EndBackupCalls, 1)
			assert.Equal(t, topodatapb.TabletType_BACKUP, tm.Tablet().Type)
			assert.False(t, qsc.IsServing())
			assert.Equal(t, tc.wantSemiSync, fake.SemiSyncPrimaryEnabled)
			assert.False(t, tm.SemiSyncMonitor.IsOpen())
			require.NoError(t, tm.lock(ctx))
			tm.unlock()
			factory.SetError(nil)
			ti, err := tm.TopoServer.GetTablet(ctx, tm.Tablet().Alias)
			require.NoError(t, err)
			assert.Equal(t, topodatapb.TabletType_BACKUP, ti.Type)
		})
	}
}

// TestBackupRestoresPrimaryTypeWithItsOwnBudget checks that a primary whose
// semi-sync enable succeeds just as its budget ends still gets a budget of its
// own to change back to PRIMARY.
func TestBackupRestoresPrimaryTypeWithItsOwnBudget(t *testing.T) {
	useShortRemoteOperationTimeout(t)
	ctx, tm, fake, qsc, _ := newBackupTest(t, topodatapb.TabletType_PRIMARY)
	useFakeBackupStorage(t)
	tm.MysqlDaemon = &stalledSemiSyncMysqlDaemon{FakeMysqlDaemon: fake, completes: true}

	_, err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{AllowPrimary: true, BackupEngine: new(primaryDrainEngine)})
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	ti, err := tm.TopoServer.GetTablet(ctx, tm.Tablet().Alias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, ti.Type)
	assert.True(t, qsc.IsServing())
	assert.True(t, fake.SemiSyncPrimaryEnabled)
}

// TestBackupStartsMonitorWhenPrimaryRestoreFailsAfterPublishing checks that a
// primary whose type change fails after it is PRIMARY locally runs the semi-sync
// monitor, and that Backup reports the failure.
func TestBackupStartsMonitorWhenPrimaryRestoreFailsAfterPublishing(t *testing.T) {
	ctx, tm, fake, qsc, _ := newBackupTest(t, topodatapb.TabletType_PRIMARY)
	handle := useFakeBackupStorage(t)
	servingErr := errors.New("injected serving type failure")
	mysqlctl.BackupRestoreEngineMap[primaryDrainEngine] = &duringBackupEngine{
		FakeBackupEngine: mysqlctl.BackupRestoreEngineMap[primaryDrainEngine].(*mysqlctl.FakeBackupEngine),
		during:           func() { qsc.SetServingTypeError = servingErr },
	}

	_, err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{AllowPrimary: true, BackupEngine: new(primaryDrainEngine)})
	require.ErrorContains(t, err, servingErr.Error())
	require.ErrorContains(t, err, "backup completed, but")
	assert.Len(t, handle.EndBackupCalls, 1)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	assert.True(t, fake.SemiSyncPrimaryEnabled)
	assert.True(t, tm.SemiSyncMonitor.IsOpen())
}

// delayedBackupSchemaDaemon answers schema reads after 300ms, longer than the
// shortened restore budget.
type delayedBackupSchemaDaemon struct {
	*mysqlctl.FakeMysqlDaemon
}

func (d *delayedBackupSchemaDaemon) GetSchema(ctx context.Context, dbName string, req *tabletmanagerdatapb.GetSchemaRequest) (*tabletmanagerdatapb.SchemaDefinition, error) {
	ready, cancel := context.WithTimeout(context.WithoutCancel(ctx), 300*time.Millisecond)
	defer cancel()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-ready.Done():
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return d.FakeMysqlDaemon.GetSchema(ctx, dbName, req)
}

// TestBackupRestoresDeniedTablesAfterSlowSchemaRead checks that a tablet with
// denied tables serves again with their rule after a backup, even if reading
// the schema takes longer than the restore budget. Entering BACKUP clears the
// rule.
func TestBackupRestoresDeniedTablesAfterSlowSchemaRead(t *testing.T) {
	for _, tabletType := range []topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA} {
		t.Run(tabletType.String(), func(t *testing.T) {
			useShortRemoteOperationTimeout(t)
			ctx, tm, fake, qsc, _ := newBackupTest(t, tabletType)
			handle := useFakeBackupStorage(t)
			fake.Schema = &tabletmanagerdatapb.SchemaDefinition{TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{Name: "customer"}}}
			tm.tmState.mu.Lock()
			tm.tmState.deniedTables[tabletType] = []string{"customer"}
			err := tm.tmState.applyDenyList(ctx)
			tm.tmState.mu.Unlock()
			require.NoError(t, err)
			require.NotNil(t, qsc.GetQueryRules(denyListQueryList).Find("denied_table"))
			mysqlctl.BackupRestoreEngineMap[primaryDrainEngine] = &duringBackupEngine{
				FakeBackupEngine: mysqlctl.BackupRestoreEngineMap[primaryDrainEngine].(*mysqlctl.FakeBackupEngine),
				during: func() {
					require.False(t, qsc.IsServing())
					require.Nil(t, qsc.GetQueryRules(denyListQueryList).Find("denied_table"), "BACKUP must clear the original type's rule")
					tm.MysqlDaemon = &delayedBackupSchemaDaemon{FakeMysqlDaemon: fake}
				},
			}

			_, err = tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{AllowPrimary: tabletType == topodatapb.TabletType_PRIMARY, BackupEngine: new(primaryDrainEngine)})
			require.NoError(t, err)
			assert.True(t, qsc.IsServing())
			assert.NotNil(t, qsc.GetQueryRules(denyListQueryList).Find("denied_table"), "serving without the routing fence")
			assert.Len(t, handle.EndBackupCalls, 1)
			ti, err := tm.TopoServer.GetTablet(ctx, tm.Tablet().Alias)
			require.NoError(t, err)
			assert.Equal(t, tabletType, ti.Type)
			require.NoError(t, tm.lock(ctx))
			tm.unlock()
		})
	}
}

// TestBackupWithoutSemiSyncDoesNotRepeatDisable checks that a primary whose
// durability policy doesn't require semi-sync doesn't turn it off again after a
// backup, so a failure there can't keep it in BACKUP.
func TestBackupWithoutSemiSyncDoesNotRepeatDisable(t *testing.T) {
	ctx, tm, fake, qsc, _ := newBackupTest(t, topodatapb.TabletType_PRIMARY)
	reparenttestutil.SetKeyspaceDurability(ctx, t, tm.TopoServer, "ks", policy.DurabilityNone)
	handle := useFakeBackupStorage(t)
	var disables int
	fake.SetSemiSyncEnabledFunc = func(primary, replica bool) error {
		if !primary && !replica {
			disables++
			if disables > 1 {
				return errors.New("redundant semi-sync disable failed")
			}
		}
		return nil
	}

	_, err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{AllowPrimary: true, BackupEngine: new(primaryDrainEngine)})
	require.NoError(t, err)
	assert.Equal(t, 1, disables)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	assert.True(t, qsc.IsServing())
	assert.False(t, fake.SemiSyncPrimaryEnabled)
	assert.Len(t, handle.EndBackupCalls, 1)
}

// backupPluginCheckDaemon returns a fixed result from the semi-sync plugin
// check.
type backupPluginCheckDaemon struct {
	*mysqlctl.FakeMysqlDaemon
	plugin mysql.SemiSyncType
	err    error
}

func (d *backupPluginCheckDaemon) SemiSyncExtensionLoaded(context.Context) (mysql.SemiSyncType, error) {
	return d.plugin, d.err
}

// TestBackupPrimaryWithoutReplicationConfiguration checks that a primary
// without replication configured, whose status read returns ErrNotReplica, is
// backed up and restored.
func TestBackupPrimaryWithoutReplicationConfiguration(t *testing.T) {
	ctx, tm, fake, qsc, _ := newBackupTest(t, topodatapb.TabletType_PRIMARY)
	fake.ReplicationStatusError = mysql.ErrNotReplica
	handle := useFakeBackupStorage(t)

	_, err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{AllowPrimary: true, BackupEngine: new(primaryDrainEngine)})
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, tm.Tablet().Type)
	ti, err := tm.TopoServer.GetTablet(ctx, tm.Tablet().Alias)
	require.NoError(t, err)
	assert.Equal(t, topodatapb.TabletType_PRIMARY, ti.Type)
	assert.True(t, qsc.IsServing())
	assert.True(t, fake.SemiSyncPrimaryEnabled)
	assert.Len(t, handle.EndBackupCalls, 1)
}

// TestBackupReturnsMysqlctlOutcome checks that the tablet manager hands back
// mysqlctl.Backup's outcome unchanged, since the gRPC server builds the terminal
// Backup message from it.
func TestBackupReturnsMysqlctlOutcome(t *testing.T) {
	const engineName = "test-backup-outcome"

	tcs := []struct {
		name     string
		result   mysqlctl.BackupResult
		manifest string
	}{
		{name: "usable", result: mysqlctl.BackupUsable, manifest: `{"BackupName":"x"}`},
		{name: "empty", result: mysqlctl.BackupEmpty},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			ts := memorytopo.NewServer(ctx, "cell1")
			t.Cleanup(ts.Close)
			tablet := newTestTablet(t, 100, "ks", "-", nil)
			require.NoError(t, ts.CreateTablet(ctx, tablet))

			fakedb := fakesqldb.New(t)
			t.Cleanup(fakedb.Close)
			tm := newTestReplicationTM(tablet, mysqlctl.NewFakeMysqlDaemon(fakedb), ts)
			tm.Cnf = &mysqlctl.Mycnf{}

			engine := &mysqlctl.FakeBackupEngine{
				ExecuteBackupReturn:   mysqlctl.FakeBackupEngineExecuteBackupReturn{Res: tc.result},
				ExecuteBackupManifest: tc.manifest,
			}
			storage := &mysqlctl.FakeBackupStorage{
				StartBackupReturn: mysqlctl.FakeBackupStorageStartBackupReturn{BackupHandle: &mysqlctl.FakeBackupHandle{}},
			}
			storage.WithParamsReturn = storage
			previousStorage := backupstorage.BackupStorageImplementation
			mysqlctl.BackupRestoreEngineMap[engineName] = engine
			backupstorage.BackupStorageMap[engineName] = storage
			backupstorage.BackupStorageImplementation = engineName
			t.Cleanup(func() {
				delete(mysqlctl.BackupRestoreEngineMap, engineName)
				delete(backupstorage.BackupStorageMap, engineName)
				backupstorage.BackupStorageImplementation = previousStorage
			})

			backupEngine := engineName
			outcome, err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{BackupEngine: &backupEngine})
			require.NoError(t, err)
			assert.Equal(t, tc.result, outcome.Result)
			assert.Equal(t, tc.manifest, outcome.Manifest)
			if tc.result == mysqlctl.BackupUsable {
				assert.Contains(t, outcome.Name, "cell1-0000000100")
			} else {
				assert.Empty(t, outcome.Name)
			}
			assert.Len(t, engine.ExecuteBackupCalls, 1)
			assert.False(t, tm.IsBackupRunning())
		})
	}
}
