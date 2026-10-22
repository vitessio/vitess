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
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo/memorytopo"
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
