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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
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

// Backup drains a tablet by changing its type, and that type change carries a semi-sync action.
// SemiSyncActionUnset reaches fixSemiSync, which writes rpl_semi_sync_replica_enabled and can then
// stop and restart replication. On an unmanaged tablet that is a write to a MySQL Vitess was told
// not to manage, and none of the 17 guarded RPCs covers this path.
//
// An --unmanaged tablet normally cannot reach here at all: verifyUnmanagedTabletConfig requires
// DB.HasGlobalSettings(), which makes initConfig skip my.cnf, leaving tm.Cnf nil so Backup refuses
// up front. That coupling is undocumented and breaks when `unmanaged: true` comes from
// --tablet-config alone, because config.Verify() runs before the YAML is unmarshalled over the
// config. The test sets Cnf directly to stand in for that state.
func TestBackupDrainLeavesAnUnmanagedMysqlAlone(t *testing.T) {
	const engineName = "fake-unmanaged-drain"

	tests := []struct {
		name              string
		mode              topodatapb.TabletMySQLMode
		wantSemiSyncTouch bool
	}{
		{
			name:              "unmanaged tablet drains without touching semi-sync",
			mode:              topodatapb.TabletMySQLMode_UNMANAGED,
			wantSemiSyncTouch: false,
		}, {
			// Control: the same drain on a managed tablet does reach MySQL.
			name:              "managed tablet still fixes semi-sync on drain",
			mode:              topodatapb.TabletMySQLMode_MANAGED,
			wantSemiSyncTouch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()

			mysqlctl.BackupRestoreEngineMap[engineName] = &mysqlctl.FakeBackupEngine{
				ShouldDrainForBackupReturn: true,
			}
			t.Cleanup(func() { delete(mysqlctl.BackupRestoreEngineMap, engineName) })

			ts := memorytopo.NewServer(ctx, "cell1")
			t.Cleanup(ts.Close)

			tm := newTestTM(t, ts, 1, "ks", "0", nil)
			t.Cleanup(tm.Stop)

			fakeDb, ok := tm.MysqlDaemon.(*mysqlctl.FakeMysqlDaemon)
			require.True(t, ok)
			fakeDb.SemiSyncPrimaryEnabled = true
			fakeDb.SemiSyncReplicaEnabled = true

			tm.mysqlMode = tt.mode
			// Stands in for the --tablet-config path that leaves Cnf set on an unmanaged tablet.
			tm.Cnf = &mysqlctl.Mycnf{DataDir: t.TempDir()}

			// The backup itself fails: no storage is configured. The drain phase runs first,
			// which is the part under test.
			engine := engineName
			err := tm.Backup(ctx, logutil.NewMemoryLogger(), &tabletmanagerdatapb.BackupRequest{
				BackupEngine: &engine,
			})
			require.Error(t, err, "the backup is expected to fail at storage, after the drain")

			semiSyncTouched := !fakeDb.SemiSyncPrimaryEnabled || !fakeDb.SemiSyncReplicaEnabled
			assert.Equal(t, tt.wantSemiSyncTouch, semiSyncTouched,
				"semi-sync state on the external MySQL")
		})
	}
}
