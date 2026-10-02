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

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/proto/vttime"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
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
