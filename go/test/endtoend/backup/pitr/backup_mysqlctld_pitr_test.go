/*
Copyright 2019 The Vitess Authors.

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

package mysqlctld

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	backup "vitess.io/vitess/go/test/endtoend/backup/vtctlbackup"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

// TestIncrementalBackupMysqlctld - tests incremental backups using myslctld
func TestIncrementalBackupMysqlctld(t *testing.T) {
	defer cluster.PanicHandler(t)
	// setup cluster for the testing
	code, err := backup.LaunchCluster(backup.Mysqlctld, "xbstream", 0, nil)
	require.NoError(t, err, "setup failed with status code %d", code)
	defer backup.TearDownCluster()

	var fullBackupPos mysql.Position
	t.Run("full backup", func(t *testing.T) {
		manifest := backup.TestReplicaFullBackup(t)
		fullBackupPos = manifest.Position
		require.False(t, fullBackupPos.IsZero())
	})
	lastBackupPos := fullBackupPos

	// backup.ReadGTIDExecuted(t)
	tt := []struct {
		name              string
		writeBeforeBackup bool
		fromFullPosition  bool
		expectError       string
	}{
		{
			name: "first incremental backup",
		},
		{
			name:              "make writes, succeed",
			writeBeforeBackup: true,
		},
		{
			name:        "fail, no binary logs to backup",
			expectError: "no binary logs to backup",
		},
		{
			name:              "make writes again, succeed",
			writeBeforeBackup: true,
		},
		{
			name:             "from full backup position",
			fromFullPosition: true,
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			if tc.writeBeforeBackup {
				backup.InsertRowOnPrimary(t, "")
			}
			// we wait for 1 second because backups ar ewritten to a directory named after the current timestamp,
			// in 1 second resolution. We want to aoid two backups that have the same pathname. Realistically this
			// is only ever a problem in this endtoend test, not in production.
			time.Sleep(time.Second)
			incrementalFromPos := lastBackupPos
			if tc.fromFullPosition {
				incrementalFromPos = fullBackupPos
			}
			manifest := backup.TestReplicaIncrementalBackup(t, incrementalFromPos, tc.expectError)
			if tc.expectError != "" {
				return
			}
			require.False(t, manifest.FromPosition.IsZero())

			require.NotEqual(t, manifest.Position, manifest.FromPosition)
			require.True(t, manifest.Position.GTIDSet.Contains(manifest.FromPosition.GTIDSet))
			lastBackupPos = manifest.Position
		})
	}
}
