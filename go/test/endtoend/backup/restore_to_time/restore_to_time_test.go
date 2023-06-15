/*
Copyright 2022 The Vitess Authors.

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
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	backup "vitess.io/vitess/go/test/endtoend/backup/vtctlbackup"
	"vitess.io/vitess/go/test/endtoend/cluster"
	"vitess.io/vitess/go/vt/mysqlctl"
)

var (
	minimalSleepDuration       = time.Second + 100*time.Millisecond
	gracefulPostBackupDuration = 10 * time.Millisecond
)

type testedBackupInfo struct {
	rows          int
	postTimestamp time.Time
}

func waitForReplica(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pMsgs := backup.ReadRowsFromPrimary(t)
	for {
		rMsgs := backup.ReadRowsFromReplica(t)
		if len(pMsgs) == len(rMsgs) {
			// success
			return
		}
		select {
		case <-ctx.Done():
			assert.FailNow(t, "timeout waiting for replica to catch up")
			return
		case <-time.After(time.Second):
			//
		}
	}
}

// TestIncrementalBackupMysqlctld - tests incremental backups using myslctld
func TestIncrementalBackup(t *testing.T) {
	defer cluster.PanicHandler(t)

	var lastInsertedRowTimestamp time.Time
	insertRowOnPrimary := func(t *testing.T, hint string) {
		backup.InsertRowOnPrimary(t, hint)
		lastInsertedRowTimestamp = time.Now()
	}

	tcases := []struct {
		name      string
		setupType int
	}{
		{
			"BuiltinBackup", backup.BuiltinBackup,
		},
		// {
		// 	"XtraBackup", backup.XtraBackup,
		// },
		// {
		// 	"Mysqlctld", backup.Mysqlctld,
		// },
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			// setup cluster for the testing
			code, err := backup.LaunchCluster(tcase.setupType, "xbstream", 0, &backup.CompressionDetails{
				CompressorEngineName: "pgzip",
			})
			require.NoError(t, err, "setup failed with status code %d", code)
			defer backup.TearDownCluster()

			backup.InitTestTable(t)

			testedBackups := []testedBackupInfo{}

			var fullBackupPos mysql.Position
			t.Run("full backup", func(t *testing.T) {
				insertRowOnPrimary(t, "before-full-backup")
				waitForReplica(t)

				manifest, _ := backup.TestReplicaFullBackup(t)
				fullBackupPos = manifest.Position
				require.False(t, fullBackupPos.IsZero())
				//
				rows := backup.ReadRowsFromReplica(t)
				testedBackups = append(testedBackups, testedBackupInfo{len(rows), time.Now()})
			})

			lastBackupPos := fullBackupPos
			insertRowOnPrimary(t, "before-incremental-backups")

			tt := []struct {
				name              string
				writeBeforeBackup bool
				fromFullPosition  bool
				autoPosition      bool
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
					name:              "auto position, succeed",
					writeBeforeBackup: true,
					autoPosition:      true,
				},
				{
					name:         "fail auto position, no binary logs to backup",
					autoPosition: true,
					expectError:  "no binary logs to backup",
				},
				{
					name:              "auto position, make writes again, succeed",
					writeBeforeBackup: true,
					autoPosition:      true,
				},
				{
					name:             "from full backup position",
					fromFullPosition: true,
				},
			}
			var fromFullPositionBackups []string
			for _, tc := range tt {
				t.Run(tc.name, func(t *testing.T) {
					if tc.writeBeforeBackup {
						insertRowOnPrimary(t, "")
					}
					// we wait for 1 second because backups are written to a directory named after the current timestamp,
					// in 1 second resolution. We want to avoid two backups that have the same pathname. Realistically this
					// is only ever a problem in this end-to-end test, not in production.
					// Also, we gie the replica a chance to catch up.
					time.Sleep(minimalSleepDuration)
					waitForReplica(t)
					rowsBeforeBackup := backup.ReadRowsFromReplica(t)
					// configure --incremental-from-pos to either:
					// - auto
					// - explicit last backup pos
					// - back in history to the original full backup
					var incrementalFromPos mysql.Position
					if !tc.autoPosition {
						incrementalFromPos = lastBackupPos
						if tc.fromFullPosition {
							incrementalFromPos = fullBackupPos
						}
					}
					manifest, backupName := backup.TestReplicaIncrementalBackup(t, incrementalFromPos, tc.expectError)
					if tc.expectError != "" {
						return
					}
					// We wish to mark the current post-backup timestamp. We will later on retore to this point in time.
					// However, the restore is up to and _exclusive_ of the timestamp. So for test's sake, we sleep
					// an extra few milliseconds just to ensure the timestamp we read is strictly after the backup time.
					// This is basicaly to avoid weird flakiness in CI.
					time.Sleep(gracefulPostBackupDuration)
					testedBackups = append(testedBackups, testedBackupInfo{len(rowsBeforeBackup), time.Now()})
					defer func() {
						lastBackupPos = manifest.Position
					}()
					if tc.fromFullPosition {
						fromFullPositionBackups = append(fromFullPositionBackups, backupName)
					}
					require.False(t, manifest.FromPosition.IsZero())
					require.NotEqual(t, manifest.Position, manifest.FromPosition)
					require.True(t, manifest.Position.GTIDSet.Union(manifest.PurgedPosition.GTIDSet).Contains(manifest.FromPosition.GTIDSet))
					{
						incrDetails := manifest.IncrementalDetails
						require.NotNil(t, incrDetails)
						require.NotEmpty(t, incrDetails.FirstTimestamp)
						require.NotEmpty(t, incrDetails.FirstTimestampBinlog)
						require.NotEmpty(t, incrDetails.LastTimestamp)
						require.NotEmpty(t, incrDetails.LastTimestampBinlog)
						require.GreaterOrEqual(t, incrDetails.LastTimestamp, incrDetails.FirstTimestamp)

						if tc.fromFullPosition {
							require.Greater(t, incrDetails.LastTimestampBinlog, incrDetails.FirstTimestampBinlog)
						} else {
							// No binlog rotation
							require.Equal(t, incrDetails.LastTimestampBinlog, incrDetails.FirstTimestampBinlog)
						}
					}

					gtidPurgedPos, err := mysql.ParsePosition(mysql.Mysql56FlavorID, backup.GetReplicaGtidPurged(t))
					require.NoError(t, err)
					fromPositionIncludingPurged := manifest.FromPosition.GTIDSet.Union(gtidPurgedPos.GTIDSet)

					expectFromPosition := lastBackupPos.GTIDSet.Union(gtidPurgedPos.GTIDSet)
					if !incrementalFromPos.IsZero() {
						expectFromPosition = incrementalFromPos.GTIDSet.Union(gtidPurgedPos.GTIDSet)
					}
					require.Equalf(t, expectFromPosition, fromPositionIncludingPurged, "expected: %v, found: %v, gtid_purged: %v,  manifest.Position: %v", expectFromPosition, fromPositionIncludingPurged, gtidPurgedPos, manifest.Position)
					backupIndex := len(testedBackups) - 1
					t.Logf("tested backup num%v: %v rows before %v", backupIndex, testedBackups[backupIndex].rows, mysqlctl.FormatRFC3339(testedBackups[backupIndex].postTimestamp))
				})
			}

			testRestores := func(t *testing.T) {
				numFailedRestores := 0
				numSuccessfulRestores := 0
				for _, backupIndex := range rand.Perm(len(testedBackups)) {
					testedBackup := testedBackups[backupIndex]
					testName := fmt.Sprintf("backup num%v at %v, %v rows", backupIndex, mysqlctl.FormatRFC3339(testedBackup.postTimestamp), testedBackup.rows)
					t.Run(testName, func(t *testing.T) {
						expectError := ""
						if testedBackup.postTimestamp.After(lastInsertedRowTimestamp) {
							// The restore_to_timestamp value is beyond the last incremental backup.
							// There is no path to restore to this timestamp.
							expectError = "no path found"
							t.Logf("expecting error")
						}
						backup.TestReplicaRestoreToTimestamp(t, testedBackup.postTimestamp, expectError)
						if expectError == "" {
							msgs := backup.ReadRowsFromReplica(t)
							assert.Equalf(t, testedBackup.rows, len(msgs), "messages: %v", msgs)
							numSuccessfulRestores++
						} else {
							numFailedRestores++
						}
					})
				}
				// Integrity check for the test itself: ensure we have both successful and failed restores.
				require.NotZero(t, numFailedRestores)
				require.NotZero(t, numSuccessfulRestores)
			}
			t.Run("PITR", func(t *testing.T) {
				testRestores(t)
			})
			t.Run("remove full position backups", func(t *testing.T) {
				// Delete the fromFullPosition backup(s), which leaves us with less restore options. Try again.
				for _, backupName := range fromFullPositionBackups {
					backup.RemoveBackup(t, backupName)
				}
			})
			t.Run("PITR-2", func(t *testing.T) {
				testRestores(t)
			})
		})
	}
}
