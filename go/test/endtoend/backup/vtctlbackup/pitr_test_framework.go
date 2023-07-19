/*
Copyright 2023 The Vitess Authors.

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

package vtctlbackup

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/test/endtoend/cluster"
)

const (
	postWriteSleepDuration = 2*time.Second + 100*time.Millisecond
)

const (
	operationFullBackup = iota
	operationIncrementalBackup
	operationRestore
)

type PITRTestCase struct {
	Name           string
	SetupType      int
	ComprssDetails *CompressionDetails
}

func waitForReplica(t *testing.T, replicaIndex int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pMsgs := ReadRowsFromPrimary(t)
	for {
		rMsgs := ReadRowsFromReplica(t, replicaIndex)
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

// ExecTestIncrementalBackupAndRestoreToPos runs a series of backups: a full backup and multiple incremental backups.
// in between, it makes writes to the database, and takes notes: what data was available in what backup.
// It then restores each and every one of those backups, in random order, and expects to find the specific data associated with the backup.
func ExecTestIncrementalBackupAndRestoreToPos(t *testing.T, tcase *PITRTestCase) {
	defer cluster.PanicHandler(t)

	t.Run(tcase.Name, func(t *testing.T) {
		// setup cluster for the testing
		code, err := LaunchCluster(tcase.SetupType, "xbstream", 0, tcase.ComprssDetails)
		require.NoError(t, err, "setup failed with status code %d", code)
		defer TearDownCluster()

		InitTestTable(t)

		rowsPerPosition := map[string]int{}
		backupPositions := []string{}

		recordRowsPerPosition := func(t *testing.T) {
			pos := GetReplicaPosition(t, 0)
			msgs := ReadRowsFromReplica(t, 0)
			if _, ok := rowsPerPosition[pos]; !ok {
				backupPositions = append(backupPositions, pos)
				rowsPerPosition[pos] = len(msgs)
			}
		}

		var fullBackupPos mysql.Position
		t.Run("full backup", func(t *testing.T) {
			InsertRowOnPrimary(t, "before-full-backup")
			waitForReplica(t, 0)

			manifest := TestReplicaFullBackup(t, 0)
			fullBackupPos = manifest.Position
			require.False(t, fullBackupPos.IsZero())
			//
			msgs := ReadRowsFromReplica(t, 0)
			pos := mysql.EncodePosition(fullBackupPos)
			backupPositions = append(backupPositions, pos)
			rowsPerPosition[pos] = len(msgs)
		})

		lastBackupPos := fullBackupPos
		InsertRowOnPrimary(t, "before-incremental-backups")

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
					InsertRowOnPrimary(t, "")
				}
				// we wait for 1 second because backups are written to a directory named after the current timestamp,
				// in 1 second resolution. We want to avoid two backups that have the same pathname. Realistically this
				// is only ever a problem in this end-to-end test, not in production.
				// Also, we gie the replica a chance to catch up.
				time.Sleep(postWriteSleepDuration)
				waitForReplica(t, 0)
				recordRowsPerPosition(t)
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
				// always use same 1st replica
				manifest, backupName := TestReplicaIncrementalBackup(t, 0, incrementalFromPos, tc.expectError)
				if tc.expectError != "" {
					return
				}
				defer func() {
					lastBackupPos = manifest.Position
				}()
				if tc.fromFullPosition {
					fromFullPositionBackups = append(fromFullPositionBackups, backupName)
				}
				require.False(t, manifest.FromPosition.IsZero())
				require.NotEqual(t, manifest.Position, manifest.FromPosition)
				require.True(t, manifest.Position.GTIDSet.Union(manifest.PurgedPosition.GTIDSet).Contains(manifest.FromPosition.GTIDSet))

				gtidPurgedPos, err := mysql.ParsePosition(mysql.Mysql56FlavorID, GetReplicaGtidPurged(t, 0))
				require.NoError(t, err)
				fromPositionIncludingPurged := manifest.FromPosition.GTIDSet.Union(gtidPurgedPos.GTIDSet)

				expectFromPosition := lastBackupPos.GTIDSet
				if !incrementalFromPos.IsZero() {
					expectFromPosition = incrementalFromPos.GTIDSet.Union(gtidPurgedPos.GTIDSet)
				}
				require.Equalf(t, expectFromPosition, fromPositionIncludingPurged, "expected: %v, found: %v, gtid_purged: %v,  manifest.Position: %v", expectFromPosition, fromPositionIncludingPurged, gtidPurgedPos, manifest.Position)
			})
		}

		testRestores := func(t *testing.T) {
			for _, r := range rand.Perm(len(backupPositions)) {
				pos := backupPositions[r]
				testName := fmt.Sprintf("%s, %d records", pos, rowsPerPosition[pos])
				t.Run(testName, func(t *testing.T) {
					restoreToPos, err := mysql.DecodePosition(pos)
					require.NoError(t, err)
					TestReplicaRestoreToPos(t, 0, restoreToPos, "")
					msgs := ReadRowsFromReplica(t, 0)
					count, ok := rowsPerPosition[pos]
					require.True(t, ok)
					assert.Equalf(t, count, len(msgs), "messages: %v", msgs)
				})
			}
		}
		t.Run("PITR", func(t *testing.T) {
			testRestores(t)
		})
		t.Run("remove full position backups", func(t *testing.T) {
			// Delete the fromFullPosition backup(s), which leaves us with less restore options. Try again.
			for _, backupName := range fromFullPositionBackups {
				RemoveBackup(t, backupName)
			}
		})
		t.Run("PITR-2", func(t *testing.T) {
			testRestores(t)
		})
	})
}

// ExecTestIncrementalBackupAndRestoreToPos runs a series of interleaved backups on two different replicas: full and incremental.
// Specifically, it's designed to test how incremental backups are taken by interleaved replicas, so that they successfully build on
// one another.
func ExecTestIncrementalBackupOnTwoTablets(t *testing.T, tcase *PITRTestCase) {
	defer cluster.PanicHandler(t)

	t.Run(tcase.Name, func(t *testing.T) {
		// setup cluster for the testing
		code, err := LaunchCluster(tcase.SetupType, "xbstream", 0, tcase.ComprssDetails)
		require.NoError(t, err, "setup failed with status code %d", code)
		defer TearDownCluster()

		InitTestTable(t)

		rowsPerPosition := map[string]int{}

		recordRowsPerPosition := func(t *testing.T, replicaIndex int) {
			pos := GetReplicaPosition(t, replicaIndex)
			msgs := ReadRowsFromReplica(t, replicaIndex)
			if _, ok := rowsPerPosition[pos]; !ok {
				rowsPerPosition[pos] = len(msgs)
			}
		}

		var lastBackupPos mysql.Position
		InsertRowOnPrimary(t, "before-incremental-backups")
		waitForReplica(t, 0)
		waitForReplica(t, 1)

		tt := []struct {
			name          string
			operationType int
			replicaIndex  int
			expectError   string
		}{
			{
				name:          "full1",
				operationType: operationFullBackup,
			},
			{
				name:          "incremental1",
				operationType: operationIncrementalBackup,
			},
			{
				name:          "restore1",
				operationType: operationRestore,
			},
			{
				name:          "incremental2",
				operationType: operationIncrementalBackup,
				replicaIndex:  1,
			},
			{
				name:          "full2",
				operationType: operationFullBackup,
				replicaIndex:  1,
			},
			{
				name:          "incremental2-after-full2",
				operationType: operationIncrementalBackup,
				replicaIndex:  1,
			},
			{
				name:          "restore2",
				operationType: operationRestore,
				replicaIndex:  1,
			},
			{
				name:          "incremental-replica1",
				operationType: operationIncrementalBackup,
			},
			{
				name:          "incremental-replica2",
				operationType: operationIncrementalBackup,
				replicaIndex:  1,
			},
			{
				name:          "incremental-replica1",
				operationType: operationIncrementalBackup,
			},
			{
				name:          "incremental-replica2",
				operationType: operationIncrementalBackup,
				replicaIndex:  1,
			},
		}
		insertRowAndWait := func(t *testing.T, replicaIndex int, data string) {
			t.Run("insert row and wait", func(t *testing.T) {
				InsertRowOnPrimary(t, data)
				time.Sleep(postWriteSleepDuration)
				waitForReplica(t, replicaIndex)
				recordRowsPerPosition(t, replicaIndex)
			})
		}
		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				insertRowAndWait(t, tc.replicaIndex, tc.name)
				t.Run("running operation", func(t *testing.T) {
					switch tc.operationType {
					case operationFullBackup:
						manifest := TestReplicaFullBackup(t, tc.replicaIndex)
						fullBackupPos := manifest.Position
						require.False(t, fullBackupPos.IsZero())
						//
						msgs := ReadRowsFromReplica(t, tc.replicaIndex)
						pos := mysql.EncodePosition(fullBackupPos)
						rowsPerPosition[pos] = len(msgs)

						lastBackupPos = fullBackupPos
					case operationIncrementalBackup:
						var incrementalFromPos mysql.Position // keep zero, we will use "auto"
						manifest, _ := TestReplicaIncrementalBackup(t, tc.replicaIndex, incrementalFromPos, tc.expectError)
						if tc.expectError != "" {
							return
						}
						defer func() {
							lastBackupPos = manifest.Position
						}()
						require.False(t, manifest.FromPosition.IsZero())
						require.NotEqual(t, manifest.Position, manifest.FromPosition)
						require.True(t, manifest.Position.GTIDSet.Union(manifest.PurgedPosition.GTIDSet).Contains(manifest.FromPosition.GTIDSet))

						gtidPurgedPos, err := mysql.ParsePosition(mysql.Mysql56FlavorID, GetReplicaGtidPurged(t, tc.replicaIndex))
						require.NoError(t, err)
						fromPositionIncludingPurged := manifest.FromPosition.GTIDSet.Union(gtidPurgedPos.GTIDSet)

						require.True(t, lastBackupPos.GTIDSet.Contains(fromPositionIncludingPurged), "expected: %v to contain %v", lastBackupPos.GTIDSet, fromPositionIncludingPurged)
					case operationRestore:
						TestReplicaFullRestore(t, tc.replicaIndex, "")
						// should return into replication stream
						insertRowAndWait(t, tc.replicaIndex, "post-restore-check")
					default:
						require.FailNowf(t, "unknown operation type", "operation: %d", tc.operationType)
					}
				})
			})
		}
	})
}
