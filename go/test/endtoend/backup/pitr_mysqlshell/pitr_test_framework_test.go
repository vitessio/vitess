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

package mysqlctld

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/vitesst"
	"vitess.io/vitess/go/vt/mysqlctl"
)

var (
	gracefulPostBackupDuration = 10 * time.Millisecond
	backupTimeoutDuration      = 3 * time.Minute
)

const (
	postWriteSleepDuration = 2 * time.Second // Nice for debugging purposes: clearly distinguishes the timestamps of certain operations, and as results the names/timestamps of backups.
)

type incrementalFromPosType int

const (
	incrementalFromPosPosition incrementalFromPosType = iota
	incrementalFromPosAuto
	incrementalFromPosBackupName
)

type PITRTestCase struct {
	Name           string
	SetupType      int
	ComprssDetails *CompressionDetails
}

type testedBackupTimestampInfo struct {
	rows          int
	postTimestamp time.Time
}

// waitForReplica waits for the replica to have same row set as on primary.
func waitForReplica(t *testing.T, replicaIndex int) int {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	pMsgs := ReadRowsFromPrimary(t)
	for {
		rMsgs := ReadRowsFromReplica(t, replicaIndex)
		if len(pMsgs) == len(rMsgs) {
			// success
			return len(pMsgs)
		}
		select {
		case <-ctx.Done():
			assert.FailNow(t, "timeout waiting for replica to catch up")
			return 0
		case <-ticker.C:
			//
		}
	}
}

// ExecTestIncrementalBackupAndRestoreToPos runs a series of backups: a full backup and multiple incremental backups.
// in between, it makes writes to the database, and takes notes: what data was available in what backup.
// It then restores each and every one of those backups, in random order, and expects to find the specific data associated with the backup.
func ExecTestIncrementalBackupAndRestoreToPos(t *testing.T, tcase *PITRTestCase) {
	t.Run(tcase.Name, func(t *testing.T) {
		// setup cluster for the testing
		LaunchCluster(t, tcase.ComprssDetails)

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

		var fullBackupPos replication.Position
		var lastBackupName string
		t.Run("full backup", func(t *testing.T) {
			InsertRowOnPrimary(t, "before-full-backup")
			waitForReplica(t, 0)

			manifest := replicaFullBackup(t, 0)
			fullBackupPos = manifest.Position
			require.False(t, fullBackupPos.IsZero())
			//
			msgs := ReadRowsFromReplica(t, 0)
			pos := replication.EncodePosition(fullBackupPos)
			backupPositions = append(backupPositions, pos)
			rowsPerPosition[pos] = len(msgs)

			lastBackupName = manifest.BackupName
		})

		lastBackupPos := fullBackupPos
		InsertRowOnPrimary(t, "before-incremental-backups")

		tt := []struct {
			name              string
			writeBeforeBackup bool
			fromFullPosition  bool
			expectEmpty       bool
			incrementalFrom   incrementalFromPosType
			expectError       string
		}{
			{
				name:            "first incremental backup",
				incrementalFrom: incrementalFromPosPosition,
			},
			{
				name:            "empty1",
				incrementalFrom: incrementalFromPosPosition,
				expectEmpty:     true,
			},
			{
				name:            "empty2",
				incrementalFrom: incrementalFromPosAuto,
				expectEmpty:     true,
			},
			{
				name:            "empty3",
				incrementalFrom: incrementalFromPosPosition,
				expectEmpty:     true,
			},
			{
				name:              "make writes, succeed",
				writeBeforeBackup: true,
				incrementalFrom:   incrementalFromPosPosition,
			},
			{
				name:            "empty again",
				incrementalFrom: incrementalFromPosPosition,
				expectEmpty:     true,
			},
			{
				name:              "make writes again, succeed",
				writeBeforeBackup: true,
				incrementalFrom:   incrementalFromPosBackupName,
			},
			{
				name:              "auto position, succeed",
				writeBeforeBackup: true,
				incrementalFrom:   incrementalFromPosAuto,
			},
			{
				name:            "empty again, based on auto position",
				incrementalFrom: incrementalFromPosAuto,
				expectEmpty:     true,
			},
			{
				name:              "auto position, make writes again, succeed",
				writeBeforeBackup: true,
				incrementalFrom:   incrementalFromPosAuto,
			},
			{
				name:             "from full backup position",
				fromFullPosition: true,
				incrementalFrom:  incrementalFromPosPosition,
			},
		}
		var fromFullPositionBackups []string
		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				if tc.writeBeforeBackup {
					InsertRowOnPrimary(t, "")
				}
				// we wait for >1 second because backups are written to a directory named after the current timestamp,
				// in 1 second resolution. We want to avoid two backups that have the same pathname. Realistically this
				// is only ever a problem in this end-to-end test, not in production.
				// Also, we give the replica a chance to catch up.
				time.Sleep(postWriteSleepDuration)
				// randomly flush binary logs 0, 1 or 2 times
				FlushBinaryLogsOnReplica(t, 0, rand.IntN(3))
				waitForReplica(t, 0)
				recordRowsPerPosition(t)
				// configure --incremental-from-pos to either:
				// - auto
				// - explicit last backup pos
				// - back in history to the original full backup
				var incrementalFromPos string
				switch tc.incrementalFrom {
				case incrementalFromPosAuto:
					incrementalFromPos = mysqlctl.AutoIncrementalFromPos
				case incrementalFromPosBackupName:
					incrementalFromPos = lastBackupName
				case incrementalFromPosPosition:
					incrementalFromPos = replication.EncodePosition(lastBackupPos)
					if tc.fromFullPosition {
						incrementalFromPos = replication.EncodePosition(fullBackupPos)
					}
					assert.Contains(t, incrementalFromPos, "MySQL56/")
				}
				incrementalFromPosArg := incrementalFromPos
				if tc.incrementalFrom == incrementalFromPosPosition && tc.fromFullPosition {
					// Verify that backup works whether or not the MySQL56/ prefix is present.
					// We arbitrarily decide to strip the prefix when "tc.fromFullPosition" is true, and keep it when false.
					incrementalFromPosArg = strings.Replace(incrementalFromPosArg, "MySQL56/", "", 1)
					assert.NotContains(t, incrementalFromPosArg, "MySQL56/")
				}
				// always use same 1st replica
				manifest, backupName := replicaIncrementalBackup(t, 0, incrementalFromPosArg, tc.expectEmpty, tc.expectError)
				if tc.expectError != "" {
					return
				}
				if tc.expectEmpty {
					assert.Nil(t, manifest)
					return
				}
				require.NotNil(t, manifest)
				defer func() {
					lastBackupPos = manifest.Position
					lastBackupName = manifest.BackupName
				}()
				if tc.fromFullPosition {
					fromFullPositionBackups = append(fromFullPositionBackups, backupName)
				}
				require.False(t, manifest.FromPosition.IsZero())
				require.NotEqual(t, manifest.Position, manifest.FromPosition)
				require.True(t, manifest.Position.GTIDSet.Union(manifest.PurgedPosition.GTIDSet).Contains(manifest.FromPosition.GTIDSet))

				gtidPurgedPos, err := replication.ParsePosition(replication.Mysql56FlavorID, GetReplicaGtidPurged(t, 0))
				require.NoError(t, err)
				fromPositionIncludingPurged := manifest.FromPosition.GTIDSet.Union(gtidPurgedPos.GTIDSet)

				expectFromPosition := lastBackupPos.GTIDSet
				if tc.incrementalFrom == incrementalFromPosPosition {
					pos, err := replication.DecodePosition(incrementalFromPos)
					assert.NoError(t, err)
					expectFromPosition = pos.GTIDSet.Union(gtidPurgedPos.GTIDSet)
				}
				require.Equalf(t, expectFromPosition, fromPositionIncludingPurged, "expected: %v, found: %v, gtid_purged: %v,  manifest.Position: %v", expectFromPosition, fromPositionIncludingPurged, gtidPurgedPos, manifest.Position)
			})
		}

		sampleTestedBackupPos := ""
		testRestores := func(t *testing.T) {
			for _, r := range rand.Perm(len(backupPositions)) {
				pos := backupPositions[r]
				testName := fmt.Sprintf("%s, %d records", pos, rowsPerPosition[pos])
				t.Run(testName, func(t *testing.T) {
					restoreToPos, err := replication.DecodePosition(pos)
					require.NoError(t, err)
					require.False(t, restoreToPos.IsZero())
					replicaRestoreToPos(t, 0, restoreToPos, "")
					msgs := ReadRowsFromReplica(t, 0)
					count, ok := rowsPerPosition[pos]
					require.True(t, ok)
					assert.Equalf(t, count, len(msgs), "messages: %v", msgs)
					if sampleTestedBackupPos == "" {
						sampleTestedBackupPos = pos
					}
					t.Run("post-pitr, wait for replica to catch up", func(t *testing.T) {
						// Replica is DRAINED and does not have replication configuration.
						// We now connect the replica to the primary and validate it's able to catch up.
						ReconnectReplicaToPrimary(t, 0)
						waitForReplica(t, 0)
					})
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
		// Test that we can create a new tablet with --restore-from-backup --restore-to-pos and that it bootstraps
		// via PITR and ends up in DRAINED type.
		t.Run("init tablet PITR", func(t *testing.T) {
			require.NotEmpty(t, sampleTestedBackupPos)

			var tablet *vitesst.Tablet
			var err error

			t.Run("init from backup pos "+sampleTestedBackupPos, func(t *testing.T) {
				tablet, err = SetupReplica3Tablet(t, []string{"--restore-to-pos", sampleTestedBackupPos})
				assert.NoError(t, err)
			})
			t.Run("wait for drained", func(t *testing.T) {
				err = tablet.WaitForTabletType(t.Context(), backupTimeoutDuration, "drained")
				assert.NoError(t, err)
			})
			t.Run(fmt.Sprintf("validate %d rows", rowsPerPosition[sampleTestedBackupPos]), func(t *testing.T) {
				require.NotZero(t, rowsPerPosition[sampleTestedBackupPos])
				msgs := ReadRowsFromReplica(t, 2)
				assert.Equal(t, rowsPerPosition[sampleTestedBackupPos], len(msgs))
			})
		})
	})
}

// ExecTestIncrementalBackupAndRestoreToPos
func ExecTestIncrementalBackupAndRestoreToTimestamp(t *testing.T, tcase *PITRTestCase) {
	var lastInsertedRowTimestamp time.Time
	insertRowOnPrimary := func(t *testing.T, hint string) {
		InsertRowOnPrimary(t, hint)
		lastInsertedRowTimestamp = time.Now()
	}

	t.Run(tcase.Name, func(t *testing.T) {
		// setup cluster for the testing
		LaunchCluster(t, &CompressionDetails{
			CompressorEngineName: "pgzip",
		})

		InitTestTable(t)

		testedBackups := []testedBackupTimestampInfo{}

		var fullBackupPos replication.Position
		var lastBackupName string
		t.Run("full backup", func(t *testing.T) {
			insertRowOnPrimary(t, "before-full-backup")
			waitForReplica(t, 0)

			manifest := replicaFullBackup(t, 0)
			fullBackupPos = manifest.Position
			require.False(t, fullBackupPos.IsZero())
			//
			rows := ReadRowsFromReplica(t, 0)
			testedBackups = append(testedBackups, testedBackupTimestampInfo{len(rows), time.Now()})

			lastBackupName = manifest.BackupName
		})

		lastBackupPos := fullBackupPos
		insertRowOnPrimary(t, "before-incremental-backups")

		tt := []struct {
			name              string
			writeBeforeBackup bool
			fromFullPosition  bool
			expectEmpty       bool
			incrementalFrom   incrementalFromPosType
			expectError       string
		}{
			{
				name:            "first incremental backup",
				incrementalFrom: incrementalFromPosPosition,
			},
			{
				name:            "empty1",
				incrementalFrom: incrementalFromPosPosition,
				expectEmpty:     true,
			},
			{
				name:            "empty2",
				incrementalFrom: incrementalFromPosAuto,
				expectEmpty:     true,
			},
			{
				name:            "empty3",
				incrementalFrom: incrementalFromPosPosition,
				expectEmpty:     true,
			},
			{
				name:              "make writes, succeed",
				writeBeforeBackup: true,
				incrementalFrom:   incrementalFromPosPosition,
			},
			{
				name:            "empty again",
				incrementalFrom: incrementalFromPosPosition,
				expectEmpty:     true,
			},
			{
				name:              "make writes again, succeed",
				writeBeforeBackup: true,
				incrementalFrom:   incrementalFromPosBackupName,
			},
			{
				name:              "auto position, succeed",
				writeBeforeBackup: true,
				incrementalFrom:   incrementalFromPosAuto,
			},
			{
				name:            "empty again, based on auto position",
				incrementalFrom: incrementalFromPosAuto,
				expectEmpty:     true,
			},
			{
				name:              "auto position, make writes again, succeed",
				writeBeforeBackup: true,
				incrementalFrom:   incrementalFromPosAuto,
			},
			{
				name:             "from full backup position",
				fromFullPosition: true,
				incrementalFrom:  incrementalFromPosPosition,
			},
		}
		var fromFullPositionBackups []string
		for _, tc := range tt {
			t.Run(tc.name, func(t *testing.T) {
				if tc.writeBeforeBackup {
					insertRowOnPrimary(t, "")
				}
				// we wait for >1 second because backups are written to a directory named after the current timestamp,
				// in 1 second resolution. We want to avoid two backups that have the same pathname. Realistically this
				// is only ever a problem in this end-to-end test, not in production.
				// Also, we give the replica a chance to catch up.
				time.Sleep(postWriteSleepDuration)
				waitForReplica(t, 0)
				rowsBeforeBackup := ReadRowsFromReplica(t, 0)
				// configure --incremental-from-pos to either:
				// - auto
				// - explicit last backup pos
				// - back in history to the original full backup
				var incrementalFromPos string
				switch tc.incrementalFrom {
				case incrementalFromPosAuto:
					incrementalFromPos = mysqlctl.AutoIncrementalFromPos
				case incrementalFromPosBackupName:
					incrementalFromPos = lastBackupName
				case incrementalFromPosPosition:
					incrementalFromPos = replication.EncodePosition(lastBackupPos)
					if tc.fromFullPosition {
						incrementalFromPos = replication.EncodePosition(fullBackupPos)
					}
				}
				manifest, backupName := replicaIncrementalBackup(t, 0, incrementalFromPos, tc.expectEmpty, tc.expectError)
				if tc.expectError != "" {
					return
				}
				if tc.expectEmpty {
					assert.Nil(t, manifest)
					return
				}
				require.NotNil(t, manifest)
				// We wish to mark the current post-backup timestamp. We will later on restore to this point in time.
				// However, the restore is up to and _exclusive_ of the timestamp. So for test's sake, we sleep
				// an extra few milliseconds just to ensure the timestamp we read is strictly after the backup time.
				// This is basicaly to avoid weird flakiness in CI.
				time.Sleep(gracefulPostBackupDuration)
				testedBackups = append(testedBackups, testedBackupTimestampInfo{len(rowsBeforeBackup), time.Now()})
				defer func() {
					lastBackupPos = manifest.Position
					lastBackupName = manifest.BackupName
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

				gtidPurgedPos, err := replication.ParsePosition(replication.Mysql56FlavorID, GetReplicaGtidPurged(t, 0))
				require.NoError(t, err)
				fromPositionIncludingPurged := manifest.FromPosition.GTIDSet.Union(gtidPurgedPos.GTIDSet)

				expectFromPosition := lastBackupPos.GTIDSet.Union(gtidPurgedPos.GTIDSet)
				if tc.incrementalFrom == incrementalFromPosPosition {
					pos, err := replication.DecodePosition(incrementalFromPos)
					assert.NoError(t, err)
					expectFromPosition = pos.GTIDSet.Union(gtidPurgedPos.GTIDSet)
				}
				require.Equalf(t, expectFromPosition, fromPositionIncludingPurged, "expected: %v, found: %v, gtid_purged: %v,  manifest.Position: %v", expectFromPosition, fromPositionIncludingPurged, gtidPurgedPos, manifest.Position)
			})
		}

		sampleTestedBackupIndex := -1
		testRestores := func(t *testing.T) {
			numFailedRestores := 0
			numSuccessfulRestores := 0
			for _, backupIndex := range rand.Perm(len(testedBackups)) {
				testedBackup := testedBackups[backupIndex]
				testName := fmt.Sprintf("backup num%v at %v, %v rows", backupIndex, mysqlctl.FormatRFC3339(testedBackup.postTimestamp), testedBackup.rows)
				t.Run(testName, func(t *testing.T) {
					expectError := ""
					if testedBackup.postTimestamp.After(lastInsertedRowTimestamp) {
						// The restore_to_timestamp value is beyond the last incremental
						// There is no path to restore to this timestamp.
						expectError = "no path found"
					}
					replicaRestoreToTimestamp(t, testedBackup.postTimestamp, expectError)
					if expectError == "" {
						msgs := ReadRowsFromReplica(t, 0)
						assert.Equalf(t, testedBackup.rows, len(msgs), "messages: %v", msgs)
						numSuccessfulRestores++
						if sampleTestedBackupIndex < 0 {
							sampleTestedBackupIndex = backupIndex
						}
						t.Run("post-pitr, wait for replica to catch up", func(t *testing.T) {
							// Replica is DRAINED and does not have replication configuration.
							// We now connect the replica to the primary and validate it's able to catch up.
							ReconnectReplicaToPrimary(t, 0)
							waitForReplica(t, 0)
						})
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
				RemoveBackup(t, backupName)
			}
		})
		t.Run("PITR-2", func(t *testing.T) {
			testRestores(t)
		})
		// Test that we can create a new tablet with --restore-from-backup --restore-to-timestamp and that it bootstraps
		// via PITR and ends up in DRAINED type.
		t.Run("init tablet PITR", func(t *testing.T) {
			require.GreaterOrEqual(t, sampleTestedBackupIndex, 0)
			sampleTestedBackup := testedBackups[sampleTestedBackupIndex]
			restoreToTimestampArg := mysqlctl.FormatRFC3339(sampleTestedBackup.postTimestamp)

			var tablet *vitesst.Tablet
			var err error

			t.Run(fmt.Sprintf("init from backup num %d", sampleTestedBackupIndex), func(t *testing.T) {
				tablet, err = SetupReplica3Tablet(t, []string{"--restore-to-timestamp", restoreToTimestampArg})
				assert.NoError(t, err)
			})
			t.Run("wait for drained", func(t *testing.T) {
				err = tablet.WaitForTabletType(t.Context(), backupTimeoutDuration, "drained")
				assert.NoError(t, err)
			})
			t.Run(fmt.Sprintf("validate %d rows", sampleTestedBackup.rows), func(t *testing.T) {
				require.NotZero(t, sampleTestedBackup.rows)
				msgs := ReadRowsFromReplica(t, 2)
				assert.Equal(t, sampleTestedBackup.rows, len(msgs))
			})
		})
	})
}
