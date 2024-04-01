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

package logic

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/external/golib/sqlutils"
	"vitess.io/vitess/go/vt/vtorc/config"
	"vitess.io/vitess/go/vt/vtorc/db"
	"vitess.io/vitess/go/vt/vtorc/inst"
)

// TestTopologyRecovery tests various operations related to topology recovery like reading from and writing it to the database.
func TestTopologyRecovery(t *testing.T) {
	// Open the vtorc
	// After the test completes delete everything from the vitess_tablet table
	orcDb, err := db.OpenVTOrc()
	require.NoError(t, err)
	defer func() {
		_, err = orcDb.Exec("delete from topology_recovery")
		require.NoError(t, err)
	}()

	replicationAnalysis := inst.ReplicationAnalysis{
		AnalyzedInstanceAlias: "zone1-0000000101",
		TabletType:            tab101.Type,
		ClusterDetails: inst.ClusterInfo{
			Keyspace: keyspace,
			Shard:    shard,
		},
		AnalyzedKeyspace: keyspace,
		AnalyzedShard:    shard,
		Analysis:         inst.ReplicaIsWritable,
		IsReadOnly:       false,
	}
	topologyRecovery := NewTopologyRecovery(replicationAnalysis)

	t.Run("writing to topology recovery", func(t *testing.T) {
		topologyRecovery, err = writeTopologyRecovery(topologyRecovery)
		require.NoError(t, err)
		// The ID field should be populated after the insert
		require.Greater(t, topologyRecovery.ID, int64(0))
	})

	t.Run("read recoveries", func(t *testing.T) {
		recoveries, err := ReadRecentRecoveries(0)
		require.NoError(t, err)
		require.Len(t, recoveries, 1)
		// Assert that the ID field matches the one that we just wrote
		require.EqualValues(t, topologyRecovery.ID, recoveries[0].ID)
	})
}

func TestExpireTableData(t *testing.T) {
	oldVal := config.Config.AuditPurgeDays
	config.Config.AuditPurgeDays = 10
	defer func() {
		config.Config.AuditPurgeDays = oldVal
	}()

	tests := []struct {
		name             string
		tableName        string
		insertQuery      string
		expectedRowCount int
		expireFunc       func() error
	}{
		{
			name:             "ExpireRecoveryDetectionHistory",
			tableName:        "recovery_detection",
			expectedRowCount: 2,
			insertQuery: `insert into recovery_detection (detection_id, detection_timestamp, alias, analysis, keyspace, shard) values
(1, NOW() - INTERVAL 3 DAY,'a','a','a','a'),
(2, NOW() - INTERVAL 5 DAY,'a','a','a','a'),
(3, NOW() - INTERVAL 15 DAY,'a','a','a','a')`,
			expireFunc: ExpireRecoveryDetectionHistory,
		},
		{
			name:             "ExpireTopologyRecoveryHistory",
			tableName:        "topology_recovery",
			expectedRowCount: 1,
			insertQuery: `insert into topology_recovery (recovery_id, start_recovery, alias, analysis, keyspace, shard) values
(1, NOW() - INTERVAL 13 DAY,'a','a','a','a'),
(2, NOW() - INTERVAL 5 DAY,'a','a','a','a'),
(3, NOW() - INTERVAL 15 DAY,'a','a','a','a')`,
			expireFunc: ExpireTopologyRecoveryHistory,
		},
		{
			name:             "ExpireTopologyRecoveryStepsHistory",
			tableName:        "topology_recovery_steps",
			expectedRowCount: 1,
			insertQuery: `insert into topology_recovery_steps (recovery_step_id, audit_at, recovery_id, message) values
(1, NOW() - INTERVAL 13 DAY, 1, 'a'),
(2, NOW() - INTERVAL 5 DAY, 2, 'a'),
(3, NOW() - INTERVAL 15 DAY, 3, 'a')`,
			expireFunc: ExpireTopologyRecoveryStepsHistory,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
			defer func() {
				db.ClearVTOrcDatabase()
			}()
			_, err := db.ExecVTOrc(tt.insertQuery)
			require.NoError(t, err)

			err = tt.expireFunc()
			require.NoError(t, err)

			rowsCount := 0
			err = db.QueryVTOrc(`select * from `+tt.tableName, nil, func(rowMap sqlutils.RowMap) error {
				rowsCount++
				return nil
			})
			require.NoError(t, err)
			require.EqualValues(t, tt.expectedRowCount, rowsCount)
		})
	}
}

func TestInsertRecoveryDetection(t *testing.T) {
	// Clear the database after the test. The easiest way to do that is to run all the initialization commands again.
	defer func() {
		db.ClearVTOrcDatabase()
	}()
	ra := &inst.ReplicationAnalysis{
		AnalyzedInstanceAlias: "alias-1",
		Analysis:              inst.ClusterHasNoPrimary,
		ClusterDetails: inst.ClusterInfo{
			Keyspace: keyspace,
			Shard:    shard,
		},
	}
	err := InsertRecoveryDetection(ra)
	require.NoError(t, err)
	require.NotEqual(t, 0, ra.RecoveryId)

	var rows []map[string]sqlutils.CellData
	err = db.QueryVTOrc("select * from recovery_detection", nil, func(rowMap sqlutils.RowMap) error {
		rows = append(rows, rowMap)
		return nil
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.EqualValues(t, ra.AnalyzedInstanceAlias, rows[0]["alias"].String)
	require.EqualValues(t, ra.Analysis, rows[0]["analysis"].String)
	require.EqualValues(t, keyspace, rows[0]["keyspace"].String)
	require.EqualValues(t, shard, rows[0]["shard"].String)
	require.EqualValues(t, strconv.Itoa(int(ra.RecoveryId)), rows[0]["detection_id"].String)
	require.NotEqual(t, "", rows[0]["detection_timestamp"].String)
}
