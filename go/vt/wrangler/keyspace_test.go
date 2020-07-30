/*
Copyright 2020 The Vitess Authors.

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

package wrangler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestShardVInfo_String(t *testing.T) {
	got := &ShardVInfo{
		ShardName: "testShardName",
		VReplRows: []*VReplRow{{
			ID:                   1,
			Workflow:             "testWorkflow",
			Source:               "testSource",
			Pos:                  "testPos",
			StopPos:              "testStopPos",
			MaxTps:               50,
			MaxReplicationLag:    50,
			Cell:                 "testCell",
			TabletTypes:          "testTabletTypes",
			TimeUpdated:          50,
			TransactionTimestamp: 50,
			State:                "Running",
			Message:              "this is a test message",
			DbName:               "testDbName",
		}},
		SourceShards:    nil,
		TabletControls:  nil,
		MasterIsServing: true,
	}
	want := "    Shard: testShardName\n      VReplication:\n        1 testWorkflow testSource testPos testStopPos 50 50 testCell testTabletTypes 50 50 Running this is a test message testDbName\n      Master Is Serving: true\n"
	require.Equal(t, got.String(), want)
}

func TestParseVReplQuery(t *testing.T) {
	id, _ := sqltypes.NewIntegral("1")
	workflow := sqltypes.NewVarBinary("testWorkflow")
	source := sqltypes.NewVarBinary("testSource")
	pos := sqltypes.NewVarBinary("testPos")
	stopPos := sqltypes.NewVarBinary("testStopPos")
	maxTps, _ := sqltypes.NewIntegral("50")
	maxReplicationLag, _ := sqltypes.NewIntegral("50")
	cell := sqltypes.NewVarBinary("testCell")
	tabletTypes := sqltypes.NewVarBinary("testTabletTypes")
	timeUpdated, _ := sqltypes.NewIntegral("50")
	transactionTimestamp, _ := sqltypes.NewIntegral("50")
	state := sqltypes.NewVarBinary("testState")
	message := sqltypes.NewVarBinary("testMessage")
	dbName := sqltypes.NewVarBinary("testDbName")

	query := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id"},
			{Name: "workflow"},
			{Name: "source"},
			{Name: "pos"},
			{Name: "stop_pos"},
			{Name: "max_tps"},
			{Name: "max_replication_lag"},
			{Name: "cell"},
			{Name: "tablet_types"},
			{Name: "time_updated"},
			{Name: "transaction_timestamp"},
			{Name: "state"},
			{Name: "message"},
			{Name: "db_name"},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{{
			id,
			workflow,
			source,
			pos,
			stopPos,
			maxTps,
			maxReplicationLag,
			cell,
			tabletTypes,
			timeUpdated,
			transactionTimestamp,
			state,
			message,
			dbName,
		}},
	}

	got, err := parseVReplQuery(query)
	require.NoError(t, err)

	want := []*VReplRow{{
		ID:                   1,
		Workflow:             "testWorkflow",
		Source:               "testSource",
		Pos:                  "testPos",
		StopPos:              "testStopPos",
		MaxTps:               50,
		MaxReplicationLag:    50,
		Cell:                 "testCell",
		TabletTypes:          "testTabletTypes",
		TimeUpdated:          50,
		TransactionTimestamp: 50,
		State:                "testState",
		Message:              "testMessage",
		DbName:               "testDbName",
	}}
	require.Equal(t, got, want)
}
