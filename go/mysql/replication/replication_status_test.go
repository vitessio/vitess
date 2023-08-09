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

package replication

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatusReplicationRunning(t *testing.T) {
	input := &ReplicationStatus{
		IOState:  ReplicationStatusToState("yes"),
		SQLState: ReplicationStatusToState("yes"),
	}
	want := true
	if got := input.Running(); got != want {
		t.Errorf("%#v.Running() = %v, want %v", input, got, want)
	}
}

func TestStatusIOThreadNotRunning(t *testing.T) {
	input := &ReplicationStatus{
		IOState:  ReplicationStatusToState("no"),
		SQLState: ReplicationStatusToState("yes"),
	}
	want := false
	if got := input.Running(); got != want {
		t.Errorf("%#v.Running() = %v, want %v", input, got, want)
	}
}

func TestStatusSQLThreadNotRunning(t *testing.T) {
	input := &ReplicationStatus{
		IOState:  ReplicationStatusToState("yes"),
		SQLState: ReplicationStatusToState("no"),
	}
	want := false
	if got := input.Running(); got != want {
		t.Errorf("%#v.Running() = %v, want %v", input, got, want)
	}
}

func TestFindErrantGTIDs(t *testing.T) {
	sid1 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	sid2 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16}
	sid3 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 17}
	sid4 := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18}
	sourceSID := SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 19}

	set1 := Mysql56GTIDSet{
		sid1:      []interval{{20, 30}, {35, 39}, {40, 53}, {55, 75}},
		sid2:      []interval{{1, 7}, {20, 50}, {60, 70}},
		sid4:      []interval{{1, 30}},
		sourceSID: []interval{{1, 7}, {20, 30}},
	}

	set2 := Mysql56GTIDSet{
		sid1:      []interval{{20, 30}, {35, 37}, {50, 60}},
		sid2:      []interval{{3, 5}, {22, 25}, {32, 37}, {67, 70}},
		sid3:      []interval{{1, 45}},
		sourceSID: []interval{{2, 6}, {15, 40}},
	}

	set3 := Mysql56GTIDSet{
		sid1:      []interval{{20, 30}, {35, 38}, {50, 70}},
		sid2:      []interval{{3, 5}, {22, 25}, {32, 37}, {67, 70}},
		sid3:      []interval{{1, 45}},
		sourceSID: []interval{{2, 6}, {15, 45}},
	}

	testcases := []struct {
		mainRepStatus    *ReplicationStatus
		otherRepStatuses []*ReplicationStatus
		want             Mysql56GTIDSet
	}{{
		mainRepStatus: &ReplicationStatus{SourceUUID: sourceSID, RelayLogPosition: Position{GTIDSet: set1}},
		otherRepStatuses: []*ReplicationStatus{
			{SourceUUID: sourceSID, RelayLogPosition: Position{GTIDSet: set2}},
			{SourceUUID: sourceSID, RelayLogPosition: Position{GTIDSet: set3}},
		},
		want: Mysql56GTIDSet{
			sid1: []interval{{39, 39}, {40, 49}, {71, 75}},
			sid2: []interval{{1, 2}, {6, 7}, {20, 21}, {26, 31}, {38, 50}, {60, 66}},
			sid4: []interval{{1, 30}},
		},
	}, {
		mainRepStatus:    &ReplicationStatus{SourceUUID: sourceSID, RelayLogPosition: Position{GTIDSet: set1}},
		otherRepStatuses: []*ReplicationStatus{{SourceUUID: sid1, RelayLogPosition: Position{GTIDSet: set1}}},
		// servers with the same GTID sets should not be diagnosed with errant GTIDs
		want: nil,
	}}

	for _, testcase := range testcases {
		t.Run("", func(t *testing.T) {
			got, err := testcase.mainRepStatus.FindErrantGTIDs(testcase.otherRepStatuses)
			require.NoError(t, err)
			require.Equal(t, testcase.want, got)
		})
	}
}

func TestMysqlShouldGetPosition(t *testing.T) {
	resultMap := map[string]string{
		"Executed_Gtid_Set": "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
		"Position":          "1307",
		"File":              "source-bin.000003",
	}

	sid, _ := ParseSID("3e11fa47-71ca-11e1-9e33-c80aa9429562")
	want := PrimaryStatus{
		Position:     Position{GTIDSet: Mysql56GTIDSet{sid: []interval{{start: 1, end: 5}}}},
		FilePosition: Position{GTIDSet: FilePosGTID{File: "source-bin.000003", Pos: 1307}},
	}
	got, err := ParseMysqlPrimaryStatus(resultMap)
	require.NoError(t, err)
	assert.Equalf(t, got.Position.GTIDSet.String(), want.Position.GTIDSet.String(), "got Position: %v; want Position: %v", got.Position.GTIDSet, want.Position.GTIDSet)
	assert.Equalf(t, got.FilePosition.GTIDSet.String(), want.FilePosition.GTIDSet.String(), "got FilePosition: %v; want FilePosition: %v", got.FilePosition.GTIDSet, want.FilePosition.GTIDSet)
}

func TestMysqlRetrieveSourceServerId(t *testing.T) {
	resultMap := map[string]string{
		"Master_Server_Id": "1",
	}

	want := ReplicationStatus{SourceServerID: 1}
	got, err := ParseMysqlReplicationStatus(resultMap)
	require.NoError(t, err)
	assert.Equalf(t, got.SourceServerID, want.SourceServerID, "got SourceServerID: %v; want SourceServerID: %v", got.SourceServerID, want.SourceServerID)
}

func TestMysqlRetrieveFileBasedPositions(t *testing.T) {
	resultMap := map[string]string{
		"Exec_Master_Log_Pos":   "1307",
		"Relay_Master_Log_File": "master-bin.000002",
		"Read_Master_Log_Pos":   "1308",
		"Master_Log_File":       "master-bin.000003",
		"Relay_Log_Pos":         "1309",
		"Relay_Log_File":        "relay-bin.000004",
	}

	want := ReplicationStatus{
		FilePosition:                           Position{GTIDSet: FilePosGTID{File: "master-bin.000002", Pos: 1307}},
		RelayLogSourceBinlogEquivalentPosition: Position{GTIDSet: FilePosGTID{File: "master-bin.000003", Pos: 1308}},
		RelayLogFilePosition:                   Position{GTIDSet: FilePosGTID{File: "relay-bin.000004", Pos: 1309}},
	}
	got, err := ParseMysqlReplicationStatus(resultMap)
	require.NoError(t, err)
	assert.Equalf(t, got.FilePosition.GTIDSet, want.FilePosition.GTIDSet, "got FilePosition: %v; want FilePosition: %v", got.FilePosition.GTIDSet, want.FilePosition.GTIDSet)
	assert.Equalf(t, got.RelayLogFilePosition.GTIDSet, want.RelayLogFilePosition.GTIDSet, "got RelayLogFilePosition: %v; want RelayLogFilePosition: %v", got.RelayLogFilePosition.GTIDSet, want.RelayLogFilePosition.GTIDSet)
	assert.Equalf(t, got.RelayLogSourceBinlogEquivalentPosition.GTIDSet, want.RelayLogSourceBinlogEquivalentPosition.GTIDSet, "got RelayLogSourceBinlogEquivalentPosition: %v; want RelayLogSourceBinlogEquivalentPosition: %v", got.RelayLogSourceBinlogEquivalentPosition.GTIDSet, want.RelayLogSourceBinlogEquivalentPosition.GTIDSet)
}

func TestMysqlShouldGetRelayLogPosition(t *testing.T) {
	resultMap := map[string]string{
		"Executed_Gtid_Set":     "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
		"Retrieved_Gtid_Set":    "3e11fa47-71ca-11e1-9e33-c80aa9429562:6-9",
		"Exec_Master_Log_Pos":   "1307",
		"Relay_Master_Log_File": "master-bin.000002",
		"Read_Master_Log_Pos":   "1308",
		"Master_Log_File":       "master-bin.000003",
	}

	sid, _ := ParseSID("3e11fa47-71ca-11e1-9e33-c80aa9429562")
	want := ReplicationStatus{
		Position:         Position{GTIDSet: Mysql56GTIDSet{sid: []interval{{start: 1, end: 5}}}},
		RelayLogPosition: Position{GTIDSet: Mysql56GTIDSet{sid: []interval{{start: 1, end: 9}}}},
	}
	got, err := ParseMysqlReplicationStatus(resultMap)
	require.NoError(t, err)
	assert.Equalf(t, got.RelayLogPosition.GTIDSet.String(), want.RelayLogPosition.GTIDSet.String(), "got RelayLogPosition: %v; want RelayLogPosition: %v", got.RelayLogPosition.GTIDSet, want.RelayLogPosition.GTIDSet)
}

func TestMariadbRetrieveSourceServerId(t *testing.T) {
	resultMap := map[string]string{
		"Master_Server_Id": "1",
		"Gtid_Slave_Pos":   "0-101-2320",
	}

	want := ReplicationStatus{SourceServerID: 1}
	got, err := ParseMariadbReplicationStatus(resultMap)
	require.NoError(t, err)
	assert.Equal(t, got.SourceServerID, want.SourceServerID, fmt.Sprintf("got SourceServerID: %v; want SourceServerID: %v", got.SourceServerID, want.SourceServerID))
}

func TestMariadbRetrieveFileBasedPositions(t *testing.T) {
	resultMap := map[string]string{
		"Exec_Master_Log_Pos":   "1307",
		"Relay_Master_Log_File": "master-bin.000002",
		"Read_Master_Log_Pos":   "1308",
		"Master_Log_File":       "master-bin.000003",
		"Gtid_Slave_Pos":        "0-101-2320",
		"Relay_Log_Pos":         "1309",
		"Relay_Log_File":        "relay-bin.000004",
	}

	want := ReplicationStatus{
		FilePosition:                           Position{GTIDSet: FilePosGTID{File: "master-bin.000002", Pos: 1307}},
		RelayLogSourceBinlogEquivalentPosition: Position{GTIDSet: FilePosGTID{File: "master-bin.000003", Pos: 1308}},
		RelayLogFilePosition:                   Position{GTIDSet: FilePosGTID{File: "relay-bin.000004", Pos: 1309}},
	}
	got, err := ParseMariadbReplicationStatus(resultMap)
	require.NoError(t, err)
	assert.Equalf(t, got.RelayLogFilePosition.GTIDSet, want.RelayLogFilePosition.GTIDSet, "got RelayLogFilePosition: %v; want RelayLogFilePosition: %v", got.RelayLogFilePosition.GTIDSet, want.RelayLogFilePosition.GTIDSet)
	assert.Equal(t, got.FilePosition.GTIDSet, want.FilePosition.GTIDSet, fmt.Sprintf("got FilePosition: %v; want FilePosition: %v", got.FilePosition.GTIDSet, want.FilePosition.GTIDSet))
	assert.Equal(t, got.RelayLogSourceBinlogEquivalentPosition.GTIDSet, want.RelayLogSourceBinlogEquivalentPosition.GTIDSet, fmt.Sprintf("got RelayLogSourceBinlogEquivalentPosition: %v; want RelayLogSourceBinlogEquivalentPosition: %v", got.RelayLogSourceBinlogEquivalentPosition.GTIDSet, want.RelayLogSourceBinlogEquivalentPosition.GTIDSet))
}

func TestMariadbShouldGetNilRelayLogPosition(t *testing.T) {
	resultMap := map[string]string{
		"Exec_Master_Log_Pos":   "1307",
		"Relay_Master_Log_File": "master-bin.000002",
		"Read_Master_Log_Pos":   "1308",
		"Master_Log_File":       "master-bin.000003",
		"Gtid_Slave_Pos":        "0-101-2320",
	}
	got, err := ParseMariadbReplicationStatus(resultMap)
	require.NoError(t, err)
	assert.Truef(t, got.RelayLogPosition.IsZero(), "Got a filled in RelayLogPosition. For MariaDB we should get back nil, because MariaDB does not return the retrieved GTIDSet. got: %#v", got.RelayLogPosition)
}

func TestFilePosRetrieveSourceServerId(t *testing.T) {
	resultMap := map[string]string{
		"Master_Server_Id": "1",
	}

	want := ReplicationStatus{SourceServerID: 1}
	got, err := ParseFilePosReplicationStatus(resultMap)
	require.NoError(t, err)
	assert.Equalf(t, got.SourceServerID, want.SourceServerID, "got SourceServerID: %v; want SourceServerID: %v", got.SourceServerID, want.SourceServerID)
}

func TestFilePosRetrieveExecutedPosition(t *testing.T) {
	resultMap := map[string]string{
		"Exec_Master_Log_Pos":   "1307",
		"Relay_Master_Log_File": "master-bin.000002",
		"Read_Master_Log_Pos":   "1308",
		"Master_Log_File":       "master-bin.000003",
		"Relay_Log_Pos":         "1309",
		"Relay_Log_File":        "relay-bin.000004",
	}

	want := ReplicationStatus{
		Position:                               Position{GTIDSet: FilePosGTID{File: "master-bin.000002", Pos: 1307}},
		RelayLogPosition:                       Position{GTIDSet: FilePosGTID{File: "master-bin.000003", Pos: 1308}},
		FilePosition:                           Position{GTIDSet: FilePosGTID{File: "master-bin.000002", Pos: 1307}},
		RelayLogSourceBinlogEquivalentPosition: Position{GTIDSet: FilePosGTID{File: "master-bin.000003", Pos: 1308}},
		RelayLogFilePosition:                   Position{GTIDSet: FilePosGTID{File: "relay-bin.000004", Pos: 1309}},
	}
	got, err := ParseFilePosReplicationStatus(resultMap)
	require.NoError(t, err)
	assert.Equalf(t, got.Position.GTIDSet, want.Position.GTIDSet, "got Position: %v; want Position: %v", got.Position.GTIDSet, want.Position.GTIDSet)
	assert.Equalf(t, got.RelayLogPosition.GTIDSet, want.RelayLogPosition.GTIDSet, "got RelayLogPosition: %v; want RelayLogPosition: %v", got.RelayLogPosition.GTIDSet, want.RelayLogPosition.GTIDSet)
	assert.Equalf(t, got.RelayLogFilePosition.GTIDSet, want.RelayLogFilePosition.GTIDSet, "got RelayLogFilePosition: %v; want RelayLogFilePosition: %v", got.RelayLogFilePosition.GTIDSet, want.RelayLogFilePosition.GTIDSet)
	assert.Equalf(t, got.FilePosition.GTIDSet, want.FilePosition.GTIDSet, "got FilePosition: %v; want FilePosition: %v", got.FilePosition.GTIDSet, want.FilePosition.GTIDSet)
	assert.Equalf(t, got.RelayLogSourceBinlogEquivalentPosition.GTIDSet, want.RelayLogSourceBinlogEquivalentPosition.GTIDSet, "got RelayLogSourceBinlogEquivalentPosition: %v; want RelayLogSourceBinlogEquivalentPosition: %v", got.RelayLogSourceBinlogEquivalentPosition.GTIDSet, want.RelayLogSourceBinlogEquivalentPosition.GTIDSet)
	assert.Equalf(t, got.Position.GTIDSet, got.FilePosition.GTIDSet, "FilePosition and Position don't match when they should for the FilePos flavor")
	assert.Equalf(t, got.RelayLogPosition.GTIDSet, got.RelayLogSourceBinlogEquivalentPosition.GTIDSet, "RelayLogPosition and RelayLogSourceBinlogEquivalentPosition don't match when they should for the FilePos flavor")
}

func TestFilePosShouldGetPosition(t *testing.T) {
	resultMap := map[string]string{
		"Position": "1307",
		"File":     "source-bin.000003",
	}

	want := PrimaryStatus{
		Position:     Position{GTIDSet: FilePosGTID{File: "source-bin.000003", Pos: 1307}},
		FilePosition: Position{GTIDSet: FilePosGTID{File: "source-bin.000003", Pos: 1307}},
	}
	got, err := ParseFilePosPrimaryStatus(resultMap)
	require.NoError(t, err)
	assert.Equalf(t, got.Position.GTIDSet, want.Position.GTIDSet, "got Position: %v; want Position: %v", got.Position.GTIDSet, want.Position.GTIDSet)
	assert.Equalf(t, got.FilePosition.GTIDSet, want.FilePosition.GTIDSet, "got FilePosition: %v; want FilePosition: %v", got.FilePosition.GTIDSet, want.FilePosition.GTIDSet)
	assert.Equalf(t, got.Position.GTIDSet, got.FilePosition.GTIDSet, "FilePosition and Position don't match when they should for the FilePos flavor")
}
