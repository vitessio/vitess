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

package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilePosRetrieveSourceServerId(t *testing.T) {
	resultMap := map[string]string{
		"Master_Server_Id": "1",
	}

	want := ReplicationStatus{SourceServerID: 1}
	got, err := parseFilePosReplicationStatus(resultMap)
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
		Position:                               Position{GTIDSet: filePosGTID{file: "master-bin.000002", pos: 1307}},
		RelayLogPosition:                       Position{GTIDSet: filePosGTID{file: "master-bin.000003", pos: 1308}},
		FilePosition:                           Position{GTIDSet: filePosGTID{file: "master-bin.000002", pos: 1307}},
		RelayLogSourceBinlogEquivalentPosition: Position{GTIDSet: filePosGTID{file: "master-bin.000003", pos: 1308}},
		RelayLogFilePosition:                   Position{GTIDSet: filePosGTID{file: "relay-bin.000004", pos: 1309}},
	}
	got, err := parseFilePosReplicationStatus(resultMap)
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
		Position:     Position{GTIDSet: filePosGTID{file: "source-bin.000003", pos: 1307}},
		FilePosition: Position{GTIDSet: filePosGTID{file: "source-bin.000003", pos: 1307}},
	}
	got, err := parseFilePosPrimaryStatus(resultMap)
	require.NoError(t, err)
	assert.Equalf(t, got.Position.GTIDSet, want.Position.GTIDSet, "got Position: %v; want Position: %v", got.Position.GTIDSet, want.Position.GTIDSet)
	assert.Equalf(t, got.FilePosition.GTIDSet, want.FilePosition.GTIDSet, "got FilePosition: %v; want FilePosition: %v", got.FilePosition.GTIDSet, want.FilePosition.GTIDSet)
	assert.Equalf(t, got.Position.GTIDSet, got.FilePosition.GTIDSet, "FilePosition and Position don't match when they should for the FilePos flavor")
}
