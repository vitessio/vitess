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
)

func TestFilePosRetrieveMasterServerId(t *testing.T) {
	resultMap := map[string]string{
		"Master_Server_Id": "1",
	}

	want := SlaveStatus{MasterServerID: 1}
	got, err := parseFilePosSlaveStatus(resultMap)
	if err != nil {
		t.Error("Received an error when trying to parse resultMap.")
	}
	if got.MasterServerID != want.MasterServerID {
		t.Errorf("got MasterServerID: %v; want MasterServerID: %v", got.MasterServerID, want.MasterServerID)
	}
}

func TestFilePosRetrieveExecutedPosition(t *testing.T) {
	resultMap := map[string]string{
		"Exec_Master_Log_Pos":   "1307",
		"Relay_Master_Log_File": "master-bin.000002",
		"Read_Master_Log_Pos":   "1308",
		"Master_Log_File":       "master-bin.000003",
	}

	want := SlaveStatus{
		Position:             Position{GTIDSet: filePosGTID{file: "master-bin.000002", pos: 1307}},
		RelayLogPosition:     Position{GTIDSet: filePosGTID{file: "master-bin.000003", pos: 1308}},
		FilePosition:         Position{GTIDSet: filePosGTID{file: "master-bin.000002", pos: 1307}},
		FileRelayLogPosition: Position{GTIDSet: filePosGTID{file: "master-bin.000003", pos: 1308}},
	}
	got, err := parseFilePosSlaveStatus(resultMap)
	if err != nil {
		t.Error("Received an error when trying to parse resultMap.")
	}
	if got.Position.GTIDSet != want.Position.GTIDSet {
		t.Errorf("got Position: %v; want Position: %v", got.Position.GTIDSet, want.Position.GTIDSet)
	}
	if got.RelayLogPosition.GTIDSet != want.RelayLogPosition.GTIDSet {
		t.Errorf("got RelayLogPosition: %v; want RelayLogPosition: %v", got.RelayLogPosition.GTIDSet, want.RelayLogPosition.GTIDSet)
	}
	if got.FilePosition.GTIDSet != want.FilePosition.GTIDSet {
		t.Errorf("got FilePosition: %v; want FilePosition: %v", got.FilePosition.GTIDSet, want.FilePosition.GTIDSet)
	}
	if got.FileRelayLogPosition.GTIDSet != want.FileRelayLogPosition.GTIDSet {
		t.Errorf("got FileRelayLogPosition: %v; want FileRelayLogPosition: %v", got.FileRelayLogPosition.GTIDSet, want.FileRelayLogPosition.GTIDSet)
	}
	if got.Position.GTIDSet != got.FilePosition.GTIDSet {
		t.Error("FilePosition and Position don't match when they should for the FilePos flavor")
	}
	if got.RelayLogPosition.GTIDSet != got.FileRelayLogPosition.GTIDSet {
		t.Error("RelayLogPosition and FileRelayLogPosition don't match when they should for the FilePos flavor")
	}
}
