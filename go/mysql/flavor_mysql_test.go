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

package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMysql56SetMasterCommands(t *testing.T) {
	params := &ConnParams{
		Uname: "username",
		Pass:  "password",
	}
	masterHost := "localhost"
	masterPort := 123
	masterConnectRetry := 1234
	want := `CHANGE MASTER TO
  MASTER_HOST = 'localhost',
  MASTER_PORT = 123,
  MASTER_USER = 'username',
  MASTER_PASSWORD = 'password',
  MASTER_CONNECT_RETRY = 1234,
  MASTER_AUTO_POSITION = 1`

	conn := &Conn{flavor: mysqlFlavor57{}}
	got := conn.SetMasterCommand(params, masterHost, masterPort, masterConnectRetry)
	if got != want {
		t.Errorf("mysqlFlavor.SetMasterCommand(%#v, %#v, %#v, %#v) = %#v, want %#v", params, masterHost, masterPort, masterConnectRetry, got, want)
	}
}

func TestMysql56SetMasterCommandsSSL(t *testing.T) {
	params := &ConnParams{
		Uname:     "username",
		Pass:      "password",
		SslCa:     "ssl-ca",
		SslCaPath: "ssl-ca-path",
		SslCert:   "ssl-cert",
		SslKey:    "ssl-key",
	}
	params.EnableSSL()
	masterHost := "localhost"
	masterPort := 123
	masterConnectRetry := 1234
	want := `CHANGE MASTER TO
  MASTER_HOST = 'localhost',
  MASTER_PORT = 123,
  MASTER_USER = 'username',
  MASTER_PASSWORD = 'password',
  MASTER_CONNECT_RETRY = 1234,
  MASTER_SSL = 1,
  MASTER_SSL_CA = 'ssl-ca',
  MASTER_SSL_CAPATH = 'ssl-ca-path',
  MASTER_SSL_CERT = 'ssl-cert',
  MASTER_SSL_KEY = 'ssl-key',
  MASTER_AUTO_POSITION = 1`

	conn := &Conn{flavor: mysqlFlavor57{}}
	got := conn.SetMasterCommand(params, masterHost, masterPort, masterConnectRetry)
	if got != want {
		t.Errorf("mysqlFlavor.SetMasterCommands(%#v, %#v, %#v, %#v) = %#v, want %#v", params, masterHost, masterPort, masterConnectRetry, got, want)
	}
}

func TestMysqlRetrieveMasterServerId(t *testing.T) {
	resultMap := map[string]string{
		"Master_Server_Id": "1",
	}

	want := ReplicationStatus{MasterServerID: 1}
	got, err := parseMysqlReplicationStatus(resultMap)
	require.NoError(t, err)
	assert.Equalf(t, got.MasterServerID, want.MasterServerID, "got MasterServerID: %v; want MasterServerID: %v", got.MasterServerID, want.MasterServerID)
}

func TestMysqlRetrieveFileBasedPositions(t *testing.T) {
	resultMap := map[string]string{
		"Exec_Master_Log_Pos":   "1307",
		"Relay_Master_Log_File": "master-bin.000002",
		"Read_Master_Log_Pos":   "1308",
		"Master_Log_File":       "master-bin.000003",
	}

	want := ReplicationStatus{
		FilePosition:         Position{GTIDSet: filePosGTID{file: "master-bin.000002", pos: 1307}},
		FileRelayLogPosition: Position{GTIDSet: filePosGTID{file: "master-bin.000003", pos: 1308}},
	}
	got, err := parseMysqlReplicationStatus(resultMap)
	require.NoError(t, err)
	assert.Equalf(t, got.FilePosition.GTIDSet, want.FilePosition.GTIDSet, "got FilePosition: %v; want FilePosition: %v", got.FilePosition.GTIDSet, want.FilePosition.GTIDSet)
	assert.Equalf(t, got.FileRelayLogPosition.GTIDSet, want.FileRelayLogPosition.GTIDSet, "got FileRelayLogPosition: %v; want FileRelayLogPosition: %v", got.FileRelayLogPosition.GTIDSet, want.FileRelayLogPosition.GTIDSet)
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
	got, err := parseMysqlReplicationStatus(resultMap)
	require.NoError(t, err)
	assert.Equalf(t, got.RelayLogPosition.GTIDSet.String(), want.RelayLogPosition.GTIDSet.String(), "got RelayLogPosition: %v; want RelayLogPosition: %v", got.RelayLogPosition.GTIDSet, want.RelayLogPosition.GTIDSet)
}

func TestMysqlShouldGetMasterPosition(t *testing.T) {
	resultMap := map[string]string{
		"Executed_Gtid_Set": "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-5",
		"Position":          "1307",
		"File":              "source-bin.000003",
	}

	sid, _ := ParseSID("3e11fa47-71ca-11e1-9e33-c80aa9429562")
	want := MasterStatus{
		Position:     Position{GTIDSet: Mysql56GTIDSet{sid: []interval{{start: 1, end: 5}}}},
		FilePosition: Position{GTIDSet: filePosGTID{file: "source-bin.000003", pos: 1307}},
	}
	got, err := parseMysqlMasterStatus(resultMap)
	require.NoError(t, err)
	assert.Equalf(t, got.Position.GTIDSet.String(), want.Position.GTIDSet.String(), "got Position: %v; want Position: %v", got.Position.GTIDSet, want.Position.GTIDSet)
	assert.Equalf(t, got.FilePosition.GTIDSet.String(), want.FilePosition.GTIDSet.String(), "got FilePosition: %v; want FilePosition: %v", got.FilePosition.GTIDSet, want.FilePosition.GTIDSet)
}
