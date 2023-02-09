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

func TestMysql56SetReplicationSourceCommand(t *testing.T) {
	params := &ConnParams{
		Uname: "username",
		Pass:  "password",
	}
	host := "localhost"
	port := int32(123)
	connectRetry := 1234
	want := `CHANGE MASTER TO
  MASTER_HOST = 'localhost',
  MASTER_PORT = 123,
  MASTER_USER = 'username',
  MASTER_PASSWORD = 'password',
  MASTER_CONNECT_RETRY = 1234,
  MASTER_AUTO_POSITION = 1`

	conn := &Conn{flavor: mysqlFlavor57{}}
	got := conn.SetReplicationSourceCommand(params, host, port, connectRetry)
	assert.Equal(t, want, got, "mysqlFlavor.SetReplicationSourceCommand(%#v, %#v, %#v, %#v) = %#v, want %#v", params, host, port, connectRetry, got, want)

}

func TestMysql56SetReplicationSourceCommandSSL(t *testing.T) {
	params := &ConnParams{
		Uname:     "username",
		Pass:      "password",
		SslCa:     "ssl-ca",
		SslCaPath: "ssl-ca-path",
		SslCert:   "ssl-cert",
		SslKey:    "ssl-key",
	}
	params.EnableSSL()
	host := "localhost"
	port := int32(123)
	connectRetry := 1234
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
	got := conn.SetReplicationSourceCommand(params, host, port, connectRetry)
	assert.Equal(t, want, got, "mysqlFlavor.SetReplicationSourceCommand(%#v, %#v, %#v, %#v) = %#v, want %#v", params, host, port, connectRetry, got, want)

}

func TestMysqlRetrieveSourceServerId(t *testing.T) {
	resultMap := map[string]string{
		"Master_Server_Id": "1",
	}

	want := ReplicationStatus{SourceServerID: 1}
	got, err := parseMysqlReplicationStatus(resultMap)
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
		FilePosition:                           Position{GTIDSet: filePosGTID{file: "master-bin.000002", pos: 1307}},
		RelayLogSourceBinlogEquivalentPosition: Position{GTIDSet: filePosGTID{file: "master-bin.000003", pos: 1308}},
		RelayLogFilePosition:                   Position{GTIDSet: filePosGTID{file: "relay-bin.000004", pos: 1309}},
	}
	got, err := parseMysqlReplicationStatus(resultMap)
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
	got, err := parseMysqlReplicationStatus(resultMap)
	require.NoError(t, err)
	assert.Equalf(t, got.RelayLogPosition.GTIDSet.String(), want.RelayLogPosition.GTIDSet.String(), "got RelayLogPosition: %v; want RelayLogPosition: %v", got.RelayLogPosition.GTIDSet, want.RelayLogPosition.GTIDSet)
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
		FilePosition: Position{GTIDSet: filePosGTID{file: "source-bin.000003", pos: 1307}},
	}
	got, err := parseMysqlPrimaryStatus(resultMap)
	require.NoError(t, err)
	assert.Equalf(t, got.Position.GTIDSet.String(), want.Position.GTIDSet.String(), "got Position: %v; want Position: %v", got.Position.GTIDSet, want.Position.GTIDSet)
	assert.Equalf(t, got.FilePosition.GTIDSet.String(), want.FilePosition.GTIDSet.String(), "got FilePosition: %v; want FilePosition: %v", got.FilePosition.GTIDSet, want.FilePosition.GTIDSet)
}
