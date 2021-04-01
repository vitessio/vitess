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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMariadbSetMasterCommands(t *testing.T) {
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
  MASTER_USE_GTID = current_pos`

	conn := &Conn{flavor: mariadbFlavor101{}}
	got := conn.SetMasterCommand(params, masterHost, masterPort, masterConnectRetry)
	if got != want {
		t.Errorf("mariadbFlavor.SetMasterCommands(%#v, %#v, %#v, %#v) = %#v, want %#v", params, masterHost, masterPort, masterConnectRetry, got, want)
	}
}

func TestMariadbSetMasterCommandsSSL(t *testing.T) {
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
  MASTER_USE_GTID = current_pos`

	conn := &Conn{flavor: mariadbFlavor101{}}
	got := conn.SetMasterCommand(params, masterHost, masterPort, masterConnectRetry)
	if got != want {
		t.Errorf("mariadbFlavor.SetMasterCommands(%#v, %#v, %#v, %#v) = %#v, want %#v", params, masterHost, masterPort, masterConnectRetry, got, want)
	}
}

func TestMariadbRetrieveMasterServerId(t *testing.T) {
	resultMap := map[string]string{
		"Master_Server_Id": "1",
		"Gtid_Slave_Pos":   "0-101-2320",
	}

	want := ReplicationStatus{MasterServerID: 1}
	got, err := parseMariadbReplicationStatus(resultMap)
	require.NoError(t, err)
	assert.Equal(t, got.MasterServerID, want.MasterServerID, fmt.Sprintf("got MasterServerID: %v; want MasterServerID: %v", got.MasterServerID, want.MasterServerID))
}

func TestMariadbRetrieveFileBasedPositions(t *testing.T) {
	resultMap := map[string]string{
		"Exec_Master_Log_Pos":   "1307",
		"Relay_Master_Log_File": "master-bin.000002",
		"Read_Master_Log_Pos":   "1308",
		"Master_Log_File":       "master-bin.000003",
		"Gtid_Slave_Pos":        "0-101-2320",
	}

	want := ReplicationStatus{
		FilePosition:         Position{GTIDSet: filePosGTID{file: "master-bin.000002", pos: 1307}},
		FileRelayLogPosition: Position{GTIDSet: filePosGTID{file: "master-bin.000003", pos: 1308}},
	}
	got, err := parseMariadbReplicationStatus(resultMap)
	require.NoError(t, err)
	assert.Equal(t, got.FilePosition.GTIDSet, want.FilePosition.GTIDSet, fmt.Sprintf("got FilePosition: %v; want FilePosition: %v", got.FilePosition.GTIDSet, want.FilePosition.GTIDSet))
	assert.Equal(t, got.FileRelayLogPosition.GTIDSet, want.FileRelayLogPosition.GTIDSet, fmt.Sprintf("got FileRelayLogPosition: %v; want FileRelayLogPosition: %v", got.FileRelayLogPosition.GTIDSet, want.FileRelayLogPosition.GTIDSet))
}

func TestMariadbShouldGetNilRelayLogPosition(t *testing.T) {
	resultMap := map[string]string{
		"Exec_Master_Log_Pos":   "1307",
		"Relay_Master_Log_File": "master-bin.000002",
		"Read_Master_Log_Pos":   "1308",
		"Master_Log_File":       "master-bin.000003",
		"Gtid_Slave_Pos":        "0-101-2320",
	}
	got, err := parseMariadbReplicationStatus(resultMap)
	require.NoError(t, err)
	assert.Truef(t, got.RelayLogPosition.IsZero(), "Got a filled in RelayLogPosition. For MariaDB we should get back nil, because MariaDB does not return the retrieved GTIDSet. got: %#v", got.RelayLogPosition)
}
