/*
Copyright 2017 Google Inc.

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

package mysqlctl

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/mysql"
)

func TestMariadbMakeBinlogEvent(t *testing.T) {
	input := []byte{1, 2, 3}
	want := mysql.NewMariadbBinlogEvent([]byte{1, 2, 3})
	if got := (&mariaDB10{}).MakeBinlogEvent(input); !reflect.DeepEqual(got, want) {
		t.Errorf("(&mariaDB10{}).MakeBinlogEvent(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestMariadbSetSlavePositionCommands(t *testing.T) {
	pos := mysql.Position{GTIDSet: mysql.MariadbGTID{Domain: 1, Server: 41983, Sequence: 12345}}
	want := []string{
		"RESET MASTER",
		"SET GLOBAL gtid_slave_pos = '1-41983-12345'",
		"SET GLOBAL gtid_binlog_state = '1-41983-12345'",
	}

	got, err := (&mariaDB10{}).SetSlavePositionCommands(pos)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("(&mariaDB10{}).SetSlavePositionCommands(%#v) = %#v, want %#v", pos, got, want)
	}
}

func TestMariadbSetMasterCommands(t *testing.T) {
	params := &mysql.ConnParams{
		Uname: "username",
		Pass:  "password",
	}
	masterHost := "localhost"
	masterPort := 123
	masterConnectRetry := 1234
	want := []string{
		`CHANGE MASTER TO
  MASTER_HOST = 'localhost',
  MASTER_PORT = 123,
  MASTER_USER = 'username',
  MASTER_PASSWORD = 'password',
  MASTER_CONNECT_RETRY = 1234,
  MASTER_USE_GTID = current_pos`,
	}

	got, err := (&mariaDB10{}).SetMasterCommands(params, masterHost, masterPort, masterConnectRetry)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("(&mariaDB10{}).SetMasterCommands(%#v, %#v, %#v, %#v) = %#v, want %#v", params, masterHost, masterPort, masterConnectRetry, got, want)
	}
}

func TestMariadbSetMasterCommandsSSL(t *testing.T) {
	params := &mysql.ConnParams{
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
	want := []string{
		`CHANGE MASTER TO
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
  MASTER_USE_GTID = current_pos`,
	}

	got, err := (&mariaDB10{}).SetMasterCommands(params, masterHost, masterPort, masterConnectRetry)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("(&mariaDB10{}).SetMasterCommands(%#v, %#v, %#v, %#v) = %#v, want %#v", params, masterHost, masterPort, masterConnectRetry, got, want)
	}
}

func TestMariadbParseGTID(t *testing.T) {
	input := "12-34-5678"
	want := mysql.MariadbGTID{Domain: 12, Server: 34, Sequence: 5678}

	got, err := (&mariaDB10{}).ParseGTID(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("(&mariaDB10{}).ParseGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestMariadbParseReplicationPosition(t *testing.T) {
	input := "12-34-5678"
	want := mysql.Position{GTIDSet: mysql.MariadbGTID{Domain: 12, Server: 34, Sequence: 5678}}

	got, err := (&mariaDB10{}).ParseReplicationPosition(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("(&mariaDB10{}).ParseReplicationPosition(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestMariadbVersionMatch(t *testing.T) {
	table := map[string]bool{
		"10.0.13-MariaDB-1~precise-log": true,
		"5.1.63-google-log":             false,
	}
	for input, want := range table {
		if got := (&mariaDB10{}).VersionMatch(input); got != want {
			t.Errorf("(&mariaDB10{}).VersionMatch(%#v) = %v, want %v", input, got, want)
		}
	}
}
