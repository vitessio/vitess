// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/mysqlconn/replication"
	"github.com/youtube/vitess/go/sqldb"
)

func TestMariadbMakeBinlogEvent(t *testing.T) {
	input := []byte{1, 2, 3}
	want := replication.NewMariadbBinlogEvent([]byte{1, 2, 3})
	if got := (&mariaDB10{}).MakeBinlogEvent(input); !reflect.DeepEqual(got, want) {
		t.Errorf("(&mariaDB10{}).MakeBinlogEvent(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestMariadbSetSlavePositionCommands(t *testing.T) {
	pos := replication.Position{GTIDSet: replication.MariadbGTID{Domain: 1, Server: 41983, Sequence: 12345}}
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
	params := &sqldb.ConnParams{
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
	params := &sqldb.ConnParams{
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
	want := replication.MariadbGTID{Domain: 12, Server: 34, Sequence: 5678}

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
	want := replication.Position{GTIDSet: replication.MariadbGTID{Domain: 12, Server: 34, Sequence: 5678}}

	got, err := (&mariaDB10{}).ParseReplicationPosition(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("(&mariaDB10{}).ParseReplicationPosition(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestMariadbPromoteSlaveCommands(t *testing.T) {
	want := []string{"RESET SLAVE ALL"}
	if got := (&mariaDB10{}).PromoteSlaveCommands(); !reflect.DeepEqual(got, want) {
		t.Errorf("(&mariaDB10{}).PromoteSlaveCommands() = %#v, want %#v", got, want)
	}
}

func TestMariadbResetReplicationCommands(t *testing.T) {
	want := []string{
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"RESET MASTER",
		"SET GLOBAL gtid_slave_pos = ''",
	}
	if got := (&mariaDB10{}).ResetReplicationCommands(); !reflect.DeepEqual(got, want) {
		t.Errorf("(&mariaDB10{}).ResetReplicationCommands() = %#v, want %#v", got, want)
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
