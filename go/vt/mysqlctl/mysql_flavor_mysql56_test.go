// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/mysqlconn/replication"
	"github.com/youtube/vitess/go/sqldb"
)

func TestMysql56VersionMatch(t *testing.T) {
	table := map[string]bool{
		"10.0.13-MariaDB-1~precise-log": false,
		"5.1.63-google-log":             false,
		"5.6.24-log":                    true,
	}
	for input, want := range table {
		if got := (&mysql56{}).VersionMatch(input); got != want {
			t.Errorf("(&mysql56{}).VersionMatch(%#v) = %v, want %v", input, got, want)
		}
	}
}

func TestMysql56ResetReplicationCommands(t *testing.T) {
	want := []string{
		"STOP SLAVE",
		"RESET SLAVE ALL",
		"RESET MASTER",
	}
	if got := (&mysql56{}).ResetReplicationCommands(); !reflect.DeepEqual(got, want) {
		t.Errorf("(&mysql56{}).ResetReplicationCommands() = %#v, want %#v", got, want)
	}
}

func TestMysql56PromoteSlaveCommands(t *testing.T) {
	want := []string{"RESET SLAVE ALL"}
	if got := (&mysql56{}).PromoteSlaveCommands(); !reflect.DeepEqual(got, want) {
		t.Errorf("(&mysql56{}).PromoteSlaveCommands() = %#v, want %#v", got, want)
	}
}

func TestMysql56SetSlavePositionCommands(t *testing.T) {
	pos, _ := (&mysql56{}).ParseReplicationPosition("00010203-0405-0607-0809-0a0b0c0d0e0f:1-2")
	want := []string{
		"RESET MASTER",
		"SET GLOBAL gtid_purged = '00010203-0405-0607-0809-0a0b0c0d0e0f:1-2'",
	}

	got, err := (&mysql56{}).SetSlavePositionCommands(pos)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("(&mysql56{}).SetSlavePositionCommands(%#v) = %#v, want %#v", pos, got, want)
	}
}

func TestMysql56SetMasterCommands(t *testing.T) {
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
  MASTER_AUTO_POSITION = 1`,
	}

	got, err := (&mysql56{}).SetMasterCommands(params, masterHost, masterPort, masterConnectRetry)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("(&mysql56{}).SetMasterCommands(%#v, %#v, %#v, %#v) = %#v, want %#v", params, masterHost, masterPort, masterConnectRetry, got, want)
	}
}

func TestMysql56SetMasterCommandsSSL(t *testing.T) {
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
  MASTER_AUTO_POSITION = 1`,
	}

	got, err := (&mysql56{}).SetMasterCommands(params, masterHost, masterPort, masterConnectRetry)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("(&mysql56{}).SetMasterCommands(%#v, %#v, %#v, %#v) = %#v, want %#v", params, masterHost, masterPort, masterConnectRetry, got, want)
	}
}

func TestMysql56MakeBinlogEvent(t *testing.T) {
	input := []byte{1, 2, 3}
	want := replication.NewMysql56BinlogEvent([]byte{1, 2, 3})
	if got := (&mysql56{}).MakeBinlogEvent(input); !reflect.DeepEqual(got, want) {
		t.Errorf("(&mysql56{}).MakeBinlogEvent(%#v) = %#v, want %#v", input, got, want)
	}
}
