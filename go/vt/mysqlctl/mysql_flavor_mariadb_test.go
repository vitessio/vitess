// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

// TODO(enisoc): Grab MariaDB binlog event data to make unit tests for binary
// parser when MariaDB starts working.

func TestMariadbStartReplicationCommands(t *testing.T) {
	params := &mysql.ConnectionParams{
		Uname: "username",
		Pass:  "password",
	}
	status := &proto.ReplicationStatus{
		Position:           proto.ReplicationPosition{GTIDSet: proto.MariadbGTID{Domain: 1, Server: 41983, Sequence: 12345}},
		MasterHost:         "localhost",
		MasterPort:         123,
		MasterConnectRetry: 1234,
	}
	want := []string{
		"STOP SLAVE",
		"RESET SLAVE",
		"SET GLOBAL gtid_slave_pos = '1-41983-12345'",
		`CHANGE MASTER TO
  MASTER_HOST = 'localhost',
  MASTER_PORT = 123,
  MASTER_USER = 'username',
  MASTER_PASSWORD = 'password',
  MASTER_CONNECT_RETRY = 1234,
  MASTER_USE_GTID = slave_pos`,
		"START SLAVE",
	}

	got, err := (&mariaDB10{}).StartReplicationCommands(params, status)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("(&mariaDB10{}).StartReplicationCommands(%#v, %#v) = %#v, want %#v", params, status, got, want)
	}
}

func TestMariadbStartReplicationCommandsSSL(t *testing.T) {
	params := &mysql.ConnectionParams{
		Uname:     "username",
		Pass:      "password",
		SslCa:     "ssl-ca",
		SslCaPath: "ssl-ca-path",
		SslCert:   "ssl-cert",
		SslKey:    "ssl-key",
	}
	params.EnableSSL()
	status := &proto.ReplicationStatus{
		Position:           proto.ReplicationPosition{GTIDSet: proto.MariadbGTID{Domain: 1, Server: 41983, Sequence: 12345}},
		MasterHost:         "localhost",
		MasterPort:         123,
		MasterConnectRetry: 1234,
	}
	want := []string{
		"STOP SLAVE",
		"RESET SLAVE",
		"SET GLOBAL gtid_slave_pos = '1-41983-12345'",
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
  MASTER_USE_GTID = slave_pos`,
		"START SLAVE",
	}

	got, err := (&mariaDB10{}).StartReplicationCommands(params, status)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("(&mariaDB10{}).StartReplicationCommands(%#v, %#v) = %#v, want %#v", params, status, got, want)
	}
}

func TestMariadbParseGTID(t *testing.T) {
	input := "12-34-5678"
	want := proto.MariadbGTID{Domain: 12, Server: 34, Sequence: 5678}

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
	want := proto.ReplicationPosition{GTIDSet: proto.MariadbGTID{Domain: 12, Server: 34, Sequence: 5678}}

	got, err := (&mariaDB10{}).ParseReplicationPosition(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("(&mariaDB10{}).ParseReplicationPosition(%#v) = %#v, want %#v", input, got, want)
	}
}
