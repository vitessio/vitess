// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/mysql"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/vt/mysqlctl/replication"
)

// Sample event data for MySQL 5.6.
var (
	mysql56FormatEvent = NewMysql56BinlogEvent([]byte{0x78, 0x4e, 0x49, 0x55, 0xf, 0x64, 0x0, 0x0, 0x0, 0x74, 0x0, 0x0, 0x0, 0x78, 0x0, 0x0, 0x0, 0x1, 0x0, 0x4, 0x0, 0x35, 0x2e, 0x36, 0x2e, 0x32, 0x34, 0x2d, 0x6c, 0x6f, 0x67, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x78, 0x4e, 0x49, 0x55, 0x13, 0x38, 0xd, 0x0, 0x8, 0x0, 0x12, 0x0, 0x4, 0x4, 0x4, 0x4, 0x12, 0x0, 0x0, 0x5c, 0x0, 0x4, 0x1a, 0x8, 0x0, 0x0, 0x0, 0x8, 0x8, 0x8, 0x2, 0x0, 0x0, 0x0, 0xa, 0xa, 0xa, 0x19, 0x19, 0x0, 0x1, 0x18, 0x4a, 0xf, 0xca})
	mysql56GTIDEvent   = NewMysql56BinlogEvent([]byte{0xff, 0x4e, 0x49, 0x55, 0x21, 0x64, 0x0, 0x0, 0x0, 0x30, 0x0, 0x0, 0x0, 0xf5, 0x2, 0x0, 0x0, 0x0, 0x0, 0x1, 0x43, 0x91, 0x92, 0xbd, 0xf3, 0x7c, 0x11, 0xe4, 0xbb, 0xeb, 0x2, 0x42, 0xac, 0x11, 0x3, 0x5a, 0x4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x48, 0x45, 0x82, 0x27})
	mysql56QueryEvent  = NewMysql56BinlogEvent([]byte{0xff, 0x4e, 0x49, 0x55, 0x2, 0x64, 0x0, 0x0, 0x0, 0x77, 0x0, 0x0, 0x0, 0xdb, 0x3, 0x0, 0x0, 0x0, 0x0, 0x3d, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x21, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x20, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6, 0x3, 0x73, 0x74, 0x64, 0x4, 0x8, 0x0, 0x8, 0x0, 0x21, 0x0, 0xc, 0x1, 0x74, 0x65, 0x73, 0x74, 0x0, 0x74, 0x65, 0x73, 0x74, 0x0, 0x69, 0x6e, 0x73, 0x65, 0x72, 0x74, 0x20, 0x69, 0x6e, 0x74, 0x6f, 0x20, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x28, 0x6d, 0x73, 0x67, 0x29, 0x20, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x20, 0x28, 0x27, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x27, 0x29, 0x92, 0x12, 0x79, 0xc3})
)

func TestMysql56IsGTID(t *testing.T) {
	if got, want := mysql56FormatEvent.IsGTID(), false; got != want {
		t.Errorf("%#v.IsGTID() = %#v, want %#v", mysql56FormatEvent, got, want)
	}
	if got, want := mysql56QueryEvent.IsGTID(), false; got != want {
		t.Errorf("%#v.IsGTID() = %#v, want %#v", mysql56QueryEvent, got, want)
	}
	if got, want := mysql56GTIDEvent.IsGTID(), true; got != want {
		t.Errorf("%#v.IsGTID() = %#v, want %#v", mysql56GTIDEvent, got, want)
	}
}

func TestMysql56StripChecksum(t *testing.T) {
	format, err := mysql56FormatEvent.Format()
	if err != nil {
		t.Fatalf("Format() error: %v", err)
	}

	stripped, gotChecksum, err := mysql56QueryEvent.StripChecksum(format)
	if err != nil {
		t.Fatalf("StripChecksum() error: %v", err)
	}

	// Check checksum.
	if want := []byte{0x92, 0x12, 0x79, 0xc3}; !reflect.DeepEqual(gotChecksum, want) {
		t.Errorf("checksum = %#v, want %#v", gotChecksum, want)
	}

	// Check query, to make sure checksum was stripped properly.
	// Query length is defined as "the rest of the bytes after offset X",
	// so the query will be wrong if the checksum is not stripped.
	gotQuery, err := stripped.Query(format)
	if err != nil {
		t.Fatalf("Query() error: %v", err)
	}
	if want := "insert into test_table (msg) values ('hello')"; string(gotQuery.SQL) != want {
		t.Errorf("query = %#v, want %#v", string(gotQuery.SQL), want)
	}
}

func TestMysql56GTID(t *testing.T) {
	format, err := mysql56FormatEvent.Format()
	if err != nil {
		t.Fatalf("Format() error: %v", err)
	}
	input, _, err := mysql56GTIDEvent.StripChecksum(format)
	if err != nil {
		t.Fatalf("StripChecksum() error: %v", err)
	}
	if !input.IsGTID() {
		t.Fatalf("IsGTID() = false, want true")
	}

	want, _ := (&mysql56{}).ParseGTID("439192bd-f37c-11e4-bbeb-0242ac11035a:4")
	got, hasBegin, err := input.GTID(format)
	if err != nil {
		t.Fatalf("GTID() error: %v", err)
	}
	if hasBegin {
		t.Errorf("GTID() returned hasBegin")
	}
	if got != want {
		t.Errorf("GTID() = %#v, want %#v", got, want)
	}
}

func TestMysql56ParseGTID(t *testing.T) {
	input := "00010203-0405-0607-0809-0A0B0C0D0E0F:56789"
	want := replication.Mysql56GTID{
		Server:   replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
		Sequence: 56789,
	}

	got, err := (&mysql56{}).ParseGTID(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("(&mysql56{}).ParseGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestMysql56ParseReplicationPosition(t *testing.T) {
	input := "00010203-0405-0607-0809-0a0b0c0d0e0f:1-2"

	sid := replication.SID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	var set replication.GTIDSet = replication.Mysql56GTIDSet{}
	set = set.AddGTID(replication.Mysql56GTID{Server: sid, Sequence: 1})
	set = set.AddGTID(replication.Mysql56GTID{Server: sid, Sequence: 2})
	want := replication.Position{GTIDSet: set}

	got, err := (&mysql56{}).ParseReplicationPosition(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("(&mysql56{}).ParseReplicationPosition(%#v) = %#v, want %#v", input, got, want)
	}
}

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
	mysql.EnableSSL(params)
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
	want := mysql56BinlogEvent{binlogEvent: binlogEvent([]byte{1, 2, 3})}
	if got := (&mysql56{}).MakeBinlogEvent(input); !reflect.DeepEqual(got, want) {
		t.Errorf("(&mysql56{}).MakeBinlogEvent(%#v) = %#v, want %#v", input, got, want)
	}
}
