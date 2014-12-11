// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"reflect"
	"testing"

	"github.com/henryanand/vitess/go/mysql"
	blproto "github.com/henryanand/vitess/go/vt/binlog/proto"
	"github.com/henryanand/vitess/go/vt/mysqlctl/proto"
)

func TestMariadbStandaloneGTIDEventHasGTID(t *testing.T) {
	f, err := binlogEvent(mariadbFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbStandaloneGTIDEvent)}
	want := true
	if got := input.HasGTID(f); got != want {
		t.Errorf("%#v.HasGTID() = %v, want %v", input, got, want)
	}
}

func TestMariadbBeginGTIDEventHasGTID(t *testing.T) {
	f, err := binlogEvent(mariadbFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbBeginGTIDEvent)}
	want := true
	if got := input.HasGTID(f); got != want {
		t.Errorf("%#v.HasGTID() = %v, want %v", input, got, want)
	}
}

func TestMariadbBinlogEventDoesntHaveGTID(t *testing.T) {
	f, err := binlogEvent(mariadbFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbInsertEvent)}
	want := false
	if got := input.HasGTID(f); got != want {
		t.Errorf("%#v.HasGTID() = %v, want %v", input, got, want)
	}
}

func TestMariadbNotBeginGTID(t *testing.T) {
	f, err := binlogEvent(mariadbFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbStandaloneGTIDEvent)}
	want := false
	if got := input.IsBeginGTID(f); got != want {
		t.Errorf("%#v.IsBeginGTID() = %v, want %v", input, got, want)
	}
}

func TestMariadbIsBeginGTID(t *testing.T) {
	f, err := binlogEvent(mariadbFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbBeginGTIDEvent)}
	want := true
	if got := input.IsBeginGTID(f); got != want {
		t.Errorf("%#v.IsBeginGTID() = %v, want %v", input, got, want)
	}
}

func TestMariadbBinlogEventGTID(t *testing.T) {
	f, err := binlogEvent(mariadbFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbBeginGTIDEvent)}
	want := proto.MariadbGTID{Domain: 0, Server: 62344, Sequence: 10}
	got, err := input.GTID(f)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%#v.GTID() = %#v, want %#v", input, got, want)
	}
}

func TestMariadbBinlogEventFormat(t *testing.T) {
	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbFormatEvent)}
	want := blproto.BinlogFormat{
		FormatVersion:     4,
		ServerVersion:     "10.0.13-MariaDB-1~precise-log",
		HeaderLength:      19,
		ChecksumAlgorithm: 0,
	}
	got, err := input.Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("%#v.Format() = %v, want %v", input, got, want)
	}
}

func TestMariadbBinlogEventChecksumFormat(t *testing.T) {
	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbChecksumFormatEvent)}
	want := blproto.BinlogFormat{
		FormatVersion:     4,
		ServerVersion:     "10.0.13-MariaDB-1~precise-log",
		HeaderLength:      19,
		ChecksumAlgorithm: 1,
	}
	got, err := input.Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("%#v.Format() = %v, want %v", input, got, want)
	}
}

func TestMariadbBinlogEventStripChecksum(t *testing.T) {
	f, err := (mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbChecksumFormatEvent)}).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbChecksumQueryEvent)}
	wantEvent := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbChecksumStrippedQueryEvent)}
	wantChecksum := []byte{0xce, 0x49, 0x7a, 0x53}
	gotEvent, gotChecksum := input.StripChecksum(f)
	if !reflect.DeepEqual(gotEvent, wantEvent) || !reflect.DeepEqual(gotChecksum, wantChecksum) {
		t.Errorf("%#v.StripChecksum() = (%v, %v), want (%v, %v)", input, gotEvent, gotChecksum, wantEvent, wantChecksum)
	}
}

func TestMariadbBinlogEventStripChecksumNone(t *testing.T) {
	f, err := (mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbFormatEvent)}).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbStandaloneGTIDEvent)}
	want := input
	gotEvent, gotChecksum := input.StripChecksum(f)
	if !reflect.DeepEqual(gotEvent, want) || gotChecksum != nil {
		t.Errorf("%#v.StripChecksum() = (%v, %v), want (%v, nil)", input, gotEvent, gotChecksum, want)
	}
}

func TestMariadbMakeBinlogEvent(t *testing.T) {
	input := []byte{1, 2, 3}
	want := mariadbBinlogEvent{binlogEvent: binlogEvent([]byte{1, 2, 3})}
	if got := (&mariaDB10{}).MakeBinlogEvent(input); !reflect.DeepEqual(got, want) {
		t.Errorf("(&mariaDB10{}).MakeBinlogEvent(%#v) = %#v, want %#v", input, got, want)
	}
}

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

func TestMariadbPromoteSlaveCommands(t *testing.T) {
	want := []string{"RESET SLAVE"}
	if got := (&mariaDB10{}).PromoteSlaveCommands(); !reflect.DeepEqual(got, want) {
		t.Errorf("(&mariaDB10{}).PromoteSlaveCommands() = %#v, want %#v", got, want)
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
