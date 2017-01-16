// Copyright 2014, Google Inc. All rights reserved.
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

func TestMariadbStandaloneGTIDEventIsGTID(t *testing.T) {
	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbStandaloneGTIDEvent)}
	want := true
	if got := input.IsGTID(); got != want {
		t.Errorf("%#v.IsGTID() = %v, want %v", input, got, want)
	}
}

func TestMariadbBeginGTIDEventIsGTID(t *testing.T) {
	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbBeginGTIDEvent)}
	want := true
	if got := input.IsGTID(); got != want {
		t.Errorf("%#v.IsGTID() = %v, want %v", input, got, want)
	}
}

func TestMariadbBinlogEventIsntGTID(t *testing.T) {
	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbInsertEvent)}
	want := false
	if got := input.IsGTID(); got != want {
		t.Errorf("%#v.IsGTID() = %v, want %v", input, got, want)
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
	if _, got, err := input.GTID(f); got != want {
		t.Errorf("%#v.GTID() = %v (%v), want %v", input, got, err, want)
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
	if _, got, err := input.GTID(f); got != want {
		t.Errorf("%#v.IsBeginGTID() = %v (%v), want %v", input, got, err, want)
	}
}

func TestMariadbStandaloneBinlogEventGTID(t *testing.T) {
	f, err := binlogEvent(mariadbFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbStandaloneGTIDEvent)}
	want := replication.MariadbGTID{Domain: 0, Server: 62344, Sequence: 9}
	got, hasBegin, err := input.GTID(f)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if hasBegin {
		t.Errorf("unexpected hasBegin")
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%#v.GTID() = %#v, want %#v", input, got, want)
	}
}

func TestMariadbBinlogEventGTID(t *testing.T) {
	f, err := binlogEvent(mariadbFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbBeginGTIDEvent)}
	want := replication.MariadbGTID{Domain: 0, Server: 62344, Sequence: 10}
	got, hasBegin, err := input.GTID(f)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !hasBegin {
		t.Errorf("unexpected !hasBegin")
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%#v.GTID() = %#v, want %#v", input, got, want)
	}
}

func TestMariadbBinlogEventFormat(t *testing.T) {
	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbFormatEvent)}
	want := replication.BinlogFormat{
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
	want := replication.BinlogFormat{
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
		t.Fatalf("unexpected error: %v", err)
	}

	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbChecksumQueryEvent)}
	wantEvent := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbChecksumStrippedQueryEvent)}
	wantChecksum := []byte{0xce, 0x49, 0x7a, 0x53}
	gotEvent, gotChecksum, err := input.StripChecksum(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(gotEvent, wantEvent) || !reflect.DeepEqual(gotChecksum, wantChecksum) {
		t.Errorf("%#v.StripChecksum() = (%v, %v), want (%v, %v)", input, gotEvent, gotChecksum, wantEvent, wantChecksum)
	}
}

func TestMariadbBinlogEventStripChecksumNone(t *testing.T) {
	f, err := (mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbFormatEvent)}).Format()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	input := mariadbBinlogEvent{binlogEvent: binlogEvent(mariadbStandaloneGTIDEvent)}
	want := input
	gotEvent, gotChecksum, err := input.StripChecksum(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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
