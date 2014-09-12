// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/mysql"
	blproto "github.com/youtube/vitess/go/vt/binlog/proto"
	proto "github.com/youtube/vitess/go/vt/mysqlctl/proto"
)

func TestGoogleMakeBinlogEvent(t *testing.T) {
	input := []byte{1, 2, 3}
	want := googleBinlogEvent{binlogEvent: binlogEvent([]byte{1, 2, 3})}
	if got := (&googleMysql51{}).MakeBinlogEvent(input); !reflect.DeepEqual(got, want) {
		t.Errorf("(&googleMysql51{}).MakeBinlogEvent(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestGoogleBinlogEventIsGTID(t *testing.T) {
	input := googleBinlogEvent{}
	want := false
	if got := input.IsGTID(); got != want {
		t.Errorf("%#v.IsGTID() = %v, want %v", input, got, want)
	}
}

func TestGoogleBinlogEventFormat(t *testing.T) {
	input := googleBinlogEvent{binlogEvent: binlogEvent(googleFormatEvent)}
	want := blproto.BinlogFormat{
		FormatVersion: 4,
		ServerVersion: "5.1.63-google-log",
		HeaderLength:  27,
	}
	got, err := input.Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("%#v.Format() = %v, want %v", input, got, want)
	}
}

func TestGoogleBinlogEventFormatBadHeaderLength(t *testing.T) {
	buf := make([]byte, len(googleFormatEvent))
	copy(buf, googleFormatEvent)
	buf[19+2+50+4] = 12 // mess up the HeaderLength

	input := googleBinlogEvent{binlogEvent: binlogEvent(buf)}
	want := "header length = 12, should be >= 19"
	_, err := input.Format()
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); got != want {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestGoogleBinlogEventFormatBadGoogleHeaderLength(t *testing.T) {
	buf := make([]byte, len(googleFormatEvent))
	copy(buf, googleFormatEvent)
	buf[19+2+50+4] = 19 // mess up the HeaderLength

	input := googleBinlogEvent{binlogEvent: binlogEvent(buf)}
	want := "Google MySQL header length = 19, should be >= 27"
	_, err := input.Format()
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); got != want {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestGoogleBinlogEventHasGTID(t *testing.T) {
	f, err := binlogEvent(googleFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := googleBinlogEvent{binlogEvent: binlogEvent(googleQueryEvent)}
	want := true
	if got := input.HasGTID(f); got != want {
		t.Errorf("%#v.HasGTID() = %v, want %v", input, got, want)
	}
}

func TestGoogleBinlogEventFormatDescriptionDoesntHaveGTID(t *testing.T) {
	f, err := binlogEvent(googleFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := googleBinlogEvent{binlogEvent: binlogEvent(googleFormatEvent)}
	want := false
	if got := input.HasGTID(f); got != want {
		t.Errorf("%#v.HasGTID() = %v, want %v", input, got, want)
	}
}

func TestGoogleBinlogEventRotateDoesntHaveGTID(t *testing.T) {
	f, err := binlogEvent(googleFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := googleBinlogEvent{binlogEvent: binlogEvent(googleRotateEvent)}
	want := false
	if got := input.HasGTID(f); got != want {
		t.Errorf("%#v.HasGTID() = %v, want %v", input, got, want)
	}
}

func TestGoogleBinlogEventDoesntHaveGTID(t *testing.T) {
	f, err := binlogEvent(googleFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	buf := make([]byte, len(googleQueryEvent))
	copy(buf, googleFormatEvent)
	copy(buf[19:19+8], make([]byte, 8)) // set group_id = 0

	input := googleBinlogEvent{binlogEvent: binlogEvent(buf)}
	want := false
	if got := input.HasGTID(f); got != want {
		t.Errorf("%#v.HasGTID() = %v, want %v", input, got, want)
	}
}

func TestGoogleBinlogEventGTIDInvalid(t *testing.T) {
	f, err := binlogEvent(googleFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	buf := make([]byte, len(googleQueryEvent))
	copy(buf, googleFormatEvent)
	copy(buf[19:19+8], make([]byte, 8)) // set group_id = 0

	input := googleBinlogEvent{binlogEvent: binlogEvent(buf)}
	want := "invalid group_id 0"
	_, err = input.GTID(f)
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); got != want {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestGoogleBinlogEventGTID(t *testing.T) {
	f, err := binlogEvent(googleFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := googleBinlogEvent{binlogEvent: binlogEvent(googleQueryEvent)}
	want := proto.GoogleGTID{ServerID: 62344, GroupID: 0xb}
	got, err := input.GTID(f)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%#v.GTID() = %#v, want %#v", input, got, want)
	}
}

func TestGoogleStartReplicationCommands(t *testing.T) {
	params := &mysql.ConnectionParams{
		Uname: "username",
		Pass:  "password",
	}
	status := &proto.ReplicationStatus{
		Position:           proto.ReplicationPosition{GTIDSet: proto.GoogleGTID{ServerID: 41983, GroupID: 12345}},
		MasterHost:         "localhost",
		MasterPort:         123,
		MasterConnectRetry: 1234,
	}
	want := []string{
		"STOP SLAVE",
		"RESET SLAVE",
		"SET binlog_group_id = 12345, master_server_id = 41983",
		`CHANGE MASTER TO
  MASTER_HOST = 'localhost',
  MASTER_PORT = 123,
  MASTER_USER = 'username',
  MASTER_PASSWORD = 'password',
  MASTER_CONNECT_RETRY = 1234,
  CONNECT_USING_GROUP_ID`,
		"START SLAVE",
	}

	got, err := (&googleMysql51{}).StartReplicationCommands(params, status)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("(&googleMysql51{}).StartReplicationCommands(%#v, %#v) = %#v, want %#v", params, status, got, want)
	}
}

func TestGoogleStartReplicationCommandsSSL(t *testing.T) {
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
		Position:           proto.ReplicationPosition{GTIDSet: proto.GoogleGTID{ServerID: 41983, GroupID: 12345}},
		MasterHost:         "localhost",
		MasterPort:         123,
		MasterConnectRetry: 1234,
	}
	want := []string{
		"STOP SLAVE",
		"RESET SLAVE",
		"SET binlog_group_id = 12345, master_server_id = 41983",
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
  CONNECT_USING_GROUP_ID`,
		"START SLAVE",
	}

	got, err := (&googleMysql51{}).StartReplicationCommands(params, status)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("(&googleMysql51{}).StartReplicationCommands(%#v, %#v) = %#v, want %#v", params, status, got, want)
	}
}

func TestGoogleParseGTID(t *testing.T) {
	input := "123-456"
	want := proto.GoogleGTID{ServerID: 123, GroupID: 456}

	got, err := (&googleMysql51{}).ParseGTID(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if got != want {
		t.Errorf("(&googleMysql51{}).ParseGTID(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestGoogleParseReplicationPosition(t *testing.T) {
	input := "123-456"
	want := proto.ReplicationPosition{GTIDSet: proto.GoogleGTID{ServerID: 123, GroupID: 456}}

	got, err := (&googleMysql51{}).ParseReplicationPosition(input)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !got.Equal(want) {
		t.Errorf("(&googleMysql51{}).ParseReplicationPosition(%#v) = %#v, want %#v", input, got, want)
	}
}

func TestMakeBinlogDump2Command(t *testing.T) {
	want := []byte{
		// binlog_flags
		0xfe, 0xca,
		// slave_server_id
		0xef, 0xbe, 0xad, 0xde,
		// group_id
		0x78, 0x56, 0x34, 0x12, 0x78, 0x56, 0x34, 0x12,
		// event_server_id
		0x21, 0x43, 0x65, 0x87,
	}

	got := makeBinlogDump2Command(0xcafe, 0xdeadbeef, 0x1234567812345678, 0x87654321)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("makeBinlogDump2Command() = %#v, want %#v", got, want)
	}
}

func TestGooglePromoteSlaveCommands(t *testing.T) {
	want := []string{
		"RESET MASTER",
		"RESET SLAVE",
		"CHANGE MASTER TO MASTER_HOST = ''",
	}
	if got := (&googleMysql51{}).PromoteSlaveCommands(); !reflect.DeepEqual(got, want) {
		t.Errorf("(&googleMysql51{}).PromoteSlaveCommands() = %#v, want %#v", got, want)
	}
}
