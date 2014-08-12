// Copyright 2014, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package mysqlctl

import (
	"reflect"
	"testing"

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
	want := proto.GoogleGTID{GroupID: 0xb}
	got, err := input.GTID(f)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%#v.GTID() = %#v, want %#v", input, got, want)
	}
}
