/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

import (
	"reflect"
	"testing"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
)

// sample event data
var (
	garbageEvent       = []byte{92, 93, 211, 208, 16, 71, 139, 255, 83, 199, 198, 59, 148, 214, 109, 154, 122, 226, 39, 41}
	googleRotateEvent  = []byte{0x0, 0x0, 0x0, 0x0, 0x4, 0x88, 0xf3, 0x0, 0x0, 0x33, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x20, 0x0, 0x23, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x76, 0x74, 0x2d, 0x30, 0x30, 0x30, 0x30, 0x30, 0x36, 0x32, 0x33, 0x34, 0x34, 0x2d, 0x62, 0x69, 0x6e, 0x2e, 0x30, 0x30, 0x30, 0x30, 0x30, 0x31}
	googleFormatEvent  = []byte{0x52, 0x52, 0xe9, 0x53, 0xf, 0x88, 0xf3, 0x0, 0x0, 0x66, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x0, 0x35, 0x2e, 0x31, 0x2e, 0x36, 0x33, 0x2d, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2d, 0x6c, 0x6f, 0x67, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1b, 0x38, 0xd, 0x0, 0x8, 0x0, 0x12, 0x0, 0x4, 0x4, 0x4, 0x4, 0x12, 0x0, 0x0, 0x53, 0x0, 0x4, 0x1a, 0x8, 0x0, 0x0, 0x0, 0x8, 0x8, 0x8, 0x2}
	googleQueryEvent   = []byte{0x53, 0x52, 0xe9, 0x53, 0x2, 0x88, 0xf3, 0x0, 0x0, 0xad, 0x0, 0x0, 0x0, 0x9a, 0x4, 0x0, 0x0, 0x0, 0x0, 0xb, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1b, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x10, 0x0, 0x0, 0x1a, 0x0, 0x0, 0x0, 0x40, 0x0, 0x0, 0x1, 0x0, 0x0, 0x20, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6, 0x3, 0x73, 0x74, 0x64, 0x4, 0x8, 0x0, 0x8, 0x0, 0x21, 0x0, 0x76, 0x74, 0x5f, 0x74, 0x65, 0x73, 0x74, 0x5f, 0x6b, 0x65, 0x79, 0x73, 0x70, 0x61, 0x63, 0x65, 0x0, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x20, 0x69, 0x66, 0x20, 0x6e, 0x6f, 0x74, 0x20, 0x65, 0x78, 0x69, 0x73, 0x74, 0x73, 0x20, 0x76, 0x74, 0x5f, 0x61, 0x20, 0x28, 0xa, 0x65, 0x69, 0x64, 0x20, 0x62, 0x69, 0x67, 0x69, 0x6e, 0x74, 0x2c, 0xa, 0x69, 0x64, 0x20, 0x69, 0x6e, 0x74, 0x2c, 0xa, 0x70, 0x72, 0x69, 0x6d, 0x61, 0x72, 0x79, 0x20, 0x6b, 0x65, 0x79, 0x28, 0x65, 0x69, 0x64, 0x2c, 0x20, 0x69, 0x64, 0x29, 0xa, 0x29, 0x20, 0x45, 0x6e, 0x67, 0x69, 0x6e, 0x65, 0x3d, 0x49, 0x6e, 0x6e, 0x6f, 0x44, 0x42}
	googleXIDEvent     = []byte{0x53, 0x52, 0xe9, 0x53, 0x10, 0x88, 0xf3, 0x0, 0x0, 0x23, 0x0, 0x0, 0x0, 0x4e, 0xa, 0x0, 0x0, 0x0, 0x0, 0xd, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x78, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	googleIntVarEvent1 = []byte{0xea, 0xa8, 0xea, 0x53, 0x5, 0x88, 0xf3, 0x0, 0x0, 0x24, 0x0, 0x0, 0x0, 0xb8, 0x6, 0x0, 0x0, 0x0, 0x0, 0xd, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x65, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	googleIntVarEvent2 = []byte{0xea, 0xa8, 0xea, 0x53, 0x5, 0x88, 0xf3, 0x0, 0x0, 0x24, 0x0, 0x0, 0x0, 0xb8, 0x6, 0x0, 0x0, 0x0, 0x0, 0xd, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x65, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
)

func TestBinlogEventEmptyBuf(t *testing.T) {
	input := binlogEvent([]byte{})
	want := false
	if got := input.IsValid(); got != want {
		t.Errorf("%#v.IsValid() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventGarbage(t *testing.T) {
	input := binlogEvent(garbageEvent)
	want := false
	if got := input.IsValid(); got != want {
		t.Errorf("%#v.IsValid() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsValid(t *testing.T) {
	input := binlogEvent(googleRotateEvent)
	want := true
	if got := input.IsValid(); got != want {
		t.Errorf("%#v.IsValid() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventTruncatedHeader(t *testing.T) {
	input := binlogEvent(googleRotateEvent[:18])
	want := false
	if got := input.IsValid(); got != want {
		t.Errorf("%#v.IsValid() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventTruncatedData(t *testing.T) {
	input := binlogEvent(googleRotateEvent[:len(googleRotateEvent)-1])
	want := false
	if got := input.IsValid(); got != want {
		t.Errorf("%#v.IsValid() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventType(t *testing.T) {
	input := binlogEvent(googleRotateEvent)
	want := byte(0x04)
	if got := input.Type(); got != want {
		t.Errorf("%#v.Type() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventFlags(t *testing.T) {
	input := binlogEvent(googleRotateEvent)
	want := uint16(0x20)
	if got := input.Flags(); got != want {
		t.Errorf("%#v.Flags() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventTimestamp(t *testing.T) {
	input := binlogEvent(googleFormatEvent)
	want := uint32(0x53e95252)
	if got := input.Timestamp(); got != want {
		t.Errorf("%#v.Timestamp() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventServerID(t *testing.T) {
	input := binlogEvent(googleFormatEvent)
	want := uint32(62344)
	if got := input.ServerID(); got != want {
		t.Errorf("%#v.ServerID() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsFormatDescription(t *testing.T) {
	input := binlogEvent(googleFormatEvent)
	want := true
	if got := input.IsFormatDescription(); got != want {
		t.Errorf("%#v.IsFormatDescription() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsNotFormatDescription(t *testing.T) {
	input := binlogEvent(googleRotateEvent)
	want := false
	if got := input.IsFormatDescription(); got != want {
		t.Errorf("%#v.IsFormatDescription() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsQuery(t *testing.T) {
	input := binlogEvent(googleQueryEvent)
	want := true
	if got := input.IsQuery(); got != want {
		t.Errorf("%#v.IsQuery() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsNotQuery(t *testing.T) {
	input := binlogEvent(googleFormatEvent)
	want := false
	if got := input.IsQuery(); got != want {
		t.Errorf("%#v.IsQuery() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsIntVar(t *testing.T) {
	input := binlogEvent(googleIntVarEvent1)
	want := true
	if got := input.IsIntVar(); got != want {
		t.Errorf("%#v.IsIntVar() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsNotIntVar(t *testing.T) {
	input := binlogEvent(googleFormatEvent)
	want := false
	if got := input.IsIntVar(); got != want {
		t.Errorf("%#v.IsIntVar() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsRotate(t *testing.T) {
	input := binlogEvent(googleRotateEvent)
	want := true
	if got := input.IsRotate(); got != want {
		t.Errorf("%#v.IsRotate() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsNotRotate(t *testing.T) {
	input := binlogEvent(googleFormatEvent)
	want := false
	if got := input.IsRotate(); got != want {
		t.Errorf("%#v.IsRotate() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsXID(t *testing.T) {
	input := binlogEvent(googleXIDEvent)
	want := true
	if got := input.IsXID(); got != want {
		t.Errorf("%#v.IsXID() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventIsNotXID(t *testing.T) {
	input := binlogEvent(googleFormatEvent)
	want := false
	if got := input.IsXID(); got != want {
		t.Errorf("%#v.IsXID() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventFormat(t *testing.T) {
	input := binlogEvent(googleFormatEvent)
	want := BinlogFormat{
		FormatVersion: 4,
		ServerVersion: "5.1.63-google-log",
		HeaderLength:  27,
		HeaderSizes:   googleFormatEvent[76 : len(googleFormatEvent)-5],
	}
	got, err := input.Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%#v.Format() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventFormatWrongVersion(t *testing.T) {
	buf := make([]byte, len(googleFormatEvent))
	copy(buf, googleFormatEvent)
	buf[19] = 5 // mess up the FormatVersion

	input := binlogEvent(buf)
	want := "format version = 5, we only support version 4"
	_, err := input.Format()
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); got != want {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestBinlogEventFormatBadHeaderLength(t *testing.T) {
	buf := make([]byte, len(googleFormatEvent))
	copy(buf, googleFormatEvent)
	buf[19+2+50+4] = 12 // mess up the HeaderLength

	input := binlogEvent(buf)
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

func TestBinlogEventQuery(t *testing.T) {
	f, err := binlogEvent(googleFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := binlogEvent(googleQueryEvent)
	want := Query{
		Database: "vt_test_keyspace",
		Charset:  &binlogdatapb.Charset{Client: 8, Conn: 8, Server: 33},
		SQL: `create table if not exists vt_a (
eid bigint,
id int,
primary key(eid, id)
) Engine=InnoDB`,
	}
	got, err := input.Query(f)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%#v.Query() = %v, want %v", input, got, want)
	}
}

func TestBinlogEventQueryBadLength(t *testing.T) {
	f, err := binlogEvent(googleFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	buf := make([]byte, len(googleQueryEvent))
	copy(buf, googleQueryEvent)
	buf[19+8+4+4] = 200 // mess up the db_name length

	input := binlogEvent(buf)
	want := "SQL query position overflows buffer (240 > 146)"
	_, err = input.Query(f)
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); got != want {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}

func TestBinlogEventIntVar1(t *testing.T) {
	f, err := binlogEvent(googleFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := binlogEvent(googleIntVarEvent1)
	wantType := byte(IntVarLastInsertID)
	wantValue := uint64(101)
	gotType, gotValue, err := input.IntVar(f)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if gotType != wantType || gotValue != wantValue {
		t.Errorf("%#v.IntVar() = (%#v, %#v), want (%#v, %#v)", input, gotType, gotValue, wantType, wantValue)
	}
}

func TestBinlogEventIntVar2(t *testing.T) {
	f, err := binlogEvent(googleFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	input := binlogEvent(googleIntVarEvent2)
	wantType := byte(IntVarInsertID)
	wantValue := uint64(101)
	gotType, gotValue, err := input.IntVar(f)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if gotType != wantType || gotValue != wantValue {
		t.Errorf("%#v.IntVar() = (%#v, %#v), want (%#v, %#v)", input, gotType, gotValue, wantType, wantValue)
	}
}

func TestBinlogEventIntVarBadID(t *testing.T) {
	f, err := binlogEvent(googleFormatEvent).Format()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	buf := make([]byte, len(googleIntVarEvent2))
	copy(buf, googleIntVarEvent2)
	buf[19+8] = 3 // mess up the variable ID

	input := binlogEvent(buf)
	want := "invalid IntVar ID: 3"
	_, _, err = input.IntVar(f)
	if err == nil {
		t.Errorf("expected error, got none")
		return
	}
	if got := err.Error(); got != want {
		t.Errorf("wrong error, got %#v, want %#v", got, want)
	}
}
