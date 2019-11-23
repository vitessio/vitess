/*
Copyright 2019 The Vitess Authors.

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

package mysql

import (
	"reflect"
	"testing"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// TestFormatDescriptionEvent tests both MySQL 5.6 and MariaDB 10.0
// FormatDescriptionEvent is working properly.
func TestFormatDescriptionEvent(t *testing.T) {
	// MySQL 5.6
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	event := NewFormatDescriptionEvent(f, s)
	if !event.IsValid() {
		t.Fatalf("IsValid() returned false")
	}
	if !event.IsFormatDescription() {
		t.Fatalf("IsFormatDescription returned false")
	}
	gotF, err := event.Format()
	if err != nil {
		t.Fatalf("Format failed: %v", err)
	}
	if !reflect.DeepEqual(gotF, f) {
		t.Fatalf("Parsed BinlogFormat doesn't match, got:\n%v\nexpected:\n%v", gotF, f)
	}

	// MariaDB
	f = NewMariaDBBinlogFormat()
	s = NewFakeBinlogStream()

	event = NewFormatDescriptionEvent(f, s)
	if !event.IsValid() {
		t.Fatalf("IsValid() returned false")
	}
	if !event.IsFormatDescription() {
		t.Fatalf("IsFormatDescription returned false")
	}
	gotF, err = event.Format()
	if err != nil {
		t.Fatalf("Format failed: %v", err)
	}
	if !reflect.DeepEqual(gotF, f) {
		t.Fatalf("Parsed BinlogFormat doesn't match, got:\n%v\nexpected:\n%v", gotF, f)
	}
}

func TestQueryEvent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	q := Query{
		Database: "my database",
		SQL:      "my query",
		Charset: &binlogdatapb.Charset{
			Client: 0x1234,
			Conn:   0x5678,
			Server: 0x9abc,
		},
	}
	event := NewQueryEvent(f, s, q)
	if !event.IsValid() {
		t.Fatalf("NewQueryEvent returned an invalid event")
	}
	if !event.IsQuery() {
		t.Fatalf("NewQueryEvent returned a non-query event: %v", event)
	}
	event, _, err := event.StripChecksum(f)
	if err != nil {
		t.Fatalf("StripChecksum failed: %v", err)
	}

	gotQ, err := event.Query(f)
	if err != nil {
		t.Fatalf("event.Query() failed: %v", err)
	}
	if !reflect.DeepEqual(gotQ, q) {
		t.Fatalf("event.Query() returned %v was expecting %v", gotQ, q)
	}
}

func TestXIDEvent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	event := NewXIDEvent(f, s)
	if !event.IsValid() {
		t.Fatalf("NewXIDEvent().IsValid() is false")
	}
	if !event.IsXID() {
		t.Fatalf("NewXIDEvent().IsXID() is false")
	}
}

func TestIntVarEvent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	event := NewIntVarEvent(f, s, IntVarLastInsertID, 0x123456789abcdef0)
	if !event.IsValid() {
		t.Fatalf("NewIntVarEvent().IsValid() is false")
	}
	if !event.IsIntVar() {
		t.Fatalf("NewIntVarEvent().IsIntVar() is false")
	}
	name, value, err := event.IntVar(f)
	if name != IntVarLastInsertID || value != 0x123456789abcdef0 || err != nil {
		t.Fatalf("IntVar() returned %v/%v/%v", name, value, err)
	}

	event = NewIntVarEvent(f, s, IntVarInvalidInt, 0x123456789abcdef0)
	if !event.IsValid() {
		t.Fatalf("NewIntVarEvent().IsValid() is false")
	}
	if !event.IsIntVar() {
		t.Fatalf("NewIntVarEvent().IsIntVar() is false")
	}
	name, value, err = event.IntVar(f)
	if err == nil {
		t.Fatalf("IntVar(invalid) returned %v/%v/%v", name, value, err)
	}
}

func TestInvalidEvents(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	// InvalidEvent
	event := NewInvalidEvent()
	if event.IsValid() {
		t.Fatalf("NewInvalidEvent().IsValid() is true")
	}

	// InvalidFormatDescriptionEvent
	event = NewInvalidFormatDescriptionEvent(f, s)
	if !event.IsValid() {
		t.Fatalf("NewInvalidFormatDescriptionEvent().IsValid() is false")
	}
	if !event.IsFormatDescription() {
		t.Fatalf("NewInvalidFormatDescriptionEvent().IsFormatDescription() is false")
	}
	if _, err := event.Format(); err == nil {
		t.Fatalf("NewInvalidFormatDescriptionEvent().Format() returned err=nil")
	}

	// InvalidQueryEvent
	event = NewInvalidQueryEvent(f, s)
	if !event.IsValid() {
		t.Fatalf("NewInvalidQueryEvent().IsValid() is false")
	}
	if !event.IsQuery() {
		t.Fatalf("NewInvalidQueryEvent().IsQuery() is false")
	}
	if _, err := event.Query(f); err == nil {
		t.Fatalf("NewInvalidQueryEvent().Query() returned err=nil")
	}
}

func TestMariadDBGTIDEVent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()
	s.ServerID = 0x87654321

	// With built-in begin.
	event := NewMariaDBGTIDEvent(f, s, MariadbGTID{Domain: 0, Sequence: 0x123456789abcdef0}, true)
	if !event.IsValid() {
		t.Fatalf("NewMariaDBGTIDEvent().IsValid() is false")
	}
	if !event.IsGTID() {
		t.Fatalf("NewMariaDBGTIDEvent().IsGTID() if false")
	}
	event, _, err := event.StripChecksum(f)
	if err != nil {
		t.Fatalf("StripChecksum failed: %v", err)
	}

	gtid, hasBegin, err := event.GTID(f)
	if err != nil {
		t.Fatalf("NewMariaDBGTIDEvent().GTID() returned error: %v", err)
	}
	if !hasBegin {
		t.Fatalf("NewMariaDBGTIDEvent() didn't store hasBegin properly.")
	}
	mgtid, ok := gtid.(MariadbGTID)
	if !ok {
		t.Fatalf("NewMariaDBGTIDEvent().GTID() returned a non-MariaDBGTID GTID")
	}
	if mgtid.Domain != 0 || mgtid.Server != 0x87654321 || mgtid.Sequence != 0x123456789abcdef0 {
		t.Fatalf("NewMariaDBGTIDEvent().GTID() returned invalid GITD: %v", mgtid)
	}

	// Without built-in begin.
	event = NewMariaDBGTIDEvent(f, s, MariadbGTID{Domain: 0, Sequence: 0x123456789abcdef0}, false)
	if !event.IsValid() {
		t.Fatalf("NewMariaDBGTIDEvent().IsValid() is false")
	}
	if !event.IsGTID() {
		t.Fatalf("NewMariaDBGTIDEvent().IsGTID() if false")
	}
	event, _, err = event.StripChecksum(f)
	if err != nil {
		t.Fatalf("StripChecksum failed: %v", err)
	}

	gtid, hasBegin, err = event.GTID(f)
	if err != nil {
		t.Fatalf("NewMariaDBGTIDEvent().GTID() returned error: %v", err)
	}
	if hasBegin {
		t.Fatalf("NewMariaDBGTIDEvent() didn't store hasBegin properly.")
	}
	mgtid, ok = gtid.(MariadbGTID)
	if !ok {
		t.Fatalf("NewMariaDBGTIDEvent().GTID() returned a non-MariaDBGTID GTID")
	}
	if mgtid.Domain != 0 || mgtid.Server != 0x87654321 || mgtid.Sequence != 0x123456789abcdef0 {
		t.Fatalf("NewMariaDBGTIDEvent().GTID() returned invalid GITD: %v", mgtid)
	}
}

func TestTableMapEvent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	tm := &TableMap{
		Flags:    0x8090,
		Database: "my_database",
		Name:     "my_table",
		Types: []byte{
			TypeLongLong,
			TypeLongLong,
			TypeLongLong,
			TypeLongLong,
			TypeLongLong,
			TypeTime,
			TypeLongLong,
			TypeLongLong,
			TypeLongLong,
			TypeVarchar,
		},
		CanBeNull: NewServerBitmap(10),
		Metadata: []uint16{
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			0,
			384, // Length of the varchar field.
		},
	}
	tm.CanBeNull.Set(1, true)
	tm.CanBeNull.Set(2, true)
	tm.CanBeNull.Set(5, true)
	tm.CanBeNull.Set(9, true)

	event := NewTableMapEvent(f, s, 0x102030405060, tm)
	if !event.IsValid() {
		t.Fatalf("NewTableMapEvent().IsValid() is false")
	}
	if !event.IsTableMap() {
		t.Fatalf("NewTableMapEvent().IsTableMap() if false")
	}

	event, _, err := event.StripChecksum(f)
	if err != nil {
		t.Fatalf("StripChecksum failed: %v", err)
	}

	tableID := event.TableID(f)
	if tableID != 0x102030405060 {
		t.Fatalf("NewTableMapEvent().TableID returned %x", tableID)
	}
	gotTm, err := event.TableMap(f)
	if err != nil {
		t.Fatalf("NewTableMapEvent().TableMapEvent() returned error: %v", err)
	}
	if !reflect.DeepEqual(gotTm, tm) {
		t.Fatalf("NewTableMapEvent().TableMapEvent() got TableMap:\n%v\nexpected:\n%v", gotTm, tm)
	}
}

func TestRowsEvent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	tableID := uint64(0x102030405060)

	tm := &TableMap{
		Flags:    0x8090,
		Database: "my_database",
		Name:     "my_table",
		Types: []byte{
			TypeLong,
			TypeVarchar,
		},
		CanBeNull: NewServerBitmap(2),
		Metadata: []uint16{
			0,
			384,
		},
	}
	tm.CanBeNull.Set(1, true)

	// Do an update packet with all fields set.
	rows := Rows{
		Flags:           0x1234,
		IdentifyColumns: NewServerBitmap(2),
		DataColumns:     NewServerBitmap(2),
		Rows: []Row{
			{
				NullIdentifyColumns: NewServerBitmap(2),
				NullColumns:         NewServerBitmap(2),
				Identify: []byte{
					0x10, 0x20, 0x30, 0x40, // long
					0x03, 0x00, // len('abc')
					'a', 'b', 'c', // 'abc'
				},
				Data: []byte{
					0x10, 0x20, 0x30, 0x40, // long
					0x04, 0x00, // len('abcd')
					'a', 'b', 'c', 'd', // 'abcd'
				},
			},
		},
	}

	// All rows are included, none are NULL.
	rows.IdentifyColumns.Set(0, true)
	rows.IdentifyColumns.Set(1, true)
	rows.DataColumns.Set(0, true)
	rows.DataColumns.Set(1, true)

	// Test the Rows we just created, to be sure.
	// 1076895760 is 0x40302010.
	identifies, _ := rows.StringIdentifiesForTests(tm, 0)
	if expected := []string{"1076895760", "abc"}; !reflect.DeepEqual(identifies, expected) {
		t.Fatalf("bad Rows identify, got %v expected %v", identifies, expected)
	}
	values, _ := rows.StringValuesForTests(tm, 0)
	if expected := []string{"1076895760", "abcd"}; !reflect.DeepEqual(values, expected) {
		t.Fatalf("bad Rows data, got %v expected %v", values, expected)
	}

	event := NewUpdateRowsEvent(f, s, 0x102030405060, rows)
	if !event.IsValid() {
		t.Fatalf("NewRowsEvent().IsValid() is false")
	}
	if !event.IsUpdateRows() {
		t.Fatalf("NewRowsEvent().IsUpdateRows() if false")
	}

	event, _, err := event.StripChecksum(f)
	if err != nil {
		t.Fatalf("StripChecksum failed: %v", err)
	}

	tableID = event.TableID(f)
	if tableID != 0x102030405060 {
		t.Fatalf("NewRowsEvent().TableID returned %x", tableID)
	}
	gotRows, err := event.Rows(f, tm)
	if err != nil {
		t.Fatalf("NewRowsEvent().Rows() returned error: %v", err)
	}
	if !reflect.DeepEqual(gotRows, rows) {
		t.Fatalf("NewRowsEvent().Rows() got Rows:\n%v\nexpected:\n%v", gotRows, rows)
	}
}
