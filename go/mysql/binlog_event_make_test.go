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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/replication"

	"vitess.io/vitess/go/mysql/binlog"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// TestFormatDescriptionEvent tests both MySQL 5.6 and MariaDB 10.0
// FormatDescriptionEvent is working properly.
func TestFormatDescriptionEvent(t *testing.T) {
	// MySQL 5.6
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	event := NewFormatDescriptionEvent(f, s)
	require.True(t, event.IsValid(), "IsValid() returned false")
	require.True(t, event.IsFormatDescription(), "IsFormatDescription returned false")

	gotF, err := event.Format()
	require.NoError(t, err, "Format failed: %v", err)
	require.True(t, reflect.DeepEqual(gotF, f), "Parsed BinlogFormat doesn't match, got:\n%v\nexpected:\n%v", gotF, f)

	// MariaDB
	f = NewMariaDBBinlogFormat()
	s = NewFakeBinlogStream()

	event = NewFormatDescriptionEvent(f, s)
	require.True(t, event.IsValid(), "IsValid() returned false")
	require.True(t, event.IsFormatDescription(), "IsFormatDescription returned false")

	gotF, err = event.Format()
	require.NoError(t, err, "Format failed: %v", err)
	require.True(t, reflect.DeepEqual(gotF, f), "Parsed BinlogFormat doesn't match, got:\n%v\nexpected:\n%v", gotF, f)

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
	require.True(t, event.IsValid(), "NewQueryEvent returned an invalid event")
	require.True(t, event.IsQuery(), "NewQueryEvent returned a non-query event: %v", event)

	event, _, err := event.StripChecksum(f)
	require.NoError(t, err, "StripChecksum failed: %v", err)

	gotQ, err := event.Query(f)
	require.NoError(t, err, "event.Query() failed: %v", err)
	require.True(t, reflect.DeepEqual(gotQ, q), "event.Query() returned %v was expecting %v", gotQ, q)

}

func TestXIDEvent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	event := NewXIDEvent(f, s)
	require.True(t, event.IsValid(), "NewXIDEvent().IsValid() is false")
	require.True(t, event.IsXID(), "NewXIDEvent().IsXID() is false")

}

func TestIntVarEvent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	event := NewIntVarEvent(f, s, IntVarLastInsertID, 0x123456789abcdef0)
	require.True(t, event.IsValid(), "NewIntVarEvent().IsValid() is false")
	require.True(t, event.IsIntVar(), "NewIntVarEvent().IsIntVar() is false")

	name, value, err := event.IntVar(f)
	if name != IntVarLastInsertID || value != 0x123456789abcdef0 || err != nil {
		t.Fatalf("IntVar() returned %v/%v/%v", name, value, err)
	}

	event = NewIntVarEvent(f, s, IntVarInvalidInt, 0x123456789abcdef0)
	require.True(t, event.IsValid(), "NewIntVarEvent().IsValid() is false")
	require.True(t, event.IsIntVar(), "NewIntVarEvent().IsIntVar() is false")

	name, value, err = event.IntVar(f)
	require.Error(t, err, "IntVar(invalid) returned %v/%v/%v", name, value, err)

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
	require.True(t, event.IsValid(), "NewInvalidFormatDescriptionEvent().IsValid() is false")
	require.True(t, event.IsFormatDescription(), "NewInvalidFormatDescriptionEvent().IsFormatDescription() is false")

	if _, err := event.Format(); err == nil {
		t.Fatalf("NewInvalidFormatDescriptionEvent().Format() returned err=nil")
	}

	// InvalidQueryEvent
	event = NewInvalidQueryEvent(f, s)
	require.True(t, event.IsValid(), "NewInvalidQueryEvent().IsValid() is false")
	require.True(t, event.IsQuery(), "NewInvalidQueryEvent().IsQuery() is false")

	if _, err := event.Query(f); err == nil {
		t.Fatalf("NewInvalidQueryEvent().Query() returned err=nil")
	}
}

func TestMariadDBGTIDEVent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()
	s.ServerID = 0x87654321

	// With built-in begin.
	event := NewMariaDBGTIDEvent(f, s, replication.MariadbGTID{Domain: 0, Sequence: 0x123456789abcdef0}, true)
	require.True(t, event.IsValid(), "NewMariaDBGTIDEvent().IsValid() is false")
	require.True(t, event.IsGTID(), "NewMariaDBGTIDEvent().IsGTID() if false")

	event, _, err := event.StripChecksum(f)
	require.NoError(t, err, "StripChecksum failed: %v", err)

	gtid, hasBegin, err := event.GTID(f)
	require.NoError(t, err, "NewMariaDBGTIDEvent().GTID() returned error: %v", err)
	require.True(t, hasBegin, "NewMariaDBGTIDEvent() didn't store hasBegin properly.")

	mgtid, ok := gtid.(replication.MariadbGTID)
	require.True(t, ok, "NewMariaDBGTIDEvent().GTID() returned a non-MariaDBGTID GTID")

	if mgtid.Domain != 0 || mgtid.Server != 0x87654321 || mgtid.Sequence != 0x123456789abcdef0 {
		t.Fatalf("NewMariaDBGTIDEvent().GTID() returned invalid GITD: %v", mgtid)
	}

	// Without built-in begin.
	event = NewMariaDBGTIDEvent(f, s, replication.MariadbGTID{Domain: 0, Sequence: 0x123456789abcdef0}, false)
	require.True(t, event.IsValid(), "NewMariaDBGTIDEvent().IsValid() is false")
	require.True(t, event.IsGTID(), "NewMariaDBGTIDEvent().IsGTID() if false")

	event, _, err = event.StripChecksum(f)
	require.NoError(t, err, "StripChecksum failed: %v", err)

	gtid, hasBegin, err = event.GTID(f)
	require.NoError(t, err, "NewMariaDBGTIDEvent().GTID() returned error: %v", err)
	require.False(t, hasBegin, "NewMariaDBGTIDEvent() didn't store hasBegin properly.")

	mgtid, ok = gtid.(replication.MariadbGTID)
	require.True(t, ok, "NewMariaDBGTIDEvent().GTID() returned a non-MariaDBGTID GTID")

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
			binlog.TypeLongLong,
			binlog.TypeLongLong,
			binlog.TypeLongLong,
			binlog.TypeLongLong,
			binlog.TypeLongLong,
			binlog.TypeTime,
			binlog.TypeLongLong,
			binlog.TypeLongLong,
			binlog.TypeLongLong,
			binlog.TypeVarchar,
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
		ColumnCollationIDs: []collations.ID{},
	}
	tm.CanBeNull.Set(1, true)
	tm.CanBeNull.Set(2, true)
	tm.CanBeNull.Set(5, true)
	tm.CanBeNull.Set(9, true)

	event := NewTableMapEvent(f, s, 0x102030405060, tm)
	require.True(t, event.IsValid(), "NewTableMapEvent().IsValid() is false")
	require.True(t, event.IsTableMap(), "NewTableMapEvent().IsTableMap() if false")

	event, _, err := event.StripChecksum(f)
	require.NoError(t, err, "StripChecksum failed: %v", err)

	tableID := event.TableID(f)
	require.Equal(t, uint64(0x102030405060), tableID, "NewTableMapEvent().ID returned %x", tableID)

	gotTm, err := event.TableMap(f)
	require.NoError(t, err, "NewTableMapEvent().TableMapEvent() returned error: %v", err)
	require.True(t, reflect.DeepEqual(gotTm, tm), "NewTableMapEvent().TableMapEvent() got TableMap:\n%v\nexpected:\n%v", gotTm, tm)

}

func TestLargeTableMapEvent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	colLen := 256
	types := make([]byte, 0, colLen)
	metadata := make([]uint16, 0, colLen)

	for i := 0; i < colLen; i++ {
		types = append(types, binlog.TypeLongLong)
		metadata = append(metadata, 0)
	}

	tm := &TableMap{
		Flags:              0x8090,
		Database:           "my_database",
		Name:               "my_table",
		Types:              types,
		CanBeNull:          NewServerBitmap(colLen),
		Metadata:           metadata,
		ColumnCollationIDs: []collations.ID{},
	}
	tm.CanBeNull.Set(1, true)
	tm.CanBeNull.Set(2, true)
	tm.CanBeNull.Set(5, true)
	tm.CanBeNull.Set(9, true)

	event := NewTableMapEvent(f, s, 0x102030405060, tm)
	require.True(t, event.IsValid(), "NewTableMapEvent().IsValid() is false")
	require.True(t, event.IsTableMap(), "NewTableMapEvent().IsTableMap() if false")

	event, _, err := event.StripChecksum(f)
	require.NoError(t, err, "StripChecksum failed: %v", err)

	tableID := event.TableID(f)
	require.Equal(t, uint64(0x102030405060), tableID, "NewTableMapEvent().ID returned %x", tableID)

	gotTm, err := event.TableMap(f)
	require.NoError(t, err, "NewTableMapEvent().TableMapEvent() returned error: %v", err)
	require.True(t, reflect.DeepEqual(gotTm, tm), "NewTableMapEvent().TableMapEvent() got TableMap:\n%v\nexpected:\n%v", gotTm, tm)

}

func TestRowsEvent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	/*
		    Reason for nolint
		    Used in line 384 to 387
		    tableID = event.ID(f)
				if tableID != 0x102030405060 {
					t.Fatalf("NewRowsEvent().ID returned %x", tableID)
				}
	*/
	tableID := uint64(0x102030405060) //nolint

	tm := &TableMap{
		Flags:    0x8090,
		Database: "my_database",
		Name:     "my_table",
		Types: []byte{
			binlog.TypeLong,
			binlog.TypeVarchar,
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
	require.True(t, event.IsValid(), "NewRowsEvent().IsValid() is false")
	require.True(t, event.IsUpdateRows(), "NewRowsEvent().IsUpdateRows() if false")

	event, _, err := event.StripChecksum(f)
	require.NoError(t, err, "StripChecksum failed: %v", err)

	tableID = event.TableID(f)
	require.Equal(t, uint64(0x102030405060), tableID, "NewRowsEvent().ID returned %x", tableID)

	gotRows, err := event.Rows(f, tm)
	require.NoError(t, err, "NewRowsEvent().Rows() returned error: %v", err)
	require.True(t, reflect.DeepEqual(gotRows, rows), "NewRowsEvent().Rows() got Rows:\n%v\nexpected:\n%v", gotRows, rows)

	assert.NotZero(t, event.Timestamp())
}

func TestHeartbeatEvent(t *testing.T) {
	// MySQL 5.6
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()
	event := NewHeartbeatEvent(f, s)
	require.NotNil(t, event)
	assert.True(t, event.IsHeartbeat())
	assert.Zero(t, event.Timestamp())
}

func TestRotateRotateEvent(t *testing.T) {
	// MySQL 5.6
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()
	event := NewRotateEvent(f, s, 456, "mysql-bin.000123")
	require.NotNil(t, event)
	assert.True(t, event.IsRotate())
	nextFile, pos, err := event.NextLogFile(f)
	assert.NoError(t, err)
	assert.Equal(t, 456, int(pos))
	assert.Equal(t, "mysql-bin.000123", nextFile)
}

func TestFakeRotateEvent(t *testing.T) {
	// MySQL 5.6
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()
	event := NewFakeRotateEvent(f, s, "mysql-bin.000123")
	require.NotNil(t, event)
	assert.True(t, event.IsRotate())
	nextFile, pos, err := event.NextLogFile(f)
	assert.NoError(t, err)
	assert.Equal(t, 4, int(pos))
	assert.Equal(t, "mysql-bin.000123", nextFile)
}
func TestLargeRowsEvent(t *testing.T) {
	f := NewMySQL56BinlogFormat()
	s := NewFakeBinlogStream()

	/*
		    Reason for nolint
		    Used in line 384 to 387
		    tableID = event.ID(f)
				if tableID != 0x102030405060 {
					t.Fatalf("NewRowsEvent().ID returned %x", tableID)
				}
	*/
	tableID := uint64(0x102030405060) //nolint

	colLen := 256
	types := make([]byte, 0, colLen)
	metadata := make([]uint16, 0, colLen)

	for i := 0; i < colLen; i++ {
		types = append(types, binlog.TypeLong)
		metadata = append(metadata, 0)
	}

	tm := &TableMap{
		Flags:     0x8090,
		Database:  "my_database",
		Name:      "my_table",
		Types:     types,
		CanBeNull: NewServerBitmap(colLen),
		Metadata:  metadata,
	}
	tm.CanBeNull.Set(1, true)

	identify := make([]byte, 0, colLen*4)
	data := make([]byte, 0, colLen*4)
	for i := 0; i < colLen; i++ {
		identify = append(identify, 0x10, 0x20, 0x30, 0x40)
		data = append(data, 0x10, 0x20, 0x30, 0x40)
	}

	// Do an update packet with all fields set.
	rows := Rows{
		Flags:           0x1234,
		IdentifyColumns: NewServerBitmap(colLen),
		DataColumns:     NewServerBitmap(colLen),
		Rows: []Row{
			{
				NullIdentifyColumns: NewServerBitmap(colLen),
				NullColumns:         NewServerBitmap(colLen),
				Identify:            identify,
				Data:                data,
			},
		},
	}

	// All rows are included, none are NULL.
	for i := 0; i < colLen; i++ {
		rows.IdentifyColumns.Set(i, true)
		rows.DataColumns.Set(i, true)
	}

	// Test the Rows we just created, to be sure.
	// 1076895760 is 0x40302010.
	identifies, _ := rows.StringIdentifiesForTests(tm, 0)
	expected := make([]string, 0, colLen)
	for i := 0; i < colLen; i++ {
		expected = append(expected, "1076895760")
	}
	if !reflect.DeepEqual(identifies, expected) {
		t.Fatalf("bad Rows identify, got %v expected %v", identifies, expected)
	}
	values, _ := rows.StringValuesForTests(tm, 0)
	if !reflect.DeepEqual(values, expected) {
		t.Fatalf("bad Rows data, got %v expected %v", values, expected)
	}

	event := NewUpdateRowsEvent(f, s, 0x102030405060, rows)
	require.True(t, event.IsValid(), "NewRowsEvent().IsValid() is false")
	require.True(t, event.IsUpdateRows(), "NewRowsEvent().IsUpdateRows() if false")

	event, _, err := event.StripChecksum(f)
	require.NoError(t, err, "StripChecksum failed: %v", err)

	tableID = event.TableID(f)
	require.Equal(t, uint64(0x102030405060), tableID, "NewRowsEvent().ID returned %x", tableID)

	gotRows, err := event.Rows(f, tm)
	require.NoError(t, err, "NewRowsEvent().Rows() returned error: %v", err)
	require.True(t, reflect.DeepEqual(gotRows, rows), "NewRowsEvent().Rows() got Rows:\n%v\nexpected:\n%v", gotRows, rows)

}
