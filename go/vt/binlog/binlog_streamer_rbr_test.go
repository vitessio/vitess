package binlog

import (
	"fmt"
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn/replication"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/tabletserver/engines/schema"

	binlogdatapb "github.com/youtube/vitess/go/vt/proto/binlogdata"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

// This file tests the RBR events are parsed correctly.

func TestStreamerParseRBRUpdateEvent(t *testing.T) {
	f := replication.NewMySQL56BinlogFormat()
	s := replication.NewFakeBinlogStream()
	s.ServerID = 62344

	// Create a schema.Engine for this test, with just one table.
	// We only use the Columns.
	se := schema.NewEngineForTests()
	se.SetTableForTests(&schema.Table{
		Name: sqlparser.NewTableIdent("vt_a"),
		Columns: []schema.TableColumn{
			{
				Name: sqlparser.NewColIdent("id"),
				Type: querypb.Type_INT64,
			},
			{
				Name: sqlparser.NewColIdent("message"),
				Type: querypb.Type_VARCHAR,
			},
		},
	})

	// Create a tableMap event on the table.
	tableID := uint64(0x102030405060)
	tm := &replication.TableMap{
		Flags:    0x8090,
		Database: "vt_test_keyspace",
		Name:     "vt_a",
		Types: []byte{
			replication.TypeLong,
			replication.TypeVarchar,
		},
		CanBeNull: replication.NewServerBitmap(2),
		Metadata: []uint16{
			0,
			384, // A VARCHAR(128) in utf8 would result in 384.
		},
	}
	tm.CanBeNull.Set(1, true)

	// Do an update packet with all fields set.
	rows := replication.Rows{
		Flags:           0x1234,
		IdentifyColumns: replication.NewServerBitmap(2),
		DataColumns:     replication.NewServerBitmap(2),
		Rows: []replication.Row{
			{
				NullIdentifyColumns: replication.NewServerBitmap(2),
				NullColumns:         replication.NewServerBitmap(2),
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
	rows.IdentifyColumns.Set(0, true)
	rows.IdentifyColumns.Set(1, true)
	rows.DataColumns.Set(0, true)
	rows.DataColumns.Set(1, true)

	input := []replication.BinlogEvent{
		replication.NewRotateEvent(f, s, 0, ""),
		replication.NewFormatDescriptionEvent(f, s),
		replication.NewTableMapEvent(f, s, tableID, tm),
		replication.NewMariaDBGTIDEvent(f, s, replication.MariadbGTID{Domain: 0, Sequence: 0xd}, false /* hasBegin */),
		replication.NewQueryEvent(f, s, replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		replication.NewUpdateRowsEvent(f, s, tableID, rows),
		replication.NewXIDEvent(f, s),
	}

	events := make(chan replication.BinlogEvent)

	want := []binlogdatapb.BinlogTransaction{
		{
			Statements: []*binlogdatapb.BinlogTransaction_Statement{
				{
					Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
					Sql:      []byte("SET TIMESTAMP=1407805592"),
				},
				{
					Category: binlogdatapb.BinlogTransaction_Statement_BL_UPDATE,
					Sql:      []byte("UPDATE vt_a SET id=1076895760, message='abcd' WHERE id=1076895760 AND message='abc'"),
				},
			},
			EventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: replication.EncodePosition(replication.Position{
					GTIDSet: replication.MariadbGTID{
						Domain:   0,
						Server:   62344,
						Sequence: 0x0d,
					},
				}),
			},
		},
	}
	var got []binlogdatapb.BinlogTransaction
	sendTransaction := func(trans *binlogdatapb.BinlogTransaction) error {
		got = append(got, *trans)
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, se, nil, replication.Position{}, 0, sendTransaction)

	go sendTestEvents(events, input)
	_, err := bls.parseEvents(context.Background(), events)
	if err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got:\n%v\nwant:\n%v", got, want)
	}
}

func TestCellAsSQL(t *testing.T) {
	testcases := []struct {
		typ      byte
		metadata uint16
		styp     querypb.Type
		data     []byte
		out      string
	}{{
		// TypeTiny tests, unsigned and signed.
		typ:  replication.TypeTiny,
		styp: querypb.Type_UINT8,
		data: []byte{0x0a},
		out:  "10",
	}, {
		typ:  replication.TypeTiny,
		styp: querypb.Type_UINT8,
		data: []byte{0x82},
		out:  "130",
	}, {
		typ:  replication.TypeTiny,
		styp: querypb.Type_INT8,
		data: []byte{0x82},
		out:  "-126",
	}, {
		// TypeYear is always unsigned.
		typ:  replication.TypeYear,
		styp: querypb.Type_YEAR,
		data: []byte{0x82},
		out:  "2030",
	}, {
		// TypeShort tests, unsigned and signed.
		typ:  replication.TypeShort,
		styp: querypb.Type_UINT16,
		data: []byte{0x02, 0x01},
		out:  fmt.Sprintf("%v", 0x0102),
	}, {
		typ:  replication.TypeShort,
		styp: querypb.Type_UINT16,
		data: []byte{0x81, 0x82},
		out:  fmt.Sprintf("%v", 0x8281),
	}, {
		typ:  replication.TypeShort,
		styp: querypb.Type_INT16,
		data: []byte{0xfe, 0xff},
		out:  "-2",
	}, {
		// TypeInt24 tests, unsigned and signed.
		typ:  replication.TypeInt24,
		styp: querypb.Type_UINT24,
		data: []byte{0x03, 0x02, 0x01},
		out:  fmt.Sprintf("%v", 0x010203),
	}, {
		typ:  replication.TypeInt24,
		styp: querypb.Type_UINT24,
		data: []byte{0x81, 0x82, 0x83},
		out:  fmt.Sprintf("%v", 0x838281),
	}, {
		typ:  replication.TypeInt24,
		styp: querypb.Type_INT24,
		data: []byte{0xfe, 0xff, 0xff},
		out:  "-2",
	}, {
		// TypeLong tests, unsigned and signed.
		typ:  replication.TypeLong,
		styp: querypb.Type_UINT32,
		data: []byte{0x04, 0x03, 0x02, 0x01},
		out:  fmt.Sprintf("%v", 0x01020304),
	}, {
		typ:  replication.TypeLong,
		styp: querypb.Type_UINT32,
		data: []byte{0x81, 0x82, 0x83, 0x84},
		out:  fmt.Sprintf("%v", 0x84838281),
	}, {
		typ:  replication.TypeLong,
		styp: querypb.Type_INT32,
		data: []byte{0xfe, 0xff, 0xff, 0xff},
		out:  "-2",
	}, {
		// TypeTimestamp tests.
		typ:  replication.TypeTimestamp,
		styp: querypb.Type_TIMESTAMP,
		data: []byte{0x84, 0x83, 0x82, 0x81},
		out:  fmt.Sprintf("%v", 0x81828384),
	}, {
		// TypeLongLong tests, unsigned and signed.
		typ:  replication.TypeLongLong,
		styp: querypb.Type_UINT64,
		data: []byte{0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01},
		out:  fmt.Sprintf("%v", 0x0102030405060708),
	}, {
		typ:  replication.TypeLongLong,
		styp: querypb.Type_UINT64,
		data: []byte{0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88},
		out:  fmt.Sprintf("%v", uint64(0x8887868584838281)),
	}, {
		typ:  replication.TypeLongLong,
		styp: querypb.Type_INT64,
		data: []byte{0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		out:  "-2",
	}, {
		// TypeDate and TypeNewDate tests, unsigned and signed.
		// 2010 << 9 + 10 << 5 + 3 = 1029443 = 0x0fb543
		typ:  replication.TypeDate,
		styp: querypb.Type_DATE,
		data: []byte{0x43, 0xb5, 0x0f},
		out:  "'2010-10-03'",
	}, {
		typ:  replication.TypeNewDate,
		styp: querypb.Type_DATE,
		data: []byte{0x43, 0xb5, 0x0f},
		out:  "'2010-10-03'",
	}, {
		// TypeTime tests.
		// 154532 = 0x00025ba4
		typ:  replication.TypeTime,
		styp: querypb.Type_TIME,
		data: []byte{0xa4, 0x5b, 0x02, 0x00},
		out:  "'15:45:32'",
	}, {
		// TypeDateTime tests.
		// 19840304154532 = 0x120b6e4807a4
		typ:  replication.TypeDateTime,
		styp: querypb.Type_DATETIME,
		data: []byte{0xa4, 0x07, 0x48, 0x6e, 0x0b, 0x12, 0x00, 0x00},
		out:  "'1984-03-04 15:45:32'",
	}, {
		// Varchar
		typ:      replication.TypeVarchar,
		metadata: 30,
		styp:     querypb.Type_VARCHAR,
		data:     []byte{3, 'a', 'b', 'c'},
		out:      "'abc'",
	}, {
		typ:      replication.TypeVarchar,
		metadata: 300,
		styp:     querypb.Type_VARCHAR,
		data:     []byte{3, 0, 'a', '\'', 'c'},
		out:      "'a\\'c'",
	}, {
		// Bit
		typ:      replication.TypeBit,
		metadata: 0x0107,
		styp:     querypb.Type_BIT,
		data:     []byte{0x03, 0x01},
		out:      "b'0000001100000001'",
	}, {
		// Timestamp2
		typ:      replication.TypeTimestamp2,
		metadata: 0,
		styp:     querypb.Type_TIMESTAMP,
		data:     []byte{0x84, 0x83, 0x82, 0x81},
		out:      fmt.Sprintf("%v", 0x81828384),
	}, {
		typ:      replication.TypeTimestamp2,
		metadata: 1,
		styp:     querypb.Type_TIMESTAMP,
		data:     []byte{0x84, 0x83, 0x82, 0x81, 7},
		out:      fmt.Sprintf("%v.7", 0x81828384),
	}, {
		typ:      replication.TypeTimestamp2,
		metadata: 2,
		styp:     querypb.Type_TIMESTAMP,
		data:     []byte{0x84, 0x83, 0x82, 0x81, 76},
		out:      fmt.Sprintf("%v.76", 0x81828384),
	}, {
		typ:      replication.TypeTimestamp2,
		metadata: 3,
		styp:     querypb.Type_TIMESTAMP,
		// 765 = 0x02fd
		data: []byte{0x84, 0x83, 0x82, 0x81, 0xfd, 0x02},
		out:  fmt.Sprintf("%v.765", 0x81828384),
	}, {
		typ:      replication.TypeTimestamp2,
		metadata: 4,
		styp:     querypb.Type_TIMESTAMP,
		// 7654 = 0x1de6
		data: []byte{0x84, 0x83, 0x82, 0x81, 0xe6, 0x1d},
		out:  fmt.Sprintf("%v.7654", 0x81828384),
	}, {
		typ:      replication.TypeTimestamp2,
		metadata: 5,
		styp:     querypb.Type_TIMESTAMP,
		// 76543 = 0x012aff
		data: []byte{0x84, 0x83, 0x82, 0x81, 0xff, 0x2a, 0x01},
		out:  fmt.Sprintf("%v.76543", 0x81828384),
	}, {
		typ:      replication.TypeTimestamp2,
		metadata: 6,
		styp:     querypb.Type_TIMESTAMP,
		// 765432 = 0x0badf8
		data: []byte{0x84, 0x83, 0x82, 0x81, 0xf8, 0xad, 0x0b},
		out:  fmt.Sprintf("%v.765432", 0x81828384),
	}, {
		// DateTime2
		typ:      replication.TypeDateTime2,
		metadata: 0,
		styp:     querypb.Type_DATETIME,
		// (2012 * 13 + 6) << 22 + 21 << 17 + 15 << 12 + 45 << 6 + 17)
		// = 109734198097 = 0x198caafb51
		// Then have to add 0x8000000000 = 0x998caafb51
		data: []byte{0x51, 0xfb, 0xaa, 0x8c, 0x99},
		out:  "'2012-06-21 15:45:17'",
	}, {
		typ:      replication.TypeDateTime2,
		metadata: 1,
		styp:     querypb.Type_DATETIME,
		data:     []byte{0x51, 0xfb, 0xaa, 0x8c, 0x99, 7},
		out:      "'2012-06-21 15:45:17.7'",
	}, {
		typ:      replication.TypeDateTime2,
		metadata: 2,
		styp:     querypb.Type_DATETIME,
		data:     []byte{0x51, 0xfb, 0xaa, 0x8c, 0x99, 76},
		out:      "'2012-06-21 15:45:17.76'",
	}, {
		typ:      replication.TypeDateTime2,
		metadata: 3,
		styp:     querypb.Type_DATETIME,
		// 765 = 0x02fd
		data: []byte{0x51, 0xfb, 0xaa, 0x8c, 0x99, 0xfd, 0x02},
		out:  "'2012-06-21 15:45:17.765'",
	}, {
		typ:      replication.TypeDateTime2,
		metadata: 4,
		styp:     querypb.Type_DATETIME,
		// 7654 = 0x1de6
		data: []byte{0x51, 0xfb, 0xaa, 0x8c, 0x99, 0xe6, 0x1d},
		out:  "'2012-06-21 15:45:17.7654'",
	}, {
		typ:      replication.TypeDateTime2,
		metadata: 5,
		styp:     querypb.Type_DATETIME,
		// 76543 = 0x012aff
		data: []byte{0x51, 0xfb, 0xaa, 0x8c, 0x99, 0xff, 0x2a, 0x01},
		out:  "'2012-06-21 15:45:17.76543'",
	}, {
		typ:      replication.TypeDateTime2,
		metadata: 6,
		styp:     querypb.Type_DATETIME,
		// 765432 = 0x0badf8
		data: []byte{0x51, 0xfb, 0xaa, 0x8c, 0x99, 0xf8, 0xad, 0x0b},
		out:  "'2012-06-21 15:45:17.765432'",
	}, {
		// Time2
		typ:      replication.TypeTime2,
		metadata: 2,
		styp:     querypb.Type_TIME,
		data:     []byte{0xff, 0xff, 0x7f, 0x9d},
		out:      "'-00:00:00.99'",
	}, {
		typ:      replication.TypeTime2,
		metadata: 2,
		styp:     querypb.Type_TIME,
		data:     []byte{0xff, 0xff, 0x7f, 0x9d},
		out:      "'-00:00:00.99'",
	}, {
		typ:      replication.TypeTime2,
		metadata: 4,
		styp:     querypb.Type_TIME,
		data:     []byte{0xfe, 0xff, 0x7f, 0xff, 0xff},
		out:      "'-00:00:01.0001'",
	}, {
		typ:      replication.TypeTime2,
		metadata: 0,
		// 15 << 12 + 34 << 6 + 54 = 63670 = 0x00f8b6
		// and need to add 0x800000
		styp: querypb.Type_TIME,
		data: []byte{0xb6, 0xf8, 0x80},
		out:  "'15:34:54'",
	}}

	for _, tcase := range testcases {
		// Copy the data into a larger buffer (one extra byte
		// on both sides), so we make sure the 'pos' field works.
		padded := make([]byte, len(tcase.data)+2)
		copy(padded[1:], tcase.data)

		out, l, err := cellAsSQL(padded, 1, tcase.typ, tcase.metadata, tcase.styp)
		if err != nil || l != len(tcase.data) || out != tcase.out {
			t.Errorf("testcase cellAsSQL(%v,%v,%v) returned unexpected result: %v %v %v, was expecting %v %v <nil>", tcase.typ, tcase.styp, tcase.data, out, l, err, tcase.out, len(tcase.data))
		}
	}
}
