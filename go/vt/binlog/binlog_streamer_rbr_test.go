package binlog

import (
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn/replication"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/schema"

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

	// Do an insert packet with all fields set.
	insertRows := replication.Rows{
		Flags:       0x1234,
		DataColumns: replication.NewServerBitmap(2),
		Rows: []replication.Row{
			{
				NullColumns: replication.NewServerBitmap(2),
				Data: []byte{
					0x10, 0x20, 0x30, 0x40, // long
					0x04, 0x00, // len('abcd')
					'a', 'b', 'c', 'd', // 'abcd'
				},
			},
		},
	}
	insertRows.DataColumns.Set(0, true)
	insertRows.DataColumns.Set(1, true)

	// Do an update packet with all fields set.
	updateRows := replication.Rows{
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
	updateRows.IdentifyColumns.Set(0, true)
	updateRows.IdentifyColumns.Set(1, true)
	updateRows.DataColumns.Set(0, true)
	updateRows.DataColumns.Set(1, true)

	// Do a delete packet with all fields set.
	deleteRows := replication.Rows{
		Flags:           0x1234,
		IdentifyColumns: replication.NewServerBitmap(2),
		Rows: []replication.Row{
			{
				NullIdentifyColumns: replication.NewServerBitmap(2),
				Identify: []byte{
					0x10, 0x20, 0x30, 0x40, // long
					0x03, 0x00, // len('abc')
					'a', 'b', 'c', // 'abc'
				},
			},
		},
	}
	deleteRows.IdentifyColumns.Set(0, true)
	deleteRows.IdentifyColumns.Set(1, true)

	input := []replication.BinlogEvent{
		replication.NewRotateEvent(f, s, 0, ""),
		replication.NewFormatDescriptionEvent(f, s),
		replication.NewTableMapEvent(f, s, tableID, tm),
		replication.NewMariaDBGTIDEvent(f, s, replication.MariadbGTID{Domain: 0, Sequence: 0xd}, false /* hasBegin */),
		replication.NewQueryEvent(f, s, replication.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		replication.NewWriteRowsEvent(f, s, tableID, insertRows),
		replication.NewUpdateRowsEvent(f, s, tableID, updateRows),
		replication.NewDeleteRowsEvent(f, s, tableID, deleteRows),
		replication.NewXIDEvent(f, s),
	}

	events := make(chan replication.BinlogEvent)

	want := []fullBinlogTransaction{
		{
			statements: []FullBinlogStatement{
				{
					Statement: &binlogdatapb.BinlogTransaction_Statement{
						Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
						Sql:      []byte("SET TIMESTAMP=1407805592"),
					},
				},
				{
					Statement: &binlogdatapb.BinlogTransaction_Statement{
						Category: binlogdatapb.BinlogTransaction_Statement_BL_INSERT,
						Sql:      []byte("INSERT INTO vt_a SET id=1076895760, message='abcd'"),
					},
					Table: "vt_a",
				},
				{
					Statement: &binlogdatapb.BinlogTransaction_Statement{
						Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
						Sql:      []byte("SET TIMESTAMP=1407805592"),
					},
				},
				{
					Statement: &binlogdatapb.BinlogTransaction_Statement{
						Category: binlogdatapb.BinlogTransaction_Statement_BL_UPDATE,
						Sql:      []byte("UPDATE vt_a SET id=1076895760, message='abcd' WHERE id=1076895760 AND message='abc'"),
					},
					Table: "vt_a",
				},
				{
					Statement: &binlogdatapb.BinlogTransaction_Statement{
						Category: binlogdatapb.BinlogTransaction_Statement_BL_SET,
						Sql:      []byte("SET TIMESTAMP=1407805592"),
					},
				},
				{
					Statement: &binlogdatapb.BinlogTransaction_Statement{
						Category: binlogdatapb.BinlogTransaction_Statement_BL_DELETE,
						Sql:      []byte("DELETE FROM vt_a WHERE id=1076895760 AND message='abc'"),
					},
					Table: "vt_a",
				},
			},
			eventToken: &querypb.EventToken{
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
	var got []fullBinlogTransaction
	sendTransaction := func(eventToken *querypb.EventToken, statements []FullBinlogStatement) error {
		got = append(got, fullBinlogTransaction{
			eventToken: eventToken,
			statements: statements,
		})
		return nil
	}
	bls := NewStreamer("vt_test_keyspace", nil, se, nil, replication.Position{}, 0, sendTransaction)

	go sendTestEvents(events, input)
	_, err := bls.parseEvents(context.Background(), events)
	if err != ErrServerEOF {
		t.Errorf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("binlogConnStreamer.parseEvents(): got:\n%+v\nwant:\n%+v", got, want)
		for i, fbt := range got {
			t.Errorf("Got (%v)=%v", i, fbt.statements)
		}
		for i, fbt := range want {
			t.Errorf("Want(%v)=%v", i, fbt.statements)
		}
	}
}
