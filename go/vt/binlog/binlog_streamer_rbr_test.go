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

package binlog

import (
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/schema"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

// This file tests the RBR events are parsed correctly.

func TestStreamerParseRBREvents(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()
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
	tm := &mysql.TableMap{
		Flags:    0x8090,
		Database: "vt_test_keyspace",
		Name:     "vt_a",
		Types: []byte{
			mysql.TypeLong,
			mysql.TypeVarchar,
		},
		CanBeNull: mysql.NewServerBitmap(2),
		Metadata: []uint16{
			0,
			384, // A VARCHAR(128) in utf8 would result in 384.
		},
	}
	tm.CanBeNull.Set(1, true)

	// Do an insert packet with all fields set.
	insertRows := mysql.Rows{
		Flags:       0x1234,
		DataColumns: mysql.NewServerBitmap(2),
		Rows: []mysql.Row{
			{
				NullColumns: mysql.NewServerBitmap(2),
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
	updateRows := mysql.Rows{
		Flags:           0x1234,
		IdentifyColumns: mysql.NewServerBitmap(2),
		DataColumns:     mysql.NewServerBitmap(2),
		Rows: []mysql.Row{
			{
				NullIdentifyColumns: mysql.NewServerBitmap(2),
				NullColumns:         mysql.NewServerBitmap(2),
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

	// Do an update packet with an identify set to NULL, and a
	// value set to NULL.
	updateRowsNull := mysql.Rows{
		Flags:           0x1234,
		IdentifyColumns: mysql.NewServerBitmap(2),
		DataColumns:     mysql.NewServerBitmap(2),
		Rows: []mysql.Row{
			{
				NullIdentifyColumns: mysql.NewServerBitmap(2),
				NullColumns:         mysql.NewServerBitmap(2),
				Identify: []byte{
					0x10, 0x20, 0x30, 0x40, // long
				},
				Data: []byte{
					0x10, 0x20, 0x30, 0x40, // long
				},
			},
		},
	}
	updateRowsNull.IdentifyColumns.Set(0, true)
	updateRowsNull.IdentifyColumns.Set(1, true)
	updateRowsNull.DataColumns.Set(0, true)
	updateRowsNull.DataColumns.Set(1, true)
	updateRowsNull.Rows[0].NullIdentifyColumns.Set(1, true)
	updateRowsNull.Rows[0].NullColumns.Set(1, true)

	// Do a delete packet with all fields set.
	deleteRows := mysql.Rows{
		Flags:           0x1234,
		IdentifyColumns: mysql.NewServerBitmap(2),
		Rows: []mysql.Row{
			{
				NullIdentifyColumns: mysql.NewServerBitmap(2),
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

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewTableMapEvent(f, s, tableID, tm),
		mysql.NewMariaDBGTIDEvent(f, s, mysql.MariadbGTID{Domain: 0, Sequence: 0xd}, false /* hasBegin */),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewWriteRowsEvent(f, s, tableID, insertRows),
		mysql.NewUpdateRowsEvent(f, s, tableID, updateRows),
		mysql.NewUpdateRowsEvent(f, s, tableID, updateRowsNull),
		mysql.NewDeleteRowsEvent(f, s, tableID, deleteRows),
		mysql.NewXIDEvent(f, s),
	}

	events := make(chan mysql.BinlogEvent)

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
						Category: binlogdatapb.BinlogTransaction_Statement_BL_UPDATE,
						Sql:      []byte("UPDATE vt_a SET id=1076895760, message=NULL WHERE id=1076895760 AND message IS NULL"),
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
				Position: mysql.EncodePosition(mysql.Position{
					GTIDSet: mysql.MariadbGTIDSet{
						mysql.MariadbGTID{
							Domain:   0,
							Server:   62344,
							Sequence: 0x0d,
						},
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
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, se, nil, mysql.Position{}, 0, sendTransaction)

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

func TestStreamerParseRBRNameEscapes(t *testing.T) {
	f := mysql.NewMySQL56BinlogFormat()
	s := mysql.NewFakeBinlogStream()
	s.ServerID = 62344

	// Create a schema.Engine for this test using keyword names.
	se := schema.NewEngineForTests()
	se.SetTableForTests(&schema.Table{
		Name: sqlparser.NewTableIdent("insert"),
		Columns: []schema.TableColumn{
			{
				Name: sqlparser.NewColIdent("update"),
				Type: querypb.Type_INT64,
			},
			{
				Name: sqlparser.NewColIdent("delete"),
				Type: querypb.Type_VARCHAR,
			},
		},
	})

	// Create a tableMap event on the table.
	tableID := uint64(0x102030405060)
	tm := &mysql.TableMap{
		Flags:    0x8090,
		Database: "vt_test_keyspace",
		Name:     "insert",
		Types: []byte{
			mysql.TypeLong,
			mysql.TypeVarchar,
		},
		CanBeNull: mysql.NewServerBitmap(2),
		Metadata: []uint16{
			0,
			384, // A VARCHAR(128) in utf8 would result in 384.
		},
	}
	tm.CanBeNull.Set(1, true)

	// Do an insert packet with all fields set.
	insertRows := mysql.Rows{
		Flags:       0x1234,
		DataColumns: mysql.NewServerBitmap(2),
		Rows: []mysql.Row{
			{
				NullColumns: mysql.NewServerBitmap(2),
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
	updateRows := mysql.Rows{
		Flags:           0x1234,
		IdentifyColumns: mysql.NewServerBitmap(2),
		DataColumns:     mysql.NewServerBitmap(2),
		Rows: []mysql.Row{
			{
				NullIdentifyColumns: mysql.NewServerBitmap(2),
				NullColumns:         mysql.NewServerBitmap(2),
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

	// Do an update packet with an identify set to NULL, and a
	// value set to NULL.
	updateRowsNull := mysql.Rows{
		Flags:           0x1234,
		IdentifyColumns: mysql.NewServerBitmap(2),
		DataColumns:     mysql.NewServerBitmap(2),
		Rows: []mysql.Row{
			{
				NullIdentifyColumns: mysql.NewServerBitmap(2),
				NullColumns:         mysql.NewServerBitmap(2),
				Identify: []byte{
					0x10, 0x20, 0x30, 0x40, // long
				},
				Data: []byte{
					0x10, 0x20, 0x30, 0x40, // long
				},
			},
		},
	}
	updateRowsNull.IdentifyColumns.Set(0, true)
	updateRowsNull.IdentifyColumns.Set(1, true)
	updateRowsNull.DataColumns.Set(0, true)
	updateRowsNull.DataColumns.Set(1, true)
	updateRowsNull.Rows[0].NullIdentifyColumns.Set(1, true)
	updateRowsNull.Rows[0].NullColumns.Set(1, true)

	// Do a delete packet with all fields set.
	deleteRows := mysql.Rows{
		Flags:           0x1234,
		IdentifyColumns: mysql.NewServerBitmap(2),
		Rows: []mysql.Row{
			{
				NullIdentifyColumns: mysql.NewServerBitmap(2),
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

	input := []mysql.BinlogEvent{
		mysql.NewRotateEvent(f, s, 0, ""),
		mysql.NewFormatDescriptionEvent(f, s),
		mysql.NewTableMapEvent(f, s, tableID, tm),
		mysql.NewMariaDBGTIDEvent(f, s, mysql.MariadbGTID{Domain: 0, Sequence: 0xd}, false /* hasBegin */),
		mysql.NewQueryEvent(f, s, mysql.Query{
			Database: "vt_test_keyspace",
			SQL:      "BEGIN"}),
		mysql.NewWriteRowsEvent(f, s, tableID, insertRows),
		mysql.NewUpdateRowsEvent(f, s, tableID, updateRows),
		mysql.NewUpdateRowsEvent(f, s, tableID, updateRowsNull),
		mysql.NewDeleteRowsEvent(f, s, tableID, deleteRows),
		mysql.NewXIDEvent(f, s),
	}

	events := make(chan mysql.BinlogEvent)

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
						Sql:      []byte("INSERT INTO `insert` SET `update`=1076895760, `delete`='abcd'"),
					},
					Table: "insert",
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
						Sql:      []byte("UPDATE `insert` SET `update`=1076895760, `delete`='abcd' WHERE `update`=1076895760 AND `delete`='abc'"),
					},
					Table: "insert",
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
						Sql:      []byte("UPDATE `insert` SET `update`=1076895760, `delete`=NULL WHERE `update`=1076895760 AND `delete` IS NULL"),
					},
					Table: "insert",
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
						Sql:      []byte("DELETE FROM `insert` WHERE `update`=1076895760 AND `delete`='abc'"),
					},
					Table: "insert",
				},
			},
			eventToken: &querypb.EventToken{
				Timestamp: 1407805592,
				Position: mysql.EncodePosition(mysql.Position{
					GTIDSet: mysql.MariadbGTIDSet{
						mysql.MariadbGTID{
							Domain:   0,
							Server:   62344,
							Sequence: 0x0d,
						},
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
	bls := NewStreamer(&mysql.ConnParams{DbName: "vt_test_keyspace"}, se, nil, mysql.Position{}, 0, sendTransaction)

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
