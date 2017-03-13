// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package schema

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn"
	"github.com/youtube/vitess/go/mysqlconn/fakesqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/connpool"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

var errRejected = errors.New("rejected")

func TestLoadTable(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getTestLoadTableQueries() {
		db.AddQuery(query, result)
	}
	table, err := newTestLoadTable("USER_TABLE", "test table", db)
	if err != nil {
		t.Fatal(err)
	}
	if len(table.PKColumns) != 1 {
		t.Fatalf("table should have one PK column although the cardinality is invalid")
	}
	if idx := table.Indexes[0].FindColumn(sqlparser.NewColIdent("pk")); idx != 0 {
		t.Errorf("table.Indexes[0].FindColumn(pk): %d, want 0", idx)
	}
	if idx := table.Indexes[0].FindColumn(sqlparser.NewColIdent("none")); idx != -1 {
		t.Errorf("table.Indexes[0].FindColumn(none): %d, want 0", idx)
	}
	if name := table.GetPKColumn(0).Name.String(); name != "pk" {
		t.Errorf("table.GetPKColumn(0): %s, want pk", name)
	}
}

func TestLoadTableFailBecauseUnableToRetrieveTableIndex(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getTestLoadTableQueries() {
		db.AddQuery(query, result)
	}
	db.AddRejectedQuery("show index from test_table", errRejected)
	_, err := newTestLoadTable("USER_TABLE", "test table", db)
	want := "rejected"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("LoadTable: %v, must contain %s", err, want)
	}
}

func TestLoadTableSequence(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getTestLoadTableQueries() {
		db.AddQuery(query, result)
	}
	table, err := newTestLoadTable("USER_TABLE", "vitess_sequence", db)
	if err != nil {
		t.Fatal(err)
	}
	want := &Table{
		Name:         sqlparser.NewTableIdent("test_table"),
		Type:         Sequence,
		SequenceInfo: &SequenceInfo{},
	}
	table.Columns = nil
	table.Indexes = nil
	table.PKColumns = nil
	if !reflect.DeepEqual(table, want) {
		t.Errorf("Table:\n%#v, want\n%#v", table, want)
	}
}

func TestLoadTableMessage(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getMessageTableQueries() {
		db.AddQuery(query, result)
	}
	table, err := newTestLoadTable("USER_TABLE", "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30", db)
	if err != nil {
		t.Fatal(err)
	}
	want := &Table{
		Name: sqlparser.NewTableIdent("test_table"),
		Type: Message,
		MessageInfo: &MessageInfo{
			IDPKIndex: 1,
			Fields: []*querypb.Field{{
				Name: "id",
				Type: sqltypes.Int64,
			}, {
				Name: "message",
				Type: sqltypes.VarBinary,
			}},
			AckWaitDuration:    30 * time.Second,
			PurgeAfterDuration: 120 * time.Second,
			BatchSize:          1,
			CacheSize:          10,
			PollInterval:       30 * time.Second,
		},
	}
	table.Columns = nil
	table.Indexes = nil
	table.PKColumns = nil
	if !reflect.DeepEqual(table, want) {
		t.Errorf("Table:\n%+v, want\n%+v", table, want)
		t.Errorf("Table:\n%+v, want\n%+v", table.MessageInfo, want.MessageInfo)
	}

	// Missing property
	_, err = newTestLoadTable("USER_TABLE", "vitess_message,vt_ack_wait=30", db)
	wanterr := "not specified for message table"
	if err == nil || !strings.Contains(err.Error(), wanterr) {
		t.Errorf("newTestLoadTable: %v, want %s", err, wanterr)
	}

	// id column must be part of primary key.
	for query, result := range getMessageTableQueries() {
		db.AddQuery(query, result)
	}
	db.AddQuery(
		"show index from test_table",
		&sqltypes.Result{
			Fields:       mysqlconn.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysqlconn.ShowIndexFromTableRow("test_table", true, "PRIMARY", 1, "time_scheduled", false),
			},
		})
	_, err = newTestLoadTable("USER_TABLE", "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30", db)
	wanterr = "id column is not part of the primary key for message table: test_table"
	if err == nil || err.Error() != wanterr {
		t.Errorf("newTestLoadTable: %v, want %s", err, wanterr)
	}

	for query, result := range getTestLoadTableQueries() {
		db.AddQuery(query, result)
	}
	_, err = newTestLoadTable("USER_TABLE", "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30", db)
	wanterr = "time_scheduled missing from message table: test_table"
	if err == nil || err.Error() != wanterr {
		t.Errorf("newTestLoadTable: %v, want %s", err, wanterr)
	}
}

func newTestLoadTable(tableType string, comment string, db *fakesqldb.DB) (*Table, error) {
	ctx := context.Background()
	appParams := db.ConnParams()
	dbaParams := db.ConnParams()
	connPoolIdleTimeout := 10 * time.Second
	connPool := connpool.New("", 2, connPoolIdleTimeout, DummyChecker)
	connPool.Open(appParams, dbaParams)
	conn, err := connPool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	return LoadTable(conn, "test_table", tableType, comment)
}

func getTestLoadTableQueries() map[string]*sqltypes.Result {
	return map[string]*sqltypes.Result{
		"select * from test_table where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}, {
				Name: "name",
				Type: sqltypes.Int32,
			}, {
				Name: "addr",
				Type: sqltypes.Int32,
			}},
		},
		"describe test_table": {
			Fields:       mysqlconn.DescribeTableFields,
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				mysqlconn.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
				mysqlconn.DescribeTableRow("name", "int(11)", false, "", "0"),
				mysqlconn.DescribeTableRow("addr", "int(11)", false, "", "0"),
			},
		},
		"show index from test_table": {
			Fields:       mysqlconn.ShowIndexFromTableFields,
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				mysqlconn.ShowIndexFromTableRow("test_table", true, "PRIMARY", 1, "pk", false),
				mysqlconn.ShowIndexFromTableRow("test_table", true, "index", 1, "pk", false),
				mysqlconn.ShowIndexFromTableRow("test_table", true, "index", 2, "name", false),
			},
		},
	}
}

func getMessageTableQueries() map[string]*sqltypes.Result {
	return map[string]*sqltypes.Result{
		"select * from test_table where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "time_scheduled",
				Type: sqltypes.Int64,
			}, {
				Name: "id",
				Type: sqltypes.Int64,
			}, {
				Name: "time_next",
				Type: sqltypes.Int64,
			}, {
				Name: "epoch",
				Type: sqltypes.Int64,
			}, {
				Name: "time_created",
				Type: sqltypes.Int64,
			}, {
				Name: "time_acked",
				Type: sqltypes.Int64,
			}, {
				Name: "message",
				Type: sqltypes.VarBinary,
			}},
		},
		"describe test_table": {
			Fields:       mysqlconn.DescribeTableFields,
			RowsAffected: 7,
			Rows: [][]sqltypes.Value{
				mysqlconn.DescribeTableRow("time_scheduled", "bigint(20)", false, "", "0"),
				mysqlconn.DescribeTableRow("id", "bigint(20)", false, "PRI", "0"),
				mysqlconn.DescribeTableRow("time_next", "bigint(20)", false, "", "0"),
				mysqlconn.DescribeTableRow("epoch", "bigint(20)", false, "", "0"),
				mysqlconn.DescribeTableRow("time_created", "bigint(20)", false, "", "0"),
				mysqlconn.DescribeTableRow("time_acked", "bigint(20)", false, "", "0"),
				mysqlconn.DescribeTableRow("message", "bigint(20)", false, "", "0"),
			},
		},
		"show index from test_table": {
			Fields:       mysqlconn.ShowIndexFromTableFields,
			RowsAffected: 2,
			Rows: [][]sqltypes.Value{
				mysqlconn.ShowIndexFromTableRow("test_table", true, "PRIMARY", 1, "time_scheduled", false),
				mysqlconn.ShowIndexFromTableRow("test_table", true, "PRIMARY", 2, "id", false),
			},
		},
	}
}
