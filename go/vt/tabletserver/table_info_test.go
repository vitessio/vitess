// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

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
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

var errRejected = errors.New("rejected")

func TestTableInfoFailBecauseUnableToRetrieveTableIndex(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getTestTableInfoQueries() {
		db.AddQuery(query, result)
	}
	db.AddRejectedQuery("show index from test_table", errRejected)
	_, err := newTestTableInfo("USER_TABLE", "test table", db)
	if err == nil {
		t.Fatalf("table info creation should fail because it is unable to get test_table index")
	}
}

func TestTableInfoReplacePKColumn(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getTestTableInfoQueries() {
		db.AddQuery(query, result)
	}
	tableInfo, err := newTestTableInfo("USER_TABLE", "test table", db)
	if err != nil {
		t.Fatalf("failed to create a table info")
	}
	if len(tableInfo.PKColumns) != 1 {
		t.Fatalf("table should only have one PK column")
	}
	err = tableInfo.SetPK([]string{"name"})
	if err != nil {
		t.Fatalf("failed to set primary key: %v", err)
	}
	if len(tableInfo.PKColumns) != 1 {
		t.Fatalf("table should only have one PK column")
	}
}

func TestTableInfoSetPKColumn(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getTestTableInfoQueries() {
		db.AddQuery(query, result)
	}
	db.AddQuery("show index from test_table", &sqltypes.Result{
		Fields:       mysqlconn.ShowIndexFromTableFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysqlconn.ShowIndexFromTableRow("test_table", false, "index", 1, "name", true),
		},
	})
	tableInfo, err := newTestTableInfo("USER_TABLE", "test table", db)
	if err != nil {
		t.Fatalf("failed to create a table info")
	}
	if len(tableInfo.PKColumns) != 0 {
		t.Fatalf("table should not have a PK column")
	}
	err = tableInfo.SetPK([]string{"name"})
	if err != nil {
		t.Fatalf("failed to set primary key: %v", err)
	}
	if len(tableInfo.PKColumns) != 1 {
		t.Fatalf("table should only have one PK column")
	}
}

func TestTableInfoInvalidCardinalityInIndex(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getTestTableInfoQueries() {
		db.AddQuery(query, result)
	}
	row := mysqlconn.ShowIndexFromTableRow("test_table", true, "PRIMARY", 1, "pk", false)
	row[6] = sqltypes.MakeString([]byte("invalid"))
	db.AddQuery("show index from test_table", &sqltypes.Result{
		Fields:       mysqlconn.ShowIndexFromTableFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			row,
		},
	})
	tableInfo, err := newTestTableInfo("USER_TABLE", "test table", db)
	if err != nil {
		t.Fatalf("failed to create a table info: %v", err)
	}
	if len(tableInfo.PKColumns) != 1 {
		t.Fatalf("table should have one PK column although the cardinality is invalid")
	}
}

func TestTableInfoSequence(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getTestTableInfoQueries() {
		db.AddQuery(query, result)
	}
	tableInfo, err := newTestTableInfo("USER_TABLE", "vitess_sequence", db)
	if err != nil {
		t.Fatalf("failed to create a test table info")
	}
	want := &TableInfo{
		Table: &schema.Table{
			Name: sqlparser.NewTableIdent("test_table"),
			Type: schema.Sequence,
		},
	}
	tableInfo.Columns = nil
	tableInfo.Indexes = nil
	tableInfo.PKColumns = nil
	if !reflect.DeepEqual(tableInfo, want) {
		t.Errorf("TableInfo:\n%#v, want\n%#v", tableInfo, want)
	}
}

func TestTableInfoMessage(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getMessageTableInfoQueries() {
		db.AddQuery(query, result)
	}
	tableInfo, err := newTestTableInfo("USER_TABLE", "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30", db)
	if err != nil {
		t.Fatal(err)
	}
	want := &TableInfo{
		Table: &schema.Table{
			Name: sqlparser.NewTableIdent("test_table"),
			Type: schema.Message,
		},
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
	tableInfo.Columns = nil
	tableInfo.Indexes = nil
	tableInfo.PKColumns = nil
	if !reflect.DeepEqual(tableInfo, want) {
		t.Errorf("TableInfo:\n%+v, want\n%+v", tableInfo.Table, want.Table)
		t.Errorf("TableInfo:\n%+v, want\n%+v", tableInfo.MessageInfo, want.MessageInfo)
		t.Errorf("TableInfo:\n%+v, want\n%+v", tableInfo, want)
	}

	// Missing property
	_, err = newTestTableInfo("USER_TABLE", "vitess_message,vt_ack_wait=30", db)
	wanterr := "not specified for message table"
	if err == nil || !strings.Contains(err.Error(), wanterr) {
		t.Errorf("newTestTableInfo: %v, want %s", err, wanterr)
	}

	// id column must be part of primary key.
	for query, result := range getMessageTableInfoQueries() {
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
	_, err = newTestTableInfo("USER_TABLE", "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30", db)
	wanterr = "id column is not part of the primary key for message table: test_table"
	if err == nil || err.Error() != wanterr {
		t.Errorf("newTestTableInfo: %v, want %s", err, wanterr)
	}

	for query, result := range getTestTableInfoQueries() {
		db.AddQuery(query, result)
	}
	_, err = newTestTableInfo("USER_TABLE", "vitess_message,vt_ack_wait=30,vt_purge_after=120,vt_batch_size=1,vt_cache_size=10,vt_poller_interval=30", db)
	wanterr = "time_scheduled missing from message table: test_table"
	if err == nil || err.Error() != wanterr {
		t.Errorf("newTestTableInfo: %v, want %s", err, wanterr)
	}
}

func newTestTableInfo(tableType string, comment string, db *fakesqldb.DB) (*TableInfo, error) {
	ctx := context.Background()
	appParams := db.ConnParams()
	dbaParams := db.ConnParams()
	queryServiceStats := NewQueryServiceStats("", false)
	connPoolIdleTimeout := 10 * time.Second
	connPool := NewConnPool("", 2, connPoolIdleTimeout, false, queryServiceStats, DummyChecker)
	connPool.Open(appParams, dbaParams)
	conn, err := connPool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	tableName := "test_table"
	tableInfo, err := NewTableInfo(conn, tableName, tableType, comment)
	if err != nil {
		return nil, err
	}
	return tableInfo, nil
}

func getTestTableInfoQueries() map[string]*sqltypes.Result {
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

func getMessageTableInfoQueries() map[string]*sqltypes.Result {
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
