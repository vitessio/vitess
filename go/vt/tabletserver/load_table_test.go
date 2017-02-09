// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"errors"
	"fmt"
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
	"github.com/youtube/vitess/go/vt/tabletserver/connpool"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

var errRejected = errors.New("rejected")

func TestLoadTableFailBecauseUnableToRetrieveTableIndex(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getTestLoadTableQueries() {
		db.AddQuery(query, result)
	}
	db.AddRejectedQuery("show index from test_table", errRejected)
	_, err := newTestLoadTable("USER_TABLE", "test table", db)
	if err == nil {
		t.Fatalf("table info creation should fail because it is unable to get test_table index")
	}
}

func TestLoadTableReplacePKColumn(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getTestLoadTableQueries() {
		db.AddQuery(query, result)
	}
	table, err := newTestLoadTable("USER_TABLE", "test table", db)
	if err != nil {
		t.Fatalf("failed to create a table info")
	}
	if len(table.PKColumns) != 1 {
		t.Fatalf("table should only have one PK column")
	}
	err = setPK(table, []string{"name"})
	if err != nil {
		t.Fatalf("failed to set primary key: %v", err)
	}
	if len(table.PKColumns) != 1 {
		t.Fatalf("table should only have one PK column")
	}
}

func TestLoadTableSetPKColumn(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getTestLoadTableQueries() {
		db.AddQuery(query, result)
	}
	db.AddQuery("show index from test_table", &sqltypes.Result{
		Fields:       mysqlconn.ShowIndexFromTableFields,
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			mysqlconn.ShowIndexFromTableRow("test_table", false, "index", 1, "name", true),
		},
	})
	table, err := newTestLoadTable("USER_TABLE", "test table", db)
	if err != nil {
		t.Fatalf("failed to create a table info")
	}
	if len(table.PKColumns) != 0 {
		t.Fatalf("table should not have a PK column")
	}
	err = setPK(table, []string{"name"})
	if err != nil {
		t.Fatalf("failed to set primary key: %v", err)
	}
	if len(table.PKColumns) != 1 {
		t.Fatalf("table should only have one PK column")
	}
}

func TestLoadTableInvalidCardinalityInIndex(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getTestLoadTableQueries() {
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
	table, err := newTestLoadTable("USER_TABLE", "test table", db)
	if err != nil {
		t.Fatalf("failed to create a table info: %v", err)
	}
	if len(table.PKColumns) != 1 {
		t.Fatalf("table should have one PK column although the cardinality is invalid")
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
		t.Fatalf("failed to create a test table info")
	}
	want := &schema.Table{
		Name:         sqlparser.NewTableIdent("test_table"),
		Type:         schema.Sequence,
		SequenceInfo: &schema.SequenceInfo{},
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
	want := &schema.Table{
		Name: sqlparser.NewTableIdent("test_table"),
		Type: schema.Message,
		MessageInfo: &schema.MessageInfo{
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

func newTestLoadTable(tableType string, comment string, db *fakesqldb.DB) (*schema.Table, error) {
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

func setPK(ti *schema.Table, colnames []string) error {
	pkIndex := schema.NewIndex("PRIMARY")
	colnums := make([]int, len(colnames))
	for i, colname := range colnames {
		colnums[i] = ti.FindColumn(sqlparser.NewColIdent(colname))
		if colnums[i] == -1 {
			return fmt.Errorf("column %s not found", colname)
		}
		pkIndex.AddColumn(colname, 1)
	}
	for _, col := range ti.Columns {
		pkIndex.DataColumns = append(pkIndex.DataColumns, col.Name)
	}
	ti.Indexes = append(ti.Indexes, nil)
	copy(ti.Indexes[1:], ti.Indexes[:len(ti.Indexes)-1])
	ti.Indexes[0] = pkIndex
	ti.PKColumns = colnums
	return nil
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
