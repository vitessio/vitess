// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
	"github.com/youtube/vitess/go/vt/schema"
	"github.com/youtube/vitess/go/vt/sqlparser"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"golang.org/x/net/context"
)

var errRejected = errors.New("rejected")

func TestTableInfoFailBecauseUnableToRetrieveTableIndex(t *testing.T) {
	db := fakesqldb.Register()
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
	db := fakesqldb.Register()
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
	db := fakesqldb.Register()
	for query, result := range getTestTableInfoQueries() {
		db.AddQuery(query, result)
	}
	db.AddQuery("show index from test_table", &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeString([]byte{}),
				sqltypes.MakeString([]byte{}),
				sqltypes.MakeString([]byte("INDEX")),
				sqltypes.MakeString([]byte{}),
				sqltypes.MakeString([]byte("name")),
				sqltypes.MakeString([]byte{}),
				sqltypes.MakeString([]byte("300")),
			},
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
	db := fakesqldb.Register()
	for query, result := range getTestTableInfoQueries() {
		db.AddQuery(query, result)
	}
	db.AddQuery("show index from test_table", &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{
				sqltypes.MakeString([]byte{}),
				sqltypes.MakeString([]byte{}),
				sqltypes.MakeString([]byte("PRIMARY")),
				sqltypes.MakeString([]byte{}),
				sqltypes.MakeString([]byte("pk")),
				sqltypes.MakeString([]byte{}),
				sqltypes.MakeString([]byte("invalid")),
			},
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
	db := fakesqldb.Register()
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

func newTestTableInfo(tableType string, comment string, db *fakesqldb.DB) (*TableInfo, error) {
	ctx := context.Background()
	appParams := sqldb.ConnParams{Engine: db.Name}
	dbaParams := sqldb.ConnParams{Engine: db.Name}
	queryServiceStats := NewQueryServiceStats("", false)
	connPoolIdleTimeout := 10 * time.Second
	connPool := NewConnPool("", 2, connPoolIdleTimeout, false, queryServiceStats, DummyChecker)
	connPool.Open(&appParams, &dbaParams)
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
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				{
					sqltypes.MakeString([]byte("name")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				{
					sqltypes.MakeString([]byte("addr")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
			},
		},
		"show index from test_table": {
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("PRIMARY")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
				{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("INDEX")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
				{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("INDEX")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("name")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
			},
		},
	}
}
