/*
Copyright 2017 Google Inc.

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

package schema

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"

	querypb "vitess.io/vitess/go/vt/proto/query"
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
	if len(table.Indexes) != 3 {
		t.Fatalf("table should have three indexes")
	}
	if count := table.UniqueIndexes(); count != 2 {
		t.Errorf("table.UniqueIndexes(): %d expected 2", count)
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
	if unique := table.Indexes[0].Unique; unique != true {
		t.Errorf("table.Indexes[0].Unique: expected true")
	}
	if idx := table.Indexes[1].FindColumn(sqlparser.NewColIdent("pk")); idx != 0 {
		t.Errorf("table.Indexes[1].FindColumn(pk): %d, want 0", idx)
	}
	if idx := table.Indexes[1].FindColumn(sqlparser.NewColIdent("name")); idx != 1 {
		t.Errorf("table.Indexes[1].FindColumn(name): %d, want 1", idx)
	}
	if idx := table.Indexes[1].FindColumn(sqlparser.NewColIdent("addr")); idx != -1 {
		t.Errorf("table.Indexes[1].FindColumn(pk): %d, want -1", idx)
	}
	if unique := table.Indexes[1].Unique; unique != true {
		t.Errorf("table.Indexes[1].Unique: expected true")
	}
	if idx := table.Indexes[2].FindColumn(sqlparser.NewColIdent("addr")); idx != 0 {
		t.Errorf("table.Indexes[1].FindColumn(addr): %d, want 0", idx)
	}
	if unique := table.Indexes[2].Unique; unique != false {
		t.Errorf("table.Indexes[2].Unique: expected false")
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
				Name: "time_scheduled",
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
			Fields:       mysql.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.ShowIndexFromTableRow("test_table", true, "PRIMARY", 1, "time_scheduled", false),
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
	wanterr = "missing from message table: test_table"
	if err == nil || !strings.Contains(err.Error(), wanterr) {
		t.Errorf("newTestLoadTable: %v, must contain %s", err, wanterr)
	}
}

func TestLoadTableWithBitColumn(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	for query, result := range getTestLoadTableWithBitColumnQueries() {
		db.AddQuery(query, result)
	}
	table, err := newTestLoadTable("USER_TABLE", "test table", db)
	if err != nil {
		t.Fatal(err)
	}
	wantValue := sqltypes.MakeTrusted(sqltypes.Bit, []byte{1, 0, 1})
	if got, want := table.Columns[1].Default, wantValue; !reflect.DeepEqual(got, want) {
		t.Errorf("Default bit value: %v, want %v", got, want)
	}
}

func newTestLoadTable(tableType string, comment string, db *fakesqldb.DB) (*Table, error) {
	ctx := context.Background()
	appParams := db.ConnParams()
	dbaParams := db.ConnParams()
	connPoolIdleTimeout := 10 * time.Second
	connPool := connpool.New("", pools.ResourceImpl, 2, connPoolIdleTimeout, 0, DummyChecker)
	connPool.Open(appParams, dbaParams, appParams)
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
			Fields:       mysql.DescribeTableFields,
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				mysql.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
				mysql.DescribeTableRow("name", "int(11)", false, "", "0"),
				mysql.DescribeTableRow("addr", "int(11)", false, "", "0"),
			},
		},
		"show index from test_table": {
			Fields:       mysql.ShowIndexFromTableFields,
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				mysql.ShowIndexFromTableRow("test_table", true, "PRIMARY", 1, "pk", false),
				mysql.ShowIndexFromTableRow("test_table", true, "index", 1, "pk", false),
				mysql.ShowIndexFromTableRow("test_table", true, "index", 2, "name", false),
				mysql.ShowIndexFromTableRow("test_table", false, "index2", 1, "addr", false),
			},
		},
	}
}

func getMessageTableQueries() map[string]*sqltypes.Result {
	// id is intentionally after the message column to ensure that the
	// loader still makes it the first one.
	return map[string]*sqltypes.Result{
		"select * from test_table where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "time_scheduled",
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
			}, {
				Name: "id",
				Type: sqltypes.Int64,
			}},
		},
		"describe test_table": {
			Fields:       mysql.DescribeTableFields,
			RowsAffected: 7,
			Rows: [][]sqltypes.Value{
				mysql.DescribeTableRow("time_scheduled", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("time_next", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("epoch", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("time_created", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("time_acked", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("message", "bigint(20)", false, "", "0"),
				mysql.DescribeTableRow("id", "bigint(20)", false, "PRI", "0"),
			},
		},
		"show index from test_table": {
			Fields:       mysql.ShowIndexFromTableFields,
			RowsAffected: 2,
			Rows: [][]sqltypes.Value{
				mysql.ShowIndexFromTableRow("test_table", true, "PRIMARY", 1, "time_scheduled", false),
				mysql.ShowIndexFromTableRow("test_table", true, "PRIMARY", 2, "id", false),
			},
		},
	}
}

func getTestLoadTableWithBitColumnQueries() map[string]*sqltypes.Result {
	return map[string]*sqltypes.Result{
		"select * from test_table where 1 != 1": {
			Fields: []*querypb.Field{{
				Name: "pk",
				Type: sqltypes.Int32,
			}, {
				Name: "flags",
				Type: sqltypes.Bit,
			}},
		},
		"describe test_table": {
			Fields:       mysql.DescribeTableFields,
			RowsAffected: 2,
			Rows: [][]sqltypes.Value{
				mysql.DescribeTableRow("pk", "int(11)", false, "PRI", "0"),
				mysql.DescribeTableRow("flags", "int(11)", false, "", "b'101'"),
			},
		},
		"show index from test_table": {
			Fields:       mysql.ShowIndexFromTableFields,
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				mysql.ShowIndexFromTableRow("test_table", true, "PRIMARY", 1, "pk", false),
			},
		},
		"select b'101'": {
			Fields:       sqltypes.MakeTestFields("", "varbinary"),
			RowsAffected: 1,
			Rows: [][]sqltypes.Value{
				{sqltypes.MakeTrusted(sqltypes.VarBinary, []byte{1, 0, 1})},
			},
		},
	}
}
