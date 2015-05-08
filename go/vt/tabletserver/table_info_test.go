// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	mproto "github.com/youtube/vitess/go/mysql/proto"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/tabletserver/fakecacheservice"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"
	"golang.org/x/net/context"
)

func TestTableInfoNew(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getTestTableInfoQueries() {
		db.AddQuery(query, result)
	}
	cachePool := newTestTableInfoCachePool()
	cachePool.Open()
	defer cachePool.Close()
	tableInfo, err := newTestTableInfo(cachePool, "USER_TABLE", "test table")
	if err != nil {
		t.Fatalf("failed to create a test table info")
	}
	if tableInfo.Cache == nil {
		t.Fatalf("rowcache should be enabled")
	}
	stats := tableInfo.StatsJSON()
	if stats == "" || stats == "null" {
		t.Fatalf("rowcache is enabled, stats should not be empty or null")
	}
}

func TestTableInfoFailBecauseUnableToRetrieveTableIndex(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getTestTableInfoQueries() {
		db.AddQuery(query, result)
	}
	db.AddRejectedQuery("show index from `test_table`")
	cachePool := newTestTableInfoCachePool()
	cachePool.Open()
	defer cachePool.Close()
	_, err := newTestTableInfo(cachePool, "USER_TABLE", "test table")
	if err == nil {
		t.Fatalf("table info creation should fail because it is unable to get test_table index")
	}
}

func TestTableInfoWithoutRowCacheViaComment(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getTestTableInfoQueries() {
		db.AddQuery(query, result)
	}
	cachePool := newTestTableInfoCachePool()
	cachePool.Open()
	defer cachePool.Close()
	tableInfo, err := newTestTableInfo(cachePool, "USER_TABLE", "vtocc_nocache")
	if err != nil {
		t.Fatalf("failed to create a test table info")
	}
	if tableInfo.Cache != nil {
		t.Fatalf("table info's rowcache should be disabled")
	}
	if tableInfo.StatsJSON() != "null" {
		t.Fatalf("rowcache is disabled, stats should be null")
	}
}

func TestTableInfoWithoutRowCacheViaTableType(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getTestTableInfoQueries() {
		db.AddQuery(query, result)
	}
	cachePool := newTestTableInfoCachePool()
	cachePool.Open()
	defer cachePool.Close()
	tableInfo, err := newTestTableInfo(cachePool, "VIEW", "test table")
	if err != nil {
		t.Fatalf("failed to create a test table info")
	}
	if tableInfo.Cache != nil {
		t.Fatalf("table info's rowcache should be disabled")
	}
}

func TestTableInfoWithoutRowCacheViaNoPKColumn(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	db.AddQuery("show index from `test_table`", &mproto.QueryResult{})
	db.AddQuery("describe `test_table`", &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.MakeString([]byte("pk")),
				sqltypes.MakeString([]byte("int")),
				sqltypes.MakeString([]byte{}),
				sqltypes.MakeString([]byte{}),
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte{}),
			},
		},
	})

	cachePool := newTestTableInfoCachePool()
	cachePool.Open()
	defer cachePool.Close()
	tableInfo, err := newTestTableInfo(cachePool, "USER_TABLE", "test table")
	if err != nil {
		t.Fatalf("failed to create a test table info")
	}
	if tableInfo.Cache != nil {
		t.Fatalf("table info's rowcache should be disabled")
	}
}

func TestTableInfoWithoutRowCacheViaUnknownPKColumnType(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	db.AddQuery("show index from `test_table`", &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.MakeString([]byte{}),
				sqltypes.MakeString([]byte{}),
				sqltypes.MakeString([]byte("PRIMARY")),
				sqltypes.MakeString([]byte{}),
				sqltypes.MakeString([]byte("pk")),
				sqltypes.MakeString([]byte{}),
				sqltypes.MakeString([]byte("300")),
			},
		},
	})
	db.AddQuery("describe `test_table`", &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
				sqltypes.MakeString([]byte("pk")),
				sqltypes.MakeString([]byte("unknown_type")),
				sqltypes.MakeString([]byte{}),
				sqltypes.MakeString([]byte{}),
				sqltypes.MakeString([]byte("1")),
				sqltypes.MakeString([]byte{}),
			},
		},
	})

	cachePool := newTestTableInfoCachePool()
	cachePool.Open()
	defer cachePool.Close()
	tableInfo, err := newTestTableInfo(cachePool, "USER_TABLE", "test table")
	if err != nil {
		t.Fatalf("failed to create a test table info")
	}
	if tableInfo.Cache != nil {
		t.Fatalf("table info's rowcache should be disabled")
	}
}

func TestTableInfoReplacePKColumn(t *testing.T) {
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getTestTableInfoQueries() {
		db.AddQuery(query, result)
	}
	cachePool := newTestTableInfoCachePool()
	cachePool.Open()
	defer cachePool.Close()
	tableInfo, err := newTestTableInfo(cachePool, "USER_TABLE", "test table")
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
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getTestTableInfoQueries() {
		db.AddQuery(query, result)
	}
	db.AddQuery("show index from `test_table`", &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
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
	cachePool := newTestTableInfoCachePool()
	cachePool.Open()
	defer cachePool.Close()
	tableInfo, err := newTestTableInfo(cachePool, "USER_TABLE", "test table")
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
	fakecacheservice.Register()
	db := fakesqldb.Register()
	for query, result := range getTestTableInfoQueries() {
		db.AddQuery(query, result)
	}
	db.AddQuery("show index from `test_table`", &mproto.QueryResult{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			[]sqltypes.Value{
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
	cachePool := newTestTableInfoCachePool()
	cachePool.Open()
	defer cachePool.Close()
	tableInfo, err := newTestTableInfo(cachePool, "USER_TABLE", "test table")
	if err != nil {
		t.Fatalf("failed to create a table info")
	}
	if len(tableInfo.PKColumns) != 1 {
		t.Fatalf("table should have one PK column although the cardinality is invalid")
	}
}

func newTestTableInfo(cachePool *CachePool, tableType string, comment string) (*TableInfo, error) {
	ctx := context.Background()
	appParams := sqldb.ConnParams{}
	dbaParams := sqldb.ConnParams{}
	queryServiceStats := NewQueryServiceStats("", false)
	connPoolIdleTimeout := 10 * time.Second
	connPool := NewConnPool("", 2, connPoolIdleTimeout, false, queryServiceStats)
	connPool.Open(&appParams, &dbaParams)
	conn, err := connPool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Recycle()

	tableName := "test_table"
	createTime := sqltypes.MakeString([]byte("1427325875"))
	tableInfo, err := NewTableInfo(conn, tableName, tableType, createTime, comment, cachePool)
	if err != nil {
		return nil, err
	}
	return tableInfo, nil
}

func newTestTableInfoCachePool() *CachePool {
	rowCacheConfig := RowCacheConfig{
		Binary:      "ls",
		Connections: 100,
	}
	randID := rand.Int63()
	name := fmt.Sprintf("TestCachePool-TableInfo-%d-", randID)
	statsURL := fmt.Sprintf("/debug/tableinfo-cache-%d", randID)
	return NewCachePool(
		name,
		rowCacheConfig,
		1*time.Second,
		statsURL,
		false,
		NewQueryServiceStats("", false),
	)
}

func getTestTableInfoQueries() map[string]*mproto.QueryResult {
	return map[string]*mproto.QueryResult{
		"describe `test_table`": &mproto.QueryResult{
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("name")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
				[]sqltypes.Value{
					sqltypes.MakeString([]byte("addr")),
					sqltypes.MakeString([]byte("int")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("1")),
					sqltypes.MakeString([]byte{}),
				},
			},
		},
		"show index from `test_table`": &mproto.QueryResult{
			RowsAffected: 3,
			Rows: [][]sqltypes.Value{
				[]sqltypes.Value{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("PRIMARY")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
				[]sqltypes.Value{
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("INDEX")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("pk")),
					sqltypes.MakeString([]byte{}),
					sqltypes.MakeString([]byte("300")),
				},
				[]sqltypes.Value{
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
