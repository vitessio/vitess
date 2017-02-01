// Copyright 2015, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tabletserver

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/mysqlconn/fakesqldb"
	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestDBConnExec(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	testUtils := newTestUtils()
	sql := "select * from test_table limit 1000"
	expectedResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeTrusted(sqltypes.VarChar, []byte("123"))},
		},
	}
	db.AddQuery(sql, expectedResult)
	connPool := testUtils.newConnPool()
	connPool.Open(db.ConnParams(), db.ConnParams())
	defer connPool.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()
	queryServiceStats := NewQueryServiceStats("", false)
	dbConn, err := NewDBConn(connPool, db.ConnParams(), db.ConnParams(), queryServiceStats)
	defer dbConn.Close()
	if err != nil {
		t.Fatalf("should not get an error, err: %v", err)
	}
	// Exec succeed, not asking for fields.
	result, err := dbConn.Exec(ctx, sql, 1, false)
	if err != nil {
		t.Fatalf("should not get an error, err: %v", err)
	}
	expectedResult.Fields = nil
	testUtils.checkEqual(t, expectedResult, result)
	// Exec fail
	db.AddRejectedQuery(sql, &sqldb.SQLError{
		Num:     2012,
		Message: "connection fail",
		Query:   "",
	})
	_, err = dbConn.Exec(ctx, sql, 1, false)
	testUtils.checkTabletError(t, err, vtrpcpb.ErrorCode_INTERNAL_ERROR, "")
}

func TestDBConnKill(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	testUtils := newTestUtils()
	connPool := testUtils.newConnPool()
	connPool.Open(db.ConnParams(), db.ConnParams())
	defer connPool.Close()
	queryServiceStats := NewQueryServiceStats("", false)
	dbConn, err := NewDBConn(connPool, db.ConnParams(), db.ConnParams(), queryServiceStats)
	defer dbConn.Close()
	query := fmt.Sprintf("kill %d", dbConn.ID())
	db.AddQuery(query, &sqltypes.Result{})
	// Kill failed because we are not able to connect to the database
	db.EnableConnFail()
	err = dbConn.Kill("test kill")
	testUtils.checkTabletError(t, err, vtrpcpb.ErrorCode_INTERNAL_ERROR, "Failed to get conn from dba pool")
	db.DisableConnFail()

	// Kill succeed
	err = dbConn.Kill("test kill")
	if err != nil {
		t.Fatalf("kill should succeed, but got error: %v", err)
	}

	err = dbConn.reconnect()
	if err != nil {
		t.Fatalf("reconnect should succeed, but got error: %v", err)
	}
	newKillQuery := fmt.Sprintf("kill %d", dbConn.ID())
	// Kill failed because "kill query_id" failed
	db.AddRejectedQuery(newKillQuery, errRejected)
	err = dbConn.Kill("test kill")
	testUtils.checkTabletError(t, err, vtrpcpb.ErrorCode_INTERNAL_ERROR, "Could not kill query")

}

func TestDBConnStream(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	testUtils := newTestUtils()
	sql := "select * from test_table limit 1000"
	expectedResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeTrusted(sqltypes.VarChar, []byte("123"))},
		},
	}
	db.AddQuery(sql, expectedResult)
	connPool := testUtils.newConnPool()
	connPool.Open(db.ConnParams(), db.ConnParams())
	defer connPool.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()
	queryServiceStats := NewQueryServiceStats("", false)
	dbConn, err := NewDBConn(connPool, db.ConnParams(), db.ConnParams(), queryServiceStats)
	defer dbConn.Close()
	var result sqltypes.Result
	err = dbConn.Stream(
		ctx, sql, func(r *sqltypes.Result) error {
			// Aggregate Fields and Rows
			if r.Fields != nil {
				result.Fields = r.Fields
			}
			if r.Rows != nil {
				result.Rows = append(result.Rows, r.Rows...)
			}
			return nil
		}, 10, querypb.ExecuteOptions_ALL)
	if err != nil {
		t.Fatalf("should not get an error, err: %v", err)
	}
	testUtils.checkEqual(t, expectedResult, &result)
	// Stream fail
	db.Close()
	dbConn.Close()
	err = dbConn.Stream(
		ctx, sql, func(r *sqltypes.Result) error {
			return nil
		}, 10, querypb.ExecuteOptions_ALL)
	db.DisableConnFail()
	want1 := "Connection is closed"
	want2 := "use of closed network connection"
	if err == nil || (!strings.Contains(err.Error(), want1) && !strings.Contains(err.Error(), want2)) {
		t.Errorf("Error: '%v', must contain '%s' or '%s'\n", err, want1, want2)
	}
}
