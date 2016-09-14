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

	"github.com/youtube/vitess/go/sqldb"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vttest/fakesqldb"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

func TestDBConnExec(t *testing.T) {
	db := fakesqldb.Register()
	testUtils := newTestUtils()
	sql := "select * from test_table limit 1000"
	expectedResult := &sqltypes.Result{
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeString([]byte("123"))},
		},
	}
	db.AddQuery(sql, expectedResult)
	connPool := testUtils.newConnPool()
	appParams := &sqldb.ConnParams{Engine: db.Name}
	dbaParams := &sqldb.ConnParams{Engine: db.Name}
	connPool.Open(appParams, dbaParams)
	defer connPool.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()
	queryServiceStats := NewQueryServiceStats("", false)
	dbConn, err := NewDBConn(connPool, appParams, dbaParams, queryServiceStats)
	defer dbConn.Close()
	if err != nil {
		t.Fatalf("should not get an error, err: %v", err)
	}
	// Exec succeed
	result, err := dbConn.Exec(ctx, sql, 1, false)
	if err != nil {
		t.Fatalf("should not get an error, err: %v", err)
	}
	testUtils.checkEqual(t, expectedResult, result)
	// Exec fail
	db.EnableConnFail()
	_, err = dbConn.Exec(ctx, sql, 1, false)
	db.DisableConnFail()
	testUtils.checkTabletError(t, err, vtrpcpb.ErrorCode_INTERNAL_ERROR, "")
}

func TestDBConnKill(t *testing.T) {
	db := fakesqldb.Register()
	testUtils := newTestUtils()
	connPool := testUtils.newConnPool()
	appParams := &sqldb.ConnParams{Engine: db.Name}
	dbaParams := &sqldb.ConnParams{Engine: db.Name}
	connPool.Open(appParams, dbaParams)
	defer connPool.Close()
	queryServiceStats := NewQueryServiceStats("", false)
	dbConn, err := NewDBConn(connPool, appParams, dbaParams, queryServiceStats)
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
	db := fakesqldb.Register()
	testUtils := newTestUtils()
	sql := "select * from test_table limit 1000"
	expectedResult := &sqltypes.Result{
		RowsAffected: 0,
		Rows: [][]sqltypes.Value{
			{sqltypes.MakeString([]byte("123"))},
		},
	}
	db.AddQuery(sql, expectedResult)
	connPool := testUtils.newConnPool()
	appParams := &sqldb.ConnParams{Engine: db.Name}
	dbaParams := &sqldb.ConnParams{Engine: db.Name}
	connPool.Open(appParams, dbaParams)
	defer connPool.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()
	queryServiceStats := NewQueryServiceStats("", false)
	dbConn, err := NewDBConn(connPool, appParams, dbaParams, queryServiceStats)
	defer dbConn.Close()
	var result sqltypes.Result
	err = dbConn.Stream(
		ctx, sql, func(r *sqltypes.Result) error {
			result = *r
			return nil
		}, 10, false /*excludeFieldNames*/)
	if err != nil {
		t.Fatalf("should not get an error, err: %v", err)
	}
	testUtils.checkEqual(t, expectedResult, &result)
	// Stream fail
	db.EnableConnFail()
	err = dbConn.Stream(
		ctx, sql, func(r *sqltypes.Result) error {
			return nil
		}, 10, false /*excludeFieldNames*/)
	db.DisableConnFail()
	want := "connection fail"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Error: %v, must contain %s\n", err, want)
	}
}
