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

package connpool

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func compareTimingCounts(t *testing.T, op string, delta int64, before, after map[string]int64) {
	t.Helper()
	countBefore := before[op]
	countAfter := after[op]
	if countAfter-countBefore != delta {
		t.Errorf("Expected %s to increase by %d, got %d (%d => %d)", op, delta, countAfter-countBefore, countBefore, countAfter)
	}
}

func TestDBConnExec(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	startCounts := tabletenv.MySQLStats.Counts()

	sql := "select * from test_table limit 1000"
	expectedResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("123")},
		},
	}
	db.AddQuery(sql, expectedResult)
	connPool := newPool()
	connPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer connPool.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()
	dbConn, err := NewDBConn(connPool, db.ConnParams())
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
	if !reflect.DeepEqual(expectedResult, result) {
		t.Errorf("Exec: %v, want %v", expectedResult, result)
	}

	compareTimingCounts(t, "Connect", 1, startCounts, tabletenv.MySQLStats.Counts())
	compareTimingCounts(t, "Exec", 1, startCounts, tabletenv.MySQLStats.Counts())

	startCounts = tabletenv.MySQLStats.Counts()

	// Exec fail due to client side error
	db.AddRejectedQuery(sql, &mysql.SQLError{
		Num:     2012,
		Message: "connection fail",
		Query:   "",
	})
	_, err = dbConn.Exec(ctx, sql, 1, false)
	want := "connection fail"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Exec: %v, want %s", err, want)
	}

	// The client side error triggers a retry in exec.
	compareTimingCounts(t, "Connect", 1, startCounts, tabletenv.MySQLStats.Counts())
	compareTimingCounts(t, "Exec", 2, startCounts, tabletenv.MySQLStats.Counts())

	startCounts = tabletenv.MySQLStats.Counts()

	// Set the connection fail flag and try again.
	// This time the initial query fails as does the reconnect attempt.
	db.EnableConnFail()
	_, err = dbConn.Exec(ctx, sql, 1, false)
	want = "packet read failed"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Exec: %v, want %s", err, want)
	}
	db.DisableConnFail()

	compareTimingCounts(t, "Connect", 1, startCounts, tabletenv.MySQLStats.Counts())
	compareTimingCounts(t, "ConnectError", 1, startCounts, tabletenv.MySQLStats.Counts())
	compareTimingCounts(t, "Exec", 1, startCounts, tabletenv.MySQLStats.Counts())
}

func TestDBConnDeadline(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	startCounts := tabletenv.MySQLStats.Counts()
	sql := "select * from test_table limit 1000"
	expectedResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
		},
		RowsAffected: 1,
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("123")},
		},
	}
	db.AddQuery(sql, expectedResult)

	connPool := newPool()
	connPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer connPool.Close()

	db.SetConnDelay(100 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(50*time.Millisecond))
	defer cancel()

	dbConn, err := NewDBConn(connPool, db.ConnParams())
	defer dbConn.Close()
	if err != nil {
		t.Fatalf("should not get an error, err: %v", err)
	}

	_, err = dbConn.Exec(ctx, sql, 1, false)
	want := "context deadline exceeded before execution started"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Exec: %v, want %s", err, want)
	}

	compareTimingCounts(t, "Connect", 1, startCounts, tabletenv.MySQLStats.Counts())
	compareTimingCounts(t, "ConnectError", 0, startCounts, tabletenv.MySQLStats.Counts())
	compareTimingCounts(t, "Exec", 0, startCounts, tabletenv.MySQLStats.Counts())

	startCounts = tabletenv.MySQLStats.Counts()

	ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()

	result, err := dbConn.Exec(ctx, sql, 1, false)
	if err != nil {
		t.Fatalf("should not get an error, err: %v", err)
	}
	expectedResult.Fields = nil
	if !reflect.DeepEqual(expectedResult, result) {
		t.Errorf("Exec: %v, want %v", expectedResult, result)
	}

	compareTimingCounts(t, "Connect", 0, startCounts, tabletenv.MySQLStats.Counts())
	compareTimingCounts(t, "ConnectError", 0, startCounts, tabletenv.MySQLStats.Counts())
	compareTimingCounts(t, "Exec", 1, startCounts, tabletenv.MySQLStats.Counts())

	startCounts = tabletenv.MySQLStats.Counts()

	// Test with just the background context (with no deadline)
	result, err = dbConn.Exec(context.Background(), sql, 1, false)
	if err != nil {
		t.Fatalf("should not get an error, err: %v", err)
	}
	expectedResult.Fields = nil
	if !reflect.DeepEqual(expectedResult, result) {
		t.Errorf("Exec: %v, want %v", expectedResult, result)
	}

	compareTimingCounts(t, "Connect", 0, startCounts, tabletenv.MySQLStats.Counts())
	compareTimingCounts(t, "ConnectError", 0, startCounts, tabletenv.MySQLStats.Counts())
	compareTimingCounts(t, "Exec", 1, startCounts, tabletenv.MySQLStats.Counts())

}

func TestDBConnKill(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	connPool := newPool()
	connPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer connPool.Close()
	dbConn, err := NewDBConn(connPool, db.ConnParams())
	defer dbConn.Close()
	query := fmt.Sprintf("kill %d", dbConn.ID())
	db.AddQuery(query, &sqltypes.Result{})
	// Kill failed because we are not able to connect to the database
	db.EnableConnFail()
	err = dbConn.Kill("test kill", 0)
	want := "errno 2013"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Exec: %v, want %s", err, want)
	}
	db.DisableConnFail()

	// Kill succeed
	err = dbConn.Kill("test kill", 0)
	if err != nil {
		t.Fatalf("kill should succeed, but got error: %v", err)
	}

	err = dbConn.reconnect()
	if err != nil {
		t.Fatalf("reconnect should succeed, but got error: %v", err)
	}
	newKillQuery := fmt.Sprintf("kill %d", dbConn.ID())
	// Kill failed because "kill query_id" failed
	db.AddRejectedQuery(newKillQuery, errors.New("rejected"))
	err = dbConn.Kill("test kill", 0)
	want = "rejected"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Exec: %v, want %s", err, want)
	}
}

func TestDBNoPoolConnKill(t *testing.T) {
	db := fakesqldb.New(t)
	connPool := newPool()
	connPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer connPool.Close()
	defer db.Close()
	dbConn, err := NewDBConnNoPool(db.ConnParams(), connPool.dbaPool)
	defer dbConn.Close()
	query := fmt.Sprintf("kill %d", dbConn.ID())
	db.AddQuery(query, &sqltypes.Result{})
	// Kill failed because we are not able to connect to the database
	db.EnableConnFail()
	err = dbConn.Kill("test kill", 0)
	want := "errno 2013"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Exec: %v, want %s", err, want)
	}
	db.DisableConnFail()

	// Kill succeed
	err = dbConn.Kill("test kill", 0)
	if err != nil {
		t.Fatalf("kill should succeed, but got error: %v", err)
	}

	err = dbConn.reconnect()
	if err != nil {
		t.Fatalf("reconnect should succeed, but got error: %v", err)
	}
	newKillQuery := fmt.Sprintf("kill %d", dbConn.ID())
	// Kill failed because "kill query_id" failed
	db.AddRejectedQuery(newKillQuery, errors.New("rejected"))
	err = dbConn.Kill("test kill", 0)
	want = "rejected"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Exec: %v, want %s", err, want)
	}
}

func TestDBConnStream(t *testing.T) {
	db := fakesqldb.New(t)
	defer db.Close()
	sql := "select * from test_table limit 1000"
	expectedResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{
			{sqltypes.NewVarChar("123")},
		},
	}
	db.AddQuery(sql, expectedResult)
	connPool := newPool()
	connPool.Open(db.ConnParams(), db.ConnParams(), db.ConnParams())
	defer connPool.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()
	dbConn, err := NewDBConn(connPool, db.ConnParams())
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
	if !reflect.DeepEqual(expectedResult, &result) {
		t.Errorf("Exec: %v, want %v", expectedResult, &result)
	}
	// Stream fail
	db.Close()
	dbConn.Close()
	err = dbConn.Stream(
		ctx, sql, func(r *sqltypes.Result) error {
			return nil
		}, 10, querypb.ExecuteOptions_ALL)
	db.DisableConnFail()
	want := "no such file or directory (errno 2002)"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("Error: '%v', must contain '%s'", err, want)
	}
}
