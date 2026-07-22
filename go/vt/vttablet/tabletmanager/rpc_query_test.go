/*
Copyright 2020 The Vitess Authors.

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

package tabletmanager

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/mysqlctl"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletservermock"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestAnalyzeExecuteFetchAsDbaMultiQuery(t *testing.T) {
	tcases := []struct {
		query           string
		count           int
		parseable       bool
		allowZeroInDate bool
		allCreate       bool
		expectErr       bool
	}{
		{
			query:     "",
			expectErr: true,
		},
		{
			query:     "select * from t1 ; select * from t2",
			count:     2,
			parseable: true,
		},
		{
			query:     "create table t(id int)",
			count:     1,
			allCreate: true,
			parseable: true,
		},
		{
			query:     "create table t(id int); create view v as select 1 from dual",
			count:     2,
			allCreate: true,
			parseable: true,
		},
		{
			query:     "create table t(id int); create view v as select 1 from dual; drop table t3",
			count:     3,
			allCreate: false,
			parseable: true,
		},
		{
			query:           "create /*vt+ allowZeroInDate=true */ table t (id int)",
			count:           1,
			allCreate:       true,
			allowZeroInDate: true,
			parseable:       true,
		},
		{
			query:           "create table a (id int) ; create /*vt+ allowZeroInDate=true */ table b (id int)",
			count:           2,
			allCreate:       true,
			allowZeroInDate: true,
			parseable:       true,
		},
		{
			query:     "stop replica; start replica",
			count:     2,
			parseable: false,
		},
		{
			query:     "create table a (id int) ; --comment ; what",
			count:     3,
			parseable: false,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.query, func(t *testing.T) {
			parser := sqlparser.NewTestParser()
			queries, parsedStmts, parseable, countCreate, allowZeroInDate, err := analyzeExecuteFetchAsDbaMultiQuery(tcase.query, parser)
			if tcase.expectErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Len(t, queries, tcase.count)
				assert.Len(t, parsedStmts, len(queries))
				assert.Equal(t, tcase.parseable, parseable)
				assert.Equal(t, tcase.allCreate, (countCreate == len(queries)))
				assert.Equal(t, tcase.allowZeroInDate, allowZeroInDate)
				// Verify parsedStmts contract: nil entries iff parse failed.
				gotAllParsed := true
				for _, stmt := range parsedStmts {
					if stmt == nil {
						gotAllParsed = false
						break
					}
				}
				assert.Equal(t, tcase.parseable, gotAllParsed,
					"parsedStmts non-nil count must match parseable flag")
			}
		})
	}
}

func TestTabletManager_MysqlHostMetricsNilCnf(t *testing.T) {
	ctx := t.Context()
	// When using external MySQL (e.g. Cloud SQL, RDS), Cnf is nil because
	// vttablet skips loading my.cnf when connection parameters are specified.
	// MysqlHostMetrics should return an empty response instead of panicking.
	tm := &TabletManager{
		Cnf: nil,
	}
	resp, err := tm.MysqlHostMetrics(ctx, &tabletmanagerdatapb.MysqlHostMetricsRequest{})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Nil(t, resp.HostMetrics)
}

// TestTabletManager_ExecuteMultiFetchAsDbaSessionVariables verifies assignments
// execute in order before schema DDL.
func TestTabletManager_ExecuteMultiFetchAsDbaSessionVariables(t *testing.T) {
	ctx := t.Context()
	cp := mysql.ConnParams{}
	db := fakesqldb.New(t)
	db.AddQueryPattern(".*", &sqltypes.Result{})
	daemon := mysqlctl.NewFakeMysqlDaemon(db)

	dbName := "testdb"
	tm := &TabletManager{
		MysqlDaemon:            daemon,
		DBConfigs:              dbconfigs.NewTestDBConfigs(cp, cp, dbName),
		QueryServiceControl:    tabletservermock.NewController(),
		_waitForGrantsComplete: make(chan struct{}),
		Env:                    vtenv.NewTestEnv(),
	}
	close(tm._waitForGrantsComplete)

	_, err := tm.ExecuteMultiFetchAsDba(ctx, &tabletmanagerdatapb.ExecuteMultiFetchAsDbaRequest{
		Sql:     []byte("create table t (id int primary key)"),
		DbName:  dbName,
		MaxRows: 10,
		SessionVariables: []*tabletmanagerdatapb.SessionVariable{
			{Name: "innodb_strict_mode", Value: "off"},
			{Name: "sql_mode", Value: "ANSI"},
		},
	})
	require.NoError(t, err)

	got := strings.Split(db.QueryLog(), ";")
	require.Contains(t, got, "use `testdb`")
	firstVariableIdx := -1
	secondVariableIdx := -1
	createIdx := -1
	for i, q := range got {
		q = strings.ToLower(strings.TrimSpace(q))
		if q == "set @@session.innodb_strict_mode=x'6f6666'" {
			firstVariableIdx = i
		}
		if q == "set @@session.sql_mode=x'414e5349'" {
			secondVariableIdx = i
		}
		if strings.Contains(q, "create table t") {
			createIdx = i
		}
	}
	require.NotEqual(t, -1, firstVariableIdx, "expected first session variable setup in %v", got)
	require.NotEqual(t, -1, secondVariableIdx, "expected second session variable setup in %v", got)
	require.NotEqual(t, -1, createIdx, "expected schema SQL")
	assert.Less(t, firstVariableIdx, secondVariableIdx)
	assert.Less(t, secondVariableIdx, createIdx, "all session variables must be set before schema SQL")
}

// TestTabletManager_ExecuteMultiFetchAsDbaSessionVariableFailure verifies a
// failed assignment prevents schema DDL.
func TestTabletManager_ExecuteMultiFetchAsDbaSessionVariableFailure(t *testing.T) {
	ctx := t.Context()
	cp := mysql.ConnParams{}
	db := fakesqldb.New(t)
	db.AddQueryPattern(".*", &sqltypes.Result{})
	db.AddRejectedQuery("set @@session.sql_mode=X'414e5349'", errors.New("cannot set session variable"))
	daemon := mysqlctl.NewFakeMysqlDaemon(db)

	tm := &TabletManager{
		MysqlDaemon:            daemon,
		DBConfigs:              dbconfigs.NewTestDBConfigs(cp, cp, "testdb"),
		QueryServiceControl:    tabletservermock.NewController(),
		_waitForGrantsComplete: make(chan struct{}),
		Env:                    vtenv.NewTestEnv(),
	}
	close(tm._waitForGrantsComplete)

	_, err := tm.ExecuteMultiFetchAsDba(ctx, &tabletmanagerdatapb.ExecuteMultiFetchAsDbaRequest{
		Sql:    []byte("create table t (id int primary key)"),
		DbName: "testdb",
		SessionVariables: []*tabletmanagerdatapb.SessionVariable{
			{Name: "sql_mode", Value: "ANSI"},
		},
	})
	require.ErrorContains(t, err, "cannot set session variable")
	assert.NotContains(t, db.QueryLog(), "create table t")
}

func TestTabletManager_ExecuteFetchAsDba(t *testing.T) {
	ctx := t.Context()
	cp := mysql.ConnParams{}
	db := fakesqldb.New(t)
	db.AddQueryPattern(".*", &sqltypes.Result{})
	daemon := mysqlctl.NewFakeMysqlDaemon(db)

	dbName := " escap`e me "
	tm := &TabletManager{
		MysqlDaemon:            daemon,
		DBConfigs:              dbconfigs.NewTestDBConfigs(cp, cp, dbName),
		QueryServiceControl:    tabletservermock.NewController(),
		_waitForGrantsComplete: make(chan struct{}),
		Env:                    vtenv.NewTestEnv(),
	}
	close(tm._waitForGrantsComplete)

	_, err := tm.ExecuteFetchAsDba(ctx, &tabletmanagerdatapb.ExecuteFetchAsDbaRequest{
		Query:   []byte("select 42"),
		DbName:  dbName,
		MaxRows: 10,
	})
	require.NoError(t, err)
	want := []string{
		"use ` escap``e me `",
		"select 42",
	}
	got := strings.Split(db.QueryLog(), ";")
	for _, w := range want {
		require.Contains(t, got, w)
	}
}
