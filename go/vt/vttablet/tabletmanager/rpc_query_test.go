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
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
		Sql:                     []byte("create /*vt+ allowZeroInDate=true */ table t (id int primary key)"),
		DbName:                  dbName,
		MaxRows:                 10,
		DisableBinlogs:          true,
		DisableForeignKeyChecks: true,
		SessionVariables: []*tabletmanagerdatapb.SessionVariable{
			{Name: "innodb_strict_mode", Value: "off"},
			{Name: "sql_mode", Value: "NO_ZERO_DATE"},
		},
	})
	require.NoError(t, err)

	got := strings.Split(db.QueryLog(), ";")
	require.Contains(t, got, "use `testdb`")
	firstVariableIdx := -1
	secondVariableIdx := -1
	disableBinlogsIdx := -1
	disableForeignKeyChecksIdx := -1
	allowZeroInDateIdx := -1
	createIdx := -1
	for i, q := range got {
		q = strings.ToLower(strings.TrimSpace(q))
		if q == "set @@session.innodb_strict_mode=x'6f6666'" {
			firstVariableIdx = i
		}
		if q == "set @@session.sql_mode=x'4e4f5f5a45524f5f44415445'" {
			secondVariableIdx = i
		}
		if q == "set sql_log_bin = off" {
			disableBinlogsIdx = i
		}
		if q == "set session foreign_key_checks = off" {
			disableForeignKeyChecksIdx = i
		}
		if strings.Contains(q, "set @@session.sql_mode=replace") {
			allowZeroInDateIdx = i
		}
		if strings.Contains(q, "create /*vt+ allowzeroindate=true */ table t") {
			createIdx = i
		}
	}
	require.NotEqual(t, -1, firstVariableIdx, "expected first session variable setup in %v", got)
	require.NotEqual(t, -1, secondVariableIdx, "expected second session variable setup in %v", got)
	require.NotEqual(t, -1, disableBinlogsIdx, "expected binlogs disabled in %v", got)
	require.NotEqual(t, -1, disableForeignKeyChecksIdx, "expected foreign key checks disabled in %v", got)
	require.NotEqual(t, -1, allowZeroInDateIdx, "expected zero-date modes removed in %v", got)
	require.NotEqual(t, -1, createIdx, "expected schema SQL")
	assert.Less(t, firstVariableIdx, secondVariableIdx)
	assert.Less(t, secondVariableIdx, disableBinlogsIdx)
	assert.Less(t, secondVariableIdx, disableForeignKeyChecksIdx)
	assert.Less(t, secondVariableIdx, allowZeroInDateIdx)
	assert.Less(t, disableBinlogsIdx, createIdx)
	assert.Less(t, disableForeignKeyChecksIdx, createIdx)
	assert.Less(t, allowZeroInDateIdx, createIdx)
}

// TestTabletManager_ExecuteMultiFetchAsDbaSessionVariableFailure verifies a
// failed assignment prevents schema DDL.
func TestTabletManager_ExecuteMultiFetchAsDbaSessionVariableFailure(t *testing.T) {
	ctx := t.Context()
	cp := mysql.ConnParams{}
	db := fakesqldb.New(t)
	db.AddQueryPattern(".*", &sqltypes.Result{})
	db.AddRejectedQuery("set @@session.sql_mode=X'4e4f5f5a45524f5f44415445'", errors.New("cannot set session variable"))
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
			{Name: "sql_mode", Value: "NO_ZERO_DATE"},
		},
	})
	require.ErrorContains(t, err, "cannot set session variable")
	assert.NotContains(t, db.QueryLog(), "create table t")
}

// TestTabletManager_ExecuteMultiFetchAsDbaDeniedSessionVariables verifies RPC
// callers cannot bypass the session variable deny list.
func TestTabletManager_ExecuteMultiFetchAsDbaDeniedSessionVariables(t *testing.T) {
	cp := mysql.ConnParams{}
	db := fakesqldb.New(t)
	db.AddQueryPattern(".*", &sqltypes.Result{})
	daemon := mysqlctl.NewFakeMysqlDaemon(db)

	tm := &TabletManager{
		MysqlDaemon:            daemon,
		DBConfigs:              dbconfigs.NewTestDBConfigs(cp, cp, "testdb"),
		QueryServiceControl:    tabletservermock.NewController(),
		_waitForGrantsComplete: make(chan struct{}),
		Env:                    vtenv.NewTestEnv(),
	}
	close(tm._waitForGrantsComplete)

	for _, variableName := range []string{"SQL_LOG_BIN", "FOREIGN_KEY_CHECKS"} {
		t.Run(variableName, func(t *testing.T) {
			_, err := tm.ExecuteMultiFetchAsDba(
				t.Context(),
				&tabletmanagerdatapb.ExecuteMultiFetchAsDbaRequest{
					Sql:    []byte("create table t (id int primary key)"),
					DbName: "testdb",
					SessionVariables: []*tabletmanagerdatapb.SessionVariable{
						{Name: variableName, Value: "off"},
					},
				},
			)
			require.EqualError(
				t,
				err,
				`session variable "`+variableName+`" is not allowed`,
			)
			assert.NotContains(t, db.QueryLog(), "create table t")
		})
	}
}

// multiStatementRecorder is a fakesqldb query handler that records whether the
// connections it serves queries on negotiated multi statement support.
type multiStatementRecorder struct {
	db *fakesqldb.DB
	// enabled is written on the connection goroutine of the fake server, and
	// read by the test once the request it belongs to is done.
	enabled atomic.Bool

	// mu guards enabledQueries, which the connection goroutine of the fake
	// server appends to and the test reads once the request is done.
	mu sync.Mutex
	// enabledQueries are the queries that were executed while the connection
	// could send several statements at once.
	enabledQueries []string
}

func (r *multiStatementRecorder) HandleQuery(c *mysql.Conn, query string, callback func(*sqltypes.Result) error) error {
	if c.Capabilities&mysql.CapabilityClientMultiStatements != 0 {
		r.enabled.Store(true)
		r.mu.Lock()
		r.enabledQueries = append(r.enabledQueries, query)
		r.mu.Unlock()
	}
	return r.db.HandleQuery(c, query, callback)
}

func (r *multiStatementRecorder) queriesRunWithMultiStatements() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.enabledQueries)
}

// TestTabletManager_ExecuteMultiFetchAsDbaMultiStatements checks that only a
// request carrying several statements turns multi statement support on for the
// connection it runs on.
func TestTabletManager_ExecuteMultiFetchAsDbaMultiStatements(t *testing.T) {
	testCases := []struct {
		name string
		sql  string
		// statements the fake server is expected to execute, one by one.
		statements  []string
		wantEnabled bool
	}{{
		name:        "single statement",
		sql:         "create table t1 (id int primary key)",
		statements:  []string{"create table t1 (id int primary key)"},
		wantEnabled: false,
	}, {
		name: "several statements",
		// The pieces the server executes are the text between the semicolons,
		// so keep the statements free of surrounding whitespace.
		sql:         "create table t1 (id int primary key);create table t2 (id int primary key)",
		statements:  []string{"create table t1 (id int primary key)", "create table t2 (id int primary key)"},
		wantEnabled: true,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			cp := mysql.ConnParams{}
			db := fakesqldb.New(t)
			db.AddQueryPattern(".*", &sqltypes.Result{})
			recorder := &multiStatementRecorder{db: db}
			db.Handler = recorder
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

			results, err := tm.ExecuteMultiFetchAsDba(ctx, &tabletmanagerdatapb.ExecuteMultiFetchAsDbaRequest{
				Sql:     []byte(tc.sql),
				DbName:  dbName,
				MaxRows: 10,
			})
			require.NoError(t, err)
			require.Len(t, results, len(tc.statements))
			// Each statement must reach the server on its own. A batch that was
			// sent without the capability is counted under the joined query.
			for _, statement := range tc.statements {
				require.Equal(t, 1, db.GetQueryCalledNum(statement), "statement %q was not executed on its own, query log: %v", statement, db.QueryLog())
			}
			require.Equal(t, tc.wantEnabled, recorder.enabled.Load())
			// The capability is asked for as late as possible, so the only
			// thing that ever runs on a connection that can send a batch is the
			// batch itself. Everything before it -- the session variables, the
			// USE, the sql_log_bin and foreign key checks -- is a single
			// statement and has no need for the capability.
			if tc.wantEnabled {
				require.Equal(t, tc.statements, recorder.queriesRunWithMultiStatements())
			} else {
				require.Empty(t, recorder.queriesRunWithMultiStatements())
			}
		})
	}
}

// TestTabletManager_ExecuteFetchCompoundStatement checks that the RPCs which run
// a single statement do not decide for themselves what one statement is. Our
// splitter cuts a CREATE TRIGGER body at its semicolons and cannot parse the
// statement at all, so anything counting statements here would turn valid SQL
// away; MySQL is what says whether a query holds one statement or several.
func TestTabletManager_ExecuteFetchCompoundStatement(t *testing.T) {
	ctx := t.Context()
	cp := mysql.ConnParams{}
	db := fakesqldb.New(t)
	db.AddQueryPattern(".*", &sqltypes.Result{})
	daemon := mysqlctl.NewFakeMysqlDaemon(db)

	const dbName = "vt_test"
	tm := &TabletManager{
		MysqlDaemon:            daemon,
		DBConfigs:              dbconfigs.NewTestDBConfigs(cp, cp, dbName),
		QueryServiceControl:    tabletservermock.NewController(),
		_waitForGrantsComplete: make(chan struct{}),
		Env:                    vtenv.NewTestEnv(),
	}
	close(tm._waitForGrantsComplete)

	const trigger = "create trigger t1_bi before insert on t1 for each row begin set @x = 1; set @y = 2; end"
	// Three pieces, none of them valid on its own.
	pieces, err := tm.Env.Parser().SplitStatementToPieces(trigger)
	require.NoError(t, err)
	require.Len(t, pieces, 3, "this test is pointless if the splitter stops cutting the trigger body")

	_, err = tm.ExecuteFetchAsApp(ctx, &tabletmanagerdatapb.ExecuteFetchAsAppRequest{
		Query:   []byte(trigger),
		MaxRows: 10,
	})
	require.NoError(t, err)

	_, err = tm.ExecuteFetchAsAllPrivs(ctx, &tabletmanagerdatapb.ExecuteFetchAsAllPrivsRequest{
		Query:   []byte(trigger),
		DbName:  dbName,
		MaxRows: 10,
	})
	require.NoError(t, err)

	// It reaches the server whole, for MySQL to parse.
	require.Contains(t, db.QueryLog(), trigger)
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
