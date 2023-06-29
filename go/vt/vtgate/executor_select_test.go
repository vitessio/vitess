/*
Copyright 2019 The Vitess Authors.

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

package vtgate

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	_flag "vitess.io/vitess/go/internal/flag"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/cache"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/vterrors"
	_ "vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func TestSelectNext(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	query := "select next :n values from user_seq"
	bv := map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(2)}
	wantQueries := []*querypb.BoundQuery{{
		Sql:           query,
		BindVariables: map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(2)},
	}}

	// Autocommit
	session := NewAutocommitSession(&vtgatepb.Session{})
	_, err := executor.Execute(context.Background(), "TestSelectNext", session, query, bv)
	require.NoError(t, err)

	utils.MustMatch(t, wantQueries, sbclookup.Queries)
	assert.Zero(t, sbclookup.BeginCount.Load())
	assert.Zero(t, sbclookup.ReserveCount.Load())
	sbclookup.Queries = nil

	// Txn
	session = NewAutocommitSession(&vtgatepb.Session{})
	session.Session.InTransaction = true
	_, err = executor.Execute(context.Background(), "TestSelectNext", session, query, bv)
	require.NoError(t, err)

	utils.MustMatch(t, wantQueries, sbclookup.Queries)
	assert.Zero(t, sbclookup.BeginCount.Load())
	assert.Zero(t, sbclookup.ReserveCount.Load())
	sbclookup.Queries = nil

	// Reserve
	session = NewAutocommitSession(&vtgatepb.Session{})
	session.Session.InReservedConn = true
	_, err = executor.Execute(context.Background(), "TestSelectNext", session, query, bv)
	require.NoError(t, err)

	utils.MustMatch(t, wantQueries, sbclookup.Queries)
	assert.Zero(t, sbclookup.BeginCount.Load())
	assert.Zero(t, sbclookup.ReserveCount.Load())
	sbclookup.Queries = nil

	// Reserve and Txn
	session = NewAutocommitSession(&vtgatepb.Session{})
	session.Session.InReservedConn = true
	session.Session.InTransaction = true
	_, err = executor.Execute(context.Background(), "TestSelectNext", session, query, bv)
	require.NoError(t, err)

	utils.MustMatch(t, wantQueries, sbclookup.Queries)
	assert.Zero(t, sbclookup.BeginCount.Load())
	assert.Zero(t, sbclookup.ReserveCount.Load())
}

func TestSelectDBA(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()

	query := "select * from INFORMATION_SCHEMA.foo"
	_, err := executor.Execute(context.Background(), "TestSelectDBA",
		NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"}),
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{Sql: query, BindVariables: map[string]*querypb.BindVariable{}}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)

	sbc1.Queries = nil
	query = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES ist WHERE ist.table_schema = 'performance_schema' AND ist.table_name = 'foo'"
	_, err = executor.Execute(context.Background(), "TestSelectDBA",
		NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"}),
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{Sql: "select count(*) from INFORMATION_SCHEMA.`TABLES` as ist where ist.table_schema = :__vtschemaname /* VARCHAR */ and ist.table_name = :ist_table_name /* VARCHAR */",
		BindVariables: map[string]*querypb.BindVariable{
			"__vtschemaname": sqltypes.StringBindVariable("performance_schema"),
			"ist_table_name": sqltypes.StringBindVariable("foo"),
		}}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)

	sbc1.Queries = nil
	query = "select 1 from information_schema.table_constraints where constraint_schema = 'vt_ks' and table_name = 'user'"
	_, err = executor.Execute(context.Background(), "TestSelectDBA",
		NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"}),
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{Sql: "select 1 from information_schema.table_constraints where constraint_schema = :__vtschemaname /* VARCHAR */ and table_name = :table_name /* VARCHAR */",
		BindVariables: map[string]*querypb.BindVariable{
			"__vtschemaname": sqltypes.StringBindVariable("vt_ks"),
			"table_name":     sqltypes.StringBindVariable("user"),
		}}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)

	sbc1.Queries = nil
	query = "select 1 from information_schema.table_constraints where constraint_schema = 'vt_ks'"
	_, err = executor.Execute(context.Background(), "TestSelectDBA",
		NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"}),
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{Sql: "select 1 from information_schema.table_constraints where constraint_schema = :__vtschemaname /* VARCHAR */",
		BindVariables: map[string]*querypb.BindVariable{
			"__vtschemaname": sqltypes.StringBindVariable("vt_ks"),
		}}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
}

func TestSystemVariablesMySQLBelow80(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	executor.normalize = true

	sqlparser.SetParserVersion("57000")
	setVarEnabled = true

	session := NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true, TargetString: "TestExecutor"})

	sbc1.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "orig", Type: sqltypes.VarChar},
			{Name: "new", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar(""),
			sqltypes.NewVarChar("only_full_group_by"),
		}},
	}})

	_, err := executor.Execute(context.Background(), "TestSetStmt", session, "set @@sql_mode = only_full_group_by", map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	_, err = executor.Execute(context.Background(), "TestSelect", session, "select 1 from information_schema.table", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.True(t, session.InReservedConn())

	wantQueries := []*querypb.BoundQuery{
		{Sql: "select @@sql_mode orig, 'only_full_group_by' new"},
		{Sql: "set sql_mode = 'only_full_group_by'", BindVariables: map[string]*querypb.BindVariable{"vtg1": {Type: sqltypes.Int64, Value: []byte("1")}}},
		{Sql: "select :vtg1 /* INT64 */ from information_schema.`table`", BindVariables: map[string]*querypb.BindVariable{"vtg1": {Type: sqltypes.Int64, Value: []byte("1")}}},
	}

	utils.MustMatch(t, wantQueries, sbc1.Queries)
}

func TestSystemVariablesWithSetVarDisabled(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	executor.normalize = true

	sqlparser.SetParserVersion("80000")
	setVarEnabled = false
	defer func() {
		setVarEnabled = true
	}()
	session := NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true, TargetString: "TestExecutor"})

	sbc1.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "orig", Type: sqltypes.VarChar},
			{Name: "new", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar(""),
			sqltypes.NewVarChar("only_full_group_by"),
		}},
	}})

	_, err := executor.Execute(context.Background(), "TestSetStmt", session, "set @@sql_mode = only_full_group_by", map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	_, err = executor.Execute(context.Background(), "TestSelect", session, "select 1 from information_schema.table", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.True(t, session.InReservedConn())

	wantQueries := []*querypb.BoundQuery{
		{Sql: "select @@sql_mode orig, 'only_full_group_by' new"},
		{Sql: "set sql_mode = 'only_full_group_by'", BindVariables: map[string]*querypb.BindVariable{"vtg1": {Type: sqltypes.Int64, Value: []byte("1")}}},
		{Sql: "select :vtg1 /* INT64 */ from information_schema.`table`", BindVariables: map[string]*querypb.BindVariable{"vtg1": {Type: sqltypes.Int64, Value: []byte("1")}}},
	}

	utils.MustMatch(t, wantQueries, sbc1.Queries)
}

func TestSetSystemVariablesTx(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	executor.normalize = true

	sqlparser.SetParserVersion("80001")

	session := NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true, TargetString: "TestExecutor"})

	_, err := executor.Execute(context.Background(), "TestBegin", session, "begin", map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	_, err = executor.Execute(context.Background(), "TestSelect", session, "select 1 from information_schema.table", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.NotZero(t, session.ShardSessions)

	sbc1.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "orig", Type: sqltypes.VarChar},
			{Name: "new", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar(""),
			sqltypes.NewVarChar("only_full_group_by"),
		}},
	}})

	_, err = executor.Execute(context.Background(), "TestSetStmt", session, "set @@sql_mode = only_full_group_by", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.False(t, session.InReservedConn())

	_, err = executor.Execute(context.Background(), "TestSelect", session, "select 1 from information_schema.table", map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	_, err = executor.Execute(context.Background(), "TestCommit", session, "commit", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.False(t, session.InReservedConn())

	require.Zero(t, session.ShardSessions)

	wantQueries := []*querypb.BoundQuery{
		{Sql: "select :vtg1 /* INT64 */ from information_schema.`table`", BindVariables: map[string]*querypb.BindVariable{"vtg1": {Type: sqltypes.Int64, Value: []byte("1")}}},
		{Sql: "select @@sql_mode orig, 'only_full_group_by' new"},
		{Sql: "select /*+ SET_VAR(sql_mode = 'only_full_group_by') */ :vtg1 /* INT64 */ from information_schema.`table`", BindVariables: map[string]*querypb.BindVariable{"vtg1": {Type: sqltypes.Int64, Value: []byte("1")}}},
	}

	utils.MustMatch(t, wantQueries, sbc1.Queries)
}

func TestSetSystemVariables(t *testing.T) {
	executor, _, _, lookup := createExecutorEnv()
	executor.normalize = true

	sqlparser.SetParserVersion("80001")

	session := NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true, TargetString: KsTestUnsharded, SystemVariables: map[string]string{}})

	// Set @@sql_mode and execute a select statement. We should have SET_VAR in the select statement

	lookup.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "orig", Type: sqltypes.VarChar},
			{Name: "new", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar(""),
			sqltypes.NewVarChar("only_full_group_by"),
		}},
	}})
	_, err := executor.Execute(context.Background(), "TestSetStmt", session, "set @@sql_mode = only_full_group_by", map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	_, err = executor.Execute(context.Background(), "TestSelect", session, "select 1 from information_schema.table", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.False(t, session.InReservedConn())
	wantQueries := []*querypb.BoundQuery{
		{Sql: "select @@sql_mode orig, 'only_full_group_by' new"},
		{Sql: "select /*+ SET_VAR(sql_mode = 'only_full_group_by') */ :vtg1 /* INT64 */ from information_schema.`table`", BindVariables: map[string]*querypb.BindVariable{"vtg1": {Type: sqltypes.Int64, Value: []byte("1")}}},
	}
	utils.MustMatch(t, wantQueries, lookup.Queries)
	lookup.Queries = nil

	// Execute a select with a comment that needs a query hint

	_, err = executor.Execute(context.Background(), "TestSelect", session, "select /* comment */ 1 from information_schema.table", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.False(t, session.InReservedConn())
	wantQueries = []*querypb.BoundQuery{
		{Sql: "select /*+ SET_VAR(sql_mode = 'only_full_group_by') */ /* comment */ :vtg1 /* INT64 */ from information_schema.`table`", BindVariables: map[string]*querypb.BindVariable{"vtg1": {Type: sqltypes.Int64, Value: []byte("1")}}},
	}
	utils.MustMatch(t, wantQueries, lookup.Queries)
	lookup.Queries = nil

	lookup.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "sql_safe_updates", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("0"),
		}},
	}})
	_, err = executor.Execute(context.Background(), "TestSetStmt", session, "set @@sql_safe_updates = 0", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.False(t, session.InReservedConn())
	wantQueries = []*querypb.BoundQuery{
		{Sql: "select 0 from dual where @@sql_safe_updates != 0"},
	}
	utils.MustMatch(t, wantQueries, lookup.Queries)
	lookup.Queries = nil

	_, err = executor.Execute(context.Background(), "TestSetStmt", session, "set @var = @@sql_mode", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.False(t, session.InReservedConn())
	require.Nil(t, lookup.Queries)
	require.Equal(t, "only_full_group_by", string(session.UserDefinedVariables["var"].GetValue()))

	lookup.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "max_tmp_tables", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("4"),
		}},
	}})
	_, err = executor.Execute(context.Background(), "TestSetStmt", session, "set @x = @@sql_mode, @y = @@max_tmp_tables", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.False(t, session.InReservedConn())
	wantQueries = []*querypb.BoundQuery{
		{Sql: "select @@max_tmp_tables from dual", BindVariables: map[string]*querypb.BindVariable{"__vtsql_mode": sqltypes.StringBindVariable("only_full_group_by")}},
	}
	utils.MustMatch(t, wantQueries, lookup.Queries)
	require.Equal(t, "only_full_group_by", string(session.UserDefinedVariables["var"].GetValue()))
	require.Equal(t, "only_full_group_by", string(session.UserDefinedVariables["x"].GetValue()))
	require.Equal(t, "4", string(session.UserDefinedVariables["y"].GetValue()))
	lookup.Queries = nil

	// Set system variable that is not supported by SET_VAR
	// We expect the next select to not have any SET_VAR query hint, instead it will use set statements

	lookup.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "max_tmp_tables", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("1"),
		}},
	}})
	_, err = executor.Execute(context.Background(), "TestSetStmt", session, "set @@max_tmp_tables = 1", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.True(t, session.InReservedConn())

	_, err = executor.Execute(context.Background(), "TestSelect", session, "select 1 from information_schema.table", map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	wantQueries = []*querypb.BoundQuery{
		{Sql: "select 1 from dual where @@max_tmp_tables != 1"},
		{Sql: "set max_tmp_tables = '1', sql_mode = 'only_full_group_by', sql_safe_updates = '0'", BindVariables: map[string]*querypb.BindVariable{"vtg1": {Type: sqltypes.Int64, Value: []byte("1")}}},
		{Sql: "select :vtg1 /* INT64 */ from information_schema.`table`", BindVariables: map[string]*querypb.BindVariable{"vtg1": {Type: sqltypes.Int64, Value: []byte("1")}}},
	}
	utils.MustMatch(t, wantQueries, lookup.Queries)
}

func TestSetSystemVariablesWithReservedConnection(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	executor.normalize = true

	session := NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true, SystemVariables: map[string]string{}})

	sbc1.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "orig", Type: sqltypes.VarChar},
			{Name: "new", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("only_full_group_by"),
			sqltypes.NewVarChar(""),
		}},
	}})
	_, err := executor.Execute(context.Background(), "TestSetStmt", session, "set @@sql_mode = ''", map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	_, err = executor.Execute(context.Background(), "TestSelect", session, "select age, city from user group by age", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.True(t, session.InReservedConn())
	wantQueries := []*querypb.BoundQuery{
		{Sql: "select @@sql_mode orig, '' new"},
		{Sql: "set sql_mode = ''"},
		{Sql: "select age, city, weight_string(age) from `user` group by age, weight_string(age) order by age asc"},
	}
	utils.MustMatch(t, wantQueries, sbc1.Queries)

	_, err = executor.Execute(context.Background(), "TestSelect", session, "select age, city+1 from user group by age", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.True(t, session.InReservedConn())
	wantQueries = []*querypb.BoundQuery{
		{Sql: "select @@sql_mode orig, '' new"},
		{Sql: "set sql_mode = ''"},
		{Sql: "select age, city, weight_string(age) from `user` group by age, weight_string(age) order by age asc"},
		{Sql: "select age, city + :vtg1 /* INT64 */, weight_string(age) from `user` group by age, weight_string(age) order by age asc", BindVariables: map[string]*querypb.BindVariable{"vtg1": {Type: sqltypes.Int64, Value: []byte("1")}}},
	}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	require.Equal(t, "''", session.SystemVariables["sql_mode"])
	sbc1.Queries = nil
}

func TestCreateTableValidTimestamp(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	executor.normalize = true

	session := NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor", SystemVariables: map[string]string{"sql_mode": "ALLOW_INVALID_DATES"}})

	query := "create table aa(t timestamp default 0)"
	_, err := executor.Execute(context.Background(), "TestSelect", session, query, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	require.True(t, session.InReservedConn())

	wantQueries := []*querypb.BoundQuery{
		{Sql: "set sql_mode = ALLOW_INVALID_DATES", BindVariables: map[string]*querypb.BindVariable{}},
		{Sql: "create table aa (\n\tt timestamp default 0\n)", BindVariables: map[string]*querypb.BindVariable{}},
	}

	utils.MustMatch(t, wantQueries, sbc1.Queries)
}

func TestGen4SelectDBA(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	executor.normalize = true
	executor.pv = querypb.ExecuteOptions_Gen4

	query := "select * from INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
	_, err := executor.Execute(context.Background(), "TestSelectDBA",
		NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"}),
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	expected := "select CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME, TABLE_SCHEMA, TABLE_NAME, CONSTRAINT_TYPE, `ENFORCED` from INFORMATION_SCHEMA.TABLE_CONSTRAINTS"
	wantQueries := []*querypb.BoundQuery{{Sql: expected, BindVariables: map[string]*querypb.BindVariable{}}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)

	sbc1.Queries = nil
	query = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES ist WHERE ist.table_schema = 'performance_schema' AND ist.table_name = 'foo'"
	_, err = executor.Execute(context.Background(), "TestSelectDBA",
		NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"}),
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{Sql: "select count(*) from INFORMATION_SCHEMA.`TABLES` as ist where ist.table_schema = :__vtschemaname /* VARCHAR */ and ist.table_name = :ist_table_name1 /* VARCHAR */",
		BindVariables: map[string]*querypb.BindVariable{
			"ist_table_schema": sqltypes.StringBindVariable("performance_schema"),
			"__vtschemaname":   sqltypes.StringBindVariable("performance_schema"),
			"ist_table_name":   sqltypes.StringBindVariable("foo"),
			"ist_table_name1":  sqltypes.StringBindVariable("foo"),
		}}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)

	sbc1.Queries = nil
	query = "select 1 from information_schema.table_constraints where constraint_schema = 'vt_ks' and table_name = 'user'"
	_, err = executor.Execute(context.Background(), "TestSelectDBA",
		NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"}),
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{Sql: "select :vtg1 /* INT64 */ from information_schema.table_constraints where constraint_schema = :__vtschemaname /* VARCHAR */ and table_name = :table_name1 /* VARCHAR */",
		BindVariables: map[string]*querypb.BindVariable{
			"vtg1":              sqltypes.Int64BindVariable(1),
			"constraint_schema": sqltypes.StringBindVariable("vt_ks"),
			"table_name":        sqltypes.StringBindVariable("user"),
			"__vtschemaname":    sqltypes.StringBindVariable("vt_ks"),
			"table_name1":       sqltypes.StringBindVariable("user"),
		}}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)

	sbc1.Queries = nil
	query = "select 1 from information_schema.table_constraints where constraint_schema = 'vt_ks'"
	_, err = executor.Execute(context.Background(), "TestSelectDBA",
		NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"}),
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{Sql: "select :vtg1 /* INT64 */ from information_schema.table_constraints where constraint_schema = :__vtschemaname /* VARCHAR */",
		BindVariables: map[string]*querypb.BindVariable{
			"vtg1":              sqltypes.Int64BindVariable(1),
			"constraint_schema": sqltypes.StringBindVariable("vt_ks"),
			"__vtschemaname":    sqltypes.StringBindVariable("vt_ks"),
		}}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)

	sbc1.Queries = nil
	query = "select t.table_schema,t.table_name,c.column_name,c.column_type from tables t join columns c on c.table_schema = t.table_schema and c.table_name = t.table_name where t.table_schema = 'TestExecutor' and c.table_schema = 'TestExecutor' order by t.table_schema,t.table_name,c.column_name"
	_, err = executor.Execute(context.Background(), "TestSelectDBA",
		NewSafeSession(&vtgatepb.Session{TargetString: "information_schema"}),
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{Sql: "select t.table_schema, t.table_name, c.column_name, c.column_type from information_schema.`tables` as t, information_schema.`columns` as c where t.table_schema = :__vtschemaname /* VARCHAR */ and c.table_schema = :__vtschemaname /* VARCHAR */ and c.table_schema = t.table_schema and c.table_name = t.table_name order by t.table_schema asc, t.table_name asc, c.column_name asc",
		BindVariables: map[string]*querypb.BindVariable{
			"t_table_schema":        sqltypes.StringBindVariable("TestExecutor"),
			"__replacevtschemaname": sqltypes.Int64BindVariable(1),
		}}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
}

func TestUnsharded(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	_, err := executorExec(executor, "select id from music_user_map where id = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from music_user_map where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)
}

func TestUnshardedComments(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	_, err := executorExec(executor, "/* leading */ select id from music_user_map where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "/* leading */ select id from music_user_map where id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)

	_, err = executorExec(executor, "update music_user_map set id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "/* leading */ select id from music_user_map where id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "update music_user_map set id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbclookup, wantQueries)

	sbclookup.Queries = nil
	_, err = executorExec(executor, "delete from music_user_map /* trailing */", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "delete from music_user_map /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbclookup, wantQueries)

	sbclookup.Queries = nil
	_, err = executorExec(executor, "insert into music_user_map values (1) /* trailing */", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "insert into music_user_map values (1) /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	assertQueries(t, sbclookup, wantQueries)
}

func TestStreamUnsharded(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select id from music_user_map where id = 1"
	result, err := executorStream(executor, sql)
	require.NoError(t, err)
	wantResult := sandboxconn.StreamRowResult
	if !result.Equal(wantResult) {
		diff := cmp.Diff(wantResult, result)
		t.Errorf("result: %+v, want %+v\ndiff: %s", result, wantResult, diff)
	}
	testQueryLog(t, logChan, "TestExecuteStream", "SELECT", sql, 1)
}

func TestStreamBuffering(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	// This test is similar to TestStreamUnsharded except that it returns a Result > 10 bytes,
	// such that the splitting of the Result into multiple Result responses gets tested.
	sbclookup.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewVarChar("01234567890123456789"),
		}, {
			sqltypes.NewInt32(2),
			sqltypes.NewVarChar("12345678901234567890"),
		}},
	}})

	var results []*sqltypes.Result
	err := executor.StreamExecute(
		context.Background(),
		"TestStreamBuffering",
		NewSafeSession(primarySession),
		"select id from music_user_map where id = 1",
		nil,
		func(qr *sqltypes.Result) error {
			results = append(results, qr)
			return nil
		},
	)
	require.NoError(t, err)
	wantResults := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.VarChar},
		},
	}, {
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewVarChar("01234567890123456789"),
		}},
	}, {
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(2),
			sqltypes.NewVarChar("12345678901234567890"),
		}},
	}}
	utils.MustMatch(t, wantResults, results)
}

func TestStreamLimitOffset(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	// This test is similar to TestStreamUnsharded except that it returns a Result > 10 bytes,
	// such that the splitting of the Result into multiple Result responses gets tested.
	sbc1.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "textcol", Type: sqltypes.VarChar},
			{Name: "weight_string(id)", Type: sqltypes.VarBinary},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewVarChar("1234"),
			sqltypes.NULL,
		}, {
			sqltypes.NewInt32(4),
			sqltypes.NewVarChar("4567"),
			sqltypes.NULL,
		}},
	}})

	sbc2.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "textcol", Type: sqltypes.VarChar},
			{Name: "weight_string(id)", Type: sqltypes.VarBinary},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(2),
			sqltypes.NewVarChar("2345"),
			sqltypes.NULL,
		}},
	}})

	results := make(chan *sqltypes.Result, 10)
	err := executor.StreamExecute(
		context.Background(),
		"TestStreamLimitOffset",
		NewSafeSession(primarySession),
		"select id, textcol from user order by id limit 2 offset 2",
		nil,
		func(qr *sqltypes.Result) error {
			results <- qr
			return nil
		},
	)
	close(results)
	require.NoError(t, err)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "textcol", Type: sqltypes.VarChar},
		},

		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewVarChar("1234"),
		}, {
			sqltypes.NewInt32(1),
			sqltypes.NewVarChar("foo"),
		}},
	}
	var gotResults []*sqltypes.Result
	for r := range results {
		gotResults = append(gotResults, r)
	}
	res := gotResults[0]
	for i := 1; i < len(gotResults); i++ {
		res.Rows = append(res.Rows, gotResults[i].Rows...)
	}
	utils.MustMatch(t, wantResult, res, "")
}

func TestSelectLastInsertId(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	primarySession.LastInsertId = 52
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select last_insert_id()"
	result, err := executorExec(executor, sql, map[string]*querypb.BindVariable{})
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "last_insert_id()", Type: sqltypes.Uint64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewUint64(52),
		}},
	}
	require.NoError(t, err)
	utils.MustMatch(t, wantResult, result, "Mismatch")
}

func TestSelectSystemVariables(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	primarySession.ReadAfterWrite = &vtgatepb.ReadAfterWrite{
		ReadAfterWriteGtid:    "a fine gtid",
		ReadAfterWriteTimeout: 13,
		SessionTrackGtids:     true,
	}
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select @@autocommit, @@client_found_rows, @@skip_query_plan_cache, @@enable_system_settings, " +
		"@@sql_select_limit, @@transaction_mode, @@workload, @@read_after_write_gtid, " +
		"@@read_after_write_timeout, @@session_track_gtids, @@ddl_strategy, @@socket, @@query_timeout"

	result, err := executorExec(executor, sql, map[string]*querypb.BindVariable{})
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "@@autocommit", Type: sqltypes.Int64},
			{Name: "@@client_found_rows", Type: sqltypes.Int64},
			{Name: "@@skip_query_plan_cache", Type: sqltypes.Int64},
			{Name: "@@enable_system_settings", Type: sqltypes.Int64},
			{Name: "@@sql_select_limit", Type: sqltypes.Int64},
			{Name: "@@transaction_mode", Type: sqltypes.VarChar},
			{Name: "@@workload", Type: sqltypes.VarChar},
			{Name: "@@read_after_write_gtid", Type: sqltypes.VarChar},
			{Name: "@@read_after_write_timeout", Type: sqltypes.Float64},
			{Name: "@@session_track_gtids", Type: sqltypes.VarChar},
			{Name: "@@ddl_strategy", Type: sqltypes.VarChar},
			{Name: "@@socket", Type: sqltypes.VarChar},
			{Name: "@@query_timeout", Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			// the following are the uninitialised session values
			sqltypes.NewInt64(0),
			sqltypes.NewInt64(0),
			sqltypes.NewInt64(0),
			sqltypes.NewInt64(0),
			sqltypes.NewInt64(0),
			sqltypes.NewVarChar("UNSPECIFIED"),
			sqltypes.NewVarChar(""),
			// these have been set at the beginning of the test
			sqltypes.NewVarChar("a fine gtid"),
			sqltypes.NewFloat64(13),
			sqltypes.NewVarChar("own_gtid"),
			sqltypes.NewVarChar(""),
			sqltypes.NewVarChar(""),
			sqltypes.NewInt64(0),
		}},
	}
	require.NoError(t, err)
	utils.MustMatch(t, wantResult, result, "Mismatch")
}

func TestSelectInitializedVitessAwareVariable(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	primarySession.Autocommit = true
	primarySession.EnableSystemSettings = true
	primarySession.QueryTimeout = 75

	defer func() {
		primarySession.Autocommit = false
		primarySession.EnableSystemSettings = false
		primarySession.QueryTimeout = 0
	}()

	sql := "select @@autocommit, @@enable_system_settings, @@query_timeout"

	result, err := executorExec(executor, sql, nil)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "@@autocommit", Type: sqltypes.Int64},
			{Name: "@@enable_system_settings", Type: sqltypes.Int64},
			{Name: "@@query_timeout", Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(1),
			sqltypes.NewInt64(75),
		}},
	}
	require.NoError(t, err)
	utils.MustMatch(t, wantResult, result, "Mismatch")
}

func TestSelectUserDefinedVariable(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select @foo"
	result, err := executorExec(executor, sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "@foo", Type: sqltypes.Null},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NULL,
		}},
	}
	utils.MustMatch(t, wantResult, result, "Mismatch")

	primarySession = &vtgatepb.Session{UserDefinedVariables: createMap([]string{"foo"}, []any{"bar"})}
	result, err = executorExec(executor, sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	wantResult = &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "@foo", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("bar"),
		}},
	}
	utils.MustMatch(t, wantResult, result, "Mismatch")
}

func TestFoundRows(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// run this extra query so we can assert on the number of rows found
	_, err := executorExec(executor, "select 42", map[string]*querypb.BindVariable{})
	require.NoError(t, err)

	sql := "select found_rows()"
	result, err := executorExec(executor, sql, map[string]*querypb.BindVariable{})
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "found_rows()", Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1),
		}},
	}
	require.NoError(t, err)
	utils.MustMatch(t, wantResult, result, "Mismatch")
}

func TestRowCount(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	_, err := executorExec(executor, "select 42", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	testRowCount(t, executor, -1)

	_, err = executorExec(executor, "delete from user where id in (42, 24)", map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	testRowCount(t, executor, 2)
}

func testRowCount(t *testing.T, executor *Executor, wantRowCount int64) {
	t.Helper()
	result, err := executorExec(executor, "select row_count()", map[string]*querypb.BindVariable{})
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "row_count()", Type: sqltypes.Int64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(wantRowCount),
		}},
	}
	require.NoError(t, err)
	utils.MustMatch(t, wantResult, result, "Mismatch")
}

func TestSelectLastInsertIdInUnion(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	executor.normalize = true
	primarySession.LastInsertId = 52

	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(52),
		}},
	}}
	sbc1.SetResults(result1)

	sql := "select last_insert_id() as id union select last_insert_id() as id"
	got, err := executorExec(executor, sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(52),
		}},
	}
	utils.MustMatch(t, wantResult, got, "mismatch")
}

func TestSelectLastInsertIdInWhere(t *testing.T) {
	executor, _, _, lookup := createExecutorEnv()
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select id from music_user_map where id = last_insert_id()"
	_, err := executorExec(executor, sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from music_user_map where id = :__lastInsertId",
		BindVariables: map[string]*querypb.BindVariable{"__lastInsertId": sqltypes.Uint64BindVariable(0)},
	}}

	assert.Equal(t, wantQueries, lookup.Queries)
}

func TestLastInsertIDInVirtualTable(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	executor.normalize = true
	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}}
	sbc1.SetResults(result1)
	_, err := executorExec(executor, "select * from (select last_insert_id()) as t", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select * from (select :__lastInsertId as `last_insert_id()` from dual) as t",
		BindVariables: map[string]*querypb.BindVariable{"__lastInsertId": sqltypes.Uint64BindVariable(0)},
	}}

	assert.Equal(t, wantQueries, sbc1.Queries)
}

func TestLastInsertIDInSubQueryExpression(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	executor.normalize = true
	primarySession.LastInsertId = 12345
	defer func() {
		// clean up global state
		primarySession.LastInsertId = 0
	}()
	rs, err := executorExec(executor, "select (select last_insert_id()) as x", nil)
	require.NoError(t, err)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "x", Type: sqltypes.Uint64},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewUint64(12345),
		}},
	}
	utils.MustMatch(t, rs, wantResult, "Mismatch")

	// the query will get rewritten into a simpler query that can be run entirely on the vtgate
	assert.Empty(t, sbc1.Queries)
	assert.Empty(t, sbc2.Queries)
}

func TestSelectDatabase(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true
	sql := "select database()"
	newSession := proto.Clone(primarySession).(*vtgatepb.Session)
	session := NewSafeSession(newSession)
	session.TargetString = "TestExecutor@primary"
	result, err := executor.Execute(
		context.Background(),
		"TestExecute",
		session,
		sql,
		map[string]*querypb.BindVariable{})
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "database()", Type: sqltypes.VarChar},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewVarChar("TestExecutor@primary"),
		}},
	}
	require.NoError(t, err)
	utils.MustMatch(t, wantResult, result, "Mismatch")

}

func TestSelectBindvars(t *testing.T) {
	executor, sbc1, sbc2, lookup := createExecutorEnv()
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	lookup.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("b|a", "varbinary|varbinary"),
		"foo1|1",
	), sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("b|a", "varbinary|varbinary"),
		"foo2|1",
	)})

	sql := "select id from `user` where id = :id"
	_, err := executorExec(executor, sql, map[string]*querypb.BindVariable{
		"id": sqltypes.Int64BindVariable(1),
	})
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from `user` where id = :id",
		BindVariables: map[string]*querypb.BindVariable{"id": sqltypes.Int64BindVariable(1)},
	}}
	utils.MustMatch(t, sbc1.Queries, wantQueries)
	assert.Empty(t, sbc2.Queries)
	sbc1.Queries = nil
	testQueryLog(t, logChan, "TestExecute", "SELECT", sql, 1)

	// Test with StringBindVariable
	sql = "select id from `user` where `name` in (:name1, :name2)"
	_, err = executorExec(executor, sql, map[string]*querypb.BindVariable{
		"name1": sqltypes.StringBindVariable("foo1"),
		"name2": sqltypes.StringBindVariable("foo2"),
	})
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select id from `user` where `name` in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"name1":  sqltypes.StringBindVariable("foo1"),
			"name2":  sqltypes.StringBindVariable("foo2"),
			"__vals": sqltypes.TestBindVariable([]any{"foo1", "foo2"}),
		},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	sbc1.Queries = nil
	testQueryLog(t, logChan, "VindexLookup", "SELECT", "select `name`, user_id from name_user_map where `name` in ::name", 1)
	testQueryLog(t, logChan, "VindexLookup", "SELECT", "select `name`, user_id from name_user_map where `name` in ::name", 1)
	testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from `user` where `name` in (:name1, :name2)", 1)

	// Test with BytesBindVariable
	sql = "select id from `user` where `name` in (:name1, :name2)"
	_, err = executorExec(executor, sql, map[string]*querypb.BindVariable{
		"name1": sqltypes.BytesBindVariable([]byte("foo1")),
		"name2": sqltypes.BytesBindVariable([]byte("foo2")),
	})
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select id from `user` where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{
			"name1": sqltypes.BytesBindVariable([]byte("foo1")),
			"name2": sqltypes.BytesBindVariable([]byte("foo2")),
		},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	testQueryLog(t, logChan, "VindexLookup", "SELECT", "select `name`, user_id from name_user_map where `name` in ::name", 1)
	testQueryLog(t, logChan, "VindexLookup", "SELECT", "select `name`, user_id from name_user_map where `name` in ::name", 1)
	testQueryLog(t, logChan, "TestExecute", "SELECT", sql, 1)

	// Test no match in the lookup vindex
	sbc1.Queries = nil
	lookup.Queries = nil
	lookup.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "user_id", Type: sqltypes.Int32},
		},
		RowsAffected: 0,
		InsertID:     0,
		Rows:         [][]sqltypes.Value{},
	}})

	sql = "select id from user where name = :name"
	_, err = executorExec(executor, sql, map[string]*querypb.BindVariable{
		"name": sqltypes.StringBindVariable("nonexistent"),
	})
	require.NoError(t, err)

	// When there are no matching rows in the vindex, vtgate still needs the field info
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select id from `user` where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{
			"name": sqltypes.StringBindVariable("nonexistent"),
		},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)

	vars, err := sqltypes.BuildBindVariable([]any{sqltypes.NewVarChar("nonexistent")})
	require.NoError(t, err)
	wantLookupQueries := []*querypb.BoundQuery{{
		Sql: "select `name`, user_id from name_user_map where `name` in ::name",
		BindVariables: map[string]*querypb.BindVariable{
			"name": vars,
		},
	}}

	utils.MustMatch(t, wantLookupQueries, lookup.Queries)

	testQueryLog(t, logChan, "VindexLookup", "SELECT", "select `name`, user_id from name_user_map where `name` in ::name", 1)
	testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from `user` where `name` = :name", 1)

}

func TestSelectEqual(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	_, err := executorExec(executor, "select id from user where id = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from `user` where id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	sbc1.Queries = nil

	_, err = executorExec(executor, "select id from user where id = 3", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select id from `user` where id = 3",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc2.Queries)
	if execCount := sbc1.ExecCount.Load(); execCount != 1 {
		t.Errorf("sbc1.ExecCount: %v, want 1\n", execCount)
	}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}
	sbc2.Queries = nil

	_, err = executorExec(executor, "select id from user where id = '3'", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select id from `user` where id = '3'",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc2.Queries)
	if execCount := sbc1.ExecCount.Load(); execCount != 1 {
		t.Errorf("sbc1.ExecCount: %v, want 1\n", execCount)
	}
	if sbc1.Queries != nil {
		t.Errorf("sbc1.Queries: %+v, want nil\n", sbc1.Queries)
	}
	sbc2.Queries = nil

	sbclookup.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("b|a", "varbinary|varbinary"),
		"foo|1",
	)})
	_, err = executorExec(executor, "select id from user where name = 'foo'", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select id from `user` where `name` = 'foo'",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	vars, err := sqltypes.BuildBindVariable([]any{sqltypes.NewVarChar("foo")})
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select `name`, user_id from name_user_map where `name` in ::name",
		BindVariables: map[string]*querypb.BindVariable{
			"name": vars,
		},
	}}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)
}

func TestSelectINFromOR(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	executor.pv = querypb.ExecuteOptions_Gen4

	_, err := executorExec(executor, "select 1 from user where id = 1 and name = 'apa' or id = 2 and name = 'toto'", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "select 1 from `user` where id = 1 and `name` = 'apa' or id = 2 and `name` = 'toto'",
		BindVariables: map[string]*querypb.BindVariable{
			"__vals": sqltypes.TestBindVariable([]any{int64(1), int64(2)}),
		},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
}

func TestSelectDual(t *testing.T) {
	executor, sbc1, _, lookup := createExecutorEnv()

	_, err := executorExec(executor, "select @@aa.bb from dual", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select @@`aa.bb` from dual",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)

	_, err = executorExec(executor, "select @@aa.bb from TestUnsharded.dual", nil)
	require.NoError(t, err)
	utils.MustMatch(t, wantQueries, lookup.Queries)
}

func TestSelectComments(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := executorExec(executor, "/* leading */ select id from user where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "/* leading */ select id from `user` where id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	sbc1.Queries = nil
}

func TestSelectNormalize(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	executor.normalize = true

	_, err := executorExec(executor, "/* leading */ select id from user where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "/* leading */ select id from `user` where id = :id /* INT64 */ /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"id": sqltypes.TestBindVariable(int64(1)),
		},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	sbc1.Queries = nil

	// Force the query to go to the "wrong" shard and ensure that normalization still happens
	primarySession.TargetString = "TestExecutor/40-60"
	_, err = executorExec(executor, "/* leading */ select id from user where id = 1 /* trailing */", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "/* leading */ select id from `user` where id = :id /* INT64 */ /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{
			"id": sqltypes.TestBindVariable(int64(1)),
		},
	}}
	require.Empty(t, sbc1.Queries)
	utils.MustMatch(t, wantQueries, sbc2.Queries, "sbc2.Queries")
	sbc2.Queries = nil
	primarySession.TargetString = ""
}

func TestSelectCaseSensitivity(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := executorExec(executor, "select Id from user where iD = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select Id from `user` where iD = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	sbc1.Queries = nil
}

func TestStreamSelectEqual(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()

	sql := "select id from user where id = 1"
	result, err := executorStream(executor, sql)
	require.NoError(t, err)
	wantResult := sandboxconn.StreamRowResult
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestSelectKeyRange(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := executorExec(executor, "select krcol_unique, krcol from keyrange_table where krcol = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select krcol_unique, krcol from keyrange_table where krcol = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	sbc1.Queries = nil
}

func TestSelectKeyRangeUnique(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()

	_, err := executorExec(executor, "select krcol_unique, krcol from keyrange_table where krcol_unique = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select krcol_unique, krcol from keyrange_table where krcol_unique = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
	sbc1.Queries = nil
}

func TestSelectIN(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()

	// Constant in IN clause is just a number, not a bind variable.
	_, err := executorExec(executor, "select id from user where id in (1)", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "select id from `user` where id in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"__vals": sqltypes.TestBindVariable([]any{int64(1)}),
		},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}

	// Constants in IN clause are just numbers, not bind variables.
	// They result in two different queries on two shards.
	sbc1.Queries = nil
	sbc2.Queries = nil
	_, err = executorExec(executor, "select id from user where id in (1, 3)", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select id from `user` where id in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"__vals": sqltypes.TestBindVariable([]any{int64(1)}),
		},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select id from `user` where id in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"__vals": sqltypes.TestBindVariable([]any{int64(3)}),
		},
	}}
	utils.MustMatch(t, wantQueries, sbc2.Queries)

	// In is a bind variable list, that will end up on two shards.
	// This is using []any for the bind variable list.
	sbc1.Queries = nil
	sbc2.Queries = nil
	_, err = executorExec(executor, "select id from user where id in ::vals", map[string]*querypb.BindVariable{
		"vals": sqltypes.TestBindVariable([]any{int64(1), int64(3)}),
	})
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select id from `user` where id in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"__vals": sqltypes.TestBindVariable([]any{int64(1)}),
			"vals":   sqltypes.TestBindVariable([]any{int64(1), int64(3)}),
		},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select id from `user` where id in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"__vals": sqltypes.TestBindVariable([]any{int64(3)}),
			"vals":   sqltypes.TestBindVariable([]any{int64(1), int64(3)}),
		},
	}}
	utils.MustMatch(t, wantQueries, sbc2.Queries)

	// Convert a non-list bind variable.
	sbc1.Queries = nil
	sbc2.Queries = nil
	sbclookup.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("b|a", "varbinary|varbinary"),
		"foo|1",
	)})
	_, err = executorExec(executor, "select id from user where name = 'foo'", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select id from `user` where `name` = 'foo'",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	vars, err := sqltypes.BuildBindVariable([]any{sqltypes.NewVarChar("foo")})
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select `name`, user_id from name_user_map where `name` in ::name",
		BindVariables: map[string]*querypb.BindVariable{
			"name": vars,
		},
	}}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)
}

func TestStreamSelectIN(t *testing.T) {
	executor, _, _, sbclookup := createExecutorEnv()

	sql := "select id from user where id in (1)"
	result, err := executorStream(executor, sql)
	require.NoError(t, err)
	wantResult := sandboxconn.StreamRowResult
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	sql = "select id from user where id in (1, 3)"
	result, err = executorStream(executor, sql)
	require.NoError(t, err)
	wantResult = &sqltypes.Result{
		Fields: sandboxconn.StreamRowResult.Fields,
		Rows: [][]sqltypes.Value{
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
		},
		RowsAffected: 0,
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	sql = "select id from user where name = 'foo'"
	result, err = executorStream(executor, sql)
	require.NoError(t, err)
	wantResult = sandboxconn.StreamRowResult
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	vars, err := sqltypes.BuildBindVariable([]any{sqltypes.NewVarChar("foo")})
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql: "select `name`, user_id from name_user_map where `name` in ::name",
		BindVariables: map[string]*querypb.BindVariable{
			"name": vars,
		},
	}}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)
}

func createExecutor(serv *sandboxTopo, cell string, resolver *Resolver) *Executor {
	return NewExecutor(context.Background(), serv, cell, resolver, false, false, testBufferSize, cache.DefaultConfig, nil, false, querypb.ExecuteOptions_V3)
}

func TestSelectScatter(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for _, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		conns = append(conns, sbc)
	}
	executor := createExecutor(serv, cell, resolver)
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select id from `user`"
	_, err := executorExec(executor, sql, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from `user`",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		utils.MustMatch(t, wantQueries, conn.Queries)
	}
	testQueryLog(t, logChan, "TestExecute", "SELECT", sql, 8)
}

func TestSelectScatterPartial(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	primarySession = &vtgatepb.Session{
		TargetString: "@primary",
	}
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for _, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		conns = append(conns, sbc)
	}

	executor := createExecutor(serv, cell, resolver)
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// Fail 1 of N without the directive fails the whole operation
	conns[2].MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1000
	results, err := executorExec(executor, "select id from `user`", nil)
	wantErr := "TestExecutor.40-60.primary"
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Errorf("want error %v, got %v", wantErr, err)
	}
	if vterrors.Code(err) != vtrpcpb.Code_RESOURCE_EXHAUSTED {
		t.Errorf("want error code Code_RESOURCE_EXHAUSTED, but got %v", vterrors.Code(err))
	}
	if results != nil {
		t.Errorf("want nil results, got %v", results)
	}
	testQueryLog(t, logChan, "TestExecute", "SELECT", "select id from `user`", 8)

	// Fail 1 of N with the directive succeeds with 7 rows
	results, err = executorExec(executor, "select /*vt+ SCATTER_ERRORS_AS_WARNINGS=1 */ id from user", nil)
	require.NoError(t, err)
	if results == nil || len(results.Rows) != 7 {
		t.Errorf("want 7 results, got %v", results)
	}
	testQueryLog(t, logChan, "TestExecute", "SELECT", "select /*vt+ SCATTER_ERRORS_AS_WARNINGS=1 */ id from `user`", 8)

	// When all shards fail, the execution should also fail
	conns[0].MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1000
	conns[1].MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1000
	conns[3].MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1000
	conns[4].MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1000
	conns[5].MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1000
	conns[6].MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1000
	conns[7].MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1000

	_, err = executorExec(executor, "select /*vt+ SCATTER_ERRORS_AS_WARNINGS=1 */ id from user", nil)
	require.Error(t, err)
	testQueryLog(t, logChan, "TestExecute", "SELECT", "select /*vt+ SCATTER_ERRORS_AS_WARNINGS=1 */ id from `user`", 8)

	_, err = executorExec(executor, "select /*vt+ SCATTER_ERRORS_AS_WARNINGS=1 */ id from user order by id", nil)
	require.Error(t, err)
}

func TestSelectScatterPartialOLAP(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for _, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		conns = append(conns, sbc)
	}

	executor := createExecutor(serv, cell, resolver)
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// Fail 1 of N without the directive fails the whole operation
	conns[2].MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1000
	results, err := executorStream(executor, "select id from `user`")
	assert.EqualError(t, err, "target: TestExecutor.40-60.primary: RESOURCE_EXHAUSTED error")
	assert.Equal(t, vtrpcpb.Code_RESOURCE_EXHAUSTED, vterrors.Code(err))
	assert.Nil(t, results)
	testQueryLog(t, logChan, "TestExecuteStream", "SELECT", "select id from `user`", 8)

	// Fail 1 of N with the directive succeeds with 7 rows
	results, err = executorStream(executor, "select /*vt+ SCATTER_ERRORS_AS_WARNINGS=1 */ id from user")
	require.NoError(t, err)
	assert.EqualValues(t, 7, len(results.Rows))
	testQueryLog(t, logChan, "TestExecuteStream", "SELECT", "select /*vt+ SCATTER_ERRORS_AS_WARNINGS=1 */ id from `user`", 8)

	// If all shards fail, the operation should also fail
	conns[0].MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1000
	conns[1].MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1000
	conns[3].MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1000
	conns[4].MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1000
	conns[5].MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1000
	conns[6].MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1000
	conns[7].MustFailCodes[vtrpcpb.Code_RESOURCE_EXHAUSTED] = 1000

	_, err = executorStream(executor, "select /*vt+ SCATTER_ERRORS_AS_WARNINGS=1 */ id from user")
	require.Error(t, err)
	testQueryLog(t, logChan, "TestExecuteStream", "SELECT", "select /*vt+ SCATTER_ERRORS_AS_WARNINGS=1 */ id from `user`", 8)

	_, err = executorStream(executor, "select /*vt+ SCATTER_ERRORS_AS_WARNINGS=1 */ id from user order by id")
	require.Error(t, err)
}

func TestSelectScatterPartialOLAP2(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for _, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		conns = append(conns, sbc)
	}

	executor := createExecutor(serv, cell, resolver)
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	// Fail 1 of N without the directive fails the whole operation
	tablet0 := conns[2].Tablet()
	ths := hc.GetHealthyTabletStats(&querypb.Target{
		Keyspace:   tablet0.GetKeyspace(),
		Shard:      tablet0.GetShard(),
		TabletType: tablet0.GetType(),
	})
	sbc0Th := ths[0]
	sbc0Th.Serving = false

	results, err := executorStream(executor, "select id from `user`")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `no healthy tablet available for 'keyspace:"TestExecutor" shard:"40-60"`)
	assert.Equal(t, vtrpcpb.Code_UNAVAILABLE, vterrors.Code(err))
	assert.Nil(t, results)
	testQueryLog(t, logChan, "TestExecuteStream", "SELECT", "select id from `user`", 8)

	// Fail 1 of N with the directive succeeds with 7 rows
	results, err = executorStream(executor, "select /*vt+ SCATTER_ERRORS_AS_WARNINGS=1 */ id from user")
	require.NoError(t, err)
	assert.EqualValues(t, 7, len(results.Rows))
	testQueryLog(t, logChan, "TestExecuteStream", "SELECT", "select /*vt+ SCATTER_ERRORS_AS_WARNINGS=1 */ id from `user`", 8)

	// order by
	results, err = executorStream(executor, "select /*vt+ SCATTER_ERRORS_AS_WARNINGS=1 */ id from user order by id")
	require.NoError(t, err)
	assert.EqualValues(t, 7, len(results.Rows))
	testQueryLog(t, logChan, "TestExecuteStream", "SELECT", "select /*vt+ SCATTER_ERRORS_AS_WARNINGS=1 */ id from `user` order by id asc", 8)

	// order by and limit
	results, err = executorStream(executor, "select /*vt+ SCATTER_ERRORS_AS_WARNINGS=1 */ id from user order by id limit 5")
	require.NoError(t, err)
	assert.EqualValues(t, 5, len(results.Rows))
	testQueryLog(t, logChan, "TestExecuteStream", "SELECT", "select /*vt+ SCATTER_ERRORS_AS_WARNINGS=1 */ id from `user` order by id asc limit 5", 8)
}

func TestStreamSelectScatter(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	for _, shard := range shards {
		_ = hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
	}
	executor := createExecutor(serv, cell, resolver)

	sql := "select id from `user`"
	result, err := executorStream(executor, sql)
	require.NoError(t, err)
	wantResult := &sqltypes.Result{
		Fields: sandboxconn.SingleRowResult.Fields,
		Rows: [][]sqltypes.Value{
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
			sandboxconn.StreamRowResult.Rows[0],
		},
	}
	utils.MustMatch(t, wantResult, result)
}

// TestSelectScatterOrderBy will run an ORDER BY query that will scatter out to 8 shards and return the 8 rows (one per shard) sorted.
func TestSelectScatterOrderBy(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "col1", Type: sqltypes.Int32},
				{Name: "col2", Type: sqltypes.Int32},
				{Name: "weight_string(col2)", Type: sqltypes.VarBinary},
			},
			InsertID: 0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(1),
				// i%4 ensures that there are duplicates across shards.
				// This will allow us to test that cross-shard ordering
				// still works correctly.
				sqltypes.NewInt32(int32(i % 4)),
				sqltypes.NULL,
			}},
		}})
		conns = append(conns, sbc)
	}
	executor := createExecutor(serv, cell, resolver)

	query := "select col1, col2 from user order by col2 desc"
	gotResult, err := executorExec(executor, query, nil)
	require.NoError(t, err)

	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select col1, col2, weight_string(col2) from `user` order by col2 desc",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		utils.MustMatch(t, wantQueries, conn.Queries)
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "col1", Type: sqltypes.Int32},
			{Name: "col2", Type: sqltypes.Int32},
		},
		InsertID: 0,
	}
	for i := 0; i < 4; i++ {
		// There should be a duplicate for each row returned.
		for j := 0; j < 2; j++ {
			row := []sqltypes.Value{
				sqltypes.NewInt32(1),
				sqltypes.NewInt32(int32(3 - i)),
			}
			wantResult.Rows = append(wantResult.Rows, row)
		}
	}
	utils.MustMatch(t, wantResult, gotResult)
}

// TestSelectScatterOrderByVarChar will run an ORDER BY query that will scatter out to 8 shards and return the 8 rows (one per shard) sorted.
func TestSelectScatterOrderByVarChar(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "col1", Type: sqltypes.Int32},
				{Name: "textcol", Type: sqltypes.VarChar},
			},
			InsertID: 0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(1),
				// i%4 ensures that there are duplicates across shards.
				// This will allow us to test that cross-shard ordering
				// still works correctly.
				sqltypes.NewVarChar(fmt.Sprintf("%d", i%4)),
				sqltypes.NewVarBinary(fmt.Sprintf("%d", i%4)),
			}},
		}})
		conns = append(conns, sbc)
	}
	executor := createExecutor(serv, cell, resolver)

	query := "select col1, textcol from user order by textcol desc"
	gotResult, err := executorExec(executor, query, nil)
	require.NoError(t, err)

	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select col1, textcol, weight_string(textcol) from `user` order by textcol desc",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		utils.MustMatch(t, wantQueries, conn.Queries)
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "col1", Type: sqltypes.Int32},
			{Name: "textcol", Type: sqltypes.VarChar},
		},
		InsertID: 0,
	}
	for i := 0; i < 4; i++ {
		// There should be a duplicate for each row returned.
		for j := 0; j < 2; j++ {
			row := []sqltypes.Value{
				sqltypes.NewInt32(1),
				sqltypes.NewVarChar(fmt.Sprintf("%d", 3-i)),
			}
			wantResult.Rows = append(wantResult.Rows, row)
		}
	}
	utils.MustMatch(t, wantResult, gotResult)
}

func TestStreamSelectScatterOrderBy(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "id", Type: sqltypes.Int32},
				{Name: "col", Type: sqltypes.Int32},
				{Name: "weight_string(col)", Type: sqltypes.VarBinary},
			},
			InsertID: 0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(1),
				sqltypes.NewInt32(int32(i % 4)),
				sqltypes.NULL,
			}},
		}})
		conns = append(conns, sbc)
	}
	executor := createExecutor(serv, cell, resolver)

	query := "select id, col from user order by col desc"
	gotResult, err := executorStream(executor, query)
	require.NoError(t, err)

	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id, col, weight_string(col) from `user` order by col desc",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		utils.MustMatch(t, wantQueries, conn.Queries)
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
	}
	for i := 0; i < 4; i++ {
		row := []sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(int32(3 - i)),
		}
		wantResult.Rows = append(wantResult.Rows, row, row)
	}
	utils.MustMatch(t, wantResult, gotResult)
}

func TestStreamSelectScatterOrderByVarChar(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "id", Type: sqltypes.Int32},
				{Name: "textcol", Type: sqltypes.VarChar},
			},
			InsertID: 0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(1),
				sqltypes.NewVarChar(fmt.Sprintf("%d", i%4)),
				sqltypes.NewVarBinary(fmt.Sprintf("%d", i%4)),
			}},
		}})
		conns = append(conns, sbc)
	}
	executor := createExecutor(serv, cell, resolver)

	query := "select id, textcol from user order by textcol desc"
	gotResult, err := executorStream(executor, query)
	require.NoError(t, err)

	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id, textcol, weight_string(textcol) from `user` order by textcol desc",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		utils.MustMatch(t, wantQueries, conn.Queries)
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "textcol", Type: sqltypes.VarChar},
		},
	}
	for i := 0; i < 4; i++ {
		row := []sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewVarChar(fmt.Sprintf("%d", 3-i)),
		}
		wantResult.Rows = append(wantResult.Rows, row, row)
	}
	utils.MustMatch(t, wantResult, gotResult)
}

// TestSelectScatterAggregate will run an aggregate query that will scatter out to 8 shards and return 4 aggregated rows.
func TestSelectScatterAggregate(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "col", Type: sqltypes.Int32},
				{Name: "sum(foo)", Type: sqltypes.Int32},
				{Name: "weight_string(col)", Type: sqltypes.VarBinary},
			},
			InsertID: 0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(int32(i % 4)),
				sqltypes.NewInt32(int32(i)),
				sqltypes.NULL,
			}},
		}})
		conns = append(conns, sbc)
	}
	executor := createExecutor(serv, cell, resolver)

	query := "select col, sum(foo) from user group by col"
	gotResult, err := executorExec(executor, query, nil)
	require.NoError(t, err)

	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select col, sum(foo), weight_string(col) from `user` group by col, weight_string(col) order by col asc",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		utils.MustMatch(t, wantQueries, conn.Queries)
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "col", Type: sqltypes.Int32},
			{Name: "sum(foo)", Type: sqltypes.Decimal},
		},
		InsertID: 0,
	}
	for i := 0; i < 4; i++ {
		row := []sqltypes.Value{
			sqltypes.NewInt32(int32(i)),
			sqltypes.NewDecimal(fmt.Sprintf("%d", i*2+4)),
		}
		wantResult.Rows = append(wantResult.Rows, row)
	}
	utils.MustMatch(t, wantResult, gotResult)
}

func TestStreamSelectScatterAggregate(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "col", Type: sqltypes.Int32},
				{Name: "sum(foo)", Type: sqltypes.Int32},
				{Name: "weight_string(col)", Type: sqltypes.VarBinary},
			},
			InsertID: 0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(int32(i % 4)),
				sqltypes.NewInt32(int32(i)),
				sqltypes.NULL,
			}},
		}})
		conns = append(conns, sbc)
	}
	executor := createExecutor(serv, cell, resolver)

	query := "select col, sum(foo) from user group by col"
	gotResult, err := executorStream(executor, query)
	require.NoError(t, err)

	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select col, sum(foo), weight_string(col) from `user` group by col, weight_string(col) order by col asc",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	for _, conn := range conns {
		utils.MustMatch(t, wantQueries, conn.Queries)
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "col", Type: sqltypes.Int32},
			{Name: "sum(foo)", Type: sqltypes.Decimal},
		},
	}
	for i := 0; i < 4; i++ {
		row := []sqltypes.Value{
			sqltypes.NewInt32(int32(i)),
			sqltypes.NewDecimal(fmt.Sprintf("%d", i*2+4)),
		}
		wantResult.Rows = append(wantResult.Rows, row)
	}
	utils.MustMatch(t, wantResult, gotResult)
}

// TestSelectScatterLimit will run a limit query (ordered for consistency) against
// a scatter route and verify that the limit primitive works as intended.
func TestSelectScatterLimit(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "col1", Type: sqltypes.Int32},
				{Name: "col2", Type: sqltypes.Int32},
				{Name: "weight_string(col2)", Type: sqltypes.VarBinary},
			},
			InsertID: 0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(1),
				sqltypes.NewInt32(int32(i % 4)),
				sqltypes.NULL,
			}},
		}})
		conns = append(conns, sbc)
	}
	executor := createExecutor(serv, cell, resolver)

	query := "select col1, col2 from user order by col2 desc limit 3"
	gotResult, err := executorExec(executor, query, nil)
	require.NoError(t, err)

	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select col1, col2, weight_string(col2) from `user` order by col2 desc limit :__upper_limit",
		BindVariables: map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)},
	}}
	for _, conn := range conns {
		utils.MustMatch(t, wantQueries, conn.Queries)
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "col1", Type: sqltypes.Int32},
			{Name: "col2", Type: sqltypes.Int32},
		},
		InsertID: 0,
	}
	wantResult.Rows = append(wantResult.Rows,
		[]sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		},
		[]sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		},
		[]sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(2),
		})

	utils.MustMatch(t, wantResult, gotResult)
}

// TestStreamSelectScatterLimit will run a streaming limit query (ordered for consistency) against
// a scatter route and verify that the limit primitive works as intended.
func TestStreamSelectScatterLimit(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "col1", Type: sqltypes.Int32},
				{Name: "col2", Type: sqltypes.Int32},
				{Name: "weight_string(col2)", Type: sqltypes.VarBinary},
			},
			InsertID: 0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(1),
				sqltypes.NewInt32(int32(i % 4)),
				sqltypes.NULL,
			}},
		}})
		conns = append(conns, sbc)
	}
	executor := createExecutor(serv, cell, resolver)

	query := "select col1, col2 from user order by col2 desc limit 3"
	gotResult, err := executorStream(executor, query)
	require.NoError(t, err)

	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select col1, col2, weight_string(col2) from `user` order by col2 desc limit :__upper_limit",
		BindVariables: map[string]*querypb.BindVariable{"__upper_limit": sqltypes.Int64BindVariable(3)},
	}}
	for _, conn := range conns {
		utils.MustMatch(t, wantQueries, conn.Queries)
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "col1", Type: sqltypes.Int32},
			{Name: "col2", Type: sqltypes.Int32},
		},
	}
	wantResult.Rows = append(wantResult.Rows,
		[]sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		},
		[]sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		},
		[]sqltypes.Value{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(2),
		})

	utils.MustMatch(t, wantResult, gotResult)
}

// TODO(sougou): stream and non-stream testing are very similar.
// Could reuse code,
func TestSimpleJoin(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select u1.id, u2.id from user u1 join user u2 where u1.id = 1 and u2.id = 3"
	result, err := executorExec(executor, sql, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id from `user` as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select u2.id from `user` as u2 where u2.id = 3",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc2.Queries)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			sandboxconn.SingleRowResult.Fields[0],
			sandboxconn.SingleRowResult.Fields[0],
		},
		Rows: [][]sqltypes.Value{
			{
				sandboxconn.SingleRowResult.Rows[0][0],
				sandboxconn.SingleRowResult.Rows[0][0],
			},
		},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	testQueryLog(t, logChan, "TestExecute", "SELECT", "select u1.id, u2.id from `user` as u1 join `user` as u2 where u1.id = 1 and u2.id = 3", 2)
}

func TestJoinComments(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select u1.id, u2.id from user u1 join user u2 where u1.id = 1 and u2.id = 3 /* trailing */"
	_, err := executorExec(executor, sql, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id from `user` as u1 where u1.id = 1 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select u2.id from `user` as u2 where u2.id = 3 /* trailing */",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc2.Queries)

	testQueryLog(t, logChan, "TestExecute", "SELECT", "select u1.id, u2.id from `user` as u1 join `user` as u2 where u1.id = 1 and u2.id = 3 /* trailing */", 2)
}

func TestSimpleJoinStream(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select u1.id, u2.id from user u1 join user u2 where u1.id = 1 and u2.id = 3"
	result, err := executorStream(executor, sql)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id from `user` as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	wantQueries = []*querypb.BoundQuery{{
		Sql:           "select u2.id from `user` as u2 where u2.id = 3",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc2.Queries)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			sandboxconn.SingleRowResult.Fields[0],
			sandboxconn.SingleRowResult.Fields[0],
		},
		Rows: [][]sqltypes.Value{
			{
				sandboxconn.SingleRowResult.Rows[0][0],
				sandboxconn.SingleRowResult.Rows[0][0],
			},
		},
		RowsAffected: 0,
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}

	testQueryLog(t, logChan, "TestExecuteStream", "SELECT", "select u1.id, u2.id from `user` as u1 join `user` as u2 where u1.id = 1 and u2.id = 3", 2)
}

func TestVarJoin(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}}
	sbc1.SetResults(result1)
	sql := "select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1"
	_, err := executorExec(executor, sql, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id, u1.col from `user` as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	// We have to use string representation because bindvars type is too complex.
	got := fmt.Sprintf("%+v", sbc2.Queries)
	want := `[sql:"select u2.id from ` + "`user`" + ` as u2 where u2.id = :u1_col" bind_variables:{key:"u1_col" value:{type:INT32 value:"3"}}]`
	if got != want {
		t.Errorf("sbc2.Queries: %s, want %s\n", got, want)
	}

	testQueryLog(t, logChan, "TestExecute", "SELECT", "select u1.id, u2.id from `user` as u1 join `user` as u2 on u2.id = u1.col where u1.id = 1", 2)
}

func TestVarJoinStream(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}}
	sbc1.SetResults(result1)
	sql := "select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1"
	_, err := executorStream(executor, sql)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id, u1.col from `user` as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	// We have to use string representation because bindvars type is too complex.
	got := fmt.Sprintf("%+v", sbc2.Queries)
	want := `[sql:"select u2.id from ` + "`user`" + ` as u2 where u2.id = :u1_col" bind_variables:{key:"u1_col" value:{type:INT32 value:"3"}}]`
	if got != want {
		t.Errorf("sbc2.Queries: %s, want %s\n", got, want)
	}

	testQueryLog(t, logChan, "TestExecuteStream", "SELECT", "select u1.id, u2.id from `user` as u1 join `user` as u2 on u2.id = u1.col where u1.id = 1", 2)
}

func TestLeftJoin(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)
	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}}
	emptyResult := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}}
	sbc1.SetResults(result1)
	sbc2.SetResults(emptyResult)
	sql := "select u1.id, u2.id from user u1 left join user u2 on u2.id = u1.col where u1.id = 1"
	result, err := executorExec(executor, sql, nil)
	require.NoError(t, err)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			sandboxconn.SingleRowResult.Fields[0],
			sandboxconn.SingleRowResult.Fields[0],
		},
		Rows: [][]sqltypes.Value{
			{
				sandboxconn.SingleRowResult.Rows[0][0],
				{},
			},
		},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: \n%+v, want \n%+v", result, wantResult)
	}
	testQueryLog(t, logChan, "TestExecute", "SELECT", "select u1.id, u2.id from `user` as u1 left join `user` as u2 on u2.id = u1.col where u1.id = 1", 2)
}

func TestLeftJoinStream(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}}
	emptyResult := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}}
	sbc1.SetResults(result1)
	sbc2.SetResults(emptyResult)
	result, err := executorStream(executor, "select u1.id, u2.id from user u1 left join user u2 on u2.id = u1.col where u1.id = 1")
	require.NoError(t, err)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			sandboxconn.SingleRowResult.Fields[0],
			sandboxconn.SingleRowResult.Fields[0],
		},
		Rows: [][]sqltypes.Value{
			{
				sandboxconn.SingleRowResult.Rows[0][0],
				{},
			},
		},
		RowsAffected: 0,
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestEmptyJoin(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	// Empty result requires a field query for the second part of join,
	// which is sent to shard 0.
	sbc1.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
	}, {
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
	}})
	result, err := executorExec(executor, "select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id, u1.col from `user` as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "select u2.id from `user` as u2 where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{
			"u1_col": sqltypes.Int32BindVariable(0),
		},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "id", Type: sqltypes.Int32},
		},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestEmptyJoinStream(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	// Empty result requires a field query for the second part of join,
	// which is sent to shard 0.
	sbc1.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}, {
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}})
	result, err := executorStream(executor, "select u1.id, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1")
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id, u1.col from `user` as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "select u2.id from `user` as u2 where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{
			"u1_col": sqltypes.NullBindVariable,
		},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "id", Type: sqltypes.Int32},
		},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestEmptyJoinRecursive(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	// Make sure it also works recursively.
	sbc1.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}, {
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
	}, {
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}})
	result, err := executorExec(executor, "select u1.id, u2.id, u3.id from user u1 join (user u2 join user u3 on u3.id = u2.col) where u1.id = 1", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id from `user` as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "select u2.id, u2.col from `user` as u2 where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "select u3.id from `user` as u3 where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{
			"u2_col": sqltypes.NullBindVariable,
		},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "id", Type: sqltypes.Int32},
			{Name: "id", Type: sqltypes.Int32},
		},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestEmptyJoinRecursiveStream(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	// Make sure it also works recursively.
	sbc1.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}, {
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
	}, {
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
	}})
	result, err := executorStream(executor, "select u1.id, u2.id, u3.id from user u1 join (user u2 join user u3 on u3.id = u2.col) where u1.id = 1")
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id from `user` as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql:           "select u2.id, u2.col from `user` as u2 where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "select u3.id from `user` as u3 where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{
			"u2_col": sqltypes.NullBindVariable,
		},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "id", Type: sqltypes.Int32},
			{Name: "id", Type: sqltypes.Int32},
		},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestCrossShardSubquery(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}}
	sbc1.SetResults(result1)
	result, err := executorExec(executor, "select id1 from (select u1.id id1, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1) as t", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id as id1, u1.col from `user` as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	// We have to use string representation because bindvars type is too complex.
	got := fmt.Sprintf("%+v", sbc2.Queries)
	want := `[sql:"select u2.id from ` + "`user`" + ` as u2 where u2.id = :u1_col" bind_variables:{key:"u1_col" value:{type:INT32 value:"3"}}]`
	if got != want {
		t.Errorf("sbc2.Queries: %s, want %s\n", got, want)
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
		}},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestSubQueryAndQueryWithLimit(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}}
	result2 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(111),
			sqltypes.NewInt32(333),
		}},
	}}
	sbc1.SetResults(result1)
	sbc2.SetResults(result2)

	exec(executor, NewSafeSession(&vtgatepb.Session{
		TargetString: "@primary",
	}), "select id1, id2 from t1 where id1 >= ( select id1 from t1 order by id1 asc limit 1) limit 100")
	require.Equal(t, 2, len(sbc1.Queries))
	require.Equal(t, 2, len(sbc2.Queries))

	// sub query is evaluated first, and sees a limit of 1
	assert.Equal(t, `type:INT64 value:"1"`, sbc1.Queries[0].BindVariables["__upper_limit"].String())
	assert.Equal(t, `type:INT64 value:"1"`, sbc2.Queries[0].BindVariables["__upper_limit"].String())

	// outer limit is only applied to the outer query
	assert.Equal(t, `type:INT64 value:"100"`, sbc1.Queries[1].BindVariables["__upper_limit"].String())
	assert.Equal(t, `type:INT64 value:"100"`, sbc2.Queries[1].BindVariables["__upper_limit"].String())
}

func TestCrossShardSubqueryStream(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
		InsertID: 0,
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
			sqltypes.NewInt32(3),
		}},
	}}
	sbc1.SetResults(result1)
	result, err := executorStream(executor, "select id1 from (select u1.id id1, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1) as t")
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id as id1, u1.col from `user` as u1 where u1.id = 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	// We have to use string representation because bindvars type is too complex.
	got := fmt.Sprintf("%+v", sbc2.Queries)
	want := `[sql:"select u2.id from ` + "`user`" + ` as u2 where u2.id = :u1_col" bind_variables:{key:"u1_col" value:{type:INT32 value:"3"}}]`
	if got != want {
		t.Errorf("sbc2.Queries:\n%s, want\n%s\n", got, want)
	}

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
		},
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt32(1),
		}},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestCrossShardSubqueryGetFields(t *testing.T) {
	executor, sbc1, _, sbclookup := createExecutorEnv()
	sbclookup.SetResults([]*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "col", Type: sqltypes.Int32},
		},
	}})
	result1 := []*sqltypes.Result{{
		Fields: []*querypb.Field{
			{Name: "id", Type: sqltypes.Int32},
			{Name: "col", Type: sqltypes.Int32},
		},
	}}
	sbc1.SetResults(result1)
	result, err := executorExec(executor, "select main1.col, t.id1 from main1 join (select u1.id id1, u2.id from user u1 join user u2 on u2.id = u1.col where u1.id = 1) as t", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select u1.id as id1, u1.col from `user` as u1 where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{},
	}, {
		Sql: "select u2.id from `user` as u2 where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{
			"u1_col": sqltypes.NullBindVariable,
		},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)

	wantResult := &sqltypes.Result{
		Fields: []*querypb.Field{
			{Name: "col", Type: sqltypes.Int32},
			{Name: "id", Type: sqltypes.Int32},
		},
	}
	if !result.Equal(wantResult) {
		t.Errorf("result: %+v, want %+v", result, wantResult)
	}
}

func TestSelectBindvarswithPrepare(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select id from `user` where id = :id"
	_, err := executorPrepare(executor, sql, map[string]*querypb.BindVariable{
		"id": sqltypes.Int64BindVariable(1),
	})
	require.NoError(t, err)

	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select id from `user` where 1 != 1",
		BindVariables: map[string]*querypb.BindVariable{"id": sqltypes.Int64BindVariable(1)},
	}}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	if sbc2.Queries != nil {
		t.Errorf("sbc2.Queries: %+v, want nil\n", sbc2.Queries)
	}
}

func TestSelectDatabasePrepare(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	sql := "select database()"
	_, err := executorPrepare(executor, sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
}

func TestSelectWithUnionAll(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	executor.normalize = true
	sql := "select id from user where id in (1, 2, 3) union all select id from user where id in (1, 2, 3)"
	bv, _ := sqltypes.BuildBindVariable([]int64{1, 2, 3})
	bv1, _ := sqltypes.BuildBindVariable([]int64{1, 2})
	bv2, _ := sqltypes.BuildBindVariable([]int64{3})
	sbc1WantQueries := []*querypb.BoundQuery{{
		Sql: "select id from `user` where id in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"__vals": bv1,
			"vtg1":   bv,
			"vtg2":   bv,
		},
	}, {
		Sql: "select id from `user` where id in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"__vals": bv1,
			"vtg1":   bv,
			"vtg2":   bv,
		},
	}}
	sbc2WantQueries := []*querypb.BoundQuery{{
		Sql: "select id from `user` where id in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"__vals": bv2,
			"vtg1":   bv,
			"vtg2":   bv,
		},
	}, {
		Sql: "select id from `user` where id in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"__vals": bv2,
			"vtg1":   bv,
			"vtg2":   bv,
		},
	}}
	_, err := executorExec(executor, sql, map[string]*querypb.BindVariable{})
	require.NoError(t, err)
	utils.MustMatch(t, sbc1WantQueries, sbc1.Queries, "sbc1")
	utils.MustMatch(t, sbc2WantQueries, sbc2.Queries, "sbc2")

	// Reset
	sbc1.Queries = nil
	sbc2.Queries = nil

	_, err = executorStream(executor, sql)
	require.NoError(t, err)
	utils.MustMatch(t, sbc1WantQueries, sbc1.Queries, "sbc1")
	utils.MustMatch(t, sbc2WantQueries, sbc2.Queries, "sbc2")
}

func TestSelectLock(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	session := NewSafeSession(nil)
	session.Session.InTransaction = true
	session.ShardSessions = []*vtgatepb.Session_ShardSession{{
		Target: &querypb.Target{
			Keyspace:   "TestExecutor",
			Shard:      "-20",
			TabletType: topodatapb.TabletType_PRIMARY,
		},
		TransactionId: 12345,
		TabletAlias:   sbc1.Tablet().Alias,
	}}

	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select get_lock('lock name', 10) from dual",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	wantSession := &vtgatepb.Session{
		InTransaction: true,
		ShardSessions: []*vtgatepb.Session_ShardSession{{
			Target: &querypb.Target{
				Keyspace:   "TestExecutor",
				Shard:      "-20",
				TabletType: topodatapb.TabletType_PRIMARY,
			},
			TransactionId: 12345,
			TabletAlias:   sbc1.Tablet().Alias,
		}},
		LockSession: &vtgatepb.Session_ShardSession{
			Target:      &querypb.Target{Keyspace: "TestExecutor", Shard: "-20", TabletType: topodatapb.TabletType_PRIMARY},
			TabletAlias: sbc1.Tablet().Alias,
			ReservedId:  1,
		},
		AdvisoryLock: map[string]int64{"lock name": 1},
		FoundRows:    1,
		RowCount:     -1,
	}

	_, err := exec(executor, session, "select get_lock('lock name', 10) from dual")
	require.NoError(t, err)
	wantSession.LastLockHeartbeat = session.Session.LastLockHeartbeat // copying as this is current timestamp value.
	utils.MustMatch(t, wantSession, session.Session, "")
	utils.MustMatch(t, wantQueries, sbc1.Queries, "")

	wantQueries = append(wantQueries, &querypb.BoundQuery{
		Sql:           "select release_lock('lock name') from dual",
		BindVariables: map[string]*querypb.BindVariable{},
	})
	wantSession.AdvisoryLock = nil
	wantSession.LockSession = nil

	_, err = exec(executor, session, "select release_lock('lock name') from dual")
	require.NoError(t, err)
	wantSession.LastLockHeartbeat = session.Session.LastLockHeartbeat // copying as this is current timestamp value.
	utils.MustMatch(t, wantQueries, sbc1.Queries, "")
	utils.MustMatch(t, wantSession, session.Session, "")
}

func TestLockReserve(t *testing.T) {
	// no connection should be reserved for these queries.
	tcases := []string{
		"select is_free_lock('lock name') from dual",
		"select is_used_lock('lock name') from dual",
		"select release_all_locks() from dual",
		"select release_lock('lock name') from dual",
	}

	executor, _, _, _ := createExecutorEnv()
	session := NewAutocommitSession(&vtgatepb.Session{})

	for _, sql := range tcases {
		t.Run(sql, func(t *testing.T) {
			_, err := exec(executor, session, sql)
			require.NoError(t, err)
			require.Nil(t, session.LockSession)
		})
	}

	// get_lock should reserve a connection.
	_, err := exec(executor, session, "select get_lock('lock name', 10) from dual")
	require.NoError(t, err)
	require.NotNil(t, session.LockSession)

}

func TestSelectFromInformationSchema(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	session := NewSafeSession(nil)

	// check failure when trying to query two keyspaces
	_, err := exec(executor, session, "SELECT B.TABLE_NAME FROM INFORMATION_SCHEMA.TABLES AS A, INFORMATION_SCHEMA.COLUMNS AS B WHERE A.TABLE_SCHEMA = 'TestExecutor' AND A.TABLE_SCHEMA = 'TestXBadSharding'")
	require.Error(t, err)
	require.Contains(t, err.Error(), "specifying two different database in the query is not supported")

	// we pick a keyspace and query for table_schema = database(). should be routed to the picked keyspace
	session.TargetString = "TestExecutor"
	_, err = exec(executor, session, "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = database()")
	require.NoError(t, err)
	assert.Equal(t, sbc1.StringQueries(), []string{"select * from INFORMATION_SCHEMA.`TABLES` where TABLE_SCHEMA = database()"})

	// `USE TestXBadSharding` and then query info_schema about TestExecutor - should target TestExecutor and not use the default keyspace
	sbc1.Queries = nil
	session.TargetString = "TestXBadSharding"
	_, err = exec(executor, session, "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'TestExecutor'")
	require.NoError(t, err)
	assert.Equal(t, sbc1.StringQueries(), []string{"select * from INFORMATION_SCHEMA.`TABLES` where TABLE_SCHEMA = :__vtschemaname /* VARCHAR */"})
}

func TestStreamOrderByLimitWithMultipleResults(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	count := 1
	for _, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col|weight_string(id)", "int32|int32|varchar"), fmt.Sprintf("%d|%d|NULL", count, count)),
			sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col|weight_string(id)", "int32|int32|varchar"), fmt.Sprintf("%d|%d|NULL", count+10, count)),
		})
		count++
	}

	executor := NewExecutor(context.Background(), serv, cell, resolver, true, false, testBufferSize, cache.DefaultConfig, nil, false, querypb.ExecuteOptions_V3)
	before := runtime.NumGoroutine()

	query := "select id, col from user order by id limit 2"
	gotResult, err := executorStream(executor, query)
	require.NoError(t, err)

	wantResult := sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col", "int32|int32"), "1|1", "2|2")
	utils.MustMatch(t, wantResult, gotResult)
	// some sleep to close all goroutines.
	time.Sleep(100 * time.Millisecond)
	assert.GreaterOrEqual(t, before, runtime.NumGoroutine(), "left open goroutines lingering")
}

func TestSelectScatterFails(t *testing.T) {
	sess := &vtgatepb.Session{}
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(KsTestSharded)
	s.VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)

	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	for i, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, "TestExecutor", shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		sbc.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "col1", Type: sqltypes.Int32},
				{Name: "col2", Type: sqltypes.Int32},
				{Name: "weight_string(col2)", Type: sqltypes.VarBinary},
			},
			InsertID: 0,
			Rows: [][]sqltypes.Value{{
				sqltypes.NewInt32(1),
				sqltypes.NewInt32(int32(i % 4)),
				sqltypes.NULL,
			}},
		}})
	}

	executor := createExecutor(serv, cell, resolver)
	executor.allowScatter = false
	logChan := QueryLogger.Subscribe("Test")
	defer QueryLogger.Unsubscribe(logChan)

	_, err := executorExecSession(executor, "select id from `user`", nil, sess)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scatter")

	// Run the test again, to ensure it behaves the same for a cached query
	_, err = executorExecSession(executor, "select id from `user`", nil, sess)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "scatter")

	_, err = executorExecSession(executor, "select /*vt+ ALLOW_SCATTER */ id from user", nil, sess)
	require.NoError(t, err)

	_, err = executorExecSession(executor, "begin", nil, sess)
	require.NoError(t, err)

	_, err = executorExecSession(executor, "commit", nil, sess)
	require.NoError(t, err)

	_, err = executorExecSession(executor, "savepoint a", nil, sess)
	require.NoError(t, err)
}

func TestGen4SelectStraightJoin(t *testing.T) {
	executor, sbc1, _, _ := createExecutorEnv()
	executor.normalize = true
	executor.pv = querypb.ExecuteOptions_Gen4
	session := NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"})
	query := "select u.id from user u straight_join user2 u2 on u.id = u2.id"
	_, err := executor.Execute(context.Background(),
		"TestGen4SelectStraightJoin",
		session,
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select u.id from `user` as u, user2 as u2 where u.id = u2.id",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	wantWarnings := []*querypb.QueryWarning{
		{
			Code:    1235,
			Message: "straight join is converted to normal join",
		},
	}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	utils.MustMatch(t, wantWarnings, session.Warnings)
}

func TestGen4MultiColumnVindexEqual(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	executor.normalize = true
	executor.pv = querypb.ExecuteOptions_Gen4

	session := NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"})
	query := "select * from user_region where cola = 1 and colb = 2"
	_, err := executor.Execute(context.Background(),
		"TestGen4MultiColumnVindex",
		session,
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql: "select * from user_region where cola = :cola /* INT64 */ and colb = :colb /* INT64 */",
			BindVariables: map[string]*querypb.BindVariable{
				"cola": sqltypes.Int64BindVariable(1),
				"colb": sqltypes.Int64BindVariable(2),
			},
		},
	}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	require.Nil(t, sbc2.Queries)

	sbc1.Queries = nil

	query = "select * from user_region where cola = 17984 and colb = 1"
	_, err = executor.Execute(context.Background(),
		"TestGen4MultiColumnVindex",
		session,
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{
		{
			Sql: "select * from user_region where cola = :cola /* INT64 */ and colb = :colb /* INT64 */",
			BindVariables: map[string]*querypb.BindVariable{
				"cola": sqltypes.Int64BindVariable(17984),
				"colb": sqltypes.Int64BindVariable(1),
			},
		},
	}
	utils.MustMatch(t, wantQueries, sbc2.Queries)
	require.Nil(t, sbc1.Queries)
}

func TestGen4MultiColumnVindexIn(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	executor.normalize = true
	executor.pv = querypb.ExecuteOptions_Gen4

	session := NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"})
	query := "select * from user_region where cola IN (1,17984) and colb IN (2,3,4)"
	_, err := executor.Execute(context.Background(),
		"TestGen4MultiColumnVindex",
		session,
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	bv1, _ := sqltypes.BuildBindVariable([]int64{1})
	bv2, _ := sqltypes.BuildBindVariable([]int64{17984})
	bvtg1, _ := sqltypes.BuildBindVariable([]int64{1, 17984})
	bvtg2, _ := sqltypes.BuildBindVariable([]int64{2, 3, 4})
	wantQueries := []*querypb.BoundQuery{
		{
			Sql: "select * from user_region where cola in ::__vals0 and colb in ::__vals1",
			BindVariables: map[string]*querypb.BindVariable{
				"__vals0": bv1,
				"__vals1": bvtg2,
				"vtg1":    bvtg1,
				"vtg2":    bvtg2,
			},
		},
	}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	wantQueries = []*querypb.BoundQuery{
		{
			Sql: "select * from user_region where cola in ::__vals0 and colb in ::__vals1",
			BindVariables: map[string]*querypb.BindVariable{
				"__vals0": bv2,
				"__vals1": bvtg2,
				"vtg1":    bvtg1,
				"vtg2":    bvtg2,
			},
		},
	}
	utils.MustMatch(t, wantQueries, sbc2.Queries)
}

func TestGen4MultiColMixedColComparision(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	executor.normalize = true
	executor.pv = querypb.ExecuteOptions_Gen4

	session := NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"})
	query := "select * from user_region where colb = 2 and cola IN (1,17984)"
	_, err := executor.Execute(context.Background(),
		"TestGen4MultiColMixedColComparision",
		session,
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	bvtg1 := sqltypes.Int64BindVariable(2)
	bvtg2, _ := sqltypes.BuildBindVariable([]int64{1, 17984})
	vals0sbc1, _ := sqltypes.BuildBindVariable([]int64{1})
	vals0sbc2, _ := sqltypes.BuildBindVariable([]int64{17984})
	wantQueries := []*querypb.BoundQuery{
		{
			Sql: "select * from user_region where colb = :colb /* INT64 */ and cola in ::__vals0",
			BindVariables: map[string]*querypb.BindVariable{
				"__vals0": vals0sbc1,
				"colb":    bvtg1,
				"vtg1":    bvtg2,
			},
		},
	}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	wantQueries = []*querypb.BoundQuery{
		{
			Sql: "select * from user_region where colb = :colb /* INT64 */ and cola in ::__vals0",
			BindVariables: map[string]*querypb.BindVariable{
				"__vals0": vals0sbc2,
				"colb":    bvtg1,
				"vtg1":    bvtg2,
			},
		},
	}
	utils.MustMatch(t, wantQueries, sbc2.Queries)
}

func TestGen4MultiColBestVindexSel(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	executor.normalize = true
	executor.pv = querypb.ExecuteOptions_Gen4

	session := NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"})
	query := "select * from user_region where colb = 2 and cola IN (1,17984) and cola = 1"
	_, err := executor.Execute(context.Background(),
		"TestGen4MultiColBestVindexSel",
		session,
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	bvtg2, _ := sqltypes.BuildBindVariable([]int64{1, 17984})
	wantQueries := []*querypb.BoundQuery{
		{
			Sql: "select * from user_region where colb = :colb /* INT64 */ and cola in ::vtg1 and cola = :cola /* INT64 */",
			BindVariables: map[string]*querypb.BindVariable{
				"colb": sqltypes.Int64BindVariable(2),
				"vtg1": bvtg2,
				"cola": sqltypes.Int64BindVariable(1),
			},
		},
	}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	require.Nil(t, sbc2.Queries)

	// reset
	sbc1.Queries = nil

	query = "select * from user_region where colb in (10,20) and cola IN (1,17984) and cola = 1 and colb = 2"
	_, err = executor.Execute(context.Background(),
		"TestGen4MultiColBestVindexSel",
		session,
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)

	bvtg1, _ := sqltypes.BuildBindVariable([]int64{10, 20})
	wantQueries = []*querypb.BoundQuery{
		{
			Sql: "select * from user_region where colb in ::vtg1 and cola in ::vtg2 and cola = :cola /* INT64 */ and colb = :colb /* INT64 */",
			BindVariables: map[string]*querypb.BindVariable{
				"vtg1": bvtg1,
				"vtg2": bvtg2,
				"cola": sqltypes.Int64BindVariable(1),
				"colb": sqltypes.Int64BindVariable(2),
			},
		},
	}
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	require.Nil(t, sbc2.Queries)
}

func TestGen4MultiColMultiEqual(t *testing.T) {
	executor, sbc1, sbc2, _ := createExecutorEnv()
	executor.normalize = true
	executor.pv = querypb.ExecuteOptions_Gen4

	session := NewSafeSession(&vtgatepb.Session{TargetString: "TestExecutor"})
	query := "select * from user_region where (cola,colb) in ((17984,2),(17984,3))"
	_, err := executor.Execute(context.Background(),
		"TestGen4MultiColMultiEqual",
		session,
		query, map[string]*querypb.BindVariable{},
	)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql: "select * from user_region where (cola, colb) in ((:vtg1 /* INT64 */, :vtg2 /* INT64 */), (:vtg1 /* INT64 */, :vtg3 /* INT64 */))",
			BindVariables: map[string]*querypb.BindVariable{
				"vtg1": sqltypes.Int64BindVariable(17984),
				"vtg2": sqltypes.Int64BindVariable(2),
				"vtg3": sqltypes.Int64BindVariable(3),
			},
		},
	}
	require.Nil(t, sbc1.Queries)
	utils.MustMatch(t, wantQueries, sbc2.Queries)
}

func TestGen4SelectUnqualifiedReferenceTable(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()
	executor.pv = querypb.ExecuteOptions_Gen4

	query := "select * from zip_detail"
	_, err := executorExec(executor, query, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           query,
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)
	require.Nil(t, sbc1.Queries)
	require.Nil(t, sbc2.Queries)
}

func TestGen4SelectQualifiedReferenceTable(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()
	executor.pv = querypb.ExecuteOptions_Gen4

	query := fmt.Sprintf("select * from %s.zip_detail", KsTestSharded)
	_, err := executorExec(executor, query, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select * from zip_detail",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	require.Nil(t, sbclookup.Queries)
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	require.Nil(t, sbc2.Queries)
}

func TestGen4JoinUnqualifiedReferenceTable(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()
	executor.pv = querypb.ExecuteOptions_Gen4

	query := "select * from user join zip_detail on user.zip_detail_id = zip_detail.id"
	_, err := executorExec(executor, query, nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select * from `user`, zip_detail where `user`.zip_detail_id = zip_detail.id",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	require.Nil(t, sbclookup.Queries)
	utils.MustMatch(t, wantQueries, sbc1.Queries)
	utils.MustMatch(t, wantQueries, sbc2.Queries)

	sbc1.Queries = nil
	sbc2.Queries = nil

	query = "select * from simple join zip_detail on simple.zip_detail_id = zip_detail.id"
	_, err = executorExec(executor, query, nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{
		{
			Sql:           "select * from `simple` join zip_detail on `simple`.zip_detail_id = zip_detail.id",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	utils.MustMatch(t, wantQueries, sbclookup.Queries)
	require.Nil(t, sbc1.Queries)
	require.Nil(t, sbc2.Queries)
}

func TestGen4CrossShardJoinQualifiedReferenceTable(t *testing.T) {
	executor, sbc1, sbc2, sbclookup := createExecutorEnv()
	executor.pv = querypb.ExecuteOptions_Gen4

	query := "select user.id from user join TestUnsharded.zip_detail on user.zip_detail_id = TestUnsharded.zip_detail.id"
	_, err := executorExec(executor, query, nil)
	require.NoError(t, err)

	shardedWantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select `user`.id from `user`, zip_detail where `user`.zip_detail_id = zip_detail.id",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	require.Nil(t, sbclookup.Queries)
	utils.MustMatch(t, shardedWantQueries, sbc1.Queries)
	utils.MustMatch(t, shardedWantQueries, sbc2.Queries)

	sbclookup.Queries = nil
	sbc1.Queries = nil
	sbc2.Queries = nil

	query = "select simple.id from simple join TestExecutor.zip_detail on simple.zip_detail_id = TestExecutor.zip_detail.id"
	_, err = executorExec(executor, query, nil)
	require.NoError(t, err)
	unshardedWantQueries := []*querypb.BoundQuery{
		{
			Sql:           "select `simple`.id from `simple` join zip_detail on `simple`.zip_detail_id = zip_detail.id",
			BindVariables: map[string]*querypb.BindVariable{},
		},
	}
	utils.MustMatch(t, unshardedWantQueries, sbclookup.Queries)
	require.Nil(t, sbc1.Queries)
	require.Nil(t, sbc2.Queries)
}

func TestRegionRange(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.

	cell := "regioncell"
	ks := "TestExecutor"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(ks)
	s.ShardSpec = "-20-20a0-"
	s.VSchema = executorVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-20a0", "20a0-"}
	var conns []*sandboxconn.SandboxConn
	for _, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, ks, shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		conns = append(conns, sbc)
	}
	executor := createExecutor(serv, cell, resolver)
	executor.pv = querypb.ExecuteOptions_Gen4

	tcases := []struct {
		regionID          int
		noOfShardsTouched int
	}{{
		regionID:          31,
		noOfShardsTouched: 1,
	}, {
		regionID:          32,
		noOfShardsTouched: 2,
	}, {
		regionID:          33,
		noOfShardsTouched: 1,
	}}
	for _, tcase := range tcases {
		t.Run(strconv.Itoa(tcase.regionID), func(t *testing.T) {
			sql := fmt.Sprintf("select * from user_region where cola = %d", tcase.regionID)
			_, err := executor.Execute(
				context.Background(),
				"TestRegionRange",
				NewAutocommitSession(&vtgatepb.Session{}),
				sql,
				nil)
			require.NoError(t, err)
			count := 0
			for _, sbc := range conns {
				count = count + len(sbc.Queries)
				sbc.Queries = nil
			}
			require.Equal(t, tcase.noOfShardsTouched, count)
		})
	}
}

func TestMultiCol(t *testing.T) {
	// Special setup: Don't use createLegacyExecutorEnv.
	cell := "multicol"
	ks := "TestMultiCol"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(ks)
	s.ShardSpec = "-20-20a0-"
	s.VSchema = multiColVschema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-20a0", "20a0-"}
	var conns []*sandboxconn.SandboxConn
	for _, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, ks, shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		conns = append(conns, sbc)
	}
	executor := createExecutor(serv, cell, resolver)
	executor.pv = querypb.ExecuteOptions_Gen4

	tcases := []struct {
		cola, colb, colc int
		shards           []string
	}{{
		cola: 202, colb: 1, colc: 1,
		shards: []string{"-20"},
	}, {
		cola: 203, colb: 1, colc: 1,
		shards: []string{"20-20a0"},
	}, {
		cola: 204, colb: 1, colc: 1,
		shards: []string{"20a0-"},
	}}

	ctx := context.Background()
	session := NewAutocommitSession(&vtgatepb.Session{})

	for _, tcase := range tcases {
		t.Run(fmt.Sprintf("%d_%d_%d", tcase.cola, tcase.colb, tcase.colc), func(t *testing.T) {
			sql := fmt.Sprintf("select * from multicoltbl where cola = %d and colb = %d and colc = '%d'", tcase.cola, tcase.colb, tcase.colc)
			_, err := executor.Execute(ctx, "TestMultiCol", session, sql, nil)
			require.NoError(t, err)
			var shards []string
			for _, sbc := range conns {
				if len(sbc.Queries) > 0 {
					shards = append(shards, sbc.Tablet().Shard)
					sbc.Queries = nil
				}
			}
			require.Equal(t, tcase.shards, shards)
		})
	}
}

var multiColVschema = `
{
	"sharded": true,
	"vindexes": {
		"multicol_vdx": {
			"type": "multicol",
			"params": {
				"column_count": "3",
				"column_bytes": "1,3,4",
				"column_vindex": "hash,binary,unicode_loose_xxhash"
			}
        }
	},
	"tables": {
		"multicoltbl": {
			"column_vindexes": [
				{
					"columns": ["cola","colb","colc"],
					"name": "multicol_vdx"
				}
			]
		}
	}
}
`

func TestMultiColPartial(t *testing.T) {
	// Special setup: Don't use createLegacyExecutorEnv.
	cell := "multicol"
	ks := "TestMultiCol"
	hc := discovery.NewFakeHealthCheck(nil)
	s := createSandbox(ks)
	s.ShardSpec = "-20-20a0c0-"
	s.VSchema = multiColVschema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-20a0c0", "20a0c0-"}
	var conns []*sandboxconn.SandboxConn
	for _, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, ks, shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		conns = append(conns, sbc)
	}
	executor := createExecutor(serv, cell, resolver)
	executor.pv = querypb.ExecuteOptions_Gen4

	tcases := []struct {
		where  string
		shards []string
	}{{
		where:  "cola = 252",
		shards: []string{"-20"},
	}, {
		where:  "cola = 289",
		shards: []string{"20a0c0-"},
	}, {
		where:  "cola = 606",
		shards: []string{"20-20a0c0", "20a0c0-"},
	}, {
		where:  "cola = 606 and colb = _binary '\x1f'",
		shards: []string{"20-20a0c0"},
	}, {
		where:  "cola = 606 and colb = _binary '\xa0'",
		shards: []string{"20-20a0c0", "20a0c0-"},
	}, {
		where:  "cola = 606 and colb = _binary '\xa1'",
		shards: []string{"20a0c0-"},
	}}

	ctx := context.Background()
	session := NewAutocommitSession(&vtgatepb.Session{})

	for _, tcase := range tcases {
		t.Run(tcase.where, func(t *testing.T) {
			sql := fmt.Sprintf("select * from multicoltbl where %s", tcase.where)
			_, err := executor.Execute(ctx, "TestMultiCol", session, sql, nil)
			require.NoError(t, err)
			var shards []string
			for _, sbc := range conns {
				if len(sbc.Queries) > 0 {
					shards = append(shards, sbc.Tablet().Shard)
					sbc.Queries = nil
				}
			}
			require.Equal(t, tcase.shards, shards)
		})
	}
}

func TestSelectAggregationNoData(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	createSandbox(KsTestSharded).VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for _, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, KsTestSharded, shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		conns = append(conns, sbc)
	}
	executor := createExecutor(serv, cell, resolver)
	executor.pv = querypb.ExecuteOptions_Gen4

	tcases := []struct {
		sql         string
		sandboxRes  *sqltypes.Result
		expSandboxQ string
		expField    string
		expRow      string
	}{
		{
			sql:         `select count(distinct col) from user`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col", "int64")),
			expSandboxQ: "select col, weight_string(col) from `user` group by col, weight_string(col) order by col asc",
			expField:    `[name:"count(distinct col)" type:INT64]`,
			expRow:      `[[INT64(0)]]`,
		},
		{
			sql:         `select count(*) from user`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("count(*)", "int64"), "0"),
			expSandboxQ: "select count(*) from `user`",
			expField:    `[name:"count(*)" type:INT64]`,
			expRow:      `[[INT64(0)]]`,
		},
		{
			sql:         `select col, count(*) from user group by col`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col|count(*)", "int64|int64")),
			expSandboxQ: "select col, count(*), weight_string(col) from `user` group by col, weight_string(col) order by col asc",
			expField:    `[name:"col" type:INT64 name:"count(*)" type:INT64]`,
			expRow:      `[]`,
		},
		{
			sql:         `select col, count(*) from user group by col limit 2`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col|count(*)", "int64|int64")),
			expSandboxQ: "select col, count(*), weight_string(col) from `user` group by col, weight_string(col) order by col asc",
			expField:    `[name:"col" type:INT64 name:"count(*)" type:INT64]`,
			expRow:      `[]`,
		},
		{
			sql:         `select count(*) from (select col1, col2 from user limit 2) x`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col1|col2", "int64|int64")),
			expSandboxQ: "select col1, col2 from `user` limit :__upper_limit",
			expField:    `[name:"count(*)" type:INT64]`,
			expRow:      `[[INT64(0)]]`,
		},
		{
			sql:         `select col2, count(*) from (select col1, col2 from user limit 2) x group by col2`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col1|col2|weight_string(col2)", "int64|int64|varbinary")),
			expSandboxQ: "select col1, col2, weight_string(col2) from `user` order by col2 asc limit :__upper_limit",
			expField:    `[name:"col2" type:INT64 name:"count(*)" type:INT64]`,
			expRow:      `[]`,
		},
	}

	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			for _, sbc := range conns {
				sbc.SetResults([]*sqltypes.Result{tc.sandboxRes})
				sbc.Queries = nil
			}
			qr, err := executorExec(executor, tc.sql, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.expField, fmt.Sprintf("%v", qr.Fields))
			assert.Equal(t, tc.expRow, fmt.Sprintf("%v", qr.Rows))
			require.Len(t, conns[0].Queries, 1)
			assert.Equal(t, tc.expSandboxQ, conns[0].Queries[0].Sql)
		})
	}
}

func TestSelectAggregationData(t *testing.T) {
	// Special setup: Don't use createExecutorEnv.
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	createSandbox(KsTestSharded).VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for _, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, KsTestSharded, shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		conns = append(conns, sbc)
	}
	executor := createExecutor(serv, cell, resolver)
	executor.pv = querypb.ExecuteOptions_Gen4

	tcases := []struct {
		sql         string
		sandboxRes  *sqltypes.Result
		expSandboxQ string
		expField    string
		expRow      string
	}{
		{
			sql:         `select count(distinct col) from user`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col|weight_string(col)", "int64|varbinary"), "1|NULL", "2|NULL", "2|NULL", "3|NULL"),
			expSandboxQ: "select col, weight_string(col) from `user` group by col, weight_string(col) order by col asc",
			expField:    `[name:"count(distinct col)" type:INT64]`,
			expRow:      `[[INT64(3)]]`,
		},
		{
			sql:         `select count(*) from user`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("count(*)", "int64"), "3"),
			expSandboxQ: "select count(*) from `user`",
			expField:    `[name:"count(*)" type:INT64]`,
			expRow:      `[[INT64(24)]]`,
		},
		{
			sql:         `select col, count(*) from user group by col`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col|count(*)|weight_string(col)", "int64|int64|varbinary"), "1|3|NULL"),
			expSandboxQ: "select col, count(*), weight_string(col) from `user` group by col, weight_string(col) order by col asc",
			expField:    `[name:"col" type:INT64 name:"count(*)" type:INT64]`,
			expRow:      `[[INT64(1) INT64(24)]]`,
		},
		{
			sql:         `select col, count(*) from user group by col limit 2`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col|count(*)|weight_string(col)", "int64|int64|varbinary"), "1|2|NULL", "2|1|NULL", "3|4|NULL"),
			expSandboxQ: "select col, count(*), weight_string(col) from `user` group by col, weight_string(col) order by col asc",
			expField:    `[name:"col" type:INT64 name:"count(*)" type:INT64]`,
			expRow:      `[[INT64(1) INT64(16)] [INT64(2) INT64(8)]]`,
		},
		{
			sql:         `select count(*) from (select col1, col2 from user limit 2) x`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col1|col2", "int64|int64"), "1|2", "2|1"),
			expSandboxQ: "select col1, col2 from `user` limit :__upper_limit",
			expField:    `[name:"count(*)" type:INT64]`,
			expRow:      `[[INT64(2)]]`,
		},
		{
			sql:         `select col2, count(*) from (select col1, col2 from user limit 9) x group by col2`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col1|col2|weight_string(col2)", "int64|int64|varbinary"), "3|1|NULL", "2|2|NULL"),
			expSandboxQ: "select col1, col2, weight_string(col2) from `user` order by col2 asc limit :__upper_limit",
			expField:    `[name:"col2" type:INT64 name:"count(*)" type:INT64]`,
			expRow:      `[[INT64(1) INT64(8)] [INT64(2) INT64(1)]]`,
		},
		{
			sql:         `select count(col1) from (select id, col1 from user limit 2) x`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("id|col1", "int64|varchar"), "3|a", "2|b"),
			expSandboxQ: "select id, col1 from `user` limit :__upper_limit",
			expField:    `[name:"count(col1)" type:INT64]`,
			expRow:      `[[INT64(2)]]`,
		},
		{
			sql:         `select count(col1), col2 from (select col2, col1 from user limit 9) x group by col2`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col2|col1|weight_string(col2)", "int64|varchar|varbinary"), "3|a|NULL", "2|b|NULL"),
			expSandboxQ: "select col2, col1, weight_string(col2) from `user` order by col2 asc limit :__upper_limit",
			expField:    `[name:"count(col1)" type:INT64 name:"col2" type:INT64]`,
			expRow:      `[[INT64(8) INT64(2)] [INT64(1) INT64(3)]]`,
		},
		{
			sql:         `select col1, count(col2) from (select col1, col2 from user limit 9) x group by col1`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col1|col2|weight_string(col1)", "varchar|int64|varbinary"), "a|1|a", "b|null|b"),
			expSandboxQ: "select col1, col2, weight_string(col1) from `user` order by col1 asc limit :__upper_limit",
			expField:    `[name:"col1" type:VARCHAR name:"count(col2)" type:INT64]`,
			expRow:      `[[VARCHAR("a") INT64(8)] [VARCHAR("b") INT64(0)]]`,
		},
		{
			sql:         `select col1, count(col2) from (select col1, col2 from user limit 32) x group by col1`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col1|col2|weight_string(col1)", "varchar|int64|varbinary"), "null|1|null", "null|null|null", "a|1|a", "b|null|b"),
			expSandboxQ: "select col1, col2, weight_string(col1) from `user` order by col1 asc limit :__upper_limit",
			expField:    `[name:"col1" type:VARCHAR name:"count(col2)" type:INT64]`,
			expRow:      `[[NULL INT64(8)] [VARCHAR("a") INT64(8)] [VARCHAR("b") INT64(0)]]`,
		},
		{
			sql:         `select col1, sum(col2) from (select col1, col2 from user limit 4) x group by col1`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col1|col2|weight_string(col1)", "varchar|int64|varbinary"), "a|3|a"),
			expSandboxQ: "select col1, col2, weight_string(col1) from `user` order by col1 asc limit :__upper_limit",
			expField:    `[name:"col1" type:VARCHAR name:"sum(col2)" type:DECIMAL]`,
			expRow:      `[[VARCHAR("a") DECIMAL(12)]]`,
		},
		{
			sql:         `select col1, sum(col2) from (select col1, col2 from user limit 4) x group by col1`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col1|col2|weight_string(col1)", "varchar|varchar|varbinary"), "a|2|a"),
			expSandboxQ: "select col1, col2, weight_string(col1) from `user` order by col1 asc limit :__upper_limit",
			expField:    `[name:"col1" type:VARCHAR name:"sum(col2)" type:FLOAT64]`,
			expRow:      `[[VARCHAR("a") FLOAT64(8)]]`,
		},
		{
			sql:         `select col1, sum(col2) from (select col1, col2 from user limit 4) x group by col1`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col1|col2|weight_string(col1)", "varchar|varchar|varbinary"), "a|x|a"),
			expSandboxQ: "select col1, col2, weight_string(col1) from `user` order by col1 asc limit :__upper_limit",
			expField:    `[name:"col1" type:VARCHAR name:"sum(col2)" type:FLOAT64]`,
			expRow:      `[[VARCHAR("a") FLOAT64(0)]]`,
		},
		{
			sql:         `select col1, sum(col2) from (select col1, col2 from user limit 4) x group by col1`,
			sandboxRes:  sqltypes.MakeTestResult(sqltypes.MakeTestFields("col1|col2|weight_string(col1)", "varchar|varchar|varbinary"), "a|null|a"),
			expSandboxQ: "select col1, col2, weight_string(col1) from `user` order by col1 asc limit :__upper_limit",
			expField:    `[name:"col1" type:VARCHAR name:"sum(col2)" type:FLOAT64]`,
			expRow:      `[[VARCHAR("a") NULL]]`,
		},
	}

	for _, tc := range tcases {
		t.Run(tc.sql, func(t *testing.T) {
			for _, sbc := range conns {
				sbc.SetResults([]*sqltypes.Result{tc.sandboxRes})
				sbc.Queries = nil
			}
			qr, err := executorExec(executor, tc.sql, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.expField, fmt.Sprintf("%v", qr.Fields))
			assert.Equal(t, tc.expRow, fmt.Sprintf("%v", qr.Rows))
			require.Len(t, conns[0].Queries, 1)
			assert.Equal(t, tc.expSandboxQ, conns[0].Queries[0].Sql)
		})
	}
}

func TestSelectAggregationRandom(t *testing.T) {
	cell := "aa"
	hc := discovery.NewFakeHealthCheck(nil)
	createSandbox(KsTestSharded).VSchema = executorVSchema
	getSandbox(KsTestUnsharded).VSchema = unshardedVSchema
	serv := newSandboxForCells([]string{cell})
	resolver := newTestResolver(hc, serv, cell)
	shards := []string{"-20", "20-40", "40-60", "60-80", "80-a0", "a0-c0", "c0-e0", "e0-"}
	var conns []*sandboxconn.SandboxConn
	for _, shard := range shards {
		sbc := hc.AddTestTablet(cell, shard, 1, KsTestSharded, shard, topodatapb.TabletType_PRIMARY, true, 1, nil)
		conns = append(conns, sbc)

		sbc.SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
			sqltypes.MakeTestFields("a|b", "int64|int64"),
			"null|null",
		)})
	}

	conns[0].SetResults([]*sqltypes.Result{sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("a|b", "int64|int64"),
		"10|1",
	)})

	executor := createExecutor(serv, cell, resolver)
	executor.pv = querypb.ExecuteOptions_Gen4
	session := NewAutocommitSession(&vtgatepb.Session{})

	rs, err := executor.Execute(context.Background(), "TestSelectCFC", session,
		"select /*vt+ PLANNER=gen4 */ A.a, A.b, (A.a / A.b) as c from (select sum(a) as a, sum(b) as b from user) A", nil)
	require.NoError(t, err)
	assert.Equal(t, `[[DECIMAL(10) DECIMAL(1) DECIMAL(10.0000)]]`, fmt.Sprintf("%v", rs.Rows))
}

func TestSelectHexAndBit(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true
	session := NewAutocommitSession(&vtgatepb.Session{})

	qr, err := executor.Execute(context.Background(), "TestSelectHexAndBit", session,
		"select 0b1001, b'1001', 0x9, x'09'", nil)
	require.NoError(t, err)
	require.Equal(t, `[[VARBINARY("\t") VARBINARY("\t") VARBINARY("\t") VARBINARY("\t")]]`, fmt.Sprintf("%v", qr.Rows))

	qr, err = executor.Execute(context.Background(), "TestSelectHexAndBit", session,
		"select 1 + 0b1001, 1 + b'1001', 1 + 0x9, 1 + x'09'", nil)
	require.NoError(t, err)
	require.Equal(t, `[[UINT64(10) UINT64(10) UINT64(10) UINT64(10)]]`, fmt.Sprintf("%v", qr.Rows))
}

// TestSelectCFC tests validates that cfc vindex plan gets cached and same plan is getting reused.
// This also validates that cache_size is able to calculate the cfc vindex plan size.
func TestSelectCFC(t *testing.T) {
	executor, _, _, _ := createExecutorEnv()
	executor.normalize = true
	session := NewAutocommitSession(&vtgatepb.Session{})

	_, err := executor.Execute(context.Background(), "TestSelectCFC", session,
		"select /*vt+ PLANNER=gen4 */ c2 from tbl_cfc where c1 like 'A%'", nil)
	require.NoError(t, err)

	timeout := time.After(10 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatal("not able to cache a plan withing 10 seconds.")
		case <-time.After(5 * time.Millisecond):
			// should be able to find cache entry before the timeout.
			cacheItems := executor.debugCacheEntries()
			for _, item := range cacheItems {
				if strings.Contains(item.Key, "c2 from tbl_cfc where c1 like") {
					return
				}
			}
		}
	}
}

func TestSelectView(t *testing.T) {
	executor, sbc, _, _ := createExecutorEnv()
	// add the view to local vschema
	err := executor.vschema.AddView(KsTestSharded, "user_details_view", "select user.id, user_extra.col from user join user_extra on user.id = user_extra.user_id")
	require.NoError(t, err)

	executor.normalize = true
	session := NewAutocommitSession(&vtgatepb.Session{})

	_, err = executor.Execute(context.Background(), "TestSelectView", session,
		"select * from user_details_view", nil)
	require.NoError(t, err)
	wantQueries := []*querypb.BoundQuery{{
		Sql:           "select * from (select `user`.id, user_extra.col from `user` join user_extra on `user`.id = user_extra.user_id) as user_details_view",
		BindVariables: map[string]*querypb.BindVariable{},
	}}
	utils.MustMatch(t, wantQueries, sbc.Queries)

	sbc.Queries = nil
	_, err = executor.Execute(context.Background(), "TestSelectView", session,
		"select * from user_details_view where id = 2", nil)
	require.NoError(t, err)
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select * from (select `user`.id, user_extra.col from `user` join user_extra on `user`.id = user_extra.user_id) as user_details_view where id = :id /* INT64 */",
		BindVariables: map[string]*querypb.BindVariable{
			"id": sqltypes.Int64BindVariable(2),
		},
	}}
	utils.MustMatch(t, wantQueries, sbc.Queries)

	sbc.Queries = nil
	_, err = executor.Execute(context.Background(), "TestSelectView", session,
		"select * from user_details_view where id in (1,2,3,4,5)", nil)
	require.NoError(t, err)
	bvtg1, _ := sqltypes.BuildBindVariable([]int64{1, 2, 3, 4, 5})
	bvals, _ := sqltypes.BuildBindVariable([]int64{1, 2})
	wantQueries = []*querypb.BoundQuery{{
		Sql: "select * from (select `user`.id, user_extra.col from `user` join user_extra on `user`.id = user_extra.user_id) as user_details_view where id in ::__vals",
		BindVariables: map[string]*querypb.BindVariable{
			"vtg1":   bvtg1,
			"__vals": bvals,
		},
	}}
	utils.MustMatch(t, wantQueries, sbc.Queries)
}

func TestMain(m *testing.M) {
	_flag.ParseFlagsForTest()
	os.Exit(m.Run())
}
