/*
Copyright 2026 The Vitess Authors.

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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"
	"vitess.io/vitess/go/vt/vttablet/sandboxconn"
)

func lastQuery(t *testing.T, sbc *sandboxconn.SandboxConn) *querypb.BoundQuery {
	t.Helper()
	require.NotEmpty(t, sbc.Queries)
	return sbc.Queries[len(sbc.Queries)-1]
}

func sessionWithSQLMode(target, sqlMode string) *econtext.SafeSession {
	session := &vtgatepb.Session{TargetString: target}
	if sqlMode != "" {
		session.SystemVariables = map[string]string{"sql_mode": sqlMode}
	}
	return econtext.NewSafeSession(session)
}

// A session's SQL is read under the session's sql_mode: with PIPES_AS_CONCAT,
// || is concatenation, as in MySQL under that mode, and the query reaches the
// tablet as a concat() call together with the session's sql_mode, the mode
// included. Without the mode, || is logical OR. The two readings never share a
// cached plan.
func TestExecutorSessionPipesAsConcat(t *testing.T) {
	executor, sbc1, _, sbclookup, ctx := createExecutorEnvWithConfig(t, createExecutorConfigWithNormalizer())

	// under the mode || is concat(); the tablet receives the mode
	session := sessionWithSQLMode(KsTestUnsharded, "'PIPES_AS_CONCAT'")
	_, err := executorExecSession(ctx, executor, session, "select id || id from main1", nil)
	require.NoError(t, err)
	assert.Equal(t, "select /*+ SET_VAR(sql_mode = 'PIPES_AS_CONCAT') */ concat(id, id) from main1", lastQuery(t, sbclookup).Sql)

	// without it, || is logical OR
	session = sessionWithSQLMode(KsTestUnsharded, "")
	_, err = executorExecSession(ctx, executor, session, "select id || id from main1", nil)
	require.NoError(t, err)
	assert.Equal(t, "select id or id from main1", lastQuery(t, sbclookup).Sql)

	// the streaming path reads under the mode as well
	session = sessionWithSQLMode(KsTestUnsharded, "'PIPES_AS_CONCAT'")
	err = executor.StreamExecute(ctx, nil, "TestExecuteStream", session, "select id || id from main1", nil, false, func(*sqltypes.Result) error { return nil })
	require.NoError(t, err)
	assert.Equal(t, "select /*+ SET_VAR(sql_mode = 'PIPES_AS_CONCAT') */ concat(id, id) from main1", lastQuery(t, sbclookup).Sql)

	// the readings do not share a plan: a prepared statement's plan is keyed by
	// its text as written, and on the reserved-connection transport, where no
	// SET_VAR hint tells sessions apart, only the mode the text was read under
	// does
	executor.vConfig.SetVarEnabled = false
	session = sessionWithSQLMode(KsTestUnsharded, "'PIPES_AS_CONCAT'")
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "select id || id from main1", nil, true)
	require.NoError(t, err)
	assert.Equal(t, "select concat(id, id) from main1", lastQuery(t, sbclookup).Sql)
	session = sessionWithSQLMode(KsTestUnsharded, "'STRICT_TRANS_TABLES'")
	_, err = executor.Execute(ctx, nil, "TestExecute", session, "select id || id from main1", nil, true)
	require.NoError(t, err)
	assert.Equal(t, "select id or id from main1", lastQuery(t, sbclookup).Sql)
	executor.vConfig.SetVarEnabled = true

	// a statement prepared with PREPARE ... FROM is read under the session's
	// mode when it is planned
	session = sessionWithSQLMode(KsTestUnsharded, "'PIPES_AS_CONCAT'")
	_, err = executorExecSession(ctx, executor, session, "prepare stmt from 'select id || id from main1'", nil)
	require.NoError(t, err)
	_, err = executorExecSession(ctx, executor, session, "execute stmt", nil)
	require.NoError(t, err)
	assert.Equal(t, "select /*+ SET_VAR(sql_mode = 'PIPES_AS_CONCAT') */ concat(id, id) from main1", lastQuery(t, sbclookup).Sql)

	// the count query of SQL_CALC_FOUND_ROWS is built from a second parse of
	// the statement, under the session's mode as well
	session = sessionWithSQLMode(KsTestSharded, "'PIPES_AS_CONCAT'")
	sbc1.SetResults([]*sqltypes.Result{
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("id", "int64"), "1"),
		sqltypes.MakeTestResult(sqltypes.MakeTestFields("count(*)", "int64"), "1"),
	})
	_, err = executorExecSession(ctx, executor, session, "select sql_calc_found_rows id from user where id = 1 and id || id limit 1", nil)
	require.NoError(t, err)
	assert.Equal(t, "select /*+ SET_VAR(sql_mode = 'PIPES_AS_CONCAT') */ count(*) from `user` where id = :id and concat(id, id)", lastQuery(t, sbc1).Sql)
}

// SET sql_mode accepts PIPES_AS_CONCAT, stores the canonical value, forwards it
// to the tablets, and answers @@sql_mode from the session with it. A later
// assignment is judged against the session's own value.
func TestExecutorSetPipesAsConcat(t *testing.T) {
	executor, _, _, sbclookup, ctx := createExecutorEnvWithConfig(t, createExecutorConfigWithNormalizer())
	session := econtext.NewAutocommitSession(&vtgatepb.Session{EnableSystemSettings: true, TargetString: KsTestUnsharded})

	judgment := func(orig string, newValue sqltypes.Value) {
		sbclookup.SetResults([]*sqltypes.Result{{
			Fields: []*querypb.Field{
				{Name: "orig", Type: sqltypes.VarChar, Charset: uint32(collations.MySQL8().DefaultConnectionCharset())},
				{Name: "new", Type: newValue.Type(), Charset: uint32(collations.MySQL8().DefaultConnectionCharset())},
			},
			Rows: [][]sqltypes.Value{{sqltypes.NewVarChar(orig), newValue}},
		}})
	}

	judgment("STRICT_TRANS_TABLES", sqltypes.NewVarChar("pipes_as_concat,strict_trans_tables"))
	_, err := executorExecSession(ctx, executor, session, "set sql_mode = 'pipes_as_concat,strict_trans_tables'", nil)
	require.NoError(t, err)
	assert.Equal(t, "'PIPES_AS_CONCAT,STRICT_TRANS_TABLES'", session.SystemVariables["sql_mode"], "stored canonically")

	_, err = executorExecSession(ctx, executor, session, "select id || id from main1", nil)
	require.NoError(t, err)
	assert.Equal(t, "select /*+ SET_VAR(sql_mode = 'PIPES_AS_CONCAT,STRICT_TRANS_TABLES') */ concat(id, id) from main1", lastQuery(t, sbclookup).Sql)

	// @@sql_mode reports the session's value, the mode included
	_, err = executorExecSession(ctx, executor, session, "select @@sql_mode, id from main1", nil)
	require.NoError(t, err)
	query := lastQuery(t, sbclookup)
	assert.Equal(t, "select /*+ SET_VAR(sql_mode = 'PIPES_AS_CONCAT,STRICT_TRANS_TABLES') */ :__vtsql_mode as `@@sql_mode`, id from main1", query.Sql)
	assert.Equal(t, "PIPES_AS_CONCAT,STRICT_TRANS_TABLES", string(query.BindVariables["__vtsql_mode"].Value))

	// an expression over @@sql_mode is evaluated against the session's value
	judgment("STRICT_TRANS_TABLES", sqltypes.NewVarChar("PIPES_AS_CONCAT,STRICT_TRANS_TABLES,NO_ZERO_DATE"))
	_, err = executorExecSession(ctx, executor, session, "set sql_mode = concat(@@sql_mode, ',NO_ZERO_DATE')", nil)
	require.NoError(t, err)
	query = lastQuery(t, sbclookup)
	assert.Equal(t, "select @@sql_mode orig, concat(:__vtsql_mode, ',NO_ZERO_DATE') new", query.Sql)
	assert.Equal(t, "PIPES_AS_CONCAT,STRICT_TRANS_TABLES", string(query.BindVariables["__vtsql_mode"].Value))
	assert.Equal(t, "'PIPES_AS_CONCAT,STRICT_TRANS_TABLES,NO_ZERO_DATE'", session.SystemVariables["sql_mode"])

	// setting the mode back to what the pooled connection runs under is judged
	// against the session's own value, so it applies
	judgment("STRICT_TRANS_TABLES", sqltypes.NewVarChar("STRICT_TRANS_TABLES"))
	_, err = executorExecSession(ctx, executor, session, "set sql_mode = 'STRICT_TRANS_TABLES'", nil)
	require.NoError(t, err)
	assert.Equal(t, "'STRICT_TRANS_TABLES'", session.SystemVariables["sql_mode"])
	_, err = executorExecSession(ctx, executor, session, "select id || id from main1", nil)
	require.NoError(t, err)
	assert.Equal(t, "select /*+ SET_VAR(sql_mode = 'STRICT_TRANS_TABLES') */ id or id from main1", lastQuery(t, sbclookup).Sql)

	// a numeric assignment stores the names it decodes to, so the parser
	// reads the mode it asked for
	judgment("STRICT_TRANS_TABLES", sqltypes.NewInt64(2))
	_, err = executorExecSession(ctx, executor, session, "set sql_mode = 2", nil)
	require.NoError(t, err)
	assert.Equal(t, "'PIPES_AS_CONCAT'", session.SystemVariables["sql_mode"])
	_, err = executorExecSession(ctx, executor, session, "select id || id from main1", nil)
	require.NoError(t, err)
	assert.Equal(t, "select /*+ SET_VAR(sql_mode = 'PIPES_AS_CONCAT') */ concat(id, id) from main1", lastQuery(t, sbclookup).Sql)

	// the other lexer modes are still rejected
	judgment("", sqltypes.NewVarChar("PIPES_AS_CONCAT,ANSI_QUOTES"))
	_, err = executorExecSession(ctx, executor, session, "set sql_mode = 'PIPES_AS_CONCAT,ANSI_QUOTES'", nil)
	require.ErrorContains(t, err, "setting the ANSI_QUOTES sql_mode is unsupported")
	assert.Equal(t, "'PIPES_AS_CONCAT'", session.SystemVariables["sql_mode"], "the session keeps its value")
}
