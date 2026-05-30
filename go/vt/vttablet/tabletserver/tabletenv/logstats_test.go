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

package tabletenv

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/google/safehtml/testconversions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/callinfo/fakecallinfo"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestLogStats(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", streamlog.QueryLogConfig{})
	logStats.AddRewrittenSQL("sql1", time.Now())

	require.Contains(t, logStats.RewrittenSQL(), "sql1", "RewrittenSQL should contains sql: sql1")
	require.Zero(t, logStats.SizeOfResponse(), "there is no rows in log stats, estimated size should be 0 bytes")

	logStats.Rows = [][]sqltypes.Value{{sqltypes.NewVarBinary("a")}}
	require.Positive(t, logStats.SizeOfResponse(), "log stats has some rows, should have positive response size")
}

func testFormat(stats *LogStats, params url.Values) string {
	var b strings.Builder
	stats.Logf(&b, params)
	return b.String()
}

func TestLogStatsFormat(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", streamlog.NewQueryLogConfigForTest())
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)
	logStats.EndTime = time.Date(2017, time.January, 1, 1, 2, 4, 1234, time.UTC)
	logStats.OriginalSQL = "sql"
	logStats.BindVariables = map[string]*querypb.BindVariable{"intVal": sqltypes.Int64BindVariable(1)}
	logStats.AddRewrittenSQL("sql with pii", time.Now())
	logStats.MysqlResponseTime = 0
	logStats.TransactionID = 12345
	logStats.Rows = [][]sqltypes.Value{{sqltypes.NewVarBinary("a")}}
	params := map[string][]string{"full": {}}

	got := testFormat(logStats, params)
	want := "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t\t\"sql\"\t{\"intVal\": {\"type\": \"INT64\", \"value\": 1}}\t1\t\"sql with pii\"\tmysql\t0.000000\t0.000000\t0\t12345\t1\t\"\"\t\"\"\t\n"
	assert.Equal(t, want, got)

	logStats.Config.RedactDebugUIQueries = true

	got = testFormat(logStats, params)
	want = "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t\t\"sql\"\t\"[REDACTED]\"\t1\t\"[REDACTED]\"\tmysql\t0.000000\t0.000000\t0\t12345\t1\t\"\"\t\"\"\t\n"
	assert.Equal(t, want, got)

	logStats.Config.RedactDebugUIQueries = false
	logStats.Config.Format = streamlog.QueryLogFormatJSON

	got = testFormat(logStats, params)
	var parsed map[string]any
	err := json.Unmarshal([]byte(got), &parsed)
	assert.NoErrorf(t, err, "logstats format: error unmarshaling json -- got:\n%v", got)
	formatted, err := json.MarshalIndent(parsed, "", "    ")
	require.NoError(t, err)
	want = "{\n    \"BindVars\": {\n        \"intVal\": {\n            \"type\": \"INT64\",\n            \"value\": 1\n        }\n    },\n    \"CallInfo\": \"\",\n    \"ConnWaitTime\": 0,\n    \"Effective Caller\": \"\",\n    \"EmitReason\": \"\",\n    \"End\": \"2017-01-01 01:02:04.000001\",\n    \"Error\": \"\",\n    \"ImmediateCaller\": \"\",\n    \"Method\": \"test\",\n    \"MysqlTime\": 0,\n    \"OriginalSQL\": \"sql\",\n    \"PlanType\": \"\",\n    \"Queries\": 1,\n    \"QuerySources\": \"mysql\",\n    \"ResponseSize\": 1,\n    \"RewrittenSQL\": \"sql with pii\",\n    \"RowsAffected\": 0,\n    \"Start\": \"2017-01-01 01:02:03.000000\",\n    \"TotalTime\": 1.000001,\n    \"TransactionID\": 12345,\n    \"Username\": \"\"\n}"
	assert.Equal(t, want, string(formatted))

	logStats.Config.RedactDebugUIQueries = true
	logStats.Config.Format = streamlog.QueryLogFormatJSON

	got = testFormat(logStats, params)
	err = json.Unmarshal([]byte(got), &parsed)
	require.NoError(t, err)
	formatted, err = json.MarshalIndent(parsed, "", "    ")
	require.NoError(t, err)
	want = "{\n    \"BindVars\": \"[REDACTED]\",\n    \"CallInfo\": \"\",\n    \"ConnWaitTime\": 0,\n    \"Effective Caller\": \"\",\n    \"EmitReason\": \"\",\n    \"End\": \"2017-01-01 01:02:04.000001\",\n    \"Error\": \"\",\n    \"ImmediateCaller\": \"\",\n    \"Method\": \"test\",\n    \"MysqlTime\": 0,\n    \"OriginalSQL\": \"sql\",\n    \"PlanType\": \"\",\n    \"Queries\": 1,\n    \"QuerySources\": \"mysql\",\n    \"ResponseSize\": 1,\n    \"RewrittenSQL\": \"[REDACTED]\",\n    \"RowsAffected\": 0,\n    \"Start\": \"2017-01-01 01:02:03.000000\",\n    \"TotalTime\": 1.000001,\n    \"TransactionID\": 12345,\n    \"Username\": \"\"\n}"
	assert.Equal(t, want, string(formatted))

	// Make sure formatting works for string bind vars. We can't do this as part of a single
	// map because the output ordering is undefined.
	logStats.BindVariables = map[string]*querypb.BindVariable{"strVal": sqltypes.StringBindVariable("abc")}
	logStats.Config.RedactDebugUIQueries = false
	logStats.Config.Format = streamlog.QueryLogFormatText

	got = testFormat(logStats, params)
	want = "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t\t\"sql\"\t{\"strVal\": {\"type\": \"VARCHAR\", \"value\": \"abc\"}}\t1\t\"sql with pii\"\tmysql\t0.000000\t0.000000\t0\t12345\t1\t\"\"\t\"\"\t\n"
	assert.Equal(t, want, got)

	logStats.Config.RedactDebugUIQueries = false
	logStats.Config.Format = streamlog.QueryLogFormatJSON

	got = testFormat(logStats, params)
	err = json.Unmarshal([]byte(got), &parsed)
	require.NoError(t, err)
	formatted, err = json.MarshalIndent(parsed, "", "    ")
	require.NoError(t, err)
	want = "{\n    \"BindVars\": {\n        \"strVal\": {\n            \"type\": \"VARCHAR\",\n            \"value\": \"abc\"\n        }\n    },\n    \"CallInfo\": \"\",\n    \"ConnWaitTime\": 0,\n    \"Effective Caller\": \"\",\n    \"EmitReason\": \"\",\n    \"End\": \"2017-01-01 01:02:04.000001\",\n    \"Error\": \"\",\n    \"ImmediateCaller\": \"\",\n    \"Method\": \"test\",\n    \"MysqlTime\": 0,\n    \"OriginalSQL\": \"sql\",\n    \"PlanType\": \"\",\n    \"Queries\": 1,\n    \"QuerySources\": \"mysql\",\n    \"ResponseSize\": 1,\n    \"RewrittenSQL\": \"sql with pii\",\n    \"RowsAffected\": 0,\n    \"Start\": \"2017-01-01 01:02:03.000000\",\n    \"TotalTime\": 1.000001,\n    \"TransactionID\": 12345,\n    \"Username\": \"\"\n}"
	assert.Equal(t, want, string(formatted))
}

func TestLogStatsFilter(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", streamlog.NewQueryLogConfigForTest())
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)
	logStats.EndTime = time.Date(2017, time.January, 1, 1, 2, 4, 1234, time.UTC)
	logStats.OriginalSQL = "sql /* LOG_THIS_QUERY */"
	logStats.BindVariables = map[string]*querypb.BindVariable{"intVal": sqltypes.Int64BindVariable(1)}
	logStats.AddRewrittenSQL("sql with pii", time.Now())
	logStats.MysqlResponseTime = 0
	logStats.Rows = [][]sqltypes.Value{{sqltypes.NewVarBinary("a")}}
	params := map[string][]string{"full": {}}

	got := testFormat(logStats, params)
	want := "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t\t\"sql /* LOG_THIS_QUERY */\"\t{\"intVal\": {\"type\": \"INT64\", \"value\": 1}}\t1\t\"sql with pii\"\tmysql\t0.000000\t0.000000\t0\t0\t1\t\"\"\t\"\"\t\n"
	assert.Equalf(t, want, got, "logstats format")

	logStats.Config.FilterTag = "LOG_THIS_QUERY"
	got = testFormat(logStats, params)
	want = "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t\t\"sql /* LOG_THIS_QUERY */\"\t{\"intVal\": {\"type\": \"INT64\", \"value\": 1}}\t1\t\"sql with pii\"\tmysql\t0.000000\t0.000000\t0\t0\t1\t\"\"\t\"filtertag\"\t\n"
	assert.Equalf(t, want, got, "logstats format")

	logStats.Config.FilterTag = "NOT_THIS_QUERY"
	got = testFormat(logStats, params)
	want = ""
	assert.Equalf(t, want, got, "logstats format")
}

func TestLogStatsFormatQuerySources(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", streamlog.NewQueryLogConfigForTest())
	require.Equalf(t, "none", logStats.FmtQuerySources(), "should return none since log stats does not have any query source")

	logStats.QuerySources |= QuerySourceMySQL
	require.Contains(t, logStats.FmtQuerySources(), "mysql", "'mysql' should be in formatted query sources")

	logStats.QuerySources |= QuerySourceConsolidator
	require.Contains(t, logStats.FmtQuerySources(), "consolidator", "'consolidator' should be in formatted query sources")
}

func TestLogStatsContextHTML(t *testing.T) {
	html := "HtmlContext"
	callInfo := &fakecallinfo.FakeCallInfo{
		Html: testconversions.MakeHTMLForTest(html),
	}
	ctx := callinfo.NewContext(t.Context(), callInfo)
	logStats := NewLogStats(ctx, "test", streamlog.NewQueryLogConfigForTest())
	require.Equalf(t, html, logStats.ContextHTML().String(), "expect to get html: %s, but got: %s", html, logStats.ContextHTML().String())
}

func TestLogStatsErrorStr(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", streamlog.NewQueryLogConfigForTest())
	require.Emptyf(t, logStats.ErrorStr(), "should not get error in stats, but got: %s", logStats.ErrorStr())
	errStr := "unknown error"
	logStats.Error = errors.New(errStr)
	require.Containsf(t, logStats.ErrorStr(), errStr, "expect string '%s' in error message", errStr)
}

func TestLogStatsCallInfo(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", streamlog.NewQueryLogConfigForTest())
	caller, user := logStats.CallInfo()
	require.Empty(t, caller, "caller should be empty")
	require.Empty(t, user, "username should be empty")

	remoteAddr := "1.2.3.4"
	username := "vt"
	callInfo := &fakecallinfo.FakeCallInfo{
		Remote: remoteAddr,
		Method: "FakeExecute",
		User:   username,
	}
	ctx := callinfo.NewContext(t.Context(), callInfo)
	logStats = NewLogStats(ctx, "test", streamlog.NewQueryLogConfigForTest())
	caller, user = logStats.CallInfo()
	wantCaller := remoteAddr + ":FakeExecute(fakeRPC)"
	require.Equalf(t, wantCaller, caller, "expected to get caller: %s", wantCaller)
	require.Equalf(t, username, user, "expected to get username: %s", username)
}

// TestLogStatsErrorsOnly tests that LogStats only logs errors when the query log mode is set to errors only for VTTablet.
func TestLogStatsErrorsOnly(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", streamlog.NewQueryLogConfigForTest())
	logStats.Config.Mode = streamlog.QueryLogModeError

	// no error, should not log
	logOutput := testFormat(logStats, url.Values{})
	assert.Empty(t, logOutput)

	// error, should log
	logStats.Error = errors.New("test error")
	logOutput = testFormat(logStats, url.Values{})
	assert.Contains(t, logOutput, "test error")
}
