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

package logstats

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/safehtml/testconversions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/callinfo"
	"vitess.io/vitess/go/vt/callinfo/fakecallinfo"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestMain(m *testing.M) {
	hack.DisableProtoBufRandomness()
	os.Exit(m.Run())
}

func testFormat(t *testing.T, stats *LogStats, params url.Values) string {
	var b bytes.Buffer
	err := stats.Logf(&b, params)
	require.NoError(t, err)
	return b.String()
}

func TestLogStatsFormat(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", "sql1", "suuid", nil, streamlog.NewQueryLogConfigForTest())
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)
	logStats.EndTime = time.Date(2017, time.January, 1, 1, 2, 4, 1234, time.UTC)
	logStats.TablesUsed = []string{"ks1.tbl1", "ks2.tbl2"}
	logStats.TabletType = "PRIMARY"
	logStats.ActiveKeyspace = "db"
	params := map[string][]string{"full": {}}
	intBindVar := map[string]*querypb.BindVariable{"intVal": sqltypes.Int64BindVariable(1)}
	stringBindVar := map[string]*querypb.BindVariable{"strVal": sqltypes.StringBindVariable("abc")}

	tests := []struct {
		name     string
		redact   bool
		format   string
		expected string
		bindVars map[string]*querypb.BindVariable
	}{
		{ // 0
			redact:   false,
			format:   "text",
			expected: "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1\"\t{\"intVal\": {\"type\": \"INT64\", \"value\": 1}}\t0\t0\t\"\"\t\"PRIMARY\"\t\"suuid\"\tfalse\t[\"ks1.tbl1\",\"ks2.tbl2\"]\t[]\t\"db\"\t0.000000\t0.000000\t\"\"\tfalse\t\"\"\n",
			bindVars: intBindVar,
		}, { // 1
			redact:   true,
			format:   "text",
			expected: "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1\"\t\"[REDACTED]\"\t0\t0\t\"\"\t\"PRIMARY\"\t\"suuid\"\tfalse\t[\"ks1.tbl1\",\"ks2.tbl2\"]\t[]\t\"db\"\t0.000000\t0.000000\t\"\"\tfalse\t\"\"\n",
			bindVars: intBindVar,
		}, { // 2
			redact:   false,
			format:   "json",
			expected: "{\"ActiveKeyspace\":\"db\",\"BindVars\":{\"intVal\":{\"type\":\"INT64\",\"value\":1}},\"Cached Plan\":false,\"CommitTime\":0,\"Effective Caller\":\"\",\"EmitReason\":\"\",\"End\":\"2017-01-01 01:02:04.000001\",\"Error\":\"\",\"ExecuteTime\":0,\"ImmediateCaller\":\"\",\"Method\":\"test\",\"MirrorSourceExecuteTime\":0,\"MirrorTargetError\":\"\",\"MirrorTargetExecuteTime\":0,\"PlanTime\":0,\"RemoteAddr\":\"\",\"RoutingIndexesUsed\":[],\"RowsAffected\":0,\"SQL\":\"sql1\",\"SessionUUID\":\"suuid\",\"ShardQueries\":0,\"SlowQuery\":false,\"Start\":\"2017-01-01 01:02:03.000000\",\"StmtType\":\"\",\"TablesUsed\":[\"ks1.tbl1\",\"ks2.tbl2\"],\"TabletType\":\"PRIMARY\",\"TotalTime\":1.000001,\"Username\":\"\"}",
			bindVars: intBindVar,
		}, { // 3
			redact:   true,
			format:   "json",
			expected: "{\"ActiveKeyspace\":\"db\",\"BindVars\":\"[REDACTED]\",\"Cached Plan\":false,\"CommitTime\":0,\"Effective Caller\":\"\",\"EmitReason\":\"\",\"End\":\"2017-01-01 01:02:04.000001\",\"Error\":\"\",\"ExecuteTime\":0,\"ImmediateCaller\":\"\",\"Method\":\"test\",\"MirrorSourceExecuteTime\":0,\"MirrorTargetError\":\"\",\"MirrorTargetExecuteTime\":0,\"PlanTime\":0,\"RemoteAddr\":\"\",\"RoutingIndexesUsed\":[],\"RowsAffected\":0,\"SQL\":\"sql1\",\"SessionUUID\":\"suuid\",\"ShardQueries\":0,\"SlowQuery\":false,\"Start\":\"2017-01-01 01:02:03.000000\",\"StmtType\":\"\",\"TablesUsed\":[\"ks1.tbl1\",\"ks2.tbl2\"],\"TabletType\":\"PRIMARY\",\"TotalTime\":1.000001,\"Username\":\"\"}",
			bindVars: intBindVar,
		}, { // 4
			redact:   false,
			format:   "text",
			expected: "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1\"\t{\"strVal\": {\"type\": \"VARCHAR\", \"value\": \"abc\"}}\t0\t0\t\"\"\t\"PRIMARY\"\t\"suuid\"\tfalse\t[\"ks1.tbl1\",\"ks2.tbl2\"]\t[]\t\"db\"\t0.000000\t0.000000\t\"\"\tfalse\t\"\"\n",
			bindVars: stringBindVar,
		}, { // 5
			redact:   true,
			format:   "text",
			expected: "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1\"\t\"[REDACTED]\"\t0\t0\t\"\"\t\"PRIMARY\"\t\"suuid\"\tfalse\t[\"ks1.tbl1\",\"ks2.tbl2\"]\t[]\t\"db\"\t0.000000\t0.000000\t\"\"\tfalse\t\"\"\n",
			bindVars: stringBindVar,
		}, { // 6
			redact:   false,
			format:   "json",
			expected: "{\"ActiveKeyspace\":\"db\",\"BindVars\":{\"strVal\":{\"type\":\"VARCHAR\",\"value\":\"abc\"}},\"Cached Plan\":false,\"CommitTime\":0,\"Effective Caller\":\"\",\"EmitReason\":\"\",\"End\":\"2017-01-01 01:02:04.000001\",\"Error\":\"\",\"ExecuteTime\":0,\"ImmediateCaller\":\"\",\"Method\":\"test\",\"MirrorSourceExecuteTime\":0,\"MirrorTargetError\":\"\",\"MirrorTargetExecuteTime\":0,\"PlanTime\":0,\"RemoteAddr\":\"\",\"RoutingIndexesUsed\":[],\"RowsAffected\":0,\"SQL\":\"sql1\",\"SessionUUID\":\"suuid\",\"ShardQueries\":0,\"SlowQuery\":false,\"Start\":\"2017-01-01 01:02:03.000000\",\"StmtType\":\"\",\"TablesUsed\":[\"ks1.tbl1\",\"ks2.tbl2\"],\"TabletType\":\"PRIMARY\",\"TotalTime\":1.000001,\"Username\":\"\"}",
			bindVars: stringBindVar,
		}, { // 7
			redact:   true,
			format:   "json",
			expected: "{\"ActiveKeyspace\":\"db\",\"BindVars\":\"[REDACTED]\",\"Cached Plan\":false,\"CommitTime\":0,\"Effective Caller\":\"\",\"EmitReason\":\"\",\"End\":\"2017-01-01 01:02:04.000001\",\"Error\":\"\",\"ExecuteTime\":0,\"ImmediateCaller\":\"\",\"Method\":\"test\",\"MirrorSourceExecuteTime\":0,\"MirrorTargetError\":\"\",\"MirrorTargetExecuteTime\":0,\"PlanTime\":0,\"RemoteAddr\":\"\",\"RoutingIndexesUsed\":[],\"RowsAffected\":0,\"SQL\":\"sql1\",\"SessionUUID\":\"suuid\",\"ShardQueries\":0,\"SlowQuery\":false,\"Start\":\"2017-01-01 01:02:03.000000\",\"StmtType\":\"\",\"TablesUsed\":[\"ks1.tbl1\",\"ks2.tbl2\"],\"TabletType\":\"PRIMARY\",\"TotalTime\":1.000001,\"Username\":\"\"}",
			bindVars: stringBindVar,
		},
	}

	for i, test := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			logStats.BindVariables = test.bindVars
			for _, variable := range logStats.BindVariables {
				fmt.Println("->" + fmt.Sprintf("%v", variable))
			}
			logStats.Config.RedactDebugUIQueries = test.redact
			logStats.Config.Format = test.format
			if test.format == "text" {
				got := testFormat(t, logStats, params)
				t.Logf("got: %s", got)
				assert.Equal(t, test.expected, got)
				return
			}

			got := testFormat(t, logStats, params)
			t.Logf("got: %s", got)
			var parsed map[string]any
			err := json.Unmarshal([]byte(got), &parsed)
			require.NoError(t, err)
			assert.NotNil(t, parsed)
			formatted, err := json.Marshal(parsed)
			require.NoError(t, err)
			assert.Equal(t, test.expected, string(formatted))
		})
	}
}

func TestLogStatsRoutingIndexesUsed(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", "sql1", "suuid", nil, streamlog.NewQueryLogConfigForTest())
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)
	logStats.EndTime = time.Date(2017, time.January, 1, 1, 2, 4, 1234, time.UTC)
	logStats.RoutingIndexesUsed = [][3]string{
		{"ks1", "hash", "EqualUnique"},
		{"ks2", "lookup", "IN"},
	}
	params := map[string][]string{"full": {}}

	logStats.Config.Format = "text"
	got := testFormat(t, logStats, params)
	assert.Contains(t, got, `["ks1.hash.EqualUnique","ks2.lookup.IN"]`)

	logStats.Config.Format = "json"
	got = testFormat(t, logStats, params)
	var parsed map[string]any
	require.NoError(t, json.Unmarshal([]byte(got), &parsed))
	assert.Equal(t, []any{"ks1.hash.EqualUnique", "ks2.lookup.IN"}, parsed["RoutingIndexesUsed"])
}

func TestLogStatsFilter(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", "sql1 /* LOG_THIS_QUERY */", "",
		map[string]*querypb.BindVariable{"intVal": sqltypes.Int64BindVariable(1)}, streamlog.NewQueryLogConfigForTest())
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)
	logStats.EndTime = time.Date(2017, time.January, 1, 1, 2, 4, 1234, time.UTC)
	params := map[string][]string{"full": {}}

	got := testFormat(t, logStats, params)
	want := "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1 /* LOG_THIS_QUERY */\"\t{\"intVal\": {\"type\": \"INT64\", \"value\": 1}}\t0\t0\t\"\"\t\"\"\t\"\"\tfalse\t[]\t[]\t\"\"\t0.000000\t0.000000\t\"\"\tfalse\t\"\"\n"
	assert.Equal(t, want, got)

	logStats.Config.FilterTag = "LOG_THIS_QUERY"
	got = testFormat(t, logStats, params)
	want = "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1 /* LOG_THIS_QUERY */\"\t{\"intVal\": {\"type\": \"INT64\", \"value\": 1}}\t0\t0\t\"\"\t\"\"\t\"\"\tfalse\t[]\t[]\t\"\"\t0.000000\t0.000000\t\"\"\tfalse\t\"filtertag\"\n"
	assert.Equal(t, want, got)

	logStats.Config.FilterTag = "NOT_THIS_QUERY"
	got = testFormat(t, logStats, params)
	want = ""
	assert.Equal(t, want, got)
}

func TestLogStatsRowThreshold(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", "sql1 /* LOG_THIS_QUERY */", "",
		map[string]*querypb.BindVariable{"intVal": sqltypes.Int64BindVariable(1)}, streamlog.NewQueryLogConfigForTest())
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)
	logStats.EndTime = time.Date(2017, time.January, 1, 1, 2, 4, 1234, time.UTC)
	params := map[string][]string{"full": {}}

	got := testFormat(t, logStats, params)
	want := "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1 /* LOG_THIS_QUERY */\"\t{\"intVal\": {\"type\": \"INT64\", \"value\": 1}}\t0\t0\t\"\"\t\"\"\t\"\"\tfalse\t[]\t[]\t\"\"\t0.000000\t0.000000\t\"\"\tfalse\t\"\"\n"
	assert.Equal(t, want, got)

	got = testFormat(t, logStats, params)
	want = "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1 /* LOG_THIS_QUERY */\"\t{\"intVal\": {\"type\": \"INT64\", \"value\": 1}}\t0\t0\t\"\"\t\"\"\t\"\"\tfalse\t[]\t[]\t\"\"\t0.000000\t0.000000\t\"\"\tfalse\t\"\"\n"
	assert.Equal(t, want, got)

	logStats.Config.RowThreshold = 1
	got = testFormat(t, logStats, params)
	assert.Empty(t, got)
}

func TestLogStatsTimeThreshold(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", "sql1 /* LOG_THIS_QUERY */", "",
		map[string]*querypb.BindVariable{"intVal": sqltypes.Int64BindVariable(1)}, streamlog.NewQueryLogConfigForTest())
	// Query total time is 1 second and 1234 nanosecond, TimeShreshold is 1024 ns
	logStats.Config.TimeThreshold = 1024
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)
	logStats.EndTime = time.Date(2017, time.January, 1, 1, 2, 4, 1234, time.UTC)
	params := map[string][]string{"full": {}}

	got := testFormat(t, logStats, params)
	want := "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1 /* LOG_THIS_QUERY */\"\t{\"intVal\": {\"type\": \"INT64\", \"value\": 1}}\t0\t0\t\"\"\t\"\"\t\"\"\tfalse\t[]\t[]\t\"\"\t0.000000\t0.000000\t\"\"\tfalse\t\"time\"\n"
	assert.Equal(t, want, got)

	got = testFormat(t, logStats, params)
	want = "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1 /* LOG_THIS_QUERY */\"\t{\"intVal\": {\"type\": \"INT64\", \"value\": 1}}\t0\t0\t\"\"\t\"\"\t\"\"\tfalse\t[]\t[]\t\"\"\t0.000000\t0.000000\t\"\"\tfalse\t\"time\"\n"
	assert.Equal(t, want, got)

	// Set Query threshold more than query duration: 1 second and 1234 nanosecond
	logStats.Config.TimeThreshold = 2 * 1024 * 1024 * 1024
	got = testFormat(t, logStats, params)
	assert.Empty(t, got)
}

func TestLogStatsEmimtOnAnyConditionMet(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", "sql1 /* LOG_THIS_QUERY */", "",
		map[string]*querypb.BindVariable{"intVal": sqltypes.Int64BindVariable(1)}, streamlog.NewQueryLogConfigForTest())
	// Query total time is 1 second and 1234 nanosecond, TimeShreshold is 1024 ns
	logStats.Config.FilterTag = "LOG_THIS_QUERY"
	logStats.Config.TimeThreshold = 1024
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)
	logStats.EndTime = time.Date(2017, time.January, 1, 1, 2, 4, 1234, time.UTC)
	logStats.Config.EmitOnAnyConditionMet = true
	params := map[string][]string{"full": {}}

	got := testFormat(t, logStats, params)
	want := "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1 /* LOG_THIS_QUERY */\"\t{\"intVal\": {\"type\": \"INT64\", \"value\": 1}}\t0\t0\t\"\"\t\"\"\t\"\"\tfalse\t[]\t[]\t\"\"\t0.000000\t0.000000\t\"\"\tfalse\t\"filtertag,time\"\n"
	assert.Equal(t, want, got)

	got = testFormat(t, logStats, params)
	want = "test\t\t\t''\t''\t2017-01-01 01:02:03.000000\t2017-01-01 01:02:04.000001\t1.000001\t0.000000\t0.000000\t0.000000\t\t\"sql1 /* LOG_THIS_QUERY */\"\t{\"intVal\": {\"type\": \"INT64\", \"value\": 1}}\t0\t0\t\"\"\t\"\"\t\"\"\tfalse\t[]\t[]\t\"\"\t0.000000\t0.000000\t\"\"\tfalse\t\"filtertag,time\"\n"
	assert.Equal(t, want, got)

	// Set Query threshold more than query duration: 1 second and 1234 nanosecond
	logStats.Config.TimeThreshold = 2 * 1024 * 1024 * 1024
	logStats.Config.FilterTag = ""
	logStats.Config.RowThreshold = 1
	got = testFormat(t, logStats, params)
	assert.Empty(t, got)
}

func TestMarkSlowQuery(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", "sql1", "", nil, streamlog.NewQueryLogConfigForTest())
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)

	logStats.EndTime = logStats.StartTime.Add(time.Second)
	logStats.MarkSlowQuery(time.Second)
	assert.True(t, logStats.SlowQuery)

	logStats.EndTime = logStats.StartTime.Add(999 * time.Millisecond)
	logStats.MarkSlowQuery(time.Second)
	assert.False(t, logStats.SlowQuery)

	logStats.EndTime = logStats.StartTime.Add(2 * time.Second)
	logStats.MarkSlowQuery(0)
	assert.False(t, logStats.SlowQuery)
}

func TestLogStatsContextHTML(t *testing.T) {
	html := "HtmlContext"
	callInfo := &fakecallinfo.FakeCallInfo{
		Html: testconversions.MakeHTMLForTest(html),
	}
	ctx := callinfo.NewContext(t.Context(), callInfo)
	logStats := NewLogStats(ctx, "test", "sql1", "", map[string]*querypb.BindVariable{}, streamlog.NewQueryLogConfigForTest())
	require.Equalf(t, html, logStats.ContextHTML().String(), "expect to get html: %s, but got: %s", html, logStats.ContextHTML().String())
}

func TestLogStatsErrorStr(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", "sql1", "", map[string]*querypb.BindVariable{}, streamlog.NewQueryLogConfigForTest())
	require.Emptyf(t, logStats.ErrorStr(), "should not get error in stats, but got: %s", logStats.ErrorStr())
	errStr := "unknown error"
	logStats.Error = errors.New(errStr)
	require.Containsf(t, logStats.ErrorStr(), errStr, "expect string '%s' in error message, but got: %s", errStr, logStats.ErrorStr())
}

func TestLogStatsMirrorTargetErrorStr(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", "sql1", "", map[string]*querypb.BindVariable{}, streamlog.NewQueryLogConfigForTest())
	require.Emptyf(t, logStats.MirrorTargetErrorStr(), "should not get error in stats, but got: %s", logStats.ErrorStr())
	errStr := "unknown error"
	logStats.MirrorTargetError = errors.New(errStr)
	require.Containsf(t, logStats.MirrorTargetErrorStr(), errStr, "expect string '%s' in error message, but got: %s", errStr, logStats.ErrorStr())
}

func TestLogStatsRemoteAddrUsername(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", "sql1", "", map[string]*querypb.BindVariable{}, streamlog.NewQueryLogConfigForTest())
	addr, user := logStats.RemoteAddrUsername()
	require.Empty(t, addr, "remote addr should be empty")
	require.Empty(t, user, "username should be empty")

	remoteAddr := "1.2.3.4"
	username := "vt"
	callInfo := &fakecallinfo.FakeCallInfo{
		Remote: remoteAddr,
		User:   username,
	}
	ctx := callinfo.NewContext(t.Context(), callInfo)
	logStats = NewLogStats(ctx, "test", "sql1", "", map[string]*querypb.BindVariable{}, streamlog.NewQueryLogConfigForTest())
	addr, user = logStats.RemoteAddrUsername()
	require.Equalf(t, remoteAddr, addr, "expected to get remote addr: %s, but got: %s", remoteAddr, addr)
	require.Equalf(t, username, user, "expected to get username: %s, but got: %s", username, user)
}

// TestLogStatsErrorsOnly tests that LogStats only logs errors when the query log mode is set to errors only for VTGate.
func TestLogStatsErrorsOnly(t *testing.T) {
	logStats := NewLogStats(t.Context(), "test", "sql1", "", map[string]*querypb.BindVariable{}, streamlog.NewQueryLogConfigForTest())
	logStats.Config.Mode = streamlog.QueryLogModeError

	// no error, should not log
	logOutput := testFormat(t, logStats, url.Values{})
	assert.Empty(t, logOutput)

	// error, should log
	logStats.Error = errors.New("test error")
	logOutput = testFormat(t, logStats, url.Values{})
	assert.Contains(t, logOutput, "test error")
}

func TestLogStatsJSONBindValues(t *testing.T) {
	records := []struct {
		name   string
		value  string
		number string
	}{
		{name: "before", value: "clean-before", number: "1.5"},
		{
			name:   "special",
			value:  strings.Repeat("a", 32) + "\x00\a\v\x7f\xff\xc0\xaf\U000e0001é\U0001f600\u2028\u2029<>&\"\\\n",
			number: "NaN",
		},
		{name: "invalid-without-separators", value: "\xff\xfe\xed\xa0\x80\"\\\n", number: "2.5"},
		{name: "after", value: "clean-after", number: "0.75"},
	}
	for _, tc := range []struct {
		name   string
		full   bool
		redact bool
	}{
		{name: "full", full: true},
		{name: "redacted", full: true, redact: true},
		{name: "abbreviated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config := streamlog.NewQueryLogConfigForTest()
			config.Format = streamlog.QueryLogFormatJSON
			config.RedactDebugUIQueries = tc.redact
			var params url.Values
			if tc.full {
				params = url.Values{"full": {}}
			}

			var output bytes.Buffer
			for _, record := range records {
				bindVars := map[string]*querypb.BindVariable{
					"float":     {Type: querypb.Type_FLOAT64, Value: []byte(record.number)},
					"varchar":   sqltypes.StringBindVariable(record.value),
					"varbinary": sqltypes.BytesBindVariable([]byte(record.value)),
				}
				require.NoError(t, sqltypes.ValidateBindVariables(bindVars))
				stats := NewLogStats(t.Context(), "Execute", "select :varchar, :varbinary", record.name, bindVars, config)
				stats.SaveEndTime()
				require.NoError(t, stats.Logf(&output, params))
			}

			decoder := json.NewDecoder(&output)
			for _, record := range records {
				var logged struct {
					SessionUUID string
					BindVars    json.RawMessage
				}
				require.NoError(t, decoder.Decode(&logged), "record %s", record.name)
				assert.Equal(t, record.name, logged.SessionUUID)
				if tc.redact {
					assert.Equal(t, `"[REDACTED]"`, string(logged.BindVars))
					continue
				}

				var bindings map[string]struct {
					Type  string
					Value json.RawMessage
				}
				require.NoError(t, json.Unmarshal(logged.BindVars, &bindings))
				require.Len(t, bindings, 3)
				assert.Equal(t, "FLOAT64", bindings["float"].Type)
				wantNumber := record.number
				if wantNumber == "NaN" {
					wantNumber = `"NaN"`
				}
				assert.Equal(t, wantNumber, string(bindings["float"].Value))
				encoded, err := json.Marshal(record.value)
				require.NoError(t, err)
				var normalized string
				require.NoError(t, json.Unmarshal(encoded, &normalized))
				for _, name := range []string{"varchar", "varbinary"} {
					binding, ok := bindings[name]
					require.True(t, ok, "missing binding %s", name)
					assert.Equal(t, strings.ToUpper(name), binding.Type)
					var value string
					require.NoError(t, json.Unmarshal(binding.Value, &value))
					if tc.full {
						assert.Equal(t, normalized, value, "record %s, binding %s", record.name, name)
					} else {
						assert.Equal(t, strconv.Itoa(len(record.value))+" bytes", value)
					}
				}
			}
			var extra json.RawMessage
			assert.ErrorIs(t, decoder.Decode(&extra), io.EOF)
		})
	}
}

type logfBenchmarkCase struct {
	name     string
	sql      string
	bindVars map[string]*querypb.BindVariable
	full     bool
	redact   bool
}

func logfBenchmarkCases(b *testing.B) []logfBenchmarkCase {
	b.Helper()
	var cases []logfBenchmarkCase
	for _, size := range []int{16, 32, 64, 4096, 65536} {
		for _, shape := range []string{"clean", "sparse", "dense", "unicode"} {
			var text string
			switch shape {
			case "clean":
				text = strings.Repeat("a", size)
			case "sparse":
				payload := []byte(strings.Repeat("a", size))
				for i := min(size/2, 125); i < size; i += 128 {
					copy(payload[i:], "\"\\\n")
				}
				text = string(payload)
				require.Contains(b, text, "\"")
			case "dense":
				text = strings.Repeat("abc\"\\\n", size/6+1)[:size]
			case "unicode":
				text = strings.Repeat("\U0001d11e", size/4)
			}
			require.Len(b, text, size)
			require.True(b, utf8.ValidString(text))

			// Sizes describe the payload before SQL or log quoting.
			name := fmt.Sprintf("%d/%s", size, shape)
			bindVars := map[string]*querypb.BindVariable{
				"id":      sqltypes.Int64BindVariable(42),
				"payload": sqltypes.StringBindVariable(text),
			}
			const sql = "insert into docs(id,doc) values (:id,:payload)"
			cases = append(cases,
				logfBenchmarkCase{
					name: "SQL/" + name,
					sql:  "insert into docs(id,doc) values (42," + sqltypes.EncodeStringSQL(text) + ")",
					full: true,
				},
				logfBenchmarkCase{
					name:     "full-binds/" + name,
					sql:      sql,
					bindVars: bindVars,
					full:     true,
				},
			)
			if shape == "clean" && (size == 32 || size == 65536) {
				cases = append(cases,
					logfBenchmarkCase{
						name:     "abbreviated-binds/" + name,
						sql:      sql,
						bindVars: bindVars,
					},
					logfBenchmarkCase{
						name:     "redacted-binds/" + name,
						sql:      sql,
						bindVars: bindVars,
						full:     true,
						redact:   true,
					},
				)
			}
		}
	}
	return cases
}

func BenchmarkLogf(b *testing.B) {
	cases := logfBenchmarkCases(b)
	for _, format := range []string{"text", "json"} {
		b.Run(format, func(b *testing.B) {
			for _, tc := range cases {
				b.Run(tc.name, func(b *testing.B) {
					config := streamlog.NewQueryLogConfigForTest()
					config.Format = format
					config.RedactDebugUIQueries = tc.redact
					start := time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)
					stats := &LogStats{
						Config:         config,
						Ctx:            b.Context(),
						Method:         "Execute",
						PlanType:       "Insert",
						TabletType:     "PRIMARY",
						StmtType:       "INSERT",
						SQL:            tc.sql,
						BindVariables:  tc.bindVars,
						StartTime:      start,
						EndTime:        start.Add(time.Millisecond),
						ShardQueries:   1,
						RowsAffected:   1,
						PlanTime:       time.Microsecond,
						ExecuteTime:    900 * time.Microsecond,
						CommitTime:     99 * time.Microsecond,
						TablesUsed:     []string{"docs"},
						SessionUUID:    "e90fe861-aabb-4bbb-9ccc-000000000001",
						CachedPlan:     true,
						ActiveKeyspace: "main",
					}
					var params url.Values
					if tc.full {
						params = url.Values{"full": {}}
					}
					var out bytes.Buffer
					require.NoError(b, stats.Logf(&out, params))
					require.NotZero(b, out.Len(), "Logf emitted no record")
					b.ReportAllocs()
					b.SetBytes(int64(out.Len()))
					for b.Loop() {
						if err := stats.Logf(io.Discard, params); err != nil {
							require.NoError(b, err)
						}
					}
				})
			}
		})
	}
}
