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

package vtgate

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/streamlog"
	"github.com/youtube/vitess/go/vt/callinfo"
	"github.com/youtube/vitess/go/vt/callinfo/fakecallinfo"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestLogStatsFormat(t *testing.T) {
	logStats := NewLogStats(context.Background(), "test", "sql1", map[string]*querypb.BindVariable{"intVal": sqltypes.Int64BindVariable(1)})
	logStats.StartTime = time.Date(2017, time.January, 1, 1, 2, 3, 0, time.UTC)
	logStats.EndTime = time.Date(2017, time.January, 1, 1, 2, 4, 0, time.UTC)
	params := map[string][]string{"full": {}}

	*streamlog.RedactDebugUIQueries = false
	*streamlog.QueryLogFormat = "text"
	got := logStats.Format(url.Values(params))
	want := "test\t\t\t''\t''\tJan  1 01:02:03.000000\tJan  1 01:02:04.000000\t1.000000\t0.000000\t0.000000\t0.000000\t\t\"sql1\"\tmap[intVal:type:INT64 value:\"1\" ]\t0\t0\t\"\"\t\n"
	if got != want {
		t.Errorf("logstats format: got:\n%q\nwant:\n%q\n", got, want)
	}

	*streamlog.RedactDebugUIQueries = true
	*streamlog.QueryLogFormat = "text"
	got = logStats.Format(url.Values(params))
	want = "test\t\t\t''\t''\tJan  1 01:02:03.000000\tJan  1 01:02:04.000000\t1.000000\t0.000000\t0.000000\t0.000000\t\t\"sql1\"\t\"[REDACTED]\"\t0\t0\t\"\"\t\n"
	if got != want {
		t.Errorf("logstats format: got:\n%q\nwant:\n%q\n", got, want)
	}

	*streamlog.RedactDebugUIQueries = false
	*streamlog.QueryLogFormat = "json"
	got = logStats.Format(url.Values(params))
	var parsed map[string]interface{}
	err := json.Unmarshal([]byte(got), &parsed)
	if err != nil {
		t.Errorf("logstats format: error unmarshaling json: %v -- got:\n%v", err, got)
	}
	formatted, err := json.MarshalIndent(parsed, "", "    ")
	if err != nil {
		t.Errorf("logstats format: error marshaling json: %v -- got:\n%v", err, got)
	}
	want = "{\n    \"BindVars\": {\n        \"intVal\": {\n            \"type\": \"INT64\",\n            \"value\": 1\n        }\n    },\n    \"CommitTime\": 0,\n    \"Effective Caller\": \"\",\n    \"End\": \"Jan  1 01:02:04.000000\",\n    \"Error\": \"\",\n    \"ExecuteTime\": 0,\n    \"ImmediateCaller\": \"\",\n    \"Method\": \"test\",\n    \"PlanTime\": 0,\n    \"RemoteAddr\": \"\",\n    \"RowsAffected\": 0,\n    \"SQL\": \"sql1\",\n    \"ShardQueries\": 0,\n    \"Start\": \"Jan  1 01:02:03.000000\",\n    \"StmtType\": \"\",\n    \"TotalTime\": 1,\n    \"Username\": \"\"\n}"
	if string(formatted) != want {
		t.Errorf("logstats format: got:\n%q\nwant:\n%v\n", string(formatted), want)
	}

	*streamlog.RedactDebugUIQueries = true
	*streamlog.QueryLogFormat = "json"
	got = logStats.Format(url.Values(params))
	err = json.Unmarshal([]byte(got), &parsed)
	if err != nil {
		t.Errorf("logstats format: error unmarshaling json: %v -- got:\n%v", err, got)
	}
	formatted, err = json.MarshalIndent(parsed, "", "    ")
	if err != nil {
		t.Errorf("logstats format: error marshaling json: %v -- got:\n%v", err, got)
	}
	want = "{\n    \"BindVars\": \"[REDACTED]\",\n    \"CommitTime\": 0,\n    \"Effective Caller\": \"\",\n    \"End\": \"Jan  1 01:02:04.000000\",\n    \"Error\": \"\",\n    \"ExecuteTime\": 0,\n    \"ImmediateCaller\": \"\",\n    \"Method\": \"test\",\n    \"PlanTime\": 0,\n    \"RemoteAddr\": \"\",\n    \"RowsAffected\": 0,\n    \"SQL\": \"sql1\",\n    \"ShardQueries\": 0,\n    \"Start\": \"Jan  1 01:02:03.000000\",\n    \"StmtType\": \"\",\n    \"TotalTime\": 1,\n    \"Username\": \"\"\n}"
	if string(formatted) != want {
		t.Errorf("logstats format: got:\n%q\nwant:\n%v\n", string(formatted), want)
	}

	*streamlog.RedactDebugUIQueries = false

	// Make sure formatting works for string bind vars. We can't do this as part of a single
	// map because the output ordering is undefined.
	logStats.BindVariables = map[string]*querypb.BindVariable{"strVal": sqltypes.StringBindVariable("abc")}

	*streamlog.QueryLogFormat = "text"
	got = logStats.Format(url.Values(params))
	want = "test\t\t\t''\t''\tJan  1 01:02:03.000000\tJan  1 01:02:04.000000\t1.000000\t0.000000\t0.000000\t0.000000\t\t\"sql1\"\tmap[strVal:type:VARCHAR value:\"abc\" ]\t0\t0\t\"\"\t\n"
	if got != want {
		t.Errorf("logstats format: got:\n%q\nwant:\n%q\n", got, want)
	}

	*streamlog.QueryLogFormat = "json"
	got = logStats.Format(url.Values(params))
	err = json.Unmarshal([]byte(got), &parsed)
	if err != nil {
		t.Errorf("logstats format: error unmarshaling json: %v -- got:\n%v", err, got)
	}
	formatted, err = json.MarshalIndent(parsed, "", "    ")
	if err != nil {
		t.Errorf("logstats format: error marshaling json: %v -- got:\n%v", err, got)
	}
	want = "{\n    \"BindVars\": {\n        \"strVal\": {\n            \"type\": \"VARCHAR\",\n            \"value\": \"abc\"\n        }\n    },\n    \"CommitTime\": 0,\n    \"Effective Caller\": \"\",\n    \"End\": \"Jan  1 01:02:04.000000\",\n    \"Error\": \"\",\n    \"ExecuteTime\": 0,\n    \"ImmediateCaller\": \"\",\n    \"Method\": \"test\",\n    \"PlanTime\": 0,\n    \"RemoteAddr\": \"\",\n    \"RowsAffected\": 0,\n    \"SQL\": \"sql1\",\n    \"ShardQueries\": 0,\n    \"Start\": \"Jan  1 01:02:03.000000\",\n    \"StmtType\": \"\",\n    \"TotalTime\": 1,\n    \"Username\": \"\"\n}"
	if string(formatted) != want {
		t.Errorf("logstats format: got:\n%q\nwant:\n%v\n", string(formatted), want)
	}

	*streamlog.QueryLogFormat = "text"
}

func TestLogStatsFormatBindVariables(t *testing.T) {
	logStats := NewLogStats(context.Background(), "test", "sql1", map[string]*querypb.BindVariable{
		"key_1": sqltypes.StringBindVariable("val_1"),
		"key_2": sqltypes.Int64BindVariable(789),
	})

	formattedStr := logStats.FmtBindVariables(true)
	if !strings.Contains(formattedStr, "key_1") ||
		!strings.Contains(formattedStr, "val_1") {
		t.Fatalf("bind variable 'key_1': 'val_1' is not formatted")
	}
	if !strings.Contains(formattedStr, "key_2") ||
		!strings.Contains(formattedStr, "789") {
		t.Fatalf("bind variable 'key_2': '789' is not formatted")
	}

	logStats.BindVariables["key_3"] = sqltypes.BytesBindVariable([]byte("val_3"))
	formattedStr = logStats.FmtBindVariables(false)
	if !strings.Contains(formattedStr, "key_1") {
		t.Fatalf("bind variable 'key_1' is not formatted")
	}
	if !strings.Contains(formattedStr, "key_2") ||
		!strings.Contains(formattedStr, "789") {
		t.Fatalf("bind variable 'key_2': '789' is not formatted")
	}
	if !strings.Contains(formattedStr, "key_3") {
		t.Fatalf("bind variable 'key_3' is not formatted")
	}
}

func TestLogStatsContextHTML(t *testing.T) {
	html := "HtmlContext"
	callInfo := &fakecallinfo.FakeCallInfo{
		Html: html,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	logStats := NewLogStats(ctx, "test", "sql1", map[string]*querypb.BindVariable{})
	if string(logStats.ContextHTML()) != html {
		t.Fatalf("expect to get html: %s, but got: %s", html, string(logStats.ContextHTML()))
	}
}

func TestLogStatsErrorStr(t *testing.T) {
	logStats := NewLogStats(context.Background(), "test", "sql1", map[string]*querypb.BindVariable{})
	if logStats.ErrorStr() != "" {
		t.Fatalf("should not get error in stats, but got: %s", logStats.ErrorStr())
	}
	errStr := "unknown error"
	logStats.Error = errors.New(errStr)
	if !strings.Contains(logStats.ErrorStr(), errStr) {
		t.Fatalf("expect string '%s' in error message, but got: %s", errStr, logStats.ErrorStr())
	}
}

func TestLogStatsRemoteAddrUsername(t *testing.T) {
	logStats := NewLogStats(context.Background(), "test", "sql1", map[string]*querypb.BindVariable{})
	addr, user := logStats.RemoteAddrUsername()
	if addr != "" {
		t.Fatalf("remote addr should be empty")
	}
	if user != "" {
		t.Fatalf("username should be empty")
	}

	remoteAddr := "1.2.3.4"
	username := "vt"
	callInfo := &fakecallinfo.FakeCallInfo{
		Remote: remoteAddr,
		User:   username,
	}
	ctx := callinfo.NewContext(context.Background(), callInfo)
	logStats = NewLogStats(ctx, "test", "sql1", map[string]*querypb.BindVariable{})
	addr, user = logStats.RemoteAddrUsername()
	if addr != remoteAddr {
		t.Fatalf("expected to get remote addr: %s, but got: %s", remoteAddr, addr)
	}
	if user != username {
		t.Fatalf("expected to get username: %s, but got: %s", username, user)
	}
}
