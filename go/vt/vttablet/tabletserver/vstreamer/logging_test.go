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

package vstreamer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
)

// logCapture collects JSON log records. Streams log from their own
// goroutines, so writes are serialized.
type logCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// captureLogs logs JSON records at level and above to a logCapture for the
// rest of the test.
func captureLogs(t *testing.T, level slog.Level) *logCapture {
	t.Helper()
	c := &logCapture{}
	saved := log.SwapLogger(slog.New(slog.NewJSONHandler(c, &slog.HandlerOptions{Level: level})))
	t.Cleanup(func() { log.SwapLogger(saved) })
	return c
}

func (c *logCapture) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *logCapture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

// records decodes every record logged so far.
func (c *logCapture) records(t *testing.T) []map[string]any {
	t.Helper()
	var records []map[string]any
	dec := json.NewDecoder(strings.NewReader(c.String()))
	for dec.More() {
		var record map[string]any
		require.NoError(t, dec.Decode(&record))
		records = append(records, record)
	}
	return records
}

// record returns the single record logged so far with msg.
func (c *logCapture) record(t *testing.T, msg string) map[string]any {
	t.Helper()
	var found []map[string]any
	for _, record := range c.records(t) {
		if record["msg"] == msg {
			found = append(found, record)
		}
	}
	require.Len(t, found, 1, "expected a single %q log record", msg)
	return found[0]
}

// TestLazyString checks that lazyString is built only when a log.V(2) record
// is written, and that it formats as its value for slog and for fmt.
func TestLazyString(t *testing.T) {
	calls := 0
	value := lazyString(func() string {
		calls++
		return "expensive"
	})

	assert.Equal(t, "expensive", value.LogValue().String())
	assert.Equal(t, "expensive", value.String())
	// The glog backend formats attributes with fmt.
	assert.Equal(t, "value=expensive", fmt.Sprint(slog.Any("value", value)))

	t.Run("V(2) off", func(t *testing.T) {
		logs := captureLogs(t, slog.LevelInfo)
		calls = 0
		log.V(2).Info("lazy value", slog.Any("value", value))
		assert.Zero(t, calls)
		assert.Empty(t, logs.records(t))
	})

	t.Run("V(2) on", func(t *testing.T) {
		logs := captureLogs(t, slog.LevelDebug)
		calls = 0
		log.V(2).Info("lazy value", slog.Any("value", value))
		assert.Equal(t, 1, calls)
		record := logs.record(t, "lazy value")
		assert.Equal(t, "INFO", record["level"])
		assert.Equal(t, "expensive", record["value"])
	})
}

// TestStreamRowsLogsQueries checks that the filter query and the query sent to
// MySQL are logged as attributes of distinct messages.
func TestStreamRowsLogsQueries(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
		"insert into t1 values (1, 'aaa'), (2, 'bbb')",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})

	logs := captureLogs(t, slog.LevelInfo)
	lastpk := []sqltypes.Value{sqltypes.NewInt64(1)}
	err := engine.StreamRows(t.Context(), "select * from t1", lastpk, func(*binlogdatapb.VStreamRowsResponse) error {
		return nil
	}, nil)
	require.NoError(t, err)

	record := logs.record(t, "Streaming rows for query")
	assert.Equal(t, "select * from t1", record["query"])
	assert.Equal(t, "[INT64(1)]", record["last_pk"])

	record = logs.record(t, "row streamer executing query")
	assert.Equal(t, "select /*+ MAX_EXECUTION_TIME(3600000) */ id, val from t1 force index (`PRIMARY`) where (id > 1) order by id", record["query"])
}

// TestStreamRowsBuildPlanErrorLogsCode checks that a table plan failure logs
// the vterrors code of the error.
func TestStreamRowsBuildPlanErrorLogsCode(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table t1(id int, val varbinary(128), primary key(id))",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})

	logs := captureLogs(t, slog.LevelInfo)
	expectStreamError(t, "select id, missing from t1", "column missing not found in table t1")

	record := logs.record(t, "Failed to build table plan in row streamer")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "t1", record["table"])
	assert.Equal(t, "FAILED_PRECONDITION", record["error_code"])
	assert.Equal(t, "column missing not found in table t1", record["error"])
}

// TestVStreamerThrottledLog checks the attributes of the vstreamer throttle log.
func TestVStreamerThrottledLog(t *testing.T) {
	logs := captureLogs(t, slog.LevelInfo)
	ctx, cancel := context.WithCancel(t.Context())
	filter := &binlogdatapb.Filter{
		WorkflowName: "wf1",
		Rules:        []*binlogdatapb.Rule{{Match: "/.*/"}},
	}
	wg, evs := startFullyThrottledStream(ctx, t, filter, "", nil)
	defer wg.Wait()
	defer cancel()
	go func() {
		for range evs {
		}
	}()

	require.Eventually(t, func() bool {
		return strings.Contains(logs.String(), `"msg":"vstreamer throttled"`)
	}, 30*time.Second, 50*time.Millisecond)

	record := logs.record(t, "vstreamer throttled")
	assert.Equal(t, "INFO", record["level"])
	assert.Equal(t, engine.GetTabletInfo(), record["logger"])
	assert.Equal(t, "wf1", record["workflow"])
	// The test throttler client returns an empty check result for this app.
	assert.Contains(t, record, "reason")
}

// TestCopyLogs checks the attributes of the catchup and table copy logs of a
// stream that resumes a table copy from a position.
func TestCopyLogs(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	execStatements(t, []string{
		"create table t1(id11 int, id12 int, primary key(id11))",
		"insert into t1 values (1, 10), (2, 20)",
	})
	defer execStatements(t, []string{
		"drop table t1",
	})
	// The copy phase only copies tables the schema engine knows about.
	require.NoError(t, engine.se.Reload(t.Context()))

	logs := captureLogs(t, slog.LevelInfo)
	ctx, cancel := context.WithCancel(t.Context())
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{Match: "t1", Filter: "select * from t1"}},
	}
	startPos := primaryPosition(t)
	wg, evs := startStream(ctx, t, filter, startPos, []*binlogdatapb.TableLastPK{getTablePK("t1", 1)})
	defer wg.Wait()
	defer cancel()
	go func() {
		for range evs {
		}
	}()

	if !assert.Eventually(t, func() bool {
		s := logs.String()
		return strings.Contains(s, `"msg":"catchup vs.stream returned"`) && strings.Contains(s, `"msg":"Copy finished"`)
	}, 30*time.Second, 50*time.Millisecond) {
		require.FailNow(t, "copy did not finish", "logs:\n%s", logs.String())
	}

	flavor, _, ok := strings.Cut(startPos, "/")
	require.True(t, ok, "position %q has no flavor prefix", startPos)
	record := logs.record(t, "catchup vs.stream returned")
	assert.Regexp(t, "^"+regexp.QuoteMeta(flavor)+"/.", record["current_position"])

	record = logs.record(t, "Starting copyTable")
	assert.Equal(t, "t1", record["table"])
	assert.Equal(t, "select * from t1", record["filter"])
	assert.Equal(t, "[INT32(0)]", record["last_pk"])

	record = logs.record(t, "Copy finished")
	assert.Equal(t, "t1", record["table"])
	assert.Contains(t, record["last_pk"], "INT32(2)")
}
