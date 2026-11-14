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

package logutil

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/log"
)

func TestConsoleLogger(t *testing.T) {
	testConsoleLogger(t, false)
}

func TestTeeConsoleLogger(t *testing.T) {
	testConsoleLogger(t, true)
}

func testConsoleLogger(t *testing.T, tee bool) {
	originalInfoDepth := log.InfoDepth
	originalWarnDepth := log.WarnDepth
	originalErrorDepth := log.ErrorDepth

	t.Cleanup(func() {
		log.InfoDepth = originalInfoDepth
		log.WarnDepth = originalWarnDepth
		log.ErrorDepth = originalErrorDepth
	})

	var messages []string

	log.InfoDepth = func(_ int, msg string, _ ...slog.Attr) {
		messages = append(messages, msg)
	}
	log.WarnDepth = func(_ int, msg string, _ ...slog.Attr) {
		messages = append(messages, msg)
	}
	log.ErrorDepth = func(_ int, msg string, _ ...slog.Attr) {
		messages = append(messages, msg)
	}

	var logger Logger
	if tee {
		logger = NewTeeLogger(NewConsoleLogger(), NewMemoryLogger())
	} else {
		logger = NewConsoleLogger()
	}

	logger.Infof("info %v %v", 1, tee)
	logger.Warningf("warning %v %v", 2, tee)
	logger.Errorf("error %v %v", 3, tee)

	require.Len(t, messages, 3)
	require.Equal(t, fmt.Sprintf("info 1 %v", tee), messages[0])
	require.Equal(t, fmt.Sprintf("warning 2 %v", tee), messages[1])
	require.Equal(t, fmt.Sprintf("error 3 %v", tee), messages[2])
}

// TestConsoleLoggerAttrs checks that Info, Warn and Error log a record with
// the constant message, its attributes, its level and the caller's file and
// line.
func TestConsoleLoggerAttrs(t *testing.T) {
	logs := captureWithSource(t)
	cl := NewConsoleLogger()

	_, file, line, _ := runtime.Caller(0)
	cl.Info("Restore: found latest backup", slog.String("backup_name", "b1"))
	cl.Warn("Restore: skipping backup", slog.String("backup_name", "b2"))
	cl.Error("failed to close decompressor", slog.String("file", "f"))

	records := logs.records(t)
	require.Len(t, records, 3)
	want := []struct{ msg, level, key, value string }{
		{"Restore: found latest backup", "INFO", "backup_name", "b1"},
		{"Restore: skipping backup", "WARN", "backup_name", "b2"},
		{"failed to close decompressor", "ERROR", "file", "f"},
	}
	for i, w := range want {
		assert.Equal(t, w.msg, records[i]["msg"])
		assert.Equal(t, w.level, records[i]["level"])
		assert.Equal(t, w.value, records[i][w.key])
		assertSource(t, records[i], file, line+1+i)
	}
}

// sourceLogs collects JSON records with their source location.
type sourceLogs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *sourceLogs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *sourceLogs) records(t *testing.T) []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	var records []map[string]any
	dec := json.NewDecoder(bytes.NewReader(l.buf.Bytes()))
	for dec.More() {
		var r map[string]any
		require.NoError(t, dec.Decode(&r))
		records = append(records, r)
	}
	return records
}

// captureWithSource logs JSON records, with their source, to a sourceLogs
// for the rest of the test.
func captureWithSource(t *testing.T) *sourceLogs {
	l := &sourceLogs{}
	saved := log.SwapLogger(slog.New(slog.NewJSONHandler(l, &slog.HandlerOptions{AddSource: true, Level: slog.LevelDebug})))
	t.Cleanup(func() { log.SwapLogger(saved) })
	return l
}

// assertSource checks that record r was logged from file:line, as reported
// by runtime.Caller.
func assertSource(t *testing.T, r map[string]any, file string, line int) {
	t.Helper()
	src, ok := r["source"].(map[string]any)
	require.True(t, ok, "record has no source: %v", r)
	assert.Equal(t, file, src["file"])
	assert.EqualValues(t, line, src["line"])
}
