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

package logutil

import (
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/log/logtest"
)

// logDepth logs through the Depth methods with depth 1, so each line is
// attributed to logDepth's caller.
func logDepth(l Logger) {
	l.InfoDepth(1, "depth info", slog.Int("n", 1))
	l.WarningDepth(1, "depth warning", slog.Int("n", 2))
	l.ErrorDepth(1, "depth error", slog.Int("n", 3))
}

// logEveryMethod logs a line through each Logger method and returns the line
// number of the Info call. The other calls follow it, one per line.
func logEveryMethod(l Logger) int {
	_, _, line, _ := runtime.Caller(0)
	l.Info("intermediate source selected", slog.String("tablet", "zone1-0000000101"))
	l.Warn("tablet can't be repointed", slog.String("tablet", "zone1-0000000102"), slog.Any("error", errors.New("errant GTIDs")))
	l.Error("some replicas failed to reparent", slog.String("tablet", "zone1-0000000101"), slog.Any("error", errors.New("replica 102 failed")))
	logDepth(l)
	l.Infof("printf info %d", 1)
	l.Warningf("printf warning %d", 2)
	l.Errorf("printf error %d", 3)
	l.Printf("printf %d", 4)
	return line + 1
}

func TestStructuredLogger(t *testing.T) {
	type want struct {
		offset int
		record map[string]any
	}
	all := []want{
		{0, map[string]any{"msg": "test: intermediate source selected", "level": "INFO", "reparent": "ERS", "tablet": "zone1-0000000101"}},
		{1, map[string]any{"msg": "test: tablet can't be repointed", "level": "WARN", "reparent": "ERS", "tablet": "zone1-0000000102", "error": "errant GTIDs"}},
		{2, map[string]any{"msg": "test: some replicas failed to reparent", "level": "ERROR", "reparent": "ERS", "tablet": "zone1-0000000101", "error": "replica 102 failed"}},
		{3, map[string]any{"msg": "test: depth info", "level": "INFO", "reparent": "ERS", "n": float64(1)}},
		{3, map[string]any{"msg": "test: depth warning", "level": "WARN", "reparent": "ERS", "n": float64(2)}},
		{3, map[string]any{"msg": "test: depth error", "level": "ERROR", "reparent": "ERS", "n": float64(3)}},
		{4, map[string]any{"msg": "test: printf info 1", "level": "INFO", "reparent": "ERS"}},
		{5, map[string]any{"msg": "test: printf warning 2", "level": "WARN", "reparent": "ERS"}},
		{6, map[string]any{"msg": "test: printf error 3", "level": "ERROR", "reparent": "ERS"}},
		{7, map[string]any{"msg": "test: printf 4", "level": "INFO", "reparent": "ERS"}},
	}
	for _, tt := range []struct {
		level  slog.Level
		logged []string
	}{
		{level: slog.LevelInfo, logged: []string{"INFO", "WARN", "ERROR"}},
		{level: slog.LevelWarn, logged: []string{"WARN", "ERROR"}},
		{level: slog.LevelError, logged: []string{"ERROR"}},
	} {
		t.Run(tt.level.String(), func(t *testing.T) {
			logs := &logtest.Logs{}
			saved := log.SwapLogger(slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{AddSource: true})))
			t.Cleanup(func() { log.SwapLogger(saved) })

			// With a TeeLogger, the CallbackLogger gets the same events as
			// when it is used alone, whatever lines the StructuredLogger drops.
			tee := NewMemoryLogger()
			line := logEveryMethod(NewTeeLogger(NewStructuredLogger(log.NewPrefixedLogger("test"), tt.level, slog.String("reparent", "ERS")), tee))
			alone := NewMemoryLogger()
			aloneLine := logEveryMethod(alone)
			teeEvents, aloneEvents := tee.LogEvents(), alone.LogEvents()
			if assert.Len(t, teeEvents, len(aloneEvents)) {
				for i := range teeEvents {
					assert.Equal(t, aloneEvents[i].Level, teeEvents[i].Level)
					assert.Equal(t, aloneEvents[i].Value, teeEvents[i].Value)
					// TeeLogger passes no depth for Printf, so its lines are
					// attributed to TeeLogger.Printf.
					if i < len(teeEvents)-1 {
						assert.Equal(t, aloneEvents[i].File, teeEvents[i].File)
						assert.Equal(t, aloneEvents[i].Line-int64(aloneLine), teeEvents[i].Line-int64(line))
					}
				}
			}

			var wantRecords []want
			for _, w := range all {
				if slices.Contains(tt.logged, w.record["level"].(string)) {
					wantRecords = append(wantRecords, w)
				}
			}
			checkRecords := func(line int, viaTee bool) {
				records := logs.Records(t)
				if !assert.Len(t, records, len(wantRecords)) {
					return
				}
				for i, record := range records {
					if !viaTee || record["msg"] != "test: printf 4" {
						source, _ := record["source"].(map[string]any)
						assert.Equal(t, "structured_logger_test.go", filepath.Base(fmt.Sprint(source["file"])))
						assert.EqualValues(t, line+wantRecords[i].offset, source["line"], record["msg"])
					}
					record = maps.Clone(record)
					delete(record, "time")
					delete(record, "source")
					assert.Equal(t, wantRecords[i].record, record)
				}
			}
			checkRecords(line, true)

			// Alone, the StructuredLogger logs the same lines.
			logs.Reset()
			checkRecords(logEveryMethod(NewStructuredLogger(log.NewPrefixedLogger("test"), tt.level, slog.String("reparent", "ERS"))), false)
		})
	}
}
