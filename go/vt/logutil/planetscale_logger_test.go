/*
Copyright 2023 The Vitess Authors.

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
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	pslog "github.com/planetscale/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	vtlog "vitess.io/vitess/go/vt/log"
)

// MemorySink implements zap.Sink by writing all messages to a buffer.
// It's used to capture the logs.
type MemorySink struct {
	*bytes.Buffer
}

// Implement Close and Sync as no-ops to satisfy the interface. The Write
// method is provided by the embedded buffer.
func (s *MemorySink) Close() error { return nil }
func (s *MemorySink) Sync() error  { return nil }

func SetupLoggerWithMemSink() (sink *MemorySink, err error) {
	// Create a sink instance, and register it with zap for the "memory"
	// protocol.
	sink = &MemorySink{new(bytes.Buffer)}
	err = zap.RegisterSink("memory", func(*url.URL) (zap.Sink, error) {
		return sink, nil
	})
	if err != nil {
		return nil, err
	}

	testLoggerConf := pslog.NewPlanetScaleConfig(pslog.DetectEncoding(), pslog.InfoLevel)
	testLoggerConf.OutputPaths = []string{"memory://"}
	testLoggerConf.ErrorOutputPaths = []string{"memory://"}
	_, err = SetPlanetScaleLogger(&testLoggerConf)
	if err != nil {
		return nil, err
	}

	return
}

func TestPSLogger_Replacing_glog(t *testing.T) {
	type logMsg struct {
		Level string `json:"level"`
		Msg   string `json:"msg"`
	}

	type testCase struct {
		name     string
		logLevel zapcore.Level
	}

	dummyLogMessage := "testing log"
	testCases := []testCase{
		{"log info", pslog.InfoLevel},
		{"log warn", pslog.WarnLevel},
		{"log error", pslog.ErrorLevel},
	}

	sink, err := SetupLoggerWithMemSink()
	assert.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var loggingFunc func(format string, args ...interface{})
			var expectedLevel string

			switch tc.logLevel {
			case zapcore.InfoLevel:
				loggingFunc = vtlog.Infof
				expectedLevel = "info"
			case zapcore.ErrorLevel:
				loggingFunc = vtlog.Errorf
				expectedLevel = "error"
			case zapcore.WarnLevel:
				loggingFunc = vtlog.Warningf
				expectedLevel = "warn"
			}

			loggingFunc(dummyLogMessage)

			// Unmarshal the captured log. This means we're getting a struct log.
			actualLog := logMsg{}
			err = json.Unmarshal(sink.Bytes(), &actualLog)
			assert.NoError(t, err)
			// Reset the sink so that it'll contain one log per test case.
			sink.Reset()

			assert.Equal(t, expectedLevel, actualLog.Level)
			assert.Equal(t, dummyLogMessage, actualLog.Msg)

		})
	}
}
