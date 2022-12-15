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
	"go.uber.org/zap/zaptest/observer"

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
	SetPlanetScaleLogger(&testLoggerConf)

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

	// Given
	dummyLogMessage := "testing log"
	testCases := []testCase{
		{"log info", zapcore.InfoLevel},
		{"log warn", zapcore.WarnLevel},
		{"log error", zapcore.ErrorLevel},
	}

	sink, err := SetupLoggerWithMemSink()
	if err != nil {
		t.Fatal(err)
	}

	// When
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var loggingFunc func(format string, args ...interface{})
			var expectedLevel string

			switch tc.logLevel {
			case zapcore.InfoLevel:
				{
					loggingFunc = vtlog.Infof
					expectedLevel = "info"
				}
			case zapcore.ErrorLevel:
				{
					loggingFunc = vtlog.Errorf
					expectedLevel = "error"
				}
			case zapcore.WarnLevel:
				{
					loggingFunc = vtlog.Warningf
					expectedLevel = "warn"
				}
			}

			loggingFunc(dummyLogMessage)
			output := sink.String()
			sink.Reset()

			actualLog := logMsg{}
			err := json.Unmarshal([]byte(output), &actualLog)
			if err != nil {
				t.Error(err)
			}

			// Then
			assert.Equal(t, expectedLevel, actualLog.Level)
			assert.Equal(t, dummyLogMessage, actualLog.Msg)

		})
	}
}

func TestPSLogger(t *testing.T) {
	t.Run("PlanetScale logger", func(t *testing.T) {
		// Given
		SetPlanetScaleLogger(nil)
		observedZapCore, observedLogs := observer.New(zap.InfoLevel)
		observedLoggerSugared := zap.New(observedZapCore).Sugar()
		logLevels := [3]zapcore.Level{zap.InfoLevel, zap.WarnLevel, zap.ErrorLevel}

		// When
		observedLoggerSugared.Infof("testing log")
		observedLoggerSugared.Warnf("testing log")
		observedLoggerSugared.Errorf("testing log")

		// Then
		for idx, level := range logLevels {
			expectLog := observer.LoggedEntry{
				Entry: zapcore.Entry{
					Level:   level,
					Message: "testing log",
				},
				Context: nil,
			}
			actualLog := observedLogs.All()[idx]

			assert.Equal(t, expectLog.Level, actualLog.Level)
			assert.Equal(t, expectLog.Message, actualLog.Message)

		}

	})
}
