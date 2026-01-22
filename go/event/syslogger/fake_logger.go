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

package syslogger

import (
	"log/slog"
	"strings"

	"vitess.io/vitess/go/vt/log"
)

type loggerMsg struct {
	msg   string
	level string
}
type testLogger struct {
	handler *log.CaptureHandler
	restore func()
}

func NewTestLogger() *testLogger {
	handler := log.NewCaptureHandler()
	restore := log.SetLogger(slog.New(handler))

	return &testLogger{
		handler: handler,
		restore: restore,
	}
}

func (tl *testLogger) Close() {
	if tl.restore != nil {
		tl.restore()
	}
}

func (tl *testLogger) getLog() loggerMsg {
	record, ok := tl.handler.Last()
	if ok {
		return loggerMsg{
			msg:   record.Message,
			level: formatLevel(record.Level),
		}
	}
	return loggerMsg{"no logs!", "ERROR"}
}

func (tl *testLogger) GetAllLogs() []string {
	var logs []string
	for _, record := range tl.handler.Records() {
		level := formatLevel(record.Level)
		logs = append(logs, level+":"+record.Message)
	}
	return logs
}

func formatLevel(level slog.Level) string {
	if level == slog.LevelWarn {
		return "WARNING"
	}

	return strings.ToUpper(level.String())
}
