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

	"vitess.io/vitess/go/vt/log"
)

type loggerMsg struct {
	msg   string
	level string
}

type testLogger struct {
	logs      []loggerMsg
	savedInfo func(msg string, attrs ...slog.Attr)
	savedWarn func(msg string, attrs ...slog.Attr)
	savedErr  func(msg string, attrs ...slog.Attr)
}

func NewTestLogger() *testLogger {
	tl := &testLogger{
		savedInfo: log.Info,
		savedWarn: log.Warn,
		savedErr:  log.Error,
	}
	log.Info = tl.recordInfo
	log.Warn = tl.recordWarn
	log.Error = tl.recordError
	return tl
}

func (tl *testLogger) Close() {
	log.Info = tl.savedInfo
	log.Warn = tl.savedWarn
	log.Error = tl.savedErr
}

func (tl *testLogger) recordInfo(msg string, attrs ...slog.Attr) {
	tl.logs = append(tl.logs, loggerMsg{msg, "INFO"})
	tl.savedInfo(msg, attrs...)
}

func (tl *testLogger) recordWarn(msg string, attrs ...slog.Attr) {
	tl.logs = append(tl.logs, loggerMsg{msg, "WARNING"})
	tl.savedWarn(msg, attrs...)
}

func (tl *testLogger) recordError(msg string, attrs ...slog.Attr) {
	tl.logs = append(tl.logs, loggerMsg{msg, "ERROR"})
	tl.savedErr(msg, attrs...)
}

func (tl *testLogger) getLog() loggerMsg {
	if len(tl.logs) > 0 {
		return tl.logs[len(tl.logs)-1]
	}
	return loggerMsg{"no logs!", "ERROR"}
}

func (tl *testLogger) GetAllLogs() []string {
	var logs []string
	for _, l := range tl.logs {
		logs = append(logs, l.level+":"+l.msg)
	}
	return logs
}
