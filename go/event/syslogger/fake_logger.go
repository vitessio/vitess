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
	"fmt"

	"vitess.io/vitess/go/vt/log"
)

type loggerMsg struct {
	msg   string
	level string
}
type testLogger struct {
	logs            []loggerMsg
	savedInfoDepth  func(depth int, args ...any)
	savedWarnDepth  func(depth int, args ...any)
	savedErrorDepth func(depth int, args ...any)
}

func NewTestLogger() *testLogger {
	tl := &testLogger{
		savedInfoDepth:  log.InfoDepth,
		savedWarnDepth:  log.WarningDepth,
		savedErrorDepth: log.ErrorDepth,
	}
	log.InfoDepth = tl.recordInfo
	log.WarningDepth = tl.recordWarn
	log.ErrorDepth = tl.recordError
	return tl
}

func (tl *testLogger) Close() {
	log.InfoDepth = tl.savedInfoDepth
	log.WarningDepth = tl.savedWarnDepth
	log.ErrorDepth = tl.savedErrorDepth
}

func (tl *testLogger) recordInfo(depth int, args ...any) {
	msg := fmt.Sprint(args...)
	tl.logs = append(tl.logs, loggerMsg{msg, "INFO"})
	tl.savedInfoDepth(depth, msg)
}

func (tl *testLogger) recordWarn(depth int, args ...any) {
	msg := fmt.Sprint(args...)
	tl.logs = append(tl.logs, loggerMsg{msg, "WARNING"})
	tl.savedWarnDepth(depth, msg)
}

func (tl *testLogger) recordError(depth int, args ...any) {
	msg := fmt.Sprint(args...)
	tl.logs = append(tl.logs, loggerMsg{msg, "ERROR"})
	tl.savedErrorDepth(depth, msg)
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
