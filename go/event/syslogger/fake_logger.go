/*
Copyright 2023 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
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
	logs          []loggerMsg
	savedInfof    func(format string, args ...any)
	savedWarningf func(format string, args ...any)
	savedErrorf   func(format string, args ...any)
}

func NewTestLogger() *testLogger {
	tl := &testLogger{
		savedInfof:    log.Infof,
		savedWarningf: log.Warningf,
		savedErrorf:   log.Errorf,
	}
	log.Infof = tl.recordInfof
	log.Warningf = tl.recordWarningf
	log.Errorf = tl.recordErrorf
	return tl
}

func (tl *testLogger) Close() {
	log.Infof = tl.savedInfof
	log.Warningf = tl.savedWarningf
	log.Errorf = tl.savedErrorf
}

func (tl *testLogger) recordInfof(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	tl.logs = append(tl.logs, loggerMsg{msg, "INFO"})
	tl.savedInfof(msg)
}

func (tl *testLogger) recordWarningf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	tl.logs = append(tl.logs, loggerMsg{msg, "WARNING"})
	tl.savedWarningf(msg)
}

func (tl *testLogger) recordErrorf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	tl.logs = append(tl.logs, loggerMsg{msg, "ERROR"})
	tl.savedErrorf(msg)
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
