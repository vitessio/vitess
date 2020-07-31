/*
Copyright 2019 The Vitess Authors.

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
	"log/syslog"
	"strings"
	"testing"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/vt/log"
)

type TestEvent struct {
	triggered bool
	priority  syslog.Priority
	message   string
}

func (ev *TestEvent) Syslog() (syslog.Priority, string) {
	ev.triggered = true
	return ev.priority, ev.message
}

var _ Syslogger = (*TestEvent)(nil) // compile-time interface check

type fakeWriter struct {
	priority syslog.Priority
	message  string
	err      error // if non-nil, force an error to be returned
}

func (fw *fakeWriter) write(pri syslog.Priority, msg string) error {
	if fw.err != nil {
		return fw.err
	}

	fw.priority = pri
	fw.message = msg
	return nil
}
func (fw *fakeWriter) Alert(msg string) error   { return fw.write(syslog.LOG_ALERT, msg) }
func (fw *fakeWriter) Crit(msg string) error    { return fw.write(syslog.LOG_CRIT, msg) }
func (fw *fakeWriter) Debug(msg string) error   { return fw.write(syslog.LOG_DEBUG, msg) }
func (fw *fakeWriter) Emerg(msg string) error   { return fw.write(syslog.LOG_EMERG, msg) }
func (fw *fakeWriter) Err(msg string) error     { return fw.write(syslog.LOG_ERR, msg) }
func (fw *fakeWriter) Info(msg string) error    { return fw.write(syslog.LOG_INFO, msg) }
func (fw *fakeWriter) Notice(msg string) error  { return fw.write(syslog.LOG_NOTICE, msg) }
func (fw *fakeWriter) Warning(msg string) error { return fw.write(syslog.LOG_WARNING, msg) }

type loggerMsg struct {
	msg   string
	level string
}
type testLogger struct {
	logs          []loggerMsg
	savedInfof    func(format string, args ...interface{})
	savedWarningf func(format string, args ...interface{})
	savedErrorf   func(format string, args ...interface{})
}

func newTestLogger() *testLogger {
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

func (tl *testLogger) recordInfof(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	tl.logs = append(tl.logs, loggerMsg{msg, "INFO"})
	tl.savedInfof(msg)
}

func (tl *testLogger) recordWarningf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	tl.logs = append(tl.logs, loggerMsg{msg, "WARNING"})
	tl.savedWarningf(msg)
}

func (tl *testLogger) recordErrorf(format string, args ...interface{}) {
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

// TestSyslog checks that our callback works.
func TestSyslog(t *testing.T) {
	writer = &fakeWriter{}

	ev := new(TestEvent)
	event.Dispatch(ev)

	if !ev.triggered {
		t.Errorf("Syslog() was not called on event that implements Syslogger")
	}
}

// TestBadWriter verifies we are still triggering (to normal logs) if
// the syslog connection failed
func TestBadWriter(t *testing.T) {
	tl := newTestLogger()
	defer tl.Close()

	writer = nil
	wantMsg := "testing message"
	wantLevel := "ERROR"
	ev := &TestEvent{priority: syslog.LOG_ALERT, message: wantMsg}
	event.Dispatch(ev)
	if !strings.Contains(tl.getLog().msg, wantMsg) {
		t.Errorf("error log msg [%s], want msg [%s]", tl.getLog().msg, wantMsg)
	}
	if !strings.Contains(tl.getLog().level, wantLevel) {
		t.Errorf("error log level [%s], want level [%s]", tl.getLog().level, wantLevel)
	}
	ev = &TestEvent{priority: syslog.LOG_CRIT, message: wantMsg}
	event.Dispatch(ev)
	if !strings.Contains(tl.getLog().level, wantLevel) {
		t.Errorf("error log level [%s], want level [%s]", tl.getLog().level, wantLevel)
	}
	ev = &TestEvent{priority: syslog.LOG_ERR, message: wantMsg}
	event.Dispatch(ev)
	if !strings.Contains(tl.getLog().level, wantLevel) {
		t.Errorf("error log level [%s], want level [%s]", tl.getLog().level, wantLevel)
	}

	wantLevel = "WARNING"
	ev = &TestEvent{priority: syslog.LOG_WARNING, message: wantMsg}
	event.Dispatch(ev)
	if !strings.Contains(tl.getLog().level, wantLevel) {
		t.Errorf("error log level [%s], want level [%s]", tl.getLog().level, wantLevel)
	}

	wantLevel = "INFO"
	ev = &TestEvent{priority: syslog.LOG_INFO, message: wantMsg}
	event.Dispatch(ev)
	if !strings.Contains(tl.getLog().level, wantLevel) {
		t.Errorf("error log level [%s], want level [%s]", tl.getLog().level, wantLevel)
	}
	ev = &TestEvent{priority: syslog.LOG_NOTICE, message: wantMsg}
	event.Dispatch(ev)
	if !strings.Contains(tl.getLog().level, wantLevel) {
		t.Errorf("error log level [%s], want level [%s]", tl.getLog().level, wantLevel)
	}
	ev = &TestEvent{priority: syslog.LOG_DEBUG, message: wantMsg}
	event.Dispatch(ev)
	if !strings.Contains(tl.getLog().level, wantLevel) {
		t.Errorf("error log level [%s], want level [%s]", tl.getLog().level, wantLevel)
	}

	if !ev.triggered {
		t.Errorf("passed nil writer to client")
	}
}

// TestWriteError checks that we don't panic on a write error.
func TestWriteError(t *testing.T) {
	writer = &fakeWriter{err: fmt.Errorf("forced error")}

	event.Dispatch(&TestEvent{priority: syslog.LOG_EMERG})
}

func TestInvalidSeverity(t *testing.T) {
	fw := &fakeWriter{}
	writer = fw

	event.Dispatch(&TestEvent{priority: syslog.Priority(123), message: "log me"})

	if fw.message == "log me" {
		t.Errorf("message was logged despite invalid severity")
	}
}

func testSeverity(sev syslog.Priority, t *testing.T) {
	fw := &fakeWriter{}
	writer = fw

	event.Dispatch(&TestEvent{priority: sev, message: "log me"})

	if fw.priority != sev {
		t.Errorf("wrong priority: got %v, want %v", fw.priority, sev)
	}
	if fw.message != "log me" {
		t.Errorf(`wrong message: got "%v", want "%v"`, fw.message, "log me")
	}
}

func TestEmerg(t *testing.T) {
	testSeverity(syslog.LOG_EMERG, t)
}

func TestAlert(t *testing.T) {
	testSeverity(syslog.LOG_ALERT, t)
}

func TestCrit(t *testing.T) {
	testSeverity(syslog.LOG_CRIT, t)
}

func TestErr(t *testing.T) {
	testSeverity(syslog.LOG_ERR, t)
}

func TestWarning(t *testing.T) {
	testSeverity(syslog.LOG_WARNING, t)
}

func TestNotice(t *testing.T) {
	testSeverity(syslog.LOG_NOTICE, t)
}

func TestInfo(t *testing.T) {
	testSeverity(syslog.LOG_INFO, t)
}

func TestDebug(t *testing.T) {
	testSeverity(syslog.LOG_DEBUG, t)
}
