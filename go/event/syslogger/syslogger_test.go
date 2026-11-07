//go:build !windows

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

package syslogger

import (
	"errors"
	"log/slog"
	"log/syslog"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/vt/log/logtest"
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

// TestSyslog checks that our callback works.
func TestSyslog(t *testing.T) {
	writer = &fakeWriter{}

	ev := new(TestEvent)
	event.Dispatch(ev)
	assert.True(t, ev.triggered)
}

// TestBadWriter verifies we are still triggering (to normal logs) if
// the syslog connection failed
func TestBadWriter(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	writer = nil
	wantMsg := "testing message"
	for _, tc := range []struct {
		priority syslog.Priority
		level    string
	}{
		{syslog.LOG_ALERT, "ERROR"},
		{syslog.LOG_CRIT, "ERROR"},
		{syslog.LOG_ERR, "ERROR"},
		{syslog.LOG_EMERG, "ERROR"},
		{syslog.LOG_WARNING, "WARN"},
		{syslog.LOG_INFO, "INFO"},
		{syslog.LOG_NOTICE, "INFO"},
		{syslog.LOG_DEBUG, "INFO"},
	} {
		logs.Reset()
		ev := &TestEvent{priority: tc.priority, message: wantMsg}
		event.Dispatch(ev)
		assert.True(t, ev.triggered)
		record := logs.Record(t, "syslog event")
		assert.Equal(t, tc.level, record["level"])
		assert.Equal(t, wantMsg, record["syslog_message"])
	}
}

// TestWriteError checks that we don't panic on a write error, and log it.
func TestWriteError(t *testing.T) {
	logs := logtest.Capture(t, slog.LevelInfo)
	writer = &fakeWriter{err: errors.New("forced error")}

	event.Dispatch(&TestEvent{priority: syslog.LOG_EMERG})
	record := logs.Record(t, "can't write syslog event")
	assert.Equal(t, "ERROR", record["level"])
	assert.Equal(t, "forced error", record["error"])
}

func TestInvalidSeverity(t *testing.T) {
	fw := &fakeWriter{}
	writer = fw

	event.Dispatch(&TestEvent{priority: syslog.Priority(123), message: "log me"})
	assert.NotEqual(t, "log me", fw.message)
}

func testSeverity(sev syslog.Priority, t *testing.T) {
	fw := &fakeWriter{}
	writer = fw

	event.Dispatch(&TestEvent{priority: sev, message: "log me"})
	assert.Equal(t, sev, fw.priority)
	assert.Equal(t, "log me", fw.message)
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
