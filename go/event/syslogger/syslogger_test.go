package syslogger

import (
	"fmt"
	"log/syslog"
	"testing"

	"github.com/youtube/vitess/go/event"
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

	if !ev.triggered {
		t.Errorf("Syslog() was not called on event that implements Syslogger")
	}
}

// TestBadWriter checks that we don't panic when the connection fails.
func TestBadWriter(t *testing.T) {
	writer = nil

	ev := new(TestEvent)
	event.Dispatch(ev)

	if ev.triggered {
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
