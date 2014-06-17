package syslogger

import (
	"log/syslog"
	"testing"

	"github.com/youtube/vitess/go/event"
)

type TestEvent struct {
	triggered bool
}

func (ev *TestEvent) Syslog(w *syslog.Writer) {
	ev.triggered = true
}

var _ Syslogger = (*TestEvent)(nil) // compile-time interface check

func TestSyslog(t *testing.T) {
	ev := new(TestEvent)
	event.Dispatch(ev)

	if !ev.triggered {
		t.Errorf("Syslog() was not called on event that implements Syslogger")
	}
}
