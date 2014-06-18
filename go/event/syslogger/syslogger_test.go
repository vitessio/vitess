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

// TestSyslog checks that our callback works.
func TestSyslog(t *testing.T) {
	ev := new(TestEvent)
	event.Dispatch(ev)

	if !ev.triggered {
		t.Errorf("Syslog() was not called on event that implements Syslogger")
	}
}

// TestBadWriter checks that we don't panic or pass a nil Writer to the client
// when the connection fails.
func TestBadWriter(t *testing.T) {
	real_writer := writer
	writer = nil

	ev := new(TestEvent)
	event.Dispatch(ev)

	if ev.triggered {
		t.Errorf("passed nil writer to client")
	}

	writer = real_writer
}
