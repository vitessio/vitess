// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package syslogger uses the event package to listen for any event that
implements the Syslogger interface. The listener calls the Syslog method on the
event, which should return a severity and a message.

The tag for messages sent to syslog will be the program name (os.Args[0]),
and the facility number will be 1 (user-level).

For example, to declare that your event type MyEvent should be sent to syslog,
implement the Syslog() method to define how the message should be formatted and
which severity it should have (see package "log/syslog" for details).

	import (
		"fmt"
		"log/syslog"
		"syslogger"
	)

	type MyEvent struct {
		field1, field2 string
	}

	func (ev *MyEvent) Syslog() (syslog.Priority, string) {
		return syslog.LOG_INFO, fmt.Sprintf("event: %v, %v", ev.field1, ev.field2)
	}
	var _ syslogger.Syslogger = (*MyEvent)(nil) // compile-time interface check

The compile-time interface check is optional but recommended because usually
there is no other static conversion in these cases. See:

http://golang.org/doc/effective_go.html#blank_implements
*/
package syslogger

import (
	"fmt"
	"log/syslog"
	"os"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/event"
)

// Syslogger is the interface that events should implement if they want to be
// dispatched to this package.
type Syslogger interface {
	// Syslog should return a severity (not a facility) and a message.
	Syslog() (syslog.Priority, string)
}

// syslogWriter is an interface that wraps syslog.Writer so it can be faked.
type syslogWriter interface {
	Alert(string) error
	Crit(string) error
	Debug(string) error
	Emerg(string) error
	Err(string) error
	Info(string) error
	Notice(string) error
	Warning(string) error
}

// writer holds a persistent connection to the syslog daemon
var writer syslogWriter

func listener(ev Syslogger) {
	if writer == nil {
		log.Errorf("no connection, dropping syslog event: %#v", ev)
		return
	}

	// Ask the event to convert itself to a syslog message.
	sev, msg := ev.Syslog()

	// Call the corresponding Writer function.
	var err error
	switch sev {
	case syslog.LOG_EMERG:
		err = writer.Emerg(msg)
	case syslog.LOG_ALERT:
		err = writer.Alert(msg)
	case syslog.LOG_CRIT:
		err = writer.Crit(msg)
	case syslog.LOG_ERR:
		err = writer.Err(msg)
	case syslog.LOG_WARNING:
		err = writer.Warning(msg)
	case syslog.LOG_NOTICE:
		err = writer.Notice(msg)
	case syslog.LOG_INFO:
		err = writer.Info(msg)
	case syslog.LOG_DEBUG:
		err = writer.Debug(msg)
	default:
		err = fmt.Errorf("invalid syslog severity: %v", sev)
	}
	if err != nil {
		log.Errorf("can't write syslog event: %v", err)
	}
}

func init() {
	var err error
	writer, err = syslog.New(syslog.LOG_INFO|syslog.LOG_USER, os.Args[0])
	if err != nil {
		log.Errorf("can't connect to syslog")
		writer = nil
	}

	event.AddListener(listener)
}
