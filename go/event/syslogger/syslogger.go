// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package syslogger uses the event package to listen for any event that
implements the Syslogger interface. The listener calls the Syslog method on the
event, passing a syslog.Writer that can be used to send messages of any severity.

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
	func (ev *MyEvent) Syslog(w *syslog.Writer) {
		msg := fmt.Sprintf("event: %v, %v", ev.field1, ev.field2)
		w.Warning(msg)
	}
	var _ syslogger.Syslogger = (*MyEvent)(nil) // compile-time interface check

The compile-time interface check is optional but recommended because usually
there is no other static conversion in these cases. See:

http://golang.org/doc/effective_go.html#blank_implements
*/
package syslogger

import (
	"log/syslog"
	"os"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/event"
)

type Syslogger interface {
	Syslog(w *syslog.Writer)
}

func listener(ev Syslogger) {
	w, err := syslog.New(syslog.LOG_INFO|syslog.LOG_USER, os.Args[0])

	if err != nil {
		log.Errorf("can't connect to syslog to send event: %v", ev)
	}
	defer w.Close()

	ev.Syslog(w)
}

func init() {
	event.AddListener(listener)
}
