/*
Copyright 2017 Google Inc.

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
	"log/syslog"
	"os"

	"vitess.io/vitess/go/event"
	"vitess.io/vitess/go/vt/log"
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
