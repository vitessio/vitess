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

// Package sysloglogger implements an optional plugin that logs all queries to syslog.
package sysloglogger

import (
	"bytes"
	"flag"
	"log/syslog"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// syslogWriter is an interface that wraps syslog.Writer, so it can be mocked in unit tests.
type syslogWriter interface {
	Info(string) error
	Close() error
}

// writer holds a persistent connection to the syslog daemon (or a mock when under test).
var writer syslogWriter

// ch holds the tabletserver.StatsLogger channel to which this plugin subscribes (or a mock when under test).
var ch chan interface{}

// logQueries is the vttablet startup flag that must be set for this plugin to be active.
var logQueries = flag.Bool("log_queries", false, "Enable query logging to syslog.")

func init() {
	servenv.OnRun(func() {
		if *logQueries {
			var err error
			writer, err = syslog.New(syslog.LOG_INFO, "vtquerylogger")
			if err != nil {
				log.Errorf("Query logger is unable to connect to syslog: %v", err)
				return
			}
			go run()
		}
	})
}

// Run logs queries to syslog, if the "log_queries" flag is set to true when starting vttablet.
func run() {
	log.Info("Logging queries to syslog")
	defer writer.Close()

	// ch will only be non-nil in a unit test context, when a mock has been populated
	if ch == nil {
		ch = tabletenv.StatsLogger.Subscribe("gwslog")
		defer tabletenv.StatsLogger.Unsubscribe(ch)
	}

	formatParams := map[string][]string{"full": {}}
	for out := range ch {
		stats, ok := out.(*tabletenv.LogStats)
		if !ok {
			log.Errorf("Unexpected value in query logs: %#v (expecting value of type %T)", out, &tabletenv.LogStats{})
			continue
		}
		var b bytes.Buffer
		if err := stats.Logf(&b, formatParams); err != nil {
			log.Errorf("Error formatting logStats: %v", err)
			continue
		}
		if err := writer.Info(b.String()); err != nil {
			log.Errorf("Error writing to syslog: %v", err)
			continue
		}
	}
}
