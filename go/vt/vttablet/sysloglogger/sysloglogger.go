// Package sysloglogger implements an optional plugin that logs all queries to syslog.
package sysloglogger

import (
	"flag"
	"log/syslog"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/servenv"
	"github.com/youtube/vitess/go/vt/vttablet/tabletserver/tabletenv"
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
		if err := writer.Info(stats.Format(formatParams)); err != nil {
			log.Errorf("Error writing to syslog: %v", err)
			continue
		}
	}
}
