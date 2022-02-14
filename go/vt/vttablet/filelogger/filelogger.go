// Package filelogger implements an optional plugin that logs all queries to syslog.
package filelogger

import (
	"flag"

	"vitess.io/vitess/go/streamlog"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/servenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

// logQueriesToFile is the vttablet startup flag that must be set for this plugin to be active.
var logQueriesToFile = flag.String("log_queries_to_file", "", "Enable query logging to the specified file")

func init() {
	servenv.OnRun(func() {
		if *logQueriesToFile != "" {
			Init(*logQueriesToFile)
		}
	})
}

// FileLogger is an opaque interface used to control the file logging
type FileLogger interface {
	// Stop logging to the given file
	Stop()
}

type fileLogger struct {
	logChan chan interface{}
}

func (l *fileLogger) Stop() {
	tabletenv.StatsLogger.Unsubscribe(l.logChan)
}

// Init starts logging to the given file path.
func Init(path string) (FileLogger, error) {
	log.Infof("Logging queries to file %s", path)
	logChan, err := tabletenv.StatsLogger.LogToFile(path, streamlog.GetFormatter(tabletenv.StatsLogger))
	if err != nil {
		return nil, err
	}
	return &fileLogger{
		logChan: logChan,
	}, nil
}
