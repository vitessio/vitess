/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package filelogger implements an optional plugin that logs all queries to syslog.
package filelogger

import (
	"flag"

	"vitess.io/vitess/go/vt/vterrors"

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
			_, err := Init(*logQueriesToFile)
			vterrors.LogIfError(err)
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
	log.Info("Logging queries to file %s", path)
	logChan, err := tabletenv.StatsLogger.LogToFile(path, streamlog.GetFormatter(tabletenv.StatsLogger))
	if err != nil {
		return nil, err
	}
	return &fileLogger{
		logChan: logChan,
	}, nil
}
