package logutil

import (
	"fmt"

	log "github.com/golang/glog"
)

// ConsoleLogger is a Logger that uses glog directly to log.
// We can't specify the depth of the stack trace,
// So we just find it and add it to the message.
type ConsoleLogger struct{}

// NewConsoleLogger returns a simple ConsoleLogger
func NewConsoleLogger() ConsoleLogger {
	return ConsoleLogger{}
}

// Infof is part of the Logger interface
func (cl ConsoleLogger) Infof(format string, v ...interface{}) {
	file, line := fileAndLine(3)
	vals := []interface{}{file, line}
	vals = append(vals, v...)
	log.Infof("%v:%v] "+format, vals...)
}

// Warningf is part of the Logger interface
func (cl ConsoleLogger) Warningf(format string, v ...interface{}) {
	file, line := fileAndLine(3)
	vals := []interface{}{file, line}
	vals = append(vals, v...)
	log.Warningf("%v:%v] "+format, vals...)
}

// Errorf is part of the Logger interface
func (cl ConsoleLogger) Errorf(format string, v ...interface{}) {
	file, line := fileAndLine(3)
	vals := []interface{}{file, line}
	vals = append(vals, v...)
	log.Errorf("%v:%v] "+format, vals...)
}

// Printf is part of the Logger interface
func (cl ConsoleLogger) Printf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}
