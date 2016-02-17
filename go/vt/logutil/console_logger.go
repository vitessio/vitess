package logutil

import (
	"fmt"

	log "github.com/golang/glog"
)

// ConsoleLogger is a Logger that uses glog directly to log, at the right level.
type ConsoleLogger struct{}

// NewConsoleLogger returns a simple ConsoleLogger
func NewConsoleLogger() ConsoleLogger {
	return ConsoleLogger{}
}

// Infof is part of the Logger interface
func (cl ConsoleLogger) Infof(format string, v ...interface{}) {
	log.InfoDepth(2, fmt.Sprintf(format, v...))
}

// Warningf is part of the Logger interface
func (cl ConsoleLogger) Warningf(format string, v ...interface{}) {
	log.WarningDepth(2, fmt.Sprintf(format, v...))
}

// Errorf is part of the Logger interface
func (cl ConsoleLogger) Errorf(format string, v ...interface{}) {
	log.ErrorDepth(2, fmt.Sprintf(format, v...))
}

// Printf is part of the Logger interface
func (cl ConsoleLogger) Printf(format string, v ...interface{}) {
	fmt.Printf(format, v...)
}

// InfoDepth is part of the Logger interface.
func (cl ConsoleLogger) InfoDepth(depth int, s string) {
	log.InfoDepth(1+depth, s)
}

// WarningDepth is part of the Logger interface.
func (cl ConsoleLogger) WarningDepth(depth int, s string) {
	log.WarningDepth(1+depth, s)
}

// ErrorDepth is part of the Logger interface.
func (cl ConsoleLogger) ErrorDepth(depth int, s string) {
	log.ErrorDepth(1+depth, s)
}
