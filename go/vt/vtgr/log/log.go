package log

import (
	"fmt"

	"vitess.io/vitess/go/vt/log"
)

// Logger is a wrapper that prefix loglines with keyspace/shard
type Logger struct {
	prefix string
}

// NewVTGRLogger creates a new logger
func NewVTGRLogger(keyspace, shard string) *Logger {
	return &Logger{
		prefix: fmt.Sprintf("%s/%s", keyspace, shard),
	}
}

// Info formats arguments like fmt.Print
func (logger *Logger) Info(msg string) {
	log.Info(logger.annotate(msg))
}

// Infof formats arguments like fmt.Printf.
func (logger *Logger) Infof(format string, args ...interface{}) {
	log.Info(logger.annotate(fmt.Sprintf(format, args...)))
}

// Warning formats arguments like fmt.Print
func (logger *Logger) Warning(msg string) {
	log.Warning(logger.annotate(msg))
}

// Warningf formats arguments like fmt.Printf.
func (logger *Logger) Warningf(format string, args ...interface{}) {
	log.Warning(logger.annotate(fmt.Sprintf(format, args...)))
}

// Error formats arguments like fmt.Print
func (logger *Logger) Error(msg string) {
	log.Error(logger.annotate(msg))
}

// Errorf formats arguments like fmt.Printf.
func (logger *Logger) Errorf(format string, args ...interface{}) {
	log.Error(logger.annotate(fmt.Sprintf(format, args...)))
}

func (logger *Logger) annotate(input string) string {
	return fmt.Sprintf("shard=%s %s", logger.prefix, input)
}
