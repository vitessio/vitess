package log

import (
	"fmt"

	"vitess.io/vitess/go/vt/log"
)

// Logger is a wrapper that prefix loglines with keyspace/shard
type Logger struct {
	prefix string
}

// NewVTConsensusLogger creates a new logger
func NewVTConsensusLogger(keyspace, shard string) *Logger {
	return &Logger{
		prefix: fmt.Sprintf("%s/%s", keyspace, shard),
	}
}

// Info formats arguments like fmt.Print
func (logger *Logger) Info(msg string) {
	log.InfoDepth(1, logger.annotate(msg))
}

// Infof formats arguments like fmt.Printf.
func (logger *Logger) Infof(format string, args ...any) {
	log.InfoDepth(1, logger.annotate(fmt.Sprintf(format, args...)))
}

// Warning formats arguments like fmt.Print
func (logger *Logger) Warning(msg string) {
	log.WarningDepth(1, logger.annotate(msg))
}

// Warningf formats arguments like fmt.Printf.
func (logger *Logger) Warningf(format string, args ...any) {
	log.WarningDepth(1, logger.annotate(fmt.Sprintf(format, args...)))
}

// Error formats arguments like fmt.Print
func (logger *Logger) Error(msg string) {
	log.ErrorDepth(1, logger.annotate(msg))
}

// Errorf formats arguments like fmt.Printf.
func (logger *Logger) Errorf(format string, args ...any) {
	log.ErrorDepth(1, logger.annotate(fmt.Sprintf(format, args...)))
}

func (logger *Logger) annotate(input string) string {
	return fmt.Sprintf("shard=%s %s", logger.prefix, input)
}
