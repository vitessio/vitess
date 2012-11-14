// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// relog provides an alternate logger api with support for logging levels
// Although flexible, the recommended usage is to create a logger object,
// set it as the global logger, and use the package level functions to execute
// the logging.
// Multiple logger objects are only meant for those who like trouble.
package relog

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"syscall"
)

func init() {
	globalLogger = New(os.Stdout, "", log.LstdFlags, INFO)
}

var globalLogger *Logger

const (
	DEBUG = iota
	INFO
	WARNING
	ERROR
	FATAL
	NONE
)

var levelNames = []string{
	"DEBUG",
	"INFO",
	"WARNING",
	"ERROR",
	"FATAL",
	"NONE",
}

var levelPrefixes []string

func init() {
	levelPrefixes = make([]string, len(levelNames))
	for i, name := range levelNames {
		levelPrefixes[i] = name + ": "
	}
}

func Debug(format string, args ...interface{}) {
	globalLogger.Output(DEBUG, format, args...)
}

func Info(format string, args ...interface{}) {
	globalLogger.Output(INFO, format, args...)
}

func Warning(format string, args ...interface{}) {
	globalLogger.Output(WARNING, format, args...)
}

func Error(format string, args ...interface{}) {
	globalLogger.Output(ERROR, format, args...)
}

func Fatal(format string, args ...interface{}) {
	globalLogger.Output(FATAL, format, args...)
	os.Exit(1)
}

func SetLogger(logger *Logger) {
	globalLogger = logger
}

type Logger struct {
	logger *log.Logger
	level  int
}

func New(out io.Writer, prefix string, flag, level int) *Logger {
	return &Logger{logger: log.New(out, prefix, flag), level: level}
}

func (logger *Logger) Debug(format string, args ...interface{}) {
	logger.Output(DEBUG, format, args...)
}

func (logger *Logger) Info(format string, args ...interface{}) {
	logger.Output(INFO, format, args...)
}

func (logger *Logger) Warning(format string, args ...interface{}) {
	logger.Output(WARNING, format, args...)
}

func (logger *Logger) Error(format string, args ...interface{}) {
	logger.Output(ERROR, format, args...)
}

func (logger *Logger) Fatal(format string, args ...interface{}) {
	logger.Output(FATAL, format, args...)
	os.Exit(1)
}

// Redactor is an interface for types that may contain sensitive information, which shouldn't 
type Redactor interface {
	Redact() interface{}
}

func Redact(s string) string {
	return strings.Repeat("*", len(s))
}

func (logger *Logger) Output(level int, format string, args ...interface{}) {
	if logger.level > level {
		return
	}
	redactedArgs := make([]interface{}, len(args))
	for i, arg := range args {
		if redactor, ok := arg.(Redactor); ok {
			redactedArgs[i] = redactor.Redact()
		} else {
			redactedArgs[i] = arg
		}
	}
	// Judicious call depth setting: all callers are 3 levels deep
	logger.logger.Output(3, levelPrefixes[level]+fmt.Sprintf(format, redactedArgs...))
}

func (logger *Logger) SetFlags(flag int) {
	logger.logger.SetFlags(flag)
}

func (logger *Logger) SetPrefix(prefix string) {
	logger.logger.SetPrefix(prefix)
}

func (logger *Logger) SetLevel(level int) {
	logger.level = level
}

func LogNameToLogLevel(name string) int {
	s := strings.ToUpper(name)
	for i, level := range levelNames {
		if level == s {
			return i
		}
	}
	panic(fmt.Errorf("no log level: %v", name))
}

// Replace stdout and stderr with a file. This overwrites the actual file
// descriptors which means even top level panic output will show up in
// these files.
func HijackStdio(fOut *os.File, fErr *os.File) error {
	if fOut != nil {
		if err := syscall.Dup2(int(fOut.Fd()), int(os.Stdout.Fd())); err != nil {
			return err
		}
	}
	if fErr != nil {
		if err := syscall.Dup2(int(fErr.Fd()), int(os.Stderr.Fd())); err != nil {
			return err
		}
	}
	return nil
}
