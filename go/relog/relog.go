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

func (self *Logger) Debug(format string, args ...interface{}) {
	self.Output(DEBUG, format, args...)
}

func (self *Logger) Info(format string, args ...interface{}) {
	self.Output(INFO, format, args...)
}

func (self *Logger) Warning(format string, args ...interface{}) {
	self.Output(WARNING, format, args...)
}

func (self *Logger) Error(format string, args ...interface{}) {
	self.Output(ERROR, format, args...)
}

func (self *Logger) Fatal(format string, args ...interface{}) {
	self.Output(FATAL, format, args...)
	os.Exit(1)
}

func (self *Logger) Output(level int, format string, args ...interface{}) {
	if self.level > level {
		return
	}
	// Judicious call depth setting: all callers are 3 levels deep
	self.logger.Output(3, levelPrefixes[level]+fmt.Sprintf(format, args...))
}

func (self *Logger) SetFlags(flag int) {
	self.logger.SetFlags(flag)
}

func (self *Logger) SetPrefix(prefix string) {
	self.logger.SetPrefix(prefix)
}

func (self *Logger) SetLevel(level int) {
	self.level = level
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
