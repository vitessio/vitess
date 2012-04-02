/*
Copyright 2012, Google Inc.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
    * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

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

var levelPrefixes = []string{
	"DEBUG: ",
	"INFO: ",
	"WARNING: ",
	"ERROR: ",
	"FATAL: ",
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
