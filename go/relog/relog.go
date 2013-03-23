// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// relog provides an alternate logger api with support for logging
// levels.  Although flexible, the recommended usage is to create a
// logger object, set it as the global logger and use the package
// level functions to execute the logging.  Multiple logger objects
// are only meant for those who like trouble.
package relog

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	DEBUG = iota
	INFO
	WARNING
	ERROR
	LOG // Level to indicate line was output by the Go logging system.
	FATAL
)

var levelNames = []string{
	"DEBUG",
	"INFO",
	"WARNING",
	"ERROR",
	"LOG",
	"FATAL",
}

const (
	Ltsv      = 1 << iota // guarantee a single line of tab-separated values
	Lblob                 // include JSON enoded annotations
	Lstdflags = 0
)

type Logger struct {
	mu     sync.Mutex
	out    io.Writer
	prefix string
	level  int
	flags  int
}

func New(out io.Writer, prefix string, level int) *Logger {
	return &Logger{out: out, prefix: prefix, level: level}
}

func (logger *Logger) Debug(format string, args ...interface{}) {
	logger.output(DEBUG, 4, format, args...)
}

func (logger *Logger) Info(format string, args ...interface{}) {
	logger.output(INFO, 4, format, args...)
}

func (logger *Logger) Warning(format string, args ...interface{}) {
	logger.output(WARNING, 4, format, args...)
}

func (logger *Logger) Error(format string, args ...interface{}) {
	logger.output(ERROR, 4, format, args...)
}

func (logger *Logger) Fatal(format string, args ...interface{}) {
	logger.output(FATAL, 4, format, args...)
	os.Exit(1)
}

// time\tprefix\tfile:line\tlevel\tmessage\t{blob}\n
func (logger *Logger) fmtStandardFields(evtTime time.Time, level, callLevel int, msg string, data interface{}) string {
	timestamp := evtTime.Format(time.RFC3339Nano)
	levelName := levelNames[level]
	_, file, line, ok := runtime.Caller(callLevel)
	if !ok {
		file = "???"
		line = 0
	} else {
		file = path.Base(file)
	}
	if msg[len(msg)-1] == '\n' {
		msg = msg[:len(msg)-1]
	}

	// FIXME(msolomon) compute on flag set.
	// NOTE(msolomon) mu is locked while we run this, which is expensive.
	logFmt := "%v %v %v:%v %v: %v"
	if logger.flags&Lblob != 0 {
		logFmt += " %v"
	}
	logFmt += "\n"
	if logger.flags&Ltsv != 0 {
		logFmt = strings.Replace(logFmt, " ", "\t", -1)
	}

	if logger.flags&Ltsv != 0 {
		msg = strconv.Quote(msg)
		msg = msg[1 : len(msg)-1]
	}
	blob := "null"
	if logger.flags&Lblob != 0 {
		if js, err := json.Marshal(data); err != nil {
			blob = "{\"err\": " + strconv.Quote(err.Error()) + "}"
		} else {
			blob = string(js)
		}
		return fmt.Sprintf(logFmt, timestamp, logger.prefix, file, line, levelName, msg, blob)
	}
	return fmt.Sprintf(logFmt, timestamp, logger.prefix, file, line, levelName, msg)
}

func (logger *Logger) output(level, callLevel int, format string, args ...interface{}) error {
	return logger.outputWithData(level, callLevel, nil, format, args...)
}

func (logger *Logger) outputWithData(level, callLevel int, data interface{}, format string, args ...interface{}) error {
	if logger.level > level {
		return nil
	}
	now := time.Now()
	redactedArgs := make([]interface{}, len(args))
	for i, arg := range args {
		if redactor, ok := arg.(Redactor); ok {
			redactedArgs[i] = redactor.Redacted()
		} else {
			redactedArgs[i] = arg
		}
	}
	msg := fmt.Sprintf(format, redactedArgs...)
	logger.mu.Lock()
	line := logger.fmtStandardFields(now, level, callLevel, msg, data)
	_, err := io.WriteString(logger.out, line)
	logger.mu.Unlock()
	return err
}

func (logger *Logger) Prefix() string {
	logger.mu.Lock()
	defer logger.mu.Unlock()
	return logger.prefix
}

func (logger *Logger) SetPrefix(prefix string) {
	logger.mu.Lock()
	logger.prefix = prefix
	logger.mu.Unlock()
}

func (logger *Logger) SetFlags(flags int) {
	logger.mu.Lock()
	logger.flags = flags
	logger.mu.Unlock()
}

func (logger *Logger) SetLevel(level int) {
	logger.mu.Lock()
	logger.level = level
	logger.mu.Unlock()
}

// Redactor is an interface for types that may contain sensitive
// information (like passwords), which shouldn't be printed to the
// log.
type Redactor interface {
	// Redacted returns a copy of the instance with sensitive
	// information removed.
	Redacted() interface{}
}

// Redact returns a string of * having the same length as s.
func Redact(s string) string {
	return strings.Repeat("*", len(s))
}

var std = New(os.Stderr, "", INFO)

func Debug(format string, args ...interface{}) {
	std.output(DEBUG, 4, format, args...)
}

func Info(format string, args ...interface{}) {
	std.output(INFO, 4, format, args...)
}

func Warning(format string, args ...interface{}) {
	std.output(WARNING, 4, format, args...)
}

func Error(format string, args ...interface{}) {
	std.output(ERROR, 4, format, args...)
}

func Fatal(format string, args ...interface{}) {
	std.output(FATAL, 4, format, args...)
	os.Exit(1)
}

// SetOutput sets the output destination for the standard logger.
func SetOutput(w io.Writer) {
	std.mu.Lock()
	defer std.mu.Unlock()
	std.out = w
}

// SetPrefix sets the output prefix for the standard logger.
func SetPrefix(prefix string) {
	std.SetPrefix(prefix)
}

// SetLevel sets the output level for the standard logger.
func SetLevel(level int) {
	std.SetLevel(level)
}

func SetFlags(flags int) {
	std.SetFlags(flags)
}

func SetLevelByName(name string) error {
	level, err := LogNameToLogLevel(name)
	if err != nil {
		return err
	}
	std.SetLevel(level)
	return nil
}

func LogNameToLogLevel(name string) (int, error) {
	s := strings.ToUpper(name)
	for i, level := range levelNames {
		if level == s {
			return i, nil
		}
	}
	return 0, fmt.Errorf("no log level: %v", name)
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

type logShim struct {
	log *Logger
}

func (shim *logShim) Write(buf []byte) (n int, err error) {
	shim.log.output(LOG, 6, string(buf))
	return 0, nil
}

// Place a shim in the default log package to route messages to a
// single file. If rl is nil, use the standard relog instance.
func HijackLog(rl *Logger) {
	if rl == nil {
		rl = std
	}
	log.SetOutput(&logShim{rl})
	// Clear out as many features so that the only thing you get is a plain
	// write call routed to the shim.
	log.SetFlags(0)
	log.SetPrefix("")
}
