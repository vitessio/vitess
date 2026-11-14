/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package logutil

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/protoutil"
	logutilpb "vitess.io/vitess/go/vt/proto/logutil"
)

// Logger defines the interface to use for our logging interface.
// All methods should be thread safe (i.e. multiple go routines can
// call these methods simultaneously).
//
// Info, Warn and Error take a constant message and slog attributes, like
// go/vt/log. Loggers that send events instead of writing to the log render
// the attributes after the message as key=value pairs, the way
// slog.TextHandler does. New code should use Info, Warn and Error.
type Logger interface {
	// Info logs msg and attrs at INFO level.
	Info(msg string, attrs ...slog.Attr)
	// Warn logs msg and attrs at WARNING level.
	Warn(msg string, attrs ...slog.Attr)
	// Error logs msg and attrs at ERROR level.
	Error(msg string, attrs ...slog.Attr)

	// Infof logs at INFO level. A newline is appended if missing.
	Infof(format string, v ...any)
	// Warningf logs at WARNING level. A newline is appended if missing.
	Warningf(format string, v ...any)
	// Errorf logs at ERROR level. A newline is appended if missing.
	Errorf(format string, v ...any)
	// Errorf2 logs an error with stack traces at ERROR level. A newline is appended if missing.
	Errorf2(e error, message string, v ...any)

	// Printf will just display information on stdout when possible.
	// No newline is appended.
	Printf(format string, v ...any)

	// InfoDepth allows call frame depth to be adjusted when logging to INFO.
	InfoDepth(depth int, msg string, attrs ...slog.Attr)
	// WarningDepth allows call frame depth to be adjusted when logging to WARNING.
	WarningDepth(depth int, msg string, attrs ...slog.Attr)
	// ErrorDepth allows call frame depth to be adjusted when logging to ERROR.
	ErrorDepth(depth int, msg string, attrs ...slog.Attr)
}

// EventToBuffer formats an individual Event into a buffer, without the
// final '\n'
func EventToBuffer(event *logutilpb.Event, buf *strings.Builder) {
	// Avoid Fprintf, for speed. The format is so simple that we
	// can do it quickly by hand.  It's worth about 3X. Fprintf is hard.

	// Lmmdd hh:mm:ss.uuuuuu file:line]
	switch event.Level {
	case logutilpb.Level_INFO:
		buf.WriteByte('I')
	case logutilpb.Level_WARNING:
		buf.WriteByte('W')
	case logutilpb.Level_ERROR:
		buf.WriteByte('E')
	case logutilpb.Level_CONSOLE:
		buf.WriteString(event.Value)
		return
	}

	t := protoutil.TimeFromProto(event.Time).UTC()
	_, month, day := t.Date()
	hour, minute, second := t.Clock()
	twoDigits(buf, int(month))
	twoDigits(buf, day)
	buf.WriteByte(' ')
	twoDigits(buf, hour)
	buf.WriteByte(':')
	twoDigits(buf, minute)
	buf.WriteByte(':')
	twoDigits(buf, second)
	buf.WriteByte('.')
	nDigits(buf, 6, t.Nanosecond()/1000, '0')
	buf.WriteByte(' ')
	buf.WriteString(event.File)
	buf.WriteByte(':')
	someDigits(buf, event.Line)
	buf.WriteByte(']')
	buf.WriteByte(' ')
	buf.WriteString(event.Value)
}

// EventString returns the line in one string
func EventString(event *logutilpb.Event) string {
	var buf strings.Builder
	EventToBuffer(event, &buf)
	return buf.String()
}

// textHandlerLead is what slog.TextHandler writes before the attributes of
// a record with a zero time, level INFO and an empty message.
const textHandlerLead = `level=INFO msg=""`

// eventValue returns msg followed by attrs, each as key=value the way
// slog.TextHandler writes them.
func eventValue(msg string, attrs []slog.Attr) string {
	if len(attrs) == 0 {
		return msg
	}
	var buf strings.Builder
	r := slog.NewRecord(time.Time{}, slog.LevelInfo, "", 0)
	r.AddAttrs(attrs...)
	// Handle only fails if the writer does, and a strings.Builder doesn't.
	_ = slog.NewTextHandler(&buf, nil).Handle(context.Background(), r)
	return msg + strings.TrimPrefix(strings.TrimSuffix(buf.String(), "\n"), textHandlerLead)
}

// LogEvent sends an event to a Logger, using the level specified in the event.
// The event struct is converted to a string with EventString().
func LogEvent(logger Logger, event *logutilpb.Event) {
	switch event.Level {
	case logutilpb.Level_INFO:
		logger.InfoDepth(1, EventString(event))
	case logutilpb.Level_WARNING:
		logger.WarningDepth(1, EventString(event))
	case logutilpb.Level_ERROR:
		logger.ErrorDepth(1, EventString(event))
	case logutilpb.Level_CONSOLE:
		// Note we can't just pass the string, because it might contain '%'.
		logger.Printf("%s", EventString(event))
	}
}

// CallbackLogger is a logger that sends the logging event to a callback
// for consumption.
type CallbackLogger struct {
	f func(*logutilpb.Event)
}

// NewCallbackLogger returns a new logger to the given callback.
// Note this and the other objects using this object should either
// all use pointer receivers, or non-pointer receivers.
// (that is ChannelLogger and MemoryLogger). That way they can share the
// 'depth' parameter freely. In this code now, they all use pointer receivers.
func NewCallbackLogger(f func(*logutilpb.Event)) *CallbackLogger {
	return &CallbackLogger{f}
}

// InfoDepth is part of the Logger interface.
func (cl *CallbackLogger) InfoDepth(depth int, msg string, attrs ...slog.Attr) {
	cl.event(logutilpb.Level_INFO, depth+1, msg, attrs)
}

// WarningDepth is part of the Logger interface
func (cl *CallbackLogger) WarningDepth(depth int, msg string, attrs ...slog.Attr) {
	cl.event(logutilpb.Level_WARNING, depth+1, msg, attrs)
}

// ErrorDepth is part of the Logger interface
func (cl *CallbackLogger) ErrorDepth(depth int, msg string, attrs ...slog.Attr) {
	cl.event(logutilpb.Level_ERROR, depth+1, msg, attrs)
}

func (cl *CallbackLogger) event(level logutilpb.Level, depth int, msg string, attrs []slog.Attr) {
	file, line := fileAndLine(2 + depth)
	cl.f(&logutilpb.Event{
		Time:  protoutil.TimeToProto(time.Now()),
		Level: level,
		File:  file,
		Line:  line,
		Value: eventValue(msg, attrs),
	})
}

// Info is part of the Logger interface.
func (cl *CallbackLogger) Info(msg string, attrs ...slog.Attr) {
	cl.InfoDepth(1, msg, attrs...)
}

// Warn is part of the Logger interface.
func (cl *CallbackLogger) Warn(msg string, attrs ...slog.Attr) {
	cl.WarningDepth(1, msg, attrs...)
}

// Error is part of the Logger interface.
func (cl *CallbackLogger) Error(msg string, attrs ...slog.Attr) {
	cl.ErrorDepth(1, msg, attrs...)
}

// Infof is part of the Logger interface.
func (cl *CallbackLogger) Infof(format string, v ...any) {
	cl.InfoDepth(1, fmt.Sprintf(format, v...))
}

// Warningf is part of the Logger interface.
func (cl *CallbackLogger) Warningf(format string, v ...any) {
	cl.WarningDepth(1, fmt.Sprintf(format, v...))
}

// Errorf is part of the Logger interface.
func (cl *CallbackLogger) Errorf(format string, v ...any) {
	cl.ErrorDepth(1, fmt.Sprintf(format, v...))
}

// Errorf2 is part of the Logger interface
func (cl *CallbackLogger) Errorf2(err error, format string, v ...any) {
	cl.ErrorDepth(1, fmt.Sprintf(format+": %+v", append(v, err)))
}

// Printf is part of the Logger interface.
func (cl *CallbackLogger) Printf(format string, v ...any) {
	file, line := fileAndLine(2)
	cl.f(&logutilpb.Event{
		Time:  protoutil.TimeToProto(time.Now()),
		Level: logutilpb.Level_CONSOLE,
		File:  file,
		Line:  line,
		Value: fmt.Sprintf(format, v...),
	})
}

// MemoryLogger keeps the logging events in memory.
// All protected by a mutex.
type MemoryLogger struct {
	CallbackLogger

	// mu protects the Events
	mu     sync.Mutex
	Events []*logutilpb.Event
}

// NewMemoryLogger returns a new MemoryLogger
func NewMemoryLogger() *MemoryLogger {
	ml := &MemoryLogger{}
	ml.f = func(e *logutilpb.Event) {
		ml.mu.Lock()
		defer ml.mu.Unlock()
		ml.Events = append(ml.Events, e)
	}
	return ml
}

// String returns all the lines in one String, separated by '\n'
func (ml *MemoryLogger) String() string {
	var buf strings.Builder
	ml.mu.Lock()
	defer ml.mu.Unlock()
	for _, event := range ml.Events {
		EventToBuffer(event, &buf)
		buf.WriteByte('\n')
	}
	return buf.String()
}

// Clear clears the logs.
func (ml *MemoryLogger) Clear() {
	ml.mu.Lock()
	ml.Events = nil
	ml.mu.Unlock()
}

func (ml *MemoryLogger) LogEvents() []*logutilpb.Event {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	return slices.Clone(ml.Events)
}

// LoggerWriter is an adapter that implements the io.Writer interface.
type LoggerWriter struct {
	logger Logger
}

// NewLoggerWriter returns an io.Writer on top of the logger
func NewLoggerWriter(logger Logger) io.Writer {
	return LoggerWriter{
		logger: logger,
	}
}

// Write implements io.Writer
func (lw LoggerWriter) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	lw.logger.Printf("%v", string(p))
	return len(p), nil
}

// TeeLogger is a Logger that sends its logs to two underlying logger
type TeeLogger struct {
	One, Two Logger
}

// NewTeeLogger returns a logger that sends its logs to both loggers
func NewTeeLogger(one, two Logger) *TeeLogger {
	return &TeeLogger{
		One: one,
		Two: two,
	}
}

// InfoDepth is part of the Logger interface
func (tl *TeeLogger) InfoDepth(depth int, msg string, attrs ...slog.Attr) {
	tl.One.InfoDepth(1+depth, msg, attrs...)
	tl.Two.InfoDepth(1+depth, msg, attrs...)
}

// WarningDepth is part of the Logger interface
func (tl *TeeLogger) WarningDepth(depth int, msg string, attrs ...slog.Attr) {
	tl.One.WarningDepth(1+depth, msg, attrs...)
	tl.Two.WarningDepth(1+depth, msg, attrs...)
}

// ErrorDepth is part of the Logger interface
func (tl *TeeLogger) ErrorDepth(depth int, msg string, attrs ...slog.Attr) {
	tl.One.ErrorDepth(1+depth, msg, attrs...)
	tl.Two.ErrorDepth(1+depth, msg, attrs...)
}

// Info is part of the Logger interface
func (tl *TeeLogger) Info(msg string, attrs ...slog.Attr) {
	tl.InfoDepth(1, msg, attrs...)
}

// Warn is part of the Logger interface
func (tl *TeeLogger) Warn(msg string, attrs ...slog.Attr) {
	tl.WarningDepth(1, msg, attrs...)
}

// Error is part of the Logger interface
func (tl *TeeLogger) Error(msg string, attrs ...slog.Attr) {
	tl.ErrorDepth(1, msg, attrs...)
}

// Infof is part of the Logger interface
func (tl *TeeLogger) Infof(format string, v ...any) {
	tl.InfoDepth(1, fmt.Sprintf(format, v...))
}

// Warningf is part of the Logger interface
func (tl *TeeLogger) Warningf(format string, v ...any) {
	tl.WarningDepth(1, fmt.Sprintf(format, v...))
}

// Errorf is part of the Logger interface
func (tl *TeeLogger) Errorf(format string, v ...any) {
	tl.ErrorDepth(1, fmt.Sprintf(format, v...))
}

// Errorf2 is part of the Logger interface
func (tl *TeeLogger) Errorf2(err error, format string, v ...any) {
	tl.ErrorDepth(1, fmt.Sprintf(format+": %+v", append(v, err)))
}

// Printf is part of the Logger interface
func (tl *TeeLogger) Printf(format string, v ...any) {
	tl.One.Printf(format, v...)
	tl.Two.Printf(format, v...)
}

// array for fast int -> string conversion
const digits = "0123456789"

// twoDigits adds a zero-prefixed two-digit integer to buf
func twoDigits(buf *strings.Builder, value int) {
	buf.WriteByte(digits[value/10])
	buf.WriteByte(digits[value%10])
}

// nDigits adds an n-digit integer d to buf
// padding with pad on the left.
// It assumes d >= 0.
func nDigits(buf *strings.Builder, n, d int, pad byte) {
	tmp := make([]byte, n)
	j := n - 1
	for ; j >= 0 && d > 0; j-- {
		tmp[j] = digits[d%10]
		d /= 10
	}
	for ; j >= 0; j-- {
		tmp[j] = pad
	}
	buf.Write(tmp)
}

// someDigits adds a zero-prefixed variable-width integer to buf
func someDigits(buf *strings.Builder, d int64) {
	// Print into the top, then copy down.
	tmp := make([]byte, 10)
	j := 10
	for {
		j--
		tmp[j] = digits[d%10]
		d /= 10
		if d == 0 {
			break
		}
	}
	buf.Write(tmp[j:])
}

// fileAndLine returns the caller's file and line 2 levels above
func fileAndLine(depth int) (string, int64) {
	_, file, line, ok := runtime.Caller(depth)
	if !ok {
		return "???", 1
	}

	slash := strings.LastIndex(file, "/")
	if slash >= 0 {
		file = file[slash+1:]
	}
	return file, int64(line)
}
