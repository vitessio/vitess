package logutil

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Logger defines the interface to use for our logging interface.
// All methods should be thread safe (i.e. multiple go routines can
// call these methods simultaneously).
type Logger interface {
	// The three usual interfaces, format should not contain the trailing '\n'
	Infof(format string, v ...interface{})
	Warningf(format string, v ...interface{})
	Errorf(format string, v ...interface{})

	// Printf will just display information on stdout when possible,
	// and will not add any '\n'.
	Printf(format string, v ...interface{})
}

// The logger levels are used to store individual logging events
const (
	// the usual logging levels
	LOGGER_INFO = iota
	LOGGER_WARNING
	LOGGER_ERROR

	// for messages that may contains non-logging events
	LOGGER_CONSOLE
)

// LoggerEvent is used to manage individual logging events. It is used
// by ChannelLogger and MemoryLogger.
type LoggerEvent struct {
	Time  time.Time
	Level int
	File  string
	Line  int
	Value string
}

// ToBuffer formats an individual LoggerEvent into a buffer, without the
// final '\n'
func (event *LoggerEvent) ToBuffer(buf *bytes.Buffer) {
	// Avoid Fprintf, for speed. The format is so simple that we
	// can do it quickly by hand.  It's worth about 3X. Fprintf is hard.

	// Lmmdd hh:mm:ss.uuuuuu file:line]
	switch event.Level {
	case LOGGER_INFO:
		buf.WriteByte('I')
	case LOGGER_WARNING:
		buf.WriteByte('W')
	case LOGGER_ERROR:
		buf.WriteByte('E')
	case LOGGER_CONSOLE:
		buf.WriteString(event.Value)
		return
	}

	_, month, day := event.Time.Date()
	hour, minute, second := event.Time.Clock()
	twoDigits(buf, int(month))
	twoDigits(buf, day)
	buf.WriteByte(' ')
	twoDigits(buf, hour)
	buf.WriteByte(':')
	twoDigits(buf, minute)
	buf.WriteByte(':')
	twoDigits(buf, second)
	buf.WriteByte('.')
	nDigits(buf, 6, event.Time.Nanosecond()/1000, '0')
	buf.WriteByte(' ')
	buf.WriteString(event.File)
	buf.WriteByte(':')
	someDigits(buf, event.Line)
	buf.WriteByte(']')
	buf.WriteByte(' ')
	buf.WriteString(event.Value)
}

// String returns the line in one string
func (event *LoggerEvent) String() string {
	buf := new(bytes.Buffer)
	event.ToBuffer(buf)
	return buf.String()
}

// ChannelLogger is a Logger that sends the logging events through a channel for
// consumption.
type ChannelLogger chan LoggerEvent

// NewChannelLogger returns a ChannelLogger fo the given size
func NewChannelLogger(size int) ChannelLogger {
	return make(chan LoggerEvent, size)
}

// Infof is part of the Logger interface
func (cl ChannelLogger) Infof(format string, v ...interface{}) {
	file, line := fileAndLine(2)
	(chan LoggerEvent)(cl) <- LoggerEvent{
		Time:  time.Now(),
		Level: LOGGER_INFO,
		File:  file,
		Line:  line,
		Value: fmt.Sprintf(format, v...),
	}
}

// Warningf is part of the Logger interface
func (cl ChannelLogger) Warningf(format string, v ...interface{}) {
	file, line := fileAndLine(2)
	(chan LoggerEvent)(cl) <- LoggerEvent{
		Time:  time.Now(),
		Level: LOGGER_WARNING,
		File:  file,
		Line:  line,
		Value: fmt.Sprintf(format, v...),
	}
}

// Errorf is part of the Logger interface
func (cl ChannelLogger) Errorf(format string, v ...interface{}) {
	file, line := fileAndLine(2)
	(chan LoggerEvent)(cl) <- LoggerEvent{
		Time:  time.Now(),
		Level: LOGGER_ERROR,
		File:  file,
		Line:  line,
		Value: fmt.Sprintf(format, v...),
	}
}

// Errorf is part of the Logger interface
func (cl ChannelLogger) Printf(format string, v ...interface{}) {
	file, line := fileAndLine(2)
	(chan LoggerEvent)(cl) <- LoggerEvent{
		Time:  time.Now(),
		Level: LOGGER_CONSOLE,
		File:  file,
		Line:  line,
		Value: fmt.Sprintf(format, v...),
	}
}

// MemoryLogger keeps the logging events in memory.
// All protected by a mutex.
type MemoryLogger struct {
	mu     sync.Mutex
	Events []LoggerEvent
}

// NewMemoryLogger returns a new MemoryLogger
func NewMemoryLogger() *MemoryLogger {
	return &MemoryLogger{}
}

// Infof is part of the Logger interface
func (ml *MemoryLogger) Infof(format string, v ...interface{}) {
	file, line := fileAndLine(2)
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.Events = append(ml.Events, LoggerEvent{
		Time:  time.Now(),
		Level: LOGGER_INFO,
		File:  file,
		Line:  line,
		Value: fmt.Sprintf(format, v...),
	})
}

// Warningf is part of the Logger interface
func (ml *MemoryLogger) Warningf(format string, v ...interface{}) {
	file, line := fileAndLine(2)
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.Events = append(ml.Events, LoggerEvent{
		Time:  time.Now(),
		Level: LOGGER_WARNING,
		File:  file,
		Line:  line,
		Value: fmt.Sprintf(format, v...),
	})
}

// Errorf is part of the Logger interface
func (ml *MemoryLogger) Errorf(format string, v ...interface{}) {
	file, line := fileAndLine(2)
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.Events = append(ml.Events, LoggerEvent{
		Time:  time.Now(),
		Level: LOGGER_ERROR,
		File:  file,
		Line:  line,
		Value: fmt.Sprintf(format, v...),
	})
}

// Printf is part of the Logger interface
func (ml *MemoryLogger) Printf(format string, v ...interface{}) {
	file, line := fileAndLine(2)
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.Events = append(ml.Events, LoggerEvent{
		Time:  time.Now(),
		Level: LOGGER_CONSOLE,
		File:  file,
		Line:  line,
		Value: fmt.Sprintf(format, v...),
	})
}

// String returns all the lines in one String, separated by '\n'
func (ml *MemoryLogger) String() string {
	buf := new(bytes.Buffer)
	ml.mu.Lock()
	defer ml.mu.Unlock()
	for _, event := range ml.Events {
		event.ToBuffer(buf)
		buf.WriteByte('\n')
	}
	return buf.String()
}

const digits = "0123456789"

// twoDigits adds a zero-prefixed two-digit integer to buf
func twoDigits(buf *bytes.Buffer, value int) {
	buf.WriteByte(digits[value/10])
	buf.WriteByte(digits[value%10])
}

// nDigits adds an n-digit integer d to buf
// padding with pad on the left.
// It assumes d >= 0.
func nDigits(buf *bytes.Buffer, n, d int, pad byte) {
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
func someDigits(buf *bytes.Buffer, d int) {
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
func fileAndLine(depth int) (string, int) {
	_, file, line, ok := runtime.Caller(depth)
	if !ok {
		return "???", 1
	}

	slash := strings.LastIndex(file, "/")
	if slash >= 0 {
		file = file[slash+1:]
	}
	return file, line
}
