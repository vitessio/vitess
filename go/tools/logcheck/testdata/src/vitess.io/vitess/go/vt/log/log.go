/*
Copyright 2026 The Vitess Authors.

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

package log

import "log/slog"

type Level int32

var (
	Flush = func() {}

	Debug = func(msg string, attrs ...slog.Attr) {}
	Info  = func(msg string, attrs ...slog.Attr) {}
	Warn  = func(msg string, attrs ...slog.Attr) {}
	Error = func(msg string, attrs ...slog.Attr) {}

	DebugDepth = func(depth int, msg string, attrs ...slog.Attr) {}
	InfoDepth  = func(depth int, msg string, attrs ...slog.Attr) {}
	WarnDepth  = func(depth int, msg string, attrs ...slog.Attr) {}
	ErrorDepth = func(depth int, msg string, attrs ...slog.Attr) {}
)

type Verbose bool

func V(level Level) Verbose { return true }

func (v Verbose) Info(msg string, attrs ...slog.Attr) {}

type PrefixedLogger struct {
	prefix string
}

func NewPrefixedLogger(prefix string) *PrefixedLogger {
	return &PrefixedLogger{prefix: prefix}
}

func (pl *PrefixedLogger) Info(msg string, attrs ...slog.Attr) {}

func (pl *PrefixedLogger) Warn(msg string, attrs ...slog.Attr) {}

func (pl *PrefixedLogger) Error(msg string, attrs ...slog.Attr) {}

func (pl *PrefixedLogger) InfoDepth(depth int, msg string, attrs ...slog.Attr) {}

func (pl *PrefixedLogger) WarnDepth(depth int, msg string, attrs ...slog.Attr) {}

func (pl *PrefixedLogger) ErrorDepth(depth int, msg string, attrs ...slog.Attr) {}

func (pl *PrefixedLogger) V(level Level) Verbose { return V(level) }

func (pl *PrefixedLogger) Flush() {}

// The declarations below are not in the real package. They pin down which
// signatures count as logging functions.

// LogFunc is a named logging function type.
type LogFunc func(msg string, attrs ...slog.Attr)

var Notice LogFunc = func(msg string, attrs ...slog.Attr) {}

// Field is an alias of slog.Attr.
type Field = slog.Attr

func InfoFields(msg string, fields ...Field) {}

// Logger's methods are promoted from an unexported type.
type Logger struct {
	*logger
}

type logger struct{}

func (*logger) Info(msg string, attrs ...slog.Attr) {}

func NewLogger() Logger { return Logger{&logger{}} }

type Hooks struct {
	OnError func(msg string, attrs ...slog.Attr)
}

var DefaultHooks Hooks

// Not logging functions.

func Group(n int, attrs ...slog.Attr) {}

func Attrs(attrs ...slog.Attr) {}

func InfoSlice(msg string, attrs []slog.Attr) {}

func InfoValues(msg string, values ...slog.Value) {}

type Message string

func InfoMessage(msg Message, attrs ...slog.Attr) {}

type Attr struct {
	Key, Value string
}

func InfoAttrs(msg string, attrs ...Attr) {}

// reassigned mirrors the wrappers inside the real package. The analyzer
// skips this package, so none of these are findings.
func reassigned(msg string) {
	fn := Info
	fn(msg)
	InfoDepth(1, msg)
	NewPrefixedLogger(msg).Info(msg)
}
