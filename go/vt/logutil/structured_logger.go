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

package logutil

import (
	"fmt"
	"log/slog"
)

// DepthLogger is a structured logger that attributes each line to the caller
// depth frames up. Its methods are those of log.PrefixedLogger.
type DepthLogger interface {
	InfoDepth(depth int, msg string, attrs ...slog.Attr)
	WarnDepth(depth int, msg string, attrs ...slog.Attr)
	ErrorDepth(depth int, msg string, attrs ...slog.Attr)
}

// StructuredLogger is a Logger that, like a ConsoleLogger, logs each line with
// its level and the caller's file and line, but to a given DepthLogger. It
// drops lines below a level and adds attributes to every line. The printf
// methods log the formatted text as the message, Printf at INFO.
//
// Note that methods on StructuredLogger must use pointer receivers, for the
// same reason as ConsoleLogger's.
type StructuredLogger struct {
	logger DepthLogger
	level  slog.Level
	attrs  []slog.Attr
}

// NewStructuredLogger returns a StructuredLogger that logs to logger, which
// must not be nil.
func NewStructuredLogger(logger DepthLogger, level slog.Level, attrs ...slog.Attr) *StructuredLogger {
	return &StructuredLogger{logger: logger, level: level, attrs: attrs}
}

// Info is part of the Logger interface.
func (sl *StructuredLogger) Info(msg string, attrs ...slog.Attr) {
	sl.InfoDepth(1, msg, attrs...)
}

// Warn is part of the Logger interface.
func (sl *StructuredLogger) Warn(msg string, attrs ...slog.Attr) {
	sl.WarningDepth(1, msg, attrs...)
}

// Error is part of the Logger interface.
func (sl *StructuredLogger) Error(msg string, attrs ...slog.Attr) {
	sl.ErrorDepth(1, msg, attrs...)
}

// Infof is part of the Logger interface.
func (sl *StructuredLogger) Infof(format string, v ...any) {
	sl.InfoDepth(1, fmt.Sprintf(format, v...))
}

// Warningf is part of the Logger interface.
func (sl *StructuredLogger) Warningf(format string, v ...any) {
	sl.WarningDepth(1, fmt.Sprintf(format, v...))
}

// Errorf is part of the Logger interface.
func (sl *StructuredLogger) Errorf(format string, v ...any) {
	sl.ErrorDepth(1, fmt.Sprintf(format, v...))
}

// Errorf2 is part of the Logger interface.
func (sl *StructuredLogger) Errorf2(err error, format string, v ...any) {
	sl.ErrorDepth(1, fmt.Sprintf(format+": %+v", append(v, err)...))
}

// Printf is part of the Logger interface.
func (sl *StructuredLogger) Printf(format string, v ...any) {
	sl.InfoDepth(1, fmt.Sprintf(format, v...))
}

// InfoDepth is part of the Logger interface.
func (sl *StructuredLogger) InfoDepth(depth int, msg string, attrs ...slog.Attr) {
	if sl.level <= slog.LevelInfo {
		sl.logger.InfoDepth(depth+1, msg, sl.withAttrs(attrs)...)
	}
}

// WarningDepth is part of the Logger interface.
func (sl *StructuredLogger) WarningDepth(depth int, msg string, attrs ...slog.Attr) {
	if sl.level <= slog.LevelWarn {
		sl.logger.WarnDepth(depth+1, msg, sl.withAttrs(attrs)...)
	}
}

// ErrorDepth is part of the Logger interface.
func (sl *StructuredLogger) ErrorDepth(depth int, msg string, attrs ...slog.Attr) {
	if sl.level <= slog.LevelError {
		sl.logger.ErrorDepth(depth+1, msg, sl.withAttrs(attrs)...)
	}
}

func (sl *StructuredLogger) withAttrs(attrs []slog.Attr) []slog.Attr {
	if len(sl.attrs) == 0 {
		return attrs
	}
	return append(append(make([]slog.Attr, 0, len(sl.attrs)+len(attrs)), sl.attrs...), attrs...)
}
