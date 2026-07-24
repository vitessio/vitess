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

package log

import (
	"context"
	"log/slog"
	"runtime"
	"sync/atomic"
	"time"
)

var (
	// logger is the currently configured logger.
	logger atomic.Pointer[slog.Logger]

	Debug = func(msg string, attrs ...slog.Attr) { log(slog.LevelDebug, 1, msg, attrs...) }
	Info  = func(msg string, attrs ...slog.Attr) { log(slog.LevelInfo, 1, msg, attrs...) }
	Warn  = func(msg string, attrs ...slog.Attr) { log(slog.LevelWarn, 1, msg, attrs...) }
	Error = func(msg string, attrs ...slog.Attr) { log(slog.LevelError, 1, msg, attrs...) }

	DebugDepth = func(depth int, msg string, attrs ...slog.Attr) { log(slog.LevelDebug, depth+1, msg, attrs...) }
	InfoDepth  = func(depth int, msg string, attrs ...slog.Attr) { log(slog.LevelInfo, depth+1, msg, attrs...) }
	WarnDepth  = func(depth int, msg string, attrs ...slog.Attr) { log(slog.LevelWarn, depth+1, msg, attrs...) }
	ErrorDepth = func(depth int, msg string, attrs ...slog.Attr) { log(slog.LevelError, depth+1, msg, attrs...) }
)

func init() {
	logger.Store(newLogger(slog.LevelInfo))
}

// SwapLogger atomically replaces the logger with a new one
// and returns the previous logger. This is safe for concurrent use and is
// intended for tests that need to intercept log output.
func SwapLogger(newLogger *slog.Logger) *slog.Logger {
	return logger.Swap(newLogger)
}

func Enabled(level slog.Level) bool {
	l := logger.Load()
	return l != nil && l.Enabled(context.Background(), level)
}

func log(level slog.Level, depth int, msg string, attrs ...slog.Attr) {
	l := logger.Load()
	if l == nil {
		return
	}

	if !Enabled(level) {
		return
	}

	var pcs [1]uintptr
	runtime.Callers(depth+2, pcs[:])

	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	r.AddAttrs(attrs...)

	_ = l.Handler().Handle(context.Background(), r)
}
