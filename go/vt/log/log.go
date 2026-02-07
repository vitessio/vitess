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

	"github.com/golang/glog"
)

// Level is used with V() to test log verbosity.
type Level = glog.Level

var (
	// structured is whether structured logging is currently enabled or not.
	structured atomic.Bool

	// logger is the currently configured structured logger.
	logger atomic.Pointer[slog.Logger]

	Flush = glog.Flush

	Debug = func(msg string, attrs ...slog.Attr) { log(slog.LevelDebug, 1, msg, attrs...) }
	Info  = func(msg string, attrs ...slog.Attr) { log(slog.LevelInfo, 1, msg, attrs...) }
	Warn  = func(msg string, attrs ...slog.Attr) { log(slog.LevelWarn, 1, msg, attrs...) }
	Error = func(msg string, attrs ...slog.Attr) { log(slog.LevelError, 1, msg, attrs...) }

	DebugDepth = func(depth int, msg string, attrs ...slog.Attr) { log(slog.LevelDebug, depth+1, msg, attrs...) }
	InfoDepth  = func(depth int, msg string, attrs ...slog.Attr) { log(slog.LevelInfo, depth+1, msg, attrs...) }
	WarnDepth  = func(depth int, msg string, attrs ...slog.Attr) { log(slog.LevelWarn, depth+1, msg, attrs...) }
	ErrorDepth = func(depth int, msg string, attrs ...slog.Attr) { log(slog.LevelError, depth+1, msg, attrs...) }
)

// log is a helper that logs with glog or slog depending on the configured flags.
func log(level slog.Level, depth int, msg string, attrs ...slog.Attr) {
	if !structured.Load() {
		logGlog(level, depth+1, msg, attrs...)
		return
	}

	l := logger.Load()
	if l == nil {
		return
	}

	if !l.Enabled(context.Background(), level) {
		return
	}

	var pcs [1]uintptr
	runtime.Callers(depth+2, pcs[:])

	r := slog.NewRecord(time.Now(), level, msg, pcs[0])
	r.AddAttrs(attrs...)

	_ = l.Handler().Handle(context.Background(), r)
}

// logGlog is a helper that logs with glog. If structured attributes are passed, they
// are appended with the message in the format "key=value".
func logGlog(level slog.Level, depth int, msg string, attrs ...slog.Attr) {
	depth++

	args := make([]any, 0, len(attrs)+2)
	args = append(args, msg)
	for i, attr := range attrs {
		// Add a space to separate the message from the start of the attributes.
		if i == 0 {
			args = append(args, " ")
		}

		args = append(args, attr)
	}

	switch {
	case level >= slog.LevelError:
		glog.ErrorDepth(depth, args...)
	case level >= slog.LevelWarn:
		glog.WarningDepth(depth, args...)
	default:
		glog.InfoDepth(depth, args...)
	}
}

// Verbose gates logging at a given V-level. Used to temporarily support glog
// call sites.
//
// TODO: migrate call sites to normal debug logging and remove this once glog
// is removed.
type Verbose bool

func V(level Level) Verbose {
	if !structured.Load() {
		return Verbose(glog.V(level))
	}

	l := logger.Load()
	if l == nil {
		return false
	}

	return Verbose(l.Enabled(context.Background(), slog.Level(-int(level))))
}

func (v Verbose) Info(msg string, attrs ...slog.Attr) {
	if v {
		log(slog.LevelInfo, 1, msg, attrs...)
	}
}
