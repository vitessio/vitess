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

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/spf13/pflag"
)

var (
	// logFormat is the configured log format.
	logFormat string

	// logLevel is the configured log level.
	logLevel string

	// structuredLoggingEnabled controls whether structured logging is enabled. If it's disabled,
	// logging is performed through glog. If enabled, logging is instead through slog.
	structuredLoggingEnabled atomic.Bool
)

// Init configures logging based on the parsed flags.
func Init(fs *pflag.FlagSet) error {
	if fs == nil {
		return nil
	}

	formatFlag := fs.Lookup("log-fmt")
	if formatFlag == nil || !formatFlag.Changed {
		return nil
	}

	level, err := slogLevel(logLevel)
	if err != nil {
		return err
	}

	opts := &slog.HandlerOptions{AddSource: true, Level: level}
	handler, err := slogHandler(logFormat, opts)
	if err != nil {
		return err
	}

	logger := slog.New(handler)
	structuredLoggingEnabled.Store(true)
	slog.SetDefault(logger)

	return nil
}

// slogLevel maps the log-level flag value to a slog.Level.
func slogLevel(level string) (slog.Level, error) {
	normalized := strings.ToLower(strings.TrimSpace(level))

	switch normalized {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid log-level %q: expected debug, info, warn, or error", level)
	}
}

// slogHandler returns a [slog.Handler] for the given format and options.
func slogHandler(format string, opts *slog.HandlerOptions) (slog.Handler, error) {
	normalized := strings.ToLower(strings.TrimSpace(format))

	switch normalized {
	case "json":
		return slog.NewJSONHandler(os.Stderr, opts), nil
	case "logfmt":
		return slog.NewTextHandler(os.Stderr, opts), nil
	default:
		return nil, fmt.Errorf("invalid log-fmt %q: expected json or logfmt", format)
	}
}

// logS emits a structured log record when structured logging is enabled.
// When structured logging is disabled, logS forwards the call to glog
// using the severity implied by level.
func logS(level slog.Level, depth int, msg string, args ...any) {
	if !structuredLoggingEnabled.Load() {
		logGlog(level, depth, msg, args...)
		return
	}

	logger := slog.Default()

	ctx := context.Background()
	if !logger.Enabled(ctx, level) {
		return
	}

	// Adjust the caller depth (+3) to bypass the helper functions.
	var pcs [1]uintptr
	runtime.Callers(depth+3, pcs[:])

	// Rebuild the record with the proper source.
	record := slog.NewRecord(time.Now(), level, msg, pcs[0])
	record.Add(args...)

	_ = logger.Handler().Handle(ctx, record)
}

// Enabled reports whether a log call at the provided level would be emitted.
// When structured logging is enabled, Enabled consults the configured slog
// logger. When structured logging is disabled, Enabled returns true for info
// and above, and uses glog verbosity to gate debug logging.
func Enabled(level slog.Level) bool {
	if structuredLoggingEnabled.Load() {
		return slog.Default().Enabled(context.Background(), level)
	}

	if level < slog.LevelInfo {
		return bool(glog.V(glog.Level(1)))
	}

	return true
}

// logGlog formats a structured log call as a glog message.
func logGlog(level slog.Level, depth int, msg string, args ...any) {
	// Adjust depth so the reported caller skips logGlog, logS, and the wrapper.
	depth += 3

	// Preserve the slog message as the first printed element.
	args = append([]any{msg}, args...)

	switch level {
	case slog.LevelDebug, slog.LevelInfo:
		glog.InfoDepth(depth, args...)
	case slog.LevelWarn:
		glog.WarningDepth(depth, args...)
	case slog.LevelError:
		glog.ErrorDepth(depth, args...)
	default:
		glog.InfoDepth(depth, args...)
	}
}

// InfoS logs at the Info level.
func InfoS(msg string, args ...any) {
	logS(slog.LevelInfo, 0, msg, args...)
}

// InfoSDepth logs at the Info level with an adjusted caller depth.
func InfoSDepth(depth int, msg string, args ...any) {
	logS(slog.LevelInfo, depth, msg, args...)
}

// WarnS logs at the Warn level.
func WarnS(msg string, args ...any) {
	logS(slog.LevelWarn, 0, msg, args...)
}

// WarnSDepth logs at the Warn level with an adjusted caller depth.
func WarnSDepth(depth int, msg string, args ...any) {
	logS(slog.LevelWarn, depth, msg, args...)
}

// DebugS logs at the Debug level.
func DebugS(msg string, args ...any) {
	logS(slog.LevelDebug, 0, msg, args...)
}

// DebugSDepth logs at the Debug level with an adjusted caller depth.
func DebugSDepth(depth int, msg string, args ...any) {
	logS(slog.LevelDebug, depth, msg, args...)
}

// ErrorS logs at the Error level.
func ErrorS(msg string, args ...any) {
	logS(slog.LevelError, 0, msg, args...)
}

// ErrorSDepth logs at the Error level with an adjusted caller depth.
func ErrorSDepth(depth int, msg string, args ...any) {
	logS(slog.LevelError, depth, msg, args...)
}

// SetLogger replaces the structured logger used by the log package. The returned function restores
// the previous logger. Used for testing.
func SetLogger(logger *slog.Logger) func() {
	if logger == nil {
		return func() {}
	}

	previousEnabled := structuredLoggingEnabled.Load()
	previousDefault := slog.Default()

	slog.SetDefault(logger)
	structuredLoggingEnabled.Store(true)

	return func() {
		slog.SetDefault(previousDefault)
		structuredLoggingEnabled.Store(previousEnabled)
	}
}
