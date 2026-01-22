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

const (
	// logFormatJSON selects slog's JSON handler.
	logFormatJSON = "json"

	// logFormatLogfmt selects slog's text handler, which emits logfmt-compatible output.
	logFormatLogfmt = "logfmt"
)

const (
	// logLevelDebug configures slog to emit debug records.
	logLevelDebug = "debug"

	// logLevelInfo configures slog to emit info records.
	logLevelInfo = "info"

	// logLevelWarn configures slog to emit warning records.
	logLevelWarn = "warn"

	// logLevelError configures slog to emit error records.
	logLevelError = "error"
)

const (
	// slogCallerDepth accounts for logS and the public *S wrappers when
	// recording the caller program counter.
	slogCallerDepth = 3

	// glogCallerDepth accounts for logS and the public *S wrappers when
	// forwarding to glog's *Depth helpers.
	glogCallerDepth = 2
)

var (
	// logFormat stores the log-fmt flag value for Init.
	logFormat = logFormatJSON

	// logLevel stores the log-level flag value for Init.
	logLevel = logLevelInfo
)

var (
	// structuredLoggingEnabled reports whether slog is active for this process.
	structuredLoggingEnabled atomic.Bool

	// structuredLevel holds the minimum slog level configured by Init.
	structuredLevel slog.LevelVar

	// structuredLogger is the slog logger configured by Init when enabled.
	structuredLogger atomic.Pointer[slog.Logger]
)

// registeredFlagSet is the most recent FlagSet passed to RegisterFlags.
var registeredFlagSet atomic.Pointer[pflag.FlagSet]

var (
	// initDone reports whether Init has completed.
	initDone bool

	// initErr stores the outcome of the first Init call.
	initErr error
)

// rememberFlagSet records the most recent FlagSet that RegisterFlags updated.
//
// Init uses this pointer to discover which FlagSet should be queried for the
// log-fmt and log-level settings after flags have been parsed.
func rememberFlagSet(fs *pflag.FlagSet) {
	registeredFlagSet.Store(fs)
}

// latestFlagSet returns the most recently registered FlagSet.
//
// Callers should treat the returned pointer as read-only.
func latestFlagSet() *pflag.FlagSet {
	return registeredFlagSet.Load()
}

// Init configures structured logging based on the parsed flags.
//
// Init is safe to call multiple times. The first call records its error, and
// subsequent calls return that same error.
//
// Structured logging is enabled only when the log-fmt flag is explicitly set.
// When enabled, Init configures the default slog logger and updates the global
// structuredLoggingEnabled flag.
//
// The fs parameter should be the FlagSet used to parse the log flags. If fs is
// nil, Init falls back to the most recently registered FlagSet.
func Init(fs *pflag.FlagSet) error {
	if initDone {
		return initErr
	}

	initDone = true
	initErr = initStructuredLogging(fs)

	return initErr
}

// initStructuredLogging inspects the parsed flag values and configures slog
// when the log-fmt flag is explicitly set.
func initStructuredLogging(fs *pflag.FlagSet) error {
	if fs == nil {
		fs = latestFlagSet()
	}

	// The parsed FlagSet is required to observe whether log-fmt was set.
	if fs == nil {
		return nil
	}

	// The log-fmt flag must exist and be explicitly set to enable slog.
	formatFlag := fs.Lookup("log-fmt")
	if formatFlag == nil {
		return nil
	}

	// Structured logging is opt-in, so skip initialization unless log-fmt was set.
	if !formatFlag.Changed {
		return nil
	}

	// Map the log-level flag into a slog severity.
	level, err := slogLevelFromString(logLevel)
	if err != nil {
		return err
	}

	// Set the level before wiring it into the handler options.
	structuredLevel.Set(level)

	// Always include source location to match glog's default caller output.
	opts := &slog.HandlerOptions{
		AddSource: true,
		Level:     &structuredLevel,
	}

	// Select the handler based on the requested log-fmt value.
	handler, err := slogHandlerForFormat(logFormat, opts)
	if err != nil {
		return err
	}

	// The logger and global defaults become valid once the handler is ready.
	logger := slog.New(handler)
	structuredLogger.Store(logger)
	structuredLoggingEnabled.Store(true)

	// Update slog's default logger so callers can query slog.Default().Enabled.
	slog.SetDefault(logger)

	return nil
}

// slogLevelFromString maps the log-level flag value to a slog.Level.
func slogLevelFromString(value string) (slog.Level, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))

	switch normalized {
	case logLevelDebug:
		return slog.LevelDebug, nil
	case logLevelInfo:
		return slog.LevelInfo, nil
	case logLevelWarn:
		return slog.LevelWarn, nil
	case logLevelError:
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("invalid log-level %q: expected debug, info, warn, or error", value)
	}
}

// slogHandlerForFormat selects the appropriate slog handler for log-fmt.
//
// The handler always writes to standard error so it does not inherit any glog
// file or stderr routing configuration.
func slogHandlerForFormat(value string, opts *slog.HandlerOptions) (slog.Handler, error) {
	normalized := strings.ToLower(strings.TrimSpace(value))

	switch normalized {
	case logFormatJSON:
		return slog.NewJSONHandler(os.Stderr, opts), nil
	case logFormatLogfmt:
		return slog.NewTextHandler(os.Stderr, opts), nil
	default:
		return nil, fmt.Errorf("invalid log-fmt %q: expected json or logfmt", value)
	}
}

// logS emits a structured log record when structured logging is enabled.
//
// When structured logging is disabled, logS forwards the call to glog using
// the severity implied by level.
func logS(level slog.Level, depth int, msg string, args ...any) {
	if !structuredLoggingEnabled.Load() {
		logGlog(level, depth, msg, args...)
		return
	}

	// Prefer the configured logger, but fall back to slog.Default when needed.
	logger := structuredLogger.Load()
	if logger == nil {
		logger = slog.Default()
	}

	// Avoid record construction when the handler would discard the level.
	ctx := context.Background()
	if !logger.Enabled(ctx, level) {
		return
	}

	// Capture the caller program counter so slog can report the correct source.
	var pcs [1]uintptr
	runtime.Callers(slogCallerDepth+depth, pcs[:])

	// Build the record with the message and any structured attributes.
	record := slog.NewRecord(time.Now(), level, msg, pcs[0])
	record.Add(args...)

	// Ignore handler errors to match glog's fire-and-forget behavior.
	_ = logger.Handler().Handle(ctx, record)
}

// Enabled reports whether a log call at the provided level would be emitted.
//
// When structured logging is enabled, Enabled consults the configured slog
// logger. When structured logging is disabled, Enabled returns true for info
// and above, and uses glog verbosity to gate debug logging.
func Enabled(level slog.Level) bool {
	if structuredLoggingEnabled.Load() {
		logger := structuredLogger.Load()
		if logger == nil {
			logger = slog.Default()
		}
		return logger.Enabled(context.Background(), level)
	}

	if level < slog.LevelInfo {
		return bool(glog.V(glog.Level(1)))
	}

	return true
}

// logGlog formats a structured log call as a glog message.
//
// This path is used when structured logging is disabled.
func logGlog(level slog.Level, depth int, msg string, args ...any) {
	// Adjust depth so the reported caller skips logS and the public wrapper.
	depth += glogCallerDepth

	// Preserve the slog message as the first printed element.
	args = append([]any{msg}, args...)

	switch level {
	case slog.LevelDebug, slog.LevelInfo:
		InfoDepth(depth, args...)
	case slog.LevelWarn:
		WarningDepth(depth, args...)
	case slog.LevelError:
		ErrorDepth(depth, args...)
	default:
		InfoDepth(depth, args...)
	}
}

// InfoS logs an informational message using slog when enabled.
//
// The args are interpreted as slog attributes when structured logging is
// enabled. When structured logging is disabled, the arguments are forwarded to
// glog.
func InfoS(msg string, args ...any) {
	logS(slog.LevelInfo, 0, msg, args...)
}

// InfoSDepth logs an informational message using slog with an adjusted call depth.
//
// Depth behaves like glog.InfoDepth and skips additional call frames beyond the
// InfoSDepth wrapper.
func InfoSDepth(depth int, msg string, args ...any) {
	logS(slog.LevelInfo, depth, msg, args...)
}

// WarnS logs a warning message using slog when enabled.
//
// The args are interpreted as slog attributes when structured logging is
// enabled. When structured logging is disabled, the arguments are forwarded to
// glog.
func WarnS(msg string, args ...any) {
	logS(slog.LevelWarn, 0, msg, args...)
}

// WarnSDepth logs a warning message using slog with an adjusted call depth.
//
// Depth behaves like glog.WarningDepth and skips additional call frames beyond
// the WarnSDepth wrapper.
func WarnSDepth(depth int, msg string, args ...any) {
	logS(slog.LevelWarn, depth, msg, args...)
}

// DebugS logs a debug message using slog when enabled.
//
// The args are interpreted as slog attributes when structured logging is
// enabled. When structured logging is disabled, the arguments are forwarded to
// glog.
func DebugS(msg string, args ...any) {
	logS(slog.LevelDebug, 0, msg, args...)
}

// DebugSDepth logs a debug message using slog with an adjusted call depth.
//
// Depth behaves like glog.InfoDepth and skips additional call frames beyond the
// DebugSDepth wrapper.
func DebugSDepth(depth int, msg string, args ...any) {
	logS(slog.LevelDebug, depth, msg, args...)
}

// ErrorS logs an error message using slog when enabled.
//
// The args are interpreted as slog attributes when structured logging is
// enabled. When structured logging is disabled, the arguments are forwarded to
// glog.
func ErrorS(msg string, args ...any) {
	logS(slog.LevelError, 0, msg, args...)
}

// ErrorSDepth logs an error message using slog with an adjusted call depth.
//
// Depth behaves like glog.ErrorDepth and skips additional call frames beyond
// the ErrorSDepth wrapper.
func ErrorSDepth(depth int, msg string, args ...any) {
	logS(slog.LevelError, depth, msg, args...)
}
