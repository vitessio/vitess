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
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	"github.com/rs/zerolog"
	"github.com/spf13/pflag"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Package-level flag variables for structured logging configuration.
var (
	structuredLogging       bool
	structuredLoggingLevel  string
	structuredLoggingPretty bool
	structuredLoggingFile   string
)

// StructuredLogging returns true if structured JSON logging is enabled.
func StructuredLogging() bool {
	return structuredLogging
}

// slogger is the default structured logger. It defaults to glog unless structured logging is
// enabled with --structured-logging.
var slogger = zerolog.New(&glogWriter{}).With().Timestamp().Logger()

// Init configures the global logger based on the --structured-logging flag. In structured mode,
// logs are written as JSON to stdout or to a file with rotation. In glog mode, zerolog builds JSON
// but routes it through glog for output.
func Init(fs *pflag.FlagSet) error {
	if err := validateFlags(fs); err != nil {
		return fmt.Errorf("%w", err)
	}

	// exitLevel is a custom level used to mimic glog's Exit, which is like Fatal except it does
	// not print a stack trace. By default, zerolog will print the number since it is an unknown level
	// (i.e. {"level": 8}). We override that here so that it prints out "fatal" instead.
	zerolog.LevelFieldMarshalFunc = func(l zerolog.Level) string {
		if l == exitLevel {
			return "fatal"
		}

		return l.String()
	}

	if structuredLogging {
		level, err := zerolog.ParseLevel(structuredLoggingLevel)
		if err != nil {
			return fmt.Errorf("invalid --structured-logging-level %q: %w", structuredLoggingLevel, err)
		}
		zerolog.SetGlobalLevel(level)

		// Build the appropriate output writer depending on the configured flags.
		output := structuredLoggingOutput()

		slogger = zerolog.New(output).With().Timestamp().Logger()
		return nil
	}

	slogger = zerolog.New(&glogWriter{}).With().Timestamp().Logger()
	return nil
}

// structuredLoggingOutput returns the appropriate io.Writer for structured logging based on the
// configured flags. If a file path is specified, it returns a lumberjack logger with rotation,
// configured with the --log-rotate-max-size flag. Otherwise, it returns stdout.
func structuredLoggingOutput() io.Writer {
	var out io.Writer = os.Stdout
	if structuredLoggingFile != "" {
		// Convert glog.MaxSize from bytes to megabytes. Lumberjack requires at least 1 MB.
		maxSizeMB := int(atomic.LoadUint64(&glog.MaxSize) / (1024 * 1024))
		if maxSizeMB < 1 {
			maxSizeMB = 1
		}

		out = &lumberjack.Logger{
			Filename: structuredLoggingFile,
			MaxSize:  maxSizeMB,
		}
	}

	if structuredLoggingPretty {
		return zerolog.ConsoleWriter{
			Out:        out,
			TimeFormat: time.RFC3339Nano,
		}
	}

	return out
}

// validateFlags checks for conflicting flag combinations. When structured logging is enabled, glog
// flags should not be set. When structured logging is disabled, structured logging flags should not
// be set.
func validateFlags(fs *pflag.FlagSet) error {
	if structuredLogging {
		// If structured logging is enabled, glog flags should not be set.
		glogFlags := []string{"logtostderr", "alsologtostderr", "stderrthreshold", "log_dir", "log_backtrace_at", "vmodule", "v"}

		for _, name := range glogFlags {
			if fs.Changed(name) {
				return fmt.Errorf("cannot use --%s with --structured-logging; glog flags are ignored when structured logging is enabled", name)
			}
		}

		return nil
	}

	// If structured logging is not enabled, other structured logging flags should not be set.
	structuredFlags := []string{
		"structured-logging-level",
		"structured-logging-pretty",
		"structured-logging-file",
	}

	for _, name := range structuredFlags {
		if fs.Changed(name) {
			return fmt.Errorf("--%s requires --structured-logging", name)
		}
	}

	return nil
}

// NewStructuredLogger returns a zerolog logger configured for the given mode. When structured is
// false, logs are routed through glog. When structured is true, logs are written directly to
// stdout.
func NewStructuredLogger(structured bool) zerolog.Logger {
	if structured {
		return zerolog.New(os.Stdout).With().Timestamp().Logger()
	}

	return zerolog.New(&glogWriter{}).With().Timestamp().Logger()
}

// callerSkip returns the skip value to use, defaulting to 1 if not provided.
func callerSkip(skip []int) int {
	if len(skip) > 0 {
		return skip[0]
	}
	return 1
}

// STrace starts a new message with trace level. Optional skip adjusts caller frame depth.
func STrace(skip ...int) *zerolog.Event { return slogger.Trace().Caller(callerSkip(skip)) }

// SDebug starts a new message with debug level. Optional skip adjusts caller frame depth.
func SDebug(skip ...int) *zerolog.Event { return slogger.Debug().Caller(callerSkip(skip)) }

// SInfo starts a new message with info level. Optional skip adjusts caller frame depth.
func SInfo(skip ...int) *zerolog.Event { return slogger.Info().Caller(callerSkip(skip)) }

// SWarn starts a new message with warn level. Optional skip adjusts caller frame depth.
func SWarn(skip ...int) *zerolog.Event { return slogger.Warn().Caller(callerSkip(skip)) }

// SError starts a new message with error level. Optional skip adjusts caller frame depth.
func SError(skip ...int) *zerolog.Event { return slogger.Error().Caller(callerSkip(skip)) }

// SExit starts a new message with exit level. Optional skip adjusts caller frame depth.
// This is a custom level used to mimic glog's Exit, which is like Fatal except it does not
// print a stack trace.
func SExit(skip ...int) *zerolog.Event {
	return slogger.WithLevel(zerolog.Level(exitLevel)).Caller(callerSkip(skip))
}

// SFatal starts a new message with fatal level. Optional skip adjusts caller frame depth.
func SFatal(skip ...int) *zerolog.Event { return slogger.Fatal().Caller(callerSkip(skip)) }

// SPanic starts a new message with panic level. Optional skip adjusts caller frame depth.
func SPanic(skip ...int) *zerolog.Event { return slogger.Panic().Caller(callerSkip(skip)) }

// SLog starts a new message with no level. Optional skip adjusts caller frame depth.
func SLog(skip ...int) *zerolog.Event { return slogger.Log().Caller(callerSkip(skip)) }

// SErr starts a new message with error level with err as a field if not nil or with info level if
// err is nil. Optional skip adjusts caller frame depth.
func SErr(err error, skip ...int) *zerolog.Event { return slogger.Err(err).Caller(callerSkip(skip)) }

// SWithLevel starts a new message with the specified level. Optional skip adjusts caller frame
// depth.
func SWithLevel(level zerolog.Level, skip ...int) *zerolog.Event {
	return slogger.WithLevel(level).Caller(callerSkip(skip))
}

// SWith creates a child logger with the field added to its context.
func SWith() zerolog.Context { return slogger.With() }

// SLevel creates a child logger with the minimum accepted level set to level.
func SLevel(lvl zerolog.Level) zerolog.Logger { return slogger.Level(lvl) }

// SSample returns a logger with the s sampler.
func SSample(s zerolog.Sampler) zerolog.Logger { return slogger.Sample(s) }

// SHook returns a logger with the hooks.
func SHook(hooks ...zerolog.Hook) zerolog.Logger { return slogger.Hook(hooks...) }
