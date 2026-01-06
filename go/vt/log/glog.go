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
	"os"
	"strings"

	"github.com/golang/glog"
	"github.com/rs/zerolog"
)

// glogWriter implements zerolog.LevelWriter and routes log output to glog. This allows zerolog to
// build structured JSON log entries while still outputting through glog's infrastructure when
// structured logging is disabled.
type glogWriter struct{}

// Write implements io.Writer. It delegates to WriteLevel with NoLevel.
func (w *glogWriter) Write(p []byte) (int, error) {
	return w.WriteLevel(zerolog.NoLevel, p)
}

// exitLevel is a custom level used to mimic glog's Exit, which is like Fatal except it does not
// print a stack trace.
const exitLevel = zerolog.Level(8)

// WriteLevel implements zerolog.LevelWriter. It receives the complete JSON log line from zerolog
// and routes it to the appropriate glog function based on log level.
func (w *glogWriter) WriteLevel(level zerolog.Level, p []byte) (int, error) {
	// Remove trailing newline to avoid double newlines since glog adds its own.
	s := strings.TrimSuffix(string(p), "\n")

	const depth = 4

	switch level {
	case zerolog.DebugLevel, zerolog.TraceLevel, zerolog.InfoLevel, zerolog.NoLevel:
		glog.InfoDepth(depth, s)
	case zerolog.WarnLevel:
		glog.WarningDepth(depth, s)
	case zerolog.ErrorLevel:
		glog.ErrorDepth(depth, s)
	case zerolog.FatalLevel:
		glog.FatalDepth(depth, s)
	case zerolog.PanicLevel:
		glog.ErrorDepth(depth, s)
		panic(s)
	case exitLevel:
		glog.ExitDepth(depth, s)
	default:
		glog.InfoDepth(depth, s)
	}

	return len(p), nil
}

// glogInfo logs at info level, routing to structured logging or glog based on mode.
func glogInfo(depth int, args ...any) {
	if structuredLogging {
		SInfo(depth + 2).Msg(fmt.Sprint(args...))
	} else {
		glog.InfoDepth(depth+1, args...)
	}
}

// glogInfof logs at info level with formatting, routing to structured logging or glog based on
// mode.
func glogInfof(depth int, format string, args ...any) {
	if structuredLogging {
		SInfo(depth+2).Msgf(format, args...)
	} else {
		glog.InfoDepthf(depth+1, format, args...)
	}
}

// glogWarning logs at warning level, routing to structured logging or glog based on mode.
func glogWarning(depth int, args ...any) {
	if structuredLogging {
		SWarn(depth + 2).Msg(fmt.Sprint(args...))
	} else {
		glog.WarningDepth(depth+1, args...)
	}
}

// glogWarningf logs at warning level with formatting, routing to structured logging or glog based
// on mode.
func glogWarningf(depth int, format string, args ...any) {
	if structuredLogging {
		SWarn(depth+2).Msgf(format, args...)
	} else {
		glog.WarningDepth(depth+1, fmt.Sprintf(format, args...))
	}
}

// glogError logs at error level, routing to structured logging or glog based on mode.
func glogError(depth int, args ...any) {
	if structuredLogging {
		SError(depth + 2).Msg(fmt.Sprint(args...))
	} else {
		glog.ErrorDepth(depth+1, args...)
	}
}

// glogErrorf logs at error level with formatting, routing to structured logging or glog based on
// mode.
func glogErrorf(depth int, format string, args ...any) {
	if structuredLogging {
		SError(depth+2).Msgf(format, args...)
	} else {
		glog.ErrorDepth(depth+1, fmt.Sprintf(format, args...))
	}
}

// glogExit logs at fatal level and exits, routing to structured logging or glog based on mode.
func glogExit(depth int, args ...any) {
	if structuredLogging {
		SExit(depth + 2).Msg(fmt.Sprint(args...))
		os.Exit(1)
	} else {
		glog.ExitDepth(depth+1, args...)
	}
}

// glogExitf logs at fatal level with formatting and exits, routing to structured logging or glog
// based on mode.
func glogExitf(depth int, format string, args ...any) {
	if structuredLogging {
		SExit(depth+2).Msgf(format, args...)
		os.Exit(1)
	} else {
		glog.ExitDepth(depth+1, fmt.Sprintf(format, args...))
	}
}

// glogFatal logs at fatal level and terminates, routing to structured logging or glog based on
// mode.
func glogFatal(depth int, args ...any) {
	if structuredLogging {
		SFatal(depth + 2).Msg(fmt.Sprint(args...))
	} else {
		glog.FatalDepth(depth+1, args...)
	}
}

// glogFatalf logs at fatal level with formatting and terminates, routing to structured logging or
// glog based on mode.
func glogFatalf(depth int, format string, args ...any) {
	if structuredLogging {
		SFatal(depth+2).Msgf(format, args...)
	} else {
		glog.FatalDepth(depth+1, fmt.Sprintf(format, args...))
	}
}

// glogFlush flushes any buffered log entries.
func glogFlush() {
	if !structuredLogging {
		glog.Flush()
	}
}
