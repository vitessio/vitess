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
	"log/slog"
	"os"
	"testing"

	"github.com/lmittmann/tint"
	"github.com/mattn/go-isatty"
	"github.com/spf13/pflag"
)

// removedFlagMessage is the deprecation message of the logging flags that have no effect.
const removedFlagMessage = "it has no effect and will be removed in v26"

var (
	logLevel  = "info"
	logFormat = "json"

	// logStructured is the value of the removed --log-structured flag. Init fails when it is false.
	logStructured = true
)

// RegisterFlags registers the logging flags on fs.
func RegisterFlags(fs *pflag.FlagSet) {
	fs.StringVar(&logLevel, "log-level", logLevel, "minimum log level (debug, info, warn, error)")
	fs.StringVar(&logFormat, "log-format", logFormat, "log output format: json for machine-readable JSON, text for human-readable colored output")

	registerRemovedFlags(fs)
}

// registerRemovedFlags registers the removed logging flags that v24 did not mark as deprecated. The flags let old
// startup arguments parse for one more release.
func registerRemovedFlags(fs *pflag.FlagSet) {
	fs.BoolVar(&logStructured, "log-structured", logStructured, "")
	_ = fs.MarkDeprecated("log-structured", removedFlagMessage)

	for _, name := range []string{"log-rotate-max-size", "keep-logs", "keep-logs-by-mtime", "purge-logs-interval"} {
		fs.String(name, "", "")
		_ = fs.MarkDeprecated(name, removedFlagMessage)
	}
}

// RegisterRemovedClientFlags registers the removed glog flags that vtctldclient and vtctlclient accepted without a
// deprecation warning in v24. The flags let old scripts run for one more release.
func RegisterRemovedClientFlags(fs *pflag.FlagSet) {
	for _, name := range []string{"logtostderr", "alsologtostderr"} {
		fs.Bool(name, false, "")
		_ = fs.MarkDeprecated(name, removedFlagMessage)
	}
}

// Init configures the logger.
func Init() error {
	// Fail on --log-structured=false. The caller expects glog log files, which Vitess does not write.
	if !logStructured {
		return fmt.Errorf("log: --log-structured=false is not supported, glog was removed in v25")
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(logLevel)); err != nil {
		return fmt.Errorf("log: invalid --log-level %q: %w", logLevel, err)
	}

	l := newLogger(level)
	logger.Store(l)

	return nil
}

// newLogger creates a new structured logger. When the log format is "text", or we detect
// we're running tests, logs are outputted in a human-readable format (and optionally colored).
// Otherwise, or when the log format is "json", logs are outputted as machine-readable JSON.
func newLogger(level slog.Level) *slog.Logger {
	if logFormat == "text" || testing.Testing() {
		w := os.Stderr
		return slog.New(tint.NewTextHandler(w, &tint.Options{
			AddSource:  true,
			Level:      level,
			TimeFormat: "2006-01-02 15:04:05.000 MST",

			// When running tests, colored output will be disabled since it's not printed directly to the terminal
			// (goes through the Go testing "pipeline"). So we also enable coloring if we're in tests to for
			// improved readability.
			NoColor: !isatty.IsTerminal(w.Fd()) && !testing.Testing(),
		}))
	}

	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{AddSource: true, Level: level}))
}
