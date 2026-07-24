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

var (
	logLevel  string
	logFormat string
)

func RegisterFlags(fs *pflag.FlagSet) {
	fs.StringVar(&logLevel, "log-level", "info", "minimum log level (debug, info, warn, error)")
	fs.StringVar(&logFormat, "log-format", "json", "log output format: json for machine-readable JSON, text for human-readable colored output")
}

// Init configures the logger.
func Init(_ *pflag.FlagSet) error {
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
		return slog.New(tint.NewHandler(w, &tint.Options{
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
