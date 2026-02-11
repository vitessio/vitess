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
	"strconv"
	"sync/atomic"

	"github.com/golang/glog"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/utils"
)

var (
	logStructured bool
	logLevel      string
)

func RegisterFlags(fs *pflag.FlagSet) {
	flagVal := logRotateMaxSize{
		val: strconv.FormatUint(atomic.LoadUint64(&glog.MaxSize), 10),
	}
	utils.SetFlagVar(fs, &flagVal, "log-rotate-max-size", "size in bytes at which logs are rotated (glog.MaxSize)")

	fs.BoolVar(&logStructured, "log-structured", true, "enable structured logging")
	fs.StringVar(&logLevel, "log-level", "info", "minimum log level when structured logging is enabled (debug, info, warn, error)")
}

// Init configures the logging backend. By default, a slog.JSONHandler is
// configured. If --log-structured=false is set, the deprecated glog backend
// is used instead.
func Init(fs *pflag.FlagSet) error {
	if !logStructured {
		fmt.Fprintln(os.Stderr, "WARNING: glog is deprecated and will be removed in v25")
		return nil
	}

	// Warn if any glog flags were explicitly set while structured logging is active,
	// since they have no effect.
	for _, name := range []string{"logtostderr", "alsologtostderr", "stderrthreshold", "log_dir", "log_backtrace_at", "vmodule", "v"} {
		if fs.Changed(name) {
			fmt.Fprintf(os.Stderr, "WARNING: --%s has no effect when structured logging is enabled, pass --log-structured=false to use glog flags\n", name)
		}
	}

	// Parse the level flag into an [slog.Level].
	var level slog.Level
	if err := level.UnmarshalText([]byte(logLevel)); err != nil {
		return fmt.Errorf("log: invalid --log-level %q: %w", logLevel, err)
	}

	l := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{AddSource: true, Level: level}))
	logger.Store(l)
	structured.Store(true)

	return nil
}

type logRotateMaxSize struct {
	val string
}

func (lrms *logRotateMaxSize) Set(s string) error {
	maxSize, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return err
	}

	atomic.StoreUint64(&glog.MaxSize, maxSize)
	lrms.val = s

	return nil
}

func (lrms *logRotateMaxSize) String() string {
	return lrms.val
}

func (lrms *logRotateMaxSize) Type() string {
	return "uint64"
}
