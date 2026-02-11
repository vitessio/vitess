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

// Init configures the logging backend. When --log-structured is set, a slog.JSONHandler is
// configured. Otherwise, glog is used.
func Init() error {
	if !logStructured {
		return nil
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
