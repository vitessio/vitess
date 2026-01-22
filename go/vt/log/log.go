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

// Package log provides a thin adapter around glog with optional structured
// logging via slog.
//
// By default, it uses glog and its flags. Structured logging is enabled only
// when the --log-fmt flag is explicitly set.
package log

import (
	"strconv"
	"sync/atomic"

	"github.com/golang/glog"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/utils"
)

// Flush ensures any pending I/O is written.
var Flush = glog.Flush

// Level is the glog verbosity level.
type Level = glog.Level

// RegisterFlags installs log flags on the given FlagSet.
func RegisterFlags(fs *pflag.FlagSet) {
	flagVal := logRotateMaxSize{
		val: strconv.FormatUint(atomic.LoadUint64(&glog.MaxSize), 10),
	}
	utils.SetFlagVar(fs, &flagVal, "log-rotate-max-size", "size in bytes at which logs are rotated (glog.MaxSize)")

	// Structured logging flags.
	utils.SetFlagStringVar(fs, &logFormat, "log-fmt", "json", "format for structured logging output: json or logfmt")
	utils.SetFlagStringVar(fs, &logLevel, "log-level", "info", "minimum structured logging level: info, warn, debug, or error")
}

// logRotateMaxSize implements pflag.Value and is used to
// try and provide thread-safe access to glog.MaxSize.
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
