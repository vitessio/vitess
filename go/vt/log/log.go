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

// This package provides a thin adapter around glog with optional structured
// logging via slog.
//
// By default, it uses glog and its flags. Structured logging is enabled only
// when the log-fmt flag is explicitly set. When slog is enabled, the package
// uses slog handlers directly and does not depend on glog output configuration.
//
// If you adapt to a different logging framework, you may need to use that
// framework's equivalent of *Depth() functions so the file and line number
// printed point to the real caller instead of your adapter function.

package log

import (
	"fmt"
	"strconv"
	"sync/atomic"

	"github.com/golang/glog"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/utils"
)

// Level is the glog verbosity level.
type Level = glog.Level

var (
	// Flush ensures any pending I/O is written.
	Flush = glog.Flush

	// Info formats arguments like fmt.Print.
	Info = glog.Info
	// Infof formats arguments like fmt.Printf.
	Infof = glog.Infof
	// InfoDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	InfoDepth = glog.InfoDepth

	// Warning formats arguments like fmt.Print.
	Warning = glog.Warning
	// Warningf formats arguments like fmt.Printf.
	Warningf = glog.Warningf
	// WarningDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	WarningDepth = glog.WarningDepth

	// Error formats arguments like fmt.Print.
	Error = glog.Error
	// Errorf formats arguments like fmt.Printf.
	Errorf = glog.Errorf
	// ErrorDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	ErrorDepth = glog.ErrorDepth
)

// RegisterFlags installs log flags on the given FlagSet.
//
// `go/cmd/*` entrypoints should either use servenv.ParseFlags(WithArgs), which
// calls this function, or call this function directly before parsing
// command-line arguments. When using structured logging, callers must invoke
// Init with the parsed FlagSet so these settings can be honored.
func RegisterFlags(fs *pflag.FlagSet) {
	flagVal := logRotateMaxSize{
		val: strconv.FormatUint(atomic.LoadUint64(&glog.MaxSize), 10),
	}
	utils.SetFlagVar(fs, &flagVal, "log-rotate-max-size", "size in bytes at which logs are rotated (glog.MaxSize)")

	utils.SetFlagStringVar(fs, &logFormat, "log-fmt", logFormat, "format for structured logging output: json or logfmt")
	utils.SetFlagStringVar(fs, &logLevel, "log-level", logLevel, "minimum structured logging level: info, warn, debug, or error")
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

type PrefixedLogger struct {
	prefix string
}

func NewPrefixedLogger(prefix string) *PrefixedLogger {
	return &PrefixedLogger{prefix: prefix + ": "}
}

func (pl *PrefixedLogger) Flush() {
	Flush()
}

func (pl *PrefixedLogger) Info(args ...any) {
	args = append([]any{pl.prefix}, args...)
	InfoS(fmt.Sprint(args...))
}

func (pl *PrefixedLogger) Infof(format string, args ...any) {
	args = append([]any{pl.prefix}, args...)
	InfoS(fmt.Sprintf("%s"+format, args...))
}

func (pl *PrefixedLogger) InfoDepth(depth int, args ...any) {
	args = append([]any{pl.prefix}, args...)
	InfoSDepth(depth, fmt.Sprint(args...))
}

func (pl *PrefixedLogger) Warning(args ...any) {
	args = append([]any{pl.prefix}, args...)
	WarnS(fmt.Sprint(args...))
}

func (pl *PrefixedLogger) Warningf(format string, args ...any) {
	args = append([]any{pl.prefix}, args...)
	WarnS(fmt.Sprintf("%s"+format, args...))
}

func (pl *PrefixedLogger) WarningDepth(depth int, args ...any) {
	args = append([]any{pl.prefix}, args...)
	WarnSDepth(depth, fmt.Sprint(args...))
}

func (pl *PrefixedLogger) Error(args ...any) {
	args = append([]any{pl.prefix}, args...)
	ErrorS(fmt.Sprint(args...))
}

func (pl *PrefixedLogger) Errorf(format string, args ...any) {
	args = append([]any{pl.prefix}, args...)
	ErrorS(fmt.Sprintf("%s"+format, args...))
}

func (pl *PrefixedLogger) ErrorDepth(depth int, args ...any) {
	args = append([]any{pl.prefix}, args...)
	ErrorSDepth(depth, fmt.Sprint(args...))
}
