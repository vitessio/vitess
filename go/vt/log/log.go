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

// You can modify this file to hook up a different logging library instead of glog.
// If you adapt to a different logging framework, you may need to use that
// framework's equivalent of *Depth() functions so the file and line number printed
// point to the real caller instead of your adapter function.

package log

import (
	"strconv"
	"sync/atomic"

	"github.com/golang/glog"
	"github.com/spf13/pflag"

	"vitess.io/vitess/go/vt/utils"
)

// Level is used with V() to test log verbosity.
type Level = glog.Level

var (
	// V quickly checks if the logging verbosity meets a threshold.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SDebug() or log.STrace().
	V = glog.V

	// Flush ensures any pending I/O is written.
	//
	// Note: This function is deprecated. Use the new structured logging API. Structured logging does not require explicit flushing.
	Flush = glogFlush

	// Info formats arguments like fmt.Print.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SInfo().Msg().
	Info = func(args ...any) { glogInfo(1, args...) }

	// Infof formats arguments like fmt.Printf.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SInfo().Msgf().
	Infof = func(format string, args ...any) { glogInfof(1, format, args...) }

	// InfoDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SInfo().Msg().
	InfoDepth = func(depth int, args ...any) { glogInfo(depth+1, args...) }

	// InfofDepth formats arguments like fmt.Printf and uses depth to choose which call frame to log.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SInfo().Msgf().
	InfofDepth = func(depth int, format string, args ...any) { glogInfof(depth+1, format, args...) }

	// Warning formats arguments like fmt.Print.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SWarn().Msg().
	Warning = func(args ...any) { glogWarning(1, args...) }

	// Warningf formats arguments like fmt.Printf.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SWarn().Msgf().
	Warningf = func(format string, args ...any) { glogWarningf(1, format, args...) }

	// WarningDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SWarn().Msg().
	WarningDepth = func(depth int, args ...any) { glogWarning(depth+1, args...) }

	// WarningfDepth formats arguments like fmt.Printf and uses depth to choose which call frame to log.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SWarn().Msgf().
	WarningfDepth = func(depth int, format string, args ...any) { glogWarningf(depth+1, format, args...) }

	// Error formats arguments like fmt.Print.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SError().Msg().
	Error = func(args ...any) { glogError(1, args...) }

	// Errorf formats arguments like fmt.Printf.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SError().Msgf().
	Errorf = func(format string, args ...any) { glogErrorf(1, format, args...) }

	// ErrorDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SError().Msg().
	ErrorDepth = func(depth int, args ...any) { glogError(depth+1, args...) }

	// ErrorfDepth formats arguments like fmt.Printf and uses depth to choose which call frame to log.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SError().Msgf().
	ErrorfDepth = func(depth int, format string, args ...any) { glogErrorf(depth+1, format, args...) }

	// Exit formats arguments like fmt.Print.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SFatal().Msg().
	Exit = func(args ...any) { glogExit(1, args...) }

	// Exitf formats arguments like fmt.Printf.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SFatal().Msgf().
	Exitf = func(format string, args ...any) { glogExitf(1, format, args...) }

	// ExitDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SFatal().Msg().
	ExitDepth = func(depth int, args ...any) { glogExit(depth+1, args...) }

	// ExitfDepth formats arguments like fmt.Printf and uses depth to choose which call frame to log.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SFatal().Msgf().
	ExitfDepth = func(depth int, format string, args ...any) { glogExitf(depth+1, format, args...) }

	// Fatal formats arguments like fmt.Print.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SFatal().Msg().
	Fatal = func(args ...any) { glogFatal(1, args...) }

	// Fatalf formats arguments like fmt.Printf.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SFatal().Msgf().
	Fatalf = func(format string, args ...any) { glogFatalf(1, format, args...) }

	// FatalDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SFatal().Msg().
	FatalDepth = func(depth int, args ...any) { glogFatal(depth+1, args...) }

	// FatalfDepth formats arguments like fmt.Printf and uses depth to choose which call frame to log.
	//
	// Note: This function is deprecated. Use the new structured logging API: log.SFatal().Msgf().
	FatalfDepth = func(depth int, format string, args ...any) { glogFatalf(depth+1, format, args...) }
)

// RegisterFlags installs log flags on the given FlagSet.
//
// `go/cmd/*` entrypoints should either use servenv.ParseFlags(WithArgs)? which calls this function,
// or call this function directly before parsing command-line arguments.
func RegisterFlags(fs *pflag.FlagSet) {
	flagVal := logRotateMaxSize{
		val: strconv.FormatUint(atomic.LoadUint64(&glog.MaxSize), 10),
	}
	utils.SetFlagVar(fs, &flagVal, "log-rotate-max-size", "size in bytes at which logs are rotated (glog.MaxSize)")

	fs.BoolVar(&structuredLogging, "structured-logging", true, "Enable structured logging")
	fs.StringVar(&structuredLoggingLevel, "structured-logging-level", "info", "Log level for structured logging: trace, debug, info, warn, error, fatal, panic, disabled")
	fs.BoolVar(&structuredLoggingPretty, "structured-logging-pretty", false, "Enable pretty console output for structured logging")
	fs.StringVar(&structuredLoggingFile, "structured-logging-file", "", "Path to log file for structured logging with rotation (empty means stdout)")
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

func (pl *PrefixedLogger) V(level glog.Level) glog.Verbose {
	return V(level)
}

func (pl *PrefixedLogger) Flush() {
	Flush()
}

func (pl *PrefixedLogger) Info(args ...any) {
	args = append([]interface{}{pl.prefix}, args...)
	InfoDepth(1, args...)
}

func (pl *PrefixedLogger) Infof(format string, args ...any) {
	InfofDepth(1, pl.prefix+format, args...)
}

func (pl *PrefixedLogger) InfoDepth(depth int, args ...any) {
	args = append([]interface{}{pl.prefix}, args...)
	InfoDepth(depth+1, args...)
}

func (pl *PrefixedLogger) Warning(args ...any) {
	args = append([]interface{}{pl.prefix}, args...)
	WarningDepth(1, args...)
}

func (pl *PrefixedLogger) Warningf(format string, args ...any) {
	WarningfDepth(1, pl.prefix+format, args...)
}

func (pl *PrefixedLogger) WarningDepth(depth int, args ...any) {
	args = append([]interface{}{pl.prefix}, args...)
	WarningDepth(depth+1, args...)
}

func (pl *PrefixedLogger) Error(args ...any) {
	args = append([]interface{}{pl.prefix}, args...)
	ErrorDepth(1, args...)
}

func (pl *PrefixedLogger) Errorf(format string, args ...any) {
	ErrorfDepth(1, pl.prefix+format, args...)
}

func (pl *PrefixedLogger) ErrorDepth(depth int, args ...any) {
	args = append([]interface{}{pl.prefix}, args...)
	ErrorDepth(depth+1, args...)
}

func (pl *PrefixedLogger) Exit(args ...any) {
	args = append([]interface{}{pl.prefix}, args...)
	ExitDepth(1, args...)
}

func (pl *PrefixedLogger) Exitf(format string, args ...any) {
	ExitfDepth(1, pl.prefix+format, args...)
}

func (pl *PrefixedLogger) ExitDepth(depth int, args ...any) {
	args = append([]interface{}{pl.prefix}, args...)
	ExitDepth(depth+1, args...)
}

func (pl *PrefixedLogger) Fatal(args ...any) {
	args = append([]interface{}{pl.prefix}, args...)
	FatalDepth(1, args...)
}

func (pl *PrefixedLogger) Fatalf(format string, args ...any) {
	FatalfDepth(1, pl.prefix+format, args...)
}

func (pl *PrefixedLogger) FatalDepth(depth int, args ...any) {
	args = append([]interface{}{pl.prefix}, args...)
	FatalDepth(depth+1, args...)
}
