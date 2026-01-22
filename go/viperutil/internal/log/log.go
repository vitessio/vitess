/*
Copyright 2023 The Vitess Authors.

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

/*
Package log provides dual-use logging between vitess's vt/log package and
viper's jww log.
*/
package log

import (
	"fmt"
	"os"

	jww "github.com/spf13/jwalterweatherman"

	"vitess.io/vitess/go/vt/log"
)

var (
	jwwlog = func(printer interface {
		Printf(format string, args ...any)
	}, vtlogger func(format string, args ...any),
	) func(format string, args ...any) {
		switch vtlogger {
		case nil:
			return printer.Printf
		default:
			return func(format string, args ...any) {
				printer.Printf(format, args...)
				vtlogger(format, args...)
			}
		}
	}

	// TRACE logs to viper's TRACE level, and nothing to vitess logs.
	TRACE = jwwlog(jww.TRACE, nil)
	// DEBUG logs to viper's DEBUG level, and nothing to vitess logs.
	DEBUG = jwwlog(jww.DEBUG, nil)
	// INFO logs to viper and vitess at INFO levels.
	INFO = jwwlog(jww.INFO, infof)
	// WARN logs to viper and vitess at WARN/WARNING levels.
	WARN = jwwlog(jww.WARN, warnf)
	// ERROR logs to viper and vitess at ERROR levels.
	ERROR = jwwlog(jww.ERROR, errorf)
	// CRITICAL logs to viper at CRITICAL level, and then fatally logs to
	// vitess, exiting the process.
	CRITICAL = jwwlog(jww.CRITICAL, criticalf)
)

// infof formats an info message and emits it through the structured logger.
func infof(format string, args ...any) {
	log.Info(fmt.Sprintf(format, args...))
}

// warnf formats a warning message and emits it through the structured logger.
func warnf(format string, args ...any) {
	log.Warn(fmt.Sprintf(format, args...))
}

// errorf formats an error message and emits it through the structured logger.
func errorf(format string, args ...any) {
	log.Error(fmt.Sprintf(format, args...))
}

// criticalf formats an error message and terminates the process.
//
// It mirrors the behavior of log.Fatalf, but uses ErrorS and os.Exit
// explicitly so callers do not depend on the removed Fatalf wrapper.
func criticalf(format string, args ...any) {
	log.Error(fmt.Sprintf(format, args...))
	os.Exit(1)
}
