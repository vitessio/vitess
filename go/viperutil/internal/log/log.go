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
	jww "github.com/spf13/jwalterweatherman"

	"vitess.io/vitess/go/vt/log"
)

var (
	jwwlog = func(printer interface {
		Printf(format string, args ...any)
	}, vtlogger func(format string, args ...any)) func(format string, args ...any) {
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
	INFO = jwwlog(jww.INFO, log.Infof)
	// WARN logs to viper and vitess at WARN/WARNING levels.
	WARN = jwwlog(jww.WARN, log.Warningf)
	// ERROR logs to viper and vitess at ERROR levels.
	ERROR = jwwlog(jww.ERROR, log.Errorf)
	// CRITICAL logs to viper at CRITICAL level, and then fatally logs to
	// vitess, exiting the process.
	CRITICAL = jwwlog(jww.CRITICAL, log.Fatalf)
)
