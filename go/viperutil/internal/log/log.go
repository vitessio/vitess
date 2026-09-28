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
	"log/slog"
	"os"

	jww "github.com/spf13/jwalterweatherman"

	vtlog "vitess.io/vitess/go/vt/log"
)

var (
	jwwlog = func(printer interface {
		Printf(format string, args ...any)
	}, vtlogger func(string, ...slog.Attr),
	) func(format string, args ...any) {
		switch vtlogger {
		case nil:
			return printer.Printf
		default:
			return func(format string, args ...any) {
				printer.Printf(format, args...)
				vtlogger(fmt.Sprintf(format, args...))
			}
		}
	}

	// TRACE logs to viper's TRACE level, and nothing to vitess logs.
	TRACE = jwwlog(jww.TRACE, nil)
	// DEBUG logs to viper's DEBUG level, and nothing to vitess logs.
	DEBUG = jwwlog(jww.DEBUG, nil)
	// INFO logs to viper and vitess at INFO levels.
	INFO = jwwlog(jww.INFO, vtlog.Info)
	// WARN logs to viper and vitess at WARN levels.
	WARN = jwwlog(jww.WARN, vtlog.Warn)
	// ERROR logs to viper and vitess at ERROR levels.
	ERROR = jwwlog(jww.ERROR, vtlog.Error)
	// CRITICAL logs to viper at CRITICAL level, and then fatally exits.
	CRITICAL = jwwlog(jww.CRITICAL, func(msg string, attrs ...slog.Attr) {
		vtlog.Error(msg, attrs...)
		os.Exit(1)
	})
)
