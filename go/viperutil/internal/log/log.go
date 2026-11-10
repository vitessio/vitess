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
Package log provides logging to viper's jww log. Callers that also log to
vitess's vt/log package call it directly, with a constant message and the
values in slog attributes.
*/
package log

import (
	"os"

	jww "github.com/spf13/jwalterweatherman"
)

var (
	// TRACE logs to viper's TRACE level.
	TRACE = jww.TRACE.Printf
	// DEBUG logs to viper's DEBUG level.
	DEBUG = jww.DEBUG.Printf
	// INFO logs to viper's INFO level.
	INFO = jww.INFO.Printf
	// WARN logs to viper's WARN level.
	WARN = jww.WARN.Printf
	// ERROR logs to viper's ERROR level.
	ERROR = jww.ERROR.Printf
	// CRITICAL logs to viper at CRITICAL level, and then fatally exits.
	CRITICAL = func(format string, args ...any) {
		jww.CRITICAL.Printf(format, args...)
		os.Exit(1)
	}
)
