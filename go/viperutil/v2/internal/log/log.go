/*
Copyright 2022 The Vitess Authors.

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

	TRACE    = jwwlog(jww.TRACE, nil)
	DEBUG    = jwwlog(jww.DEBUG, nil)
	INFO     = jwwlog(jww.INFO, log.Infof)
	WARN     = jwwlog(jww.WARN, log.Warningf)
	ERROR    = jwwlog(jww.ERROR, log.Errorf)
	CRITICAL = jwwlog(jww.CRITICAL, log.Fatalf)
)
