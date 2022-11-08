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
