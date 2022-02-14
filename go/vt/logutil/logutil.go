// package logutil provides some utilities for logging using glog and
// redirects the stdlib logging to glog.

package logutil

import (
	stdlog "log"

	"vitess.io/vitess/go/vt/log"
)

type logShim struct{}

func (shim *logShim) Write(buf []byte) (n int, err error) {
	log.Info(string(buf))
	return len(buf), nil
}

func init() {
	stdlog.SetPrefix("log: ")
	stdlog.SetFlags(0)
	stdlog.SetOutput(new(logShim))
}
