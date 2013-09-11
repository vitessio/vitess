// package logutil provides some utilities for logging using glog and
// redirects the stdlib logging to glog.

package logutil

import (
	"flag"
	stdlog "log"

	log "github.com/golang/glog"
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

// GetSubprocessFlags returns the list of flags to use to have subprocesses
// log in the same directory as the current process.
func GetSubprocessFlags() []string {
	logDir := flag.Lookup("log_dir")
	if logDir == nil {
		panic("the logging module doesn't specify a log_dir flag")
	}
	return []string{"-log_dir", logDir.Value.String()}
}
