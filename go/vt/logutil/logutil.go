package logutil

import (
	"flag"
	stdlog "log"
	"path"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/env"
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

	// The default location of the logs is /vt/logs.
	logDir := flag.Lookup("log_dir")
	if logDir == nil {
		panic("the logging module doesn't specify a log_dir flag")
	}
	vtLogs := path.Join(env.VtDataRoot(), "logs")
	logDir.DefValue = vtLogs
	logDir.Value.Set(vtLogs)
}
