package logutil

import (
	"flag"
	"path"

	"github.com/youtube/vitess/go/vt/env"
)

func init() {
	// The default location of the logs is /vt/logs.
	logDir := flag.Lookup("log_dir")
	if logDir == nil {
		panic("the logging module doesn't specify a log_dir flag")
	}
	vtLogs := path.Join(env.VtDataRoot(), "logs")
	logDir.DefValue = vtLogs
	logDir.Value.Set(vtLogs)

}
