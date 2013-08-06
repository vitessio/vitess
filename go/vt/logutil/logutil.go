package logutil

import (
	"flag"
	"fmt"
	"log"
	"path"

	"github.com/golang/glog"
	"github.com/youtube/vitess/go/vt/env"
)

type logShim struct{}

func (shim *logShim) Write(buf []byte) (n int, err error) {
	glog.Warning(string(buf))
	fmt.Println(string(buf))
	return len(buf), nil
}

func init() {
	log.SetPrefix("log: ")
	log.SetFlags(0)
	log.SetOutput(new(logShim))

	// The default location of the logs is /vt/logs.
	logDir := flag.Lookup("log_dir")
	if logDir == nil {
		panic("the logging module doesn't specify a log_dir flag")
	}
	vtLogs := path.Join(env.VtDataRoot(), "logs")
	logDir.DefValue = vtLogs
	logDir.Value.Set(vtLogs)
}
