package logutil

import (
	"fmt"
	"log"

	"github.com/golang/glog"
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
}
