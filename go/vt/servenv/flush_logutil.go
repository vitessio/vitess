package servenv

import (
	"github.com/henryanand/vitess/go/vt/logutil"
)

func init() {
	OnClose(logutil.Flush)
}
