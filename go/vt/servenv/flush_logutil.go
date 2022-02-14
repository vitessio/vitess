package servenv

import (
	"vitess.io/vitess/go/vt/logutil"
)

func init() {
	OnClose(logutil.Flush)
}
