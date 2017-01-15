package servenv

import (
	"github.com/gitql/vitess/go/vt/logutil"
)

func init() {
	OnClose(logutil.Flush)
}
