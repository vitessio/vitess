package servenv

import (
	"github.com/youtube/vitess/go/vt/logutil"
)

func init() {
	OnClose(logutil.Flush)
}
