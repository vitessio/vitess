package logutil

import (
	"vitess.io/vitess/go/vt/log"
)

func init() {
	OnFlush(log.Flush)
}
