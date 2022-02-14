package servenv

import (
	"vitess.io/vitess/go/vt/logutil"
)

func init() {
	OnInit(func() {
		go logutil.PurgeLogs()
	})

}
