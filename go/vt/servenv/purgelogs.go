package servenv

import (
	"github.com/gitql/vitess/go/vt/logutil"
)

func init() {
	onInit(func() {
		go logutil.PurgeLogs()
	})

}
