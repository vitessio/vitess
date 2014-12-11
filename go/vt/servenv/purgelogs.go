package servenv

import (
	"github.com/henryanand/vitess/go/vt/logutil"
)

func init() {
	onInit(func() {
		go logutil.PurgeLogs()
	})

}
