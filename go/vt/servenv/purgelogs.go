package servenv

import (
	"github.com/youtube/vitess/go/vt/logutil"
)

func init() {
	onInit(func() {
		go logutil.PurgeLogs()
	})

}
