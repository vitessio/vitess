package trace

import (
	"io"

	"vitess.io/vitess/go/vt/log"
)

// LogErrorsWhenClosing will close the provided Closer, and log any errors it generates
func LogErrorsWhenClosing(in io.Closer) func() {
	return func() {
		err := in.Close()
		if err != nil {
			log.Error(err)
		}
	}
}
