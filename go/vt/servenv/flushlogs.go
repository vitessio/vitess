package servenv

import (
	"fmt"
	"net/http"

	"vitess.io/vitess/go/vt/logutil"
)

func init() {
	OnInit(func() {
		http.HandleFunc("/debug/flushlogs", func(w http.ResponseWriter, r *http.Request) {
			logutil.Flush()
			fmt.Fprint(w, "flushed")
		})
	})
}
