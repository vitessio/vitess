package servenv

import (
	"fmt"
	"net/http"

	"github.com/youtube/vitess/go/vt/logutil"
)

func init() {
	onInit(func() {
		http.HandleFunc("/debug/flushlogs", func(w http.ResponseWriter, r *http.Request) {
			logutil.Flush()
			fmt.Fprint(w, "flushed")
		})
	})
}
