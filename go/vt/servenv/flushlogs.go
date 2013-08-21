package servenv

import (
	"fmt"
	"net/http"

	log "github.com/golang/glog"
)

func init() {
	onInit(func() {
		http.HandleFunc("/debug/flushlogs", func(w http.ResponseWriter, r *http.Request) {
			log.Flush()
			fmt.Fprint(w, "flushed")
		})

	})
	OnClose(log.Flush)
}
