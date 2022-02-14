// Package vtctld contains all the code to expose a vtctld server
// based on the provided topo.Server.
package vtctld

import (
	"net/http"

	"context"

	"vitess.io/vitess/go/acl"
	"vitess.io/vitess/go/vt/topo"
)

// RegisterDebugHealthHandler register a debug health http endpoint for a vtcld server
func RegisterDebugHealthHandler(ts *topo.Server) {
	http.HandleFunc("/debug/health", func(w http.ResponseWriter, r *http.Request) {
		if err := acl.CheckAccessHTTP(r, acl.MONITORING); err != nil {
			acl.SendError(w, err)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		if err := isHealthy(ts); err != nil {
			w.Write([]byte("not ok"))
			return
		}
		w.Write([]byte("ok"))
	})
}

func isHealthy(ts *topo.Server) error {
	_, err := ts.GetKeyspaces(context.Background())
	return err
}
