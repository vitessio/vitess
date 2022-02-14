package debug

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
)

// Cluster returns an http.HandlerFunc for the /debug/cluster/{cluster_id}
// route.
func Cluster(api API) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, ok := mux.Vars(r)["cluster_id"]
		if !ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("missing {cluster_id} route component"))
			return
		}

		c, ok := api.Cluster(id)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
		}

		data, err := json.Marshal(c.Debug())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "could not marshal cluster debug map: %s\n", err)
			return
		}

		w.Write(data)
		w.Write([]byte("\n"))
	}
}

// Clusters returns an http.HandlerFunc for the /debug/clusters route.
func Clusters(api API) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clusters := api.Clusters()
		m := make(map[string]map[string]interface{}, len(clusters))
		for _, c := range clusters {
			m[c.ID] = c.Debug()
		}

		data, err := json.Marshal(m)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "could not marshal cluster debug map: %s\n", err)
			return
		}

		w.Write(data)
		w.Write([]byte("\n"))
	}
}
