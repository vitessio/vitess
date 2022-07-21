/*
Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
		m := make(map[string]map[string]any, len(clusters))
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
