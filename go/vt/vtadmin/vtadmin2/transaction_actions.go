/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 the "License";
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtadmin2

import (
	"net/http"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

func (s *Server) transactionConclude(w http.ResponseWriter, r *http.Request) {
	const title = "Conclude transaction"
	if !s.beginFormAction(w, r, title) {
		return
	}

	clusterID := r.PathValue("cluster_id")
	dtid := r.PathValue("dtid")

	_, err := s.api.ConcludeTransaction(r.Context(), &vtadminpb.ConcludeTransactionRequest{
		ClusterId: clusterID,
		Dtid:      dtid,
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	// Redirect to the unfiltered transactions page: the transactions handler
	// requires both cluster_id and keyspace when any query parameters are
	// present, and this action is not keyed to a single keyspace.
	s.redirectWithFlash(w, r, "/transactions", Flash{
		Kind:    "success",
		Message: "concluded transaction " + dtid,
	})
}
