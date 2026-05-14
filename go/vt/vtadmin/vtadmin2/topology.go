/*
Copyright 2026 The Vitess Authors.

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

package vtadmin2

import (
	"net/http"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type topologyPathData struct {
	Clusters  []*vtadminpb.Cluster
	ClusterID string
	Path      string
	Response  *vtctldatapb.GetTopologyPathResponse
}

func (s *Server) topologyPath(w http.ResponseWriter, r *http.Request) {
	clustersResp, err := s.api.GetClusters(r.Context(), &vtadminpb.GetClustersRequest{})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Topology", err)
		return
	}
	clusters := clustersResp.GetClusters()
	clusterID := selectedClusterID(clusters, queryValue(r, "cluster_id"))
	if clusterID == "" {
		s.renderError(w, r, http.StatusBadRequest, "Topology", vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cluster_id is required"))
		return
	}

	path := queryValue(r, "path")
	if path == "" {
		path = "/"
	}

	resp, err := s.api.GetTopologyPath(r.Context(), &vtadminpb.GetTopologyPathRequest{
		ClusterId: clusterID,
		Path:      path,
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Topology", err)
		return
	}
	if resp == nil {
		http.NotFound(w, r)
		return
	}

	s.render(w, r, http.StatusOK, "topology.html", PageData{
		Title:  "Topology",
		Active: "topology",
		Data: topologyPathData{
			Clusters:  clusters,
			ClusterID: clusterID,
			Path:      path,
			Response:  resp,
		},
	})
}

func (s *Server) shards(w http.ResponseWriter, r *http.Request) {
	resp, err := s.api.GetShardReplicationPositions(r.Context(), &vtadminpb.GetShardReplicationPositionsRequest{
		ClusterIds:     queryValues(r, "cluster_id"),
		Keyspaces:      queryValues(r, "keyspace"),
		KeyspaceShards: queryValues(r, "keyspace_shard"),
	})
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, "Shards", err)
		return
	}

	s.render(w, r, http.StatusOK, "shards.html", PageData{
		Title:  "Shards",
		Active: "shards",
		Data:   resp.GetReplicationPositions(),
	})
}
