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

	"vitess.io/vitess/go/vt/topo/topoproto"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	tabletDetailData struct {
		ClusterID string
		Alias     string
		Tablet    *vtadminpb.Tablet
	}

	tabletFullStatusData struct {
		ClusterID string
		Alias     string
		Response  *vtctldatapb.GetFullStatusResponse
	}
)

func parseRouteTabletAlias(alias string) (*topodatapb.TabletAlias, error) {
	parsed, err := topoproto.ParseTabletAlias(alias)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to parse tablet alias %s", alias)
	}
	return parsed, nil
}

func tabletErrorStatus(err error) int {
	switch vterrors.Code(err) {
	case vtrpcpb.Code_NOT_FOUND:
		return http.StatusNotFound
	case vtrpcpb.Code_INVALID_ARGUMENT:
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

func (s *Server) tabletDetail(w http.ResponseWriter, r *http.Request) {
	clusterID := r.PathValue("cluster_id")
	alias := r.PathValue("alias")
	parsedAlias, err := parseRouteTabletAlias(alias)
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Tablet", err)
		return
	}

	tablet, err := s.api.GetTablet(r.Context(), &vtadminpb.GetTabletRequest{
		Alias:      parsedAlias,
		ClusterIds: []string{clusterID},
	})
	if err != nil {
		s.renderError(w, r, tabletErrorStatus(err), "Tablet", err)
		return
	}

	s.render(w, r, http.StatusOK, "tablet.html", PageData{
		Title:  alias,
		Active: "tablets",
		Data: tabletDetailData{
			ClusterID: clusterID,
			Alias:     alias,
			Tablet:    tablet,
		},
	})
}

func (s *Server) tabletFullStatus(w http.ResponseWriter, r *http.Request) {
	clusterID := r.PathValue("cluster_id")
	alias := r.PathValue("alias")
	parsedAlias, err := parseRouteTabletAlias(alias)
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Tablet", err)
		return
	}

	resp, err := s.api.GetFullStatus(r.Context(), &vtadminpb.GetFullStatusRequest{
		ClusterId: clusterID,
		Alias:     parsedAlias,
	})
	if err != nil {
		s.renderError(w, r, tabletErrorStatus(err), "Full Status", err)
		return
	}
	if resp == nil {
		http.NotFound(w, r)
		return
	}

	s.render(w, r, http.StatusOK, "tablet_full_status.html", PageData{
		Title:  "Full Status",
		Active: "tablets",
		Data: tabletFullStatusData{
			ClusterID: clusterID,
			Alias:     alias,
			Response:  resp,
		},
	})
}
