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

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
)

// beginTabletAction is the shared preflight for tablet mutation handlers. It
// validates the CSRF token, parses the route alias, and returns the parsed
// values; ok is false when the preflight has already rendered an error
// response.
func (s *Server) beginTabletAction(w http.ResponseWriter, r *http.Request, title string) (alias *topodatapb.TabletAlias, clusterID string, ok bool) {
	if !s.beginFormAction(w, r, title) {
		return nil, "", false
	}

	parsed, err := parseRouteTabletAlias(r.PathValue("alias"))
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return nil, "", false
	}
	return parsed, r.PathValue("cluster_id"), true
}

func tabletDetailRedirect(clusterID, alias string) string {
	return "/tablet/" + pathEscape(clusterID) + "/" + pathEscape(alias)
}

func (s *Server) tabletStartReplication(w http.ResponseWriter, r *http.Request) {
	const title = "Start replication"
	alias, clusterID, ok := s.beginTabletAction(w, r, title)
	if !ok {
		return
	}

	_, err := s.api.StartReplication(r.Context(), &vtadminpb.StartReplicationRequest{
		Alias:      alias,
		ClusterIds: []string{clusterID},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	s.redirectWithFlash(w, r, tabletDetailRedirect(clusterID, r.PathValue("alias")), Flash{
		Kind:    "success",
		Message: "replication started on tablet " + r.PathValue("alias"),
	})
}

func (s *Server) tabletStopReplication(w http.ResponseWriter, r *http.Request) {
	const title = "Stop replication"
	alias, clusterID, ok := s.beginTabletAction(w, r, title)
	if !ok {
		return
	}

	_, err := s.api.StopReplication(r.Context(), &vtadminpb.StopReplicationRequest{
		Alias:      alias,
		ClusterIds: []string{clusterID},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	s.redirectWithFlash(w, r, tabletDetailRedirect(clusterID, r.PathValue("alias")), Flash{
		Kind:    "success",
		Message: "replication stopped on tablet " + r.PathValue("alias"),
	})
}

func (s *Server) tabletRefreshReplicationSource(w http.ResponseWriter, r *http.Request) {
	const title = "Refresh replication source"
	alias, clusterID, ok := s.beginTabletAction(w, r, title)
	if !ok {
		return
	}

	resp, err := s.api.RefreshTabletReplicationSource(r.Context(), &vtadminpb.RefreshTabletReplicationSourceRequest{
		Alias:      alias,
		ClusterIds: []string{clusterID},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	message := "replication source refreshed on tablet " + r.PathValue("alias")
	if keyspace := resp.GetKeyspace(); keyspace != "" {
		message += " (now replicating from " + keyspace + "/" + resp.GetShard() + ")"
	}

	s.redirectWithFlash(w, r, tabletDetailRedirect(clusterID, r.PathValue("alias")), Flash{
		Kind:    "success",
		Message: message,
	})
}

func (s *Server) tabletSetReadOnly(w http.ResponseWriter, r *http.Request) {
	const title = "Set read-only"
	alias, clusterID, ok := s.beginTabletAction(w, r, title)
	if !ok {
		return
	}

	_, err := s.api.SetReadOnly(r.Context(), &vtadminpb.SetReadOnlyRequest{
		Alias:      alias,
		ClusterIds: []string{clusterID},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	s.redirectWithFlash(w, r, tabletDetailRedirect(clusterID, r.PathValue("alias")), Flash{
		Kind:    "success",
		Message: "tablet " + r.PathValue("alias") + " set to read-only",
	})
}

func (s *Server) tabletSetReadWrite(w http.ResponseWriter, r *http.Request) {
	const title = "Set read-write"
	alias, clusterID, ok := s.beginTabletAction(w, r, title)
	if !ok {
		return
	}

	_, err := s.api.SetReadWrite(r.Context(), &vtadminpb.SetReadWriteRequest{
		Alias:      alias,
		ClusterIds: []string{clusterID},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	s.redirectWithFlash(w, r, tabletDetailRedirect(clusterID, r.PathValue("alias")), Flash{
		Kind:    "success",
		Message: "tablet " + r.PathValue("alias") + " set to read-write",
	})
}

func (s *Server) tabletDelete(w http.ResponseWriter, r *http.Request) {
	const title = "Delete tablet"
	alias, clusterID, ok := s.beginTabletAction(w, r, title)
	if !ok {
		return
	}

	_, err := s.api.DeleteTablet(r.Context(), &vtadminpb.DeleteTabletRequest{
		Alias:        alias,
		ClusterIds:   []string{clusterID},
		AllowPrimary: r.Form.Get("allow_primary") == "on",
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	// The tablet no longer exists after deletion, so redirect to the list.
	s.redirectWithFlash(w, r, "/tablets", Flash{
		Kind:    "success",
		Message: "deleted tablet " + r.PathValue("alias"),
	})
}

func (s *Server) tabletPing(w http.ResponseWriter, r *http.Request) {
	const title = "Ping tablet"
	alias, clusterID, ok := s.beginTabletAction(w, r, title)
	if !ok {
		return
	}

	_, err := s.api.PingTablet(r.Context(), &vtadminpb.PingTabletRequest{
		Alias:      alias,
		ClusterIds: []string{clusterID},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	s.redirectWithFlash(w, r, tabletDetailRedirect(clusterID, r.PathValue("alias")), Flash{
		Kind:    "success",
		Message: "tablet " + r.PathValue("alias") + " responded to ping",
	})
}

func (s *Server) tabletRefreshState(w http.ResponseWriter, r *http.Request) {
	const title = "Refresh tablet state"
	alias, clusterID, ok := s.beginTabletAction(w, r, title)
	if !ok {
		return
	}

	resp, err := s.api.RefreshState(r.Context(), &vtadminpb.RefreshStateRequest{
		Alias:      alias,
		ClusterIds: []string{clusterID},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	message := "tablet state refreshed"
	if status := resp.GetStatus(); status != "" {
		message += ": " + status
	}

	s.redirectWithFlash(w, r, tabletDetailRedirect(clusterID, r.PathValue("alias")), Flash{
		Kind:    "success",
		Message: message,
	})
}

func (s *Server) tabletRunHealthCheck(w http.ResponseWriter, r *http.Request) {
	const title = "Run health check"
	alias, clusterID, ok := s.beginTabletAction(w, r, title)
	if !ok {
		return
	}

	resp, err := s.api.RunHealthCheck(r.Context(), &vtadminpb.RunHealthCheckRequest{
		Alias:      alias,
		ClusterIds: []string{clusterID},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	message := "health check complete"
	if status := resp.GetStatus(); status != "" {
		message += ": " + status
	}

	s.redirectWithFlash(w, r, tabletDetailRedirect(clusterID, r.PathValue("alias")), Flash{
		Kind:    "success",
		Message: message,
	})
}
