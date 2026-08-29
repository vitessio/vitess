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
	"strings"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// beginKeyspaceAction is the shared preflight for keyspace mutation handlers.
// It returns the parsed route values; ok is false when the preflight has
// already rendered an error response.
func (s *Server) beginKeyspaceAction(w http.ResponseWriter, r *http.Request, title string) (clusterID, keyspace string, ok bool) {
	if !s.beginFormAction(w, r, title) {
		return "", "", false
	}
	return r.PathValue("cluster_id"), r.PathValue("name"), true
}

func (s *Server) keyspaceValidate(w http.ResponseWriter, r *http.Request) {
	const title = "Validate keyspace"
	clusterID, keyspace, ok := s.beginKeyspaceAction(w, r, title)
	if !ok {
		return
	}

	resp, err := s.api.ValidateKeyspace(r.Context(), &vtadminpb.ValidateKeyspaceRequest{
		ClusterId:   clusterID,
		Keyspace:    keyspace,
		PingTablets: r.Form.Get("ping_tablets") == "on",
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	redirectWithFlash(w, r, keyspaceDetailPath(clusterID, keyspace), Flash{
		Kind:    "success",
		Message: "validated keyspace " + keyspace + ": " + strings.Join(resp.GetResults(), "; "),
	})
}

func (s *Server) keyspaceValidateSchema(w http.ResponseWriter, r *http.Request) {
	const title = "Validate schema"
	clusterID, keyspace, ok := s.beginKeyspaceAction(w, r, title)
	if !ok {
		return
	}

	_, err := s.api.ValidateSchemaKeyspace(r.Context(), &vtadminpb.ValidateSchemaKeyspaceRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	redirectWithFlash(w, r, keyspaceDetailPath(clusterID, keyspace), Flash{
		Kind:    "success",
		Message: "validated schema on keyspace " + keyspace,
	})
}

func (s *Server) keyspaceValidateVersion(w http.ResponseWriter, r *http.Request) {
	const title = "Validate version"
	clusterID, keyspace, ok := s.beginKeyspaceAction(w, r, title)
	if !ok {
		return
	}

	_, err := s.api.ValidateVersionKeyspace(r.Context(), &vtadminpb.ValidateVersionKeyspaceRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	redirectWithFlash(w, r, keyspaceDetailPath(clusterID, keyspace), Flash{
		Kind:    "success",
		Message: "validated versions on keyspace " + keyspace,
	})
}

func (s *Server) keyspaceRebuildGraph(w http.ResponseWriter, r *http.Request) {
	const title = "Rebuild keyspace graph"
	clusterID, keyspace, ok := s.beginKeyspaceAction(w, r, title)
	if !ok {
		return
	}

	_, err := s.api.RebuildKeyspaceGraph(r.Context(), &vtadminpb.RebuildKeyspaceGraphRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
		Cells:     splitFormList(r.Form.Get("cells")),
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	redirectWithFlash(w, r, keyspaceDetailPath(clusterID, keyspace), Flash{
		Kind:    "success",
		Message: "rebuilt keyspace graph for " + keyspace,
	})
}

func (s *Server) keyspaceRemoveCell(w http.ResponseWriter, r *http.Request) {
	const title = "Remove keyspace cell"
	clusterID, keyspace, ok := s.beginKeyspaceAction(w, r, title)
	if !ok {
		return
	}

	cell := strings.TrimSpace(r.Form.Get("cell"))
	if cell == "" {
		s.renderFormError(w, r, title, "cell is required")
		return
	}

	_, err := s.api.RemoveKeyspaceCell(r.Context(), &vtadminpb.RemoveKeyspaceCellRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
		Cell:      cell,
		Force:     r.Form.Get("force") == "on",
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	redirectWithFlash(w, r, keyspaceDetailPath(clusterID, keyspace), Flash{
		Kind:    "success",
		Message: "removed cell " + cell + " from keyspace " + keyspace,
	})
}

func (s *Server) keyspaceCreateShard(w http.ResponseWriter, r *http.Request) {
	const title = "Create shard"
	clusterID, keyspace, ok := s.beginKeyspaceAction(w, r, title)
	if !ok {
		return
	}

	shard := strings.TrimSpace(r.Form.Get("shard"))
	if shard == "" {
		s.renderFormError(w, r, title, "shard name is required")
		return
	}

	_, err := s.api.CreateShard(r.Context(), &vtadminpb.CreateShardRequest{
		ClusterId: clusterID,
		Options: &vtctldatapb.CreateShardRequest{
			Keyspace:  keyspace,
			ShardName: shard,
			Force:     r.Form.Get("force") == "on",
		},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	redirectWithFlash(w, r, shardDetailPath(clusterID, keyspace, shard), Flash{
		Kind:    "success",
		Message: "created shard " + keyspace + "/" + shard,
	})
}

func (s *Server) keyspaceReloadSchema(w http.ResponseWriter, r *http.Request) {
	const title = "Reload schema"
	clusterID, keyspace, ok := s.beginKeyspaceAction(w, r, title)
	if !ok {
		return
	}

	_, err := s.api.ReloadSchemas(r.Context(), &vtadminpb.ReloadSchemasRequest{
		ClusterIds: []string{clusterID},
		Keyspaces:  []string{keyspace},
	})
	if err != nil {
		s.renderFormErrorErr(w, r, title, err)
		return
	}

	redirectWithFlash(w, r, keyspaceDetailPath(clusterID, keyspace), Flash{
		Kind:    "success",
		Message: "reloaded schema on keyspace " + keyspace,
	})
}
