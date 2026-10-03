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
	"strconv"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	streamData struct {
		ClusterID string
		Keyspace  string
		Workflow  string
		StreamKey string
		Stream    *vtctldatapb.Workflow_Stream
	}
)

func (s *Server) streamDetail(w http.ResponseWriter, r *http.Request) {
	clusterID := r.PathValue("cluster_id")
	keyspace := r.PathValue("keyspace")
	workflow := r.PathValue("name")

	streamID, err := strconv.ParseInt(r.PathValue("stream_id"), 10, 64)
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Stream",
			vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid stream ID %q", r.PathValue("stream_id")))
		return
	}

	tabletUID, err := strconv.ParseUint(r.PathValue("tablet_uid"), 10, 32)
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "Stream",
			vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "invalid tablet UID %q", r.PathValue("tablet_uid")))
		return
	}
	tabletCell := r.PathValue("tablet_cell")

	wf, err := s.api.GetWorkflow(r.Context(), &vtadminpb.GetWorkflowRequest{
		ClusterId: clusterID,
		Keyspace:  keyspace,
		Name:      workflow,
	})
	if err != nil {
		s.renderError(w, r, tabletErrorStatus(err), "Stream", err)
		return
	}

	streamKey := formatStreamKey(tabletCell, uint32(tabletUID), streamID)
	stream := findStream(wf.GetWorkflow(), tabletCell, uint32(tabletUID), streamID)
	if stream == nil {
		s.renderError(w, r, http.StatusNotFound, "Stream",
			vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "no stream %s found in workflow %s", streamKey, workflow))
		return
	}

	s.render(w, r, http.StatusOK, "stream.html", PageData{
		Title:  streamKey,
		Active: "workflows",
		Data: streamData{
			ClusterID: clusterID,
			Keyspace:  keyspace,
			Workflow:  workflow,
			StreamKey: streamKey,
			Stream:    stream,
		},
	})
}

// findStream locates the stream matching the tablet cell/UID and stream ID
// within a workflow's shard streams.
func findStream(wf *vtctldatapb.Workflow, tabletCell string, tabletUID uint32, streamID int64) *vtctldatapb.Workflow_Stream {
	if wf == nil {
		return nil
	}
	for _, shardStream := range wf.GetShardStreams() {
		for _, stream := range shardStream.GetStreams() {
			t := stream.GetTablet()
			if t.GetCell() == tabletCell && t.GetUid() == tabletUID && stream.GetId() == streamID {
				return stream
			}
		}
	}
	return nil
}

func formatStreamKey(tabletCell string, tabletUID uint32, streamID int64) string {
	return tabletCell + "-" + strconv.FormatUint(uint64(tabletUID), 10) + ":" + strconv.FormatInt(streamID, 10)
}
