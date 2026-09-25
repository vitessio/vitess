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
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const streamDetailPath = "/workflow/local/sales/users_to_sales/stream/zone1/100/1"

type streamFakeServer struct {
	fakeVTAdminServer
}

func (f *streamFakeServer) GetWorkflow(ctx context.Context, req *vtadminpb.GetWorkflowRequest) (*vtadminpb.Workflow, error) {
	return &vtadminpb.Workflow{
		Cluster:  &vtadminpb.Cluster{Id: req.ClusterId},
		Keyspace: req.Keyspace,
		Workflow: &vtctldatapb.Workflow{
			Name:         req.Name,
			WorkflowType: "MoveTables",
			ShardStreams: map[string]*vtctldatapb.Workflow_ShardStream{
				"0": {
					Streams: []*vtctldatapb.Workflow_Stream{
						{
							Id:     1,
							Shard:  "0",
							Tablet: &topodatapb.TabletAlias{Cell: "zone1", Uid: 100},
							State:  "Running",
						},
					},
				},
			},
		},
	}, nil
}

func TestStreamDetailRendersStream(t *testing.T) {
	fake := &streamFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, streamDetailPath, nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	assert.Contains(t, body, "zone1-100:1")
	assert.Contains(t, body, "Running")
	assert.Contains(t, body, "users_to_sales")
}

func TestStreamDetailNotFound(t *testing.T) {
	fake := &streamFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/workflow/local/sales/users_to_sales/stream/zone1/999/42", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestStreamDetailInvalidStreamID(t *testing.T) {
	fake := &streamFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/workflow/local/sales/users_to_sales/stream/zone1/100/not-a-number", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestWorkflowDetailLinksToStreams(t *testing.T) {
	fake := &streamFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/workflow/local/sales/users_to_sales", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "/workflow/local/sales/users_to_sales/stream/zone1/100/1")
}
