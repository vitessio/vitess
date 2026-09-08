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
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtadminerrors "vitess.io/vitess/go/vt/vtadmin/errors"
)

type workflowFakeServer struct {
	fakeVTAdminServer

	getWorkflowsRequest      *vtadminpb.GetWorkflowsRequest
	getWorkflowRequest       *vtadminpb.GetWorkflowRequest
	getWorkflowStatusRequest *vtadminpb.GetWorkflowStatusRequest
	vdiffShowRequest         *vtadminpb.VDiffShowRequest
	getWorkflowError         error
	getClustersError         error
}

func (f *workflowFakeServer) GetWorkflows(ctx context.Context, req *vtadminpb.GetWorkflowsRequest) (*vtadminpb.GetWorkflowsResponse, error) {
	f.getWorkflowsRequest = req
	return &vtadminpb.GetWorkflowsResponse{
		WorkflowsByCluster: map[string]*vtadminpb.ClusterWorkflows{
			"local": {
				Workflows: []*vtadminpb.Workflow{
					{
						Cluster:  &vtadminpb.Cluster{Id: "local", Name: "Local"},
						Keyspace: "commerce",
						Workflow: &vtctldatapb.Workflow{Name: "wf1", WorkflowType: "MoveTables"},
					},
				},
			},
		},
	}, nil
}

func (f *workflowFakeServer) GetWorkflow(ctx context.Context, req *vtadminpb.GetWorkflowRequest) (*vtadminpb.Workflow, error) {
	f.getWorkflowRequest = req
	if f.getWorkflowError != nil {
		return nil, f.getWorkflowError
	}
	return &vtadminpb.Workflow{
		Cluster:  &vtadminpb.Cluster{Id: req.GetClusterId(), Name: "Local"},
		Keyspace: req.GetKeyspace(),
		Workflow: &vtctldatapb.Workflow{Name: req.GetName(), WorkflowType: "MoveTables"},
	}, nil
}

func (f *workflowFakeServer) GetWorkflowStatus(ctx context.Context, req *vtadminpb.GetWorkflowStatusRequest) (*vtctldatapb.WorkflowStatusResponse, error) {
	f.getWorkflowStatusRequest = req
	return &vtctldatapb.WorkflowStatusResponse{TrafficState: "Reads Not Switched"}, nil
}

func (f *workflowFakeServer) GetClusters(ctx context.Context, req *vtadminpb.GetClustersRequest) (*vtadminpb.GetClustersResponse, error) {
	if f.getClustersError != nil {
		return nil, f.getClustersError
	}
	return &vtadminpb.GetClustersResponse{Clusters: []*vtadminpb.Cluster{{Id: "local", Name: "Local"}, {Id: "prod", Name: "Prod"}}}, nil
}

func (f *workflowFakeServer) GetKeyspaces(ctx context.Context, req *vtadminpb.GetKeyspacesRequest) (*vtadminpb.GetKeyspacesResponse, error) {
	return &vtadminpb.GetKeyspacesResponse{Keyspaces: []*vtadminpb.Keyspace{
		{Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"}, Keyspace: &vtctldatapb.Keyspace{Name: "commerce"}},
		{Cluster: &vtadminpb.Cluster{Id: "local", Name: "Local"}, Keyspace: &vtctldatapb.Keyspace{Name: "customer"}},
		{Cluster: &vtadminpb.Cluster{Id: "prod", Name: "Prod"}, Keyspace: &vtctldatapb.Keyspace{Name: "commerce_prod"}},
	}}, nil
}

func (f *workflowFakeServer) VDiffShow(ctx context.Context, req *vtadminpb.VDiffShowRequest) (*vtadminpb.VDiffShowResponse, error) {
	f.vdiffShowRequest = req
	return &vtadminpb.VDiffShowResponse{
		ShardReport: map[string]*vtadminpb.VDiffShardReport{
			"commerce/0": {State: "completed", RowsCompared: 12},
		},
	}, nil
}

func TestWorkflowsPagePassesFiltersAndRendersRows(t *testing.T) {
	fake := &workflowFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/workflows?cluster_id=local&keyspace=commerce&active_only=1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.getWorkflowsRequest)
	assert.Equal(t, []string{"local"}, fake.getWorkflowsRequest.GetClusterIds())
	assert.Equal(t, []string{"commerce"}, fake.getWorkflowsRequest.GetKeyspaces())
	assert.True(t, fake.getWorkflowsRequest.GetActiveOnly())
	assert.Contains(t, rec.Body.String(), "Workflows")
	assert.Contains(t, rec.Body.String(), "wf1")
	assert.Contains(t, rec.Body.String(), "commerce")
	assert.Contains(t, rec.Body.String(), "/workflow/local/commerce/wf1")
}

func TestWorkflowPageUsesRouteValuesAndRendersDetail(t *testing.T) {
	fake := &workflowFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/workflow/local/commerce/wf1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.getWorkflowRequest)
	assert.Equal(t, "local", fake.getWorkflowRequest.GetClusterId())
	assert.Equal(t, "commerce", fake.getWorkflowRequest.GetKeyspace())
	assert.Equal(t, "wf1", fake.getWorkflowRequest.GetName())
	assert.False(t, fake.getWorkflowRequest.GetActiveOnly())
	assert.Contains(t, rec.Body.String(), "wf1")
	assert.Contains(t, rec.Body.String(), "MoveTables")
}

func TestWorkflowPageNotFoundReturnsNotFound(t *testing.T) {
	fake := &workflowFakeServer{getWorkflowError: fmt.Errorf("lookup failed: %w", vtadminerrors.ErrNoWorkflow)}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/workflow/local/commerce/missing", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "no such workflow")
}

func TestWorkflowStatusPageUsesRouteValuesAndRendersStatus(t *testing.T) {
	fake := &workflowFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/workflow/local/commerce/wf1/status", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.getWorkflowStatusRequest)
	assert.Equal(t, "local", fake.getWorkflowStatusRequest.GetClusterId())
	assert.Equal(t, "commerce", fake.getWorkflowStatusRequest.GetKeyspace())
	assert.Equal(t, "wf1", fake.getWorkflowStatusRequest.GetName())
	assert.Contains(t, rec.Body.String(), "Workflow Status")
	assert.Contains(t, rec.Body.String(), "wf1")
	assert.Contains(t, rec.Body.String(), "Reads Not Switched")
}

func TestVDiffShowPagePassesRequestAndRendersResponse(t *testing.T) {
	fake := &workflowFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vdiff/local/show?keyspace=commerce&workflow=wf1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.vdiffShowRequest)
	assert.Equal(t, "local", fake.vdiffShowRequest.GetClusterId())
	require.NotNil(t, fake.vdiffShowRequest.GetRequest())
	assert.Equal(t, "commerce", fake.vdiffShowRequest.GetRequest().GetTargetKeyspace())
	assert.Equal(t, "wf1", fake.vdiffShowRequest.GetRequest().GetWorkflow())
	assert.Equal(t, "last", fake.vdiffShowRequest.GetRequest().GetArg())
	assert.Contains(t, rec.Body.String(), "VDiff")
	assert.Contains(t, rec.Body.String(), "commerce")
	assert.Contains(t, rec.Body.String(), "wf1")
	assert.Contains(t, rec.Body.String(), `<select name="keyspace" required>`)
	assert.Contains(t, rec.Body.String(), `<option value="commerce" selected>commerce</option>`)
	assert.Contains(t, rec.Body.String(), "completed")
}

func TestVDiffShowPageRequiresKeyspace(t *testing.T) {
	fake := &workflowFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vdiff/local/show?workflow=wf1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "keyspace")
	assert.Nil(t, fake.vdiffShowRequest)
}

func TestVDiffShowPageValidatesSubmissionBeforeLoadingFormOptions(t *testing.T) {
	fake := &workflowFakeServer{getClustersError: errors.New("cluster options failed")}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vdiff/local/show?workflow=wf1", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "keyspace")
	assert.NotContains(t, rec.Body.String(), "cluster options failed")
	assert.Nil(t, fake.vdiffShowRequest)
}

func TestVDiffShowPageRequiresWorkflow(t *testing.T) {
	fake := &workflowFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/vdiff/local/show?keyspace=commerce", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "workflow")
	assert.Nil(t, fake.vdiffShowRequest)
}

func TestWorkflowsPageRejectsInvalidActiveOnly(t *testing.T) {
	fake := &workflowFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/workflows?active_only=not-bool", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "active_only")
	assert.Nil(t, fake.getWorkflowsRequest)
}
