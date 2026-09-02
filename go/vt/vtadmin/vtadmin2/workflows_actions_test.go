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
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

const (
	workflowActionBase = "/workflow/local/sales/users_to_sales"
	generatedVDiffUUID = "550e8400-e29b-41d4-a716-446655440000"
)

type workflowActionsFakeServer struct {
	fakeVTAdminServer

	startWorkflowReq         *vtadminpb.StartWorkflowRequest
	stopWorkflowReq          *vtadminpb.StopWorkflowRequest
	workflowDeleteReq        *vtadminpb.WorkflowDeleteRequest
	workflowSwitchTrafficReq *vtadminpb.WorkflowSwitchTrafficRequest
	moveTablesCompleteReq    *vtadminpb.MoveTablesCompleteRequest
	vdiffCreateReq           *vtadminpb.VDiffCreateRequest
}

func (f *workflowActionsFakeServer) GetWorkflow(ctx context.Context, req *vtadminpb.GetWorkflowRequest) (*vtadminpb.Workflow, error) {
	return &vtadminpb.Workflow{
		Cluster:  &vtadminpb.Cluster{Id: req.ClusterId},
		Keyspace: req.Keyspace,
		Workflow: &vtctldatapb.Workflow{Name: req.Name, WorkflowType: "MoveTables"},
	}, nil
}

func (f *workflowActionsFakeServer) StartWorkflow(ctx context.Context, req *vtadminpb.StartWorkflowRequest) (*vtctldatapb.WorkflowUpdateResponse, error) {
	f.startWorkflowReq = req
	return &vtctldatapb.WorkflowUpdateResponse{}, nil
}

func (f *workflowActionsFakeServer) StopWorkflow(ctx context.Context, req *vtadminpb.StopWorkflowRequest) (*vtctldatapb.WorkflowUpdateResponse, error) {
	f.stopWorkflowReq = req
	return &vtctldatapb.WorkflowUpdateResponse{}, nil
}

func (f *workflowActionsFakeServer) WorkflowDelete(ctx context.Context, req *vtadminpb.WorkflowDeleteRequest) (*vtctldatapb.WorkflowDeleteResponse, error) {
	f.workflowDeleteReq = req
	return &vtctldatapb.WorkflowDeleteResponse{}, nil
}

func (f *workflowActionsFakeServer) WorkflowSwitchTraffic(ctx context.Context, req *vtadminpb.WorkflowSwitchTrafficRequest) (*vtctldatapb.WorkflowSwitchTrafficResponse, error) {
	f.workflowSwitchTrafficReq = req
	return &vtctldatapb.WorkflowSwitchTrafficResponse{}, nil
}

func (f *workflowActionsFakeServer) MoveTablesComplete(ctx context.Context, req *vtadminpb.MoveTablesCompleteRequest) (*vtctldatapb.MoveTablesCompleteResponse, error) {
	f.moveTablesCompleteReq = req
	return &vtctldatapb.MoveTablesCompleteResponse{}, nil
}

func (f *workflowActionsFakeServer) VDiffCreate(ctx context.Context, req *vtadminpb.VDiffCreateRequest) (*vtctldatapb.VDiffCreateResponse, error) {
	f.vdiffCreateReq = req
	uuid := req.GetRequest().GetUuid()
	if uuid == "" {
		uuid = generatedVDiffUUID
	}
	return &vtctldatapb.VDiffCreateResponse{UUID: uuid}, nil
}

func newWorkflowActionsTestServer(t *testing.T, fake *workflowActionsFakeServer, readOnly bool) *Server {
	t.Helper()
	s, err := NewServer(fake, Options{ReadOnly: readOnly})
	require.NoError(t, err)
	return s
}

func TestWorkflowStartStopCallAPI(t *testing.T) {
	tests := []struct {
		action string
		verify func(t *testing.T, fake *workflowActionsFakeServer)
	}{
		{
			action: "/start",
			verify: func(t *testing.T, fake *workflowActionsFakeServer) {
				require.NotNil(t, fake.startWorkflowReq)
				assert.Equal(t, testClusterID, fake.startWorkflowReq.ClusterId)
				assert.Equal(t, "sales", fake.startWorkflowReq.Keyspace)
				assert.Equal(t, "users_to_sales", fake.startWorkflowReq.Workflow)
			},
		},
		{
			action: "/stop",
			verify: func(t *testing.T, fake *workflowActionsFakeServer) {
				require.NotNil(t, fake.stopWorkflowReq)
				assert.Equal(t, "users_to_sales", fake.stopWorkflowReq.Workflow)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			fake := &workflowActionsFakeServer{}
			s := newWorkflowActionsTestServer(t, fake, false)

			rec := postShardForm(t, s, workflowActionBase+tt.action, url.Values{})

			assert.Equal(t, http.StatusSeeOther, rec.Code)
			assert.Equal(t, workflowActionBase, rec.Header().Get("Location"))
			tt.verify(t, fake)
		})
	}
}

func TestWorkflowCancelCallsDelete(t *testing.T) {
	fake := &workflowActionsFakeServer{}
	s := newWorkflowActionsTestServer(t, fake, false)

	form := url.Values{"keep_data": {"on"}}
	rec := postShardForm(t, s, workflowActionBase+"/cancel", form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	assert.Equal(t, "/workflows", rec.Header().Get("Location"))

	req := fake.workflowDeleteReq
	require.NotNil(t, req)
	assert.Equal(t, testClusterID, req.ClusterId)
	assert.Equal(t, "sales", req.GetRequest().Keyspace)
	assert.Equal(t, "users_to_sales", req.GetRequest().Workflow)
	assert.True(t, req.GetRequest().GetKeepData())
}

func TestWorkflowCompleteCallsMoveTablesComplete(t *testing.T) {
	fake := &workflowActionsFakeServer{}
	s := newWorkflowActionsTestServer(t, fake, false)

	form := url.Values{"keep_data": {"on"}}
	rec := postShardForm(t, s, workflowActionBase+"/complete", form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	assert.Equal(t, "/workflows", rec.Header().Get("Location"))

	req := fake.moveTablesCompleteReq
	require.NotNil(t, req)
	assert.Equal(t, testClusterID, req.ClusterId)

	inner := req.GetRequest()
	assert.Equal(t, "users_to_sales", inner.Workflow)
	assert.Equal(t, "sales", inner.TargetKeyspace)
	assert.True(t, inner.GetKeepData())
}

func TestWorkflowSwitchTraffic(t *testing.T) {
	fake := &workflowActionsFakeServer{}
	s := newWorkflowActionsTestServer(t, fake, false)

	form := url.Values{
		"tablet_type":                 {"PRIMARY", "REPLICA"},
		"enable_reverse_replication":  {"on"},
		"initialize_target_sequences": {"on"},
		"timeout":                     {"30"},
		"max_replication_lag_allowed": {"5"},
		"force":                       {"on"},
	}
	rec := postShardForm(t, s, workflowActionBase+"/switch_traffic", form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	assert.Equal(t, workflowActionBase, rec.Header().Get("Location"))

	req := fake.workflowSwitchTrafficReq
	require.NotNil(t, req)
	assert.Equal(t, testClusterID, req.ClusterId)

	inner := req.GetRequest()
	assert.Equal(t, "users_to_sales", inner.Workflow)
	assert.Equal(t, "sales", inner.Keyspace)
	assert.ElementsMatch(t, []topodatapb.TabletType{topodatapb.TabletType_PRIMARY, topodatapb.TabletType_REPLICA}, inner.TabletTypes)
	assert.True(t, inner.EnableReverseReplication)
	assert.True(t, inner.InitializeTargetSequences)
	assert.True(t, inner.Force)
	assert.Equal(t, int64(30), inner.Timeout.Seconds)
	assert.Equal(t, int64(5), inner.MaxReplicationLagAllowed.Seconds)
	// Forward direction.
	assert.Equal(t, int32(0), inner.Direction)
}

func TestWorkflowReverseTraffic(t *testing.T) {
	fake := &workflowActionsFakeServer{}
	s := newWorkflowActionsTestServer(t, fake, false)

	form := url.Values{"tablet_type": {"PRIMARY"}}
	rec := postShardForm(t, s, workflowActionBase+"/reverse_traffic", form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)

	inner := fake.workflowSwitchTrafficReq.GetRequest()
	require.NotNil(t, inner)
	// Reverse direction.
	assert.Equal(t, int32(1), inner.Direction)
	// Reverse traffic always disables reverse replication.
	assert.False(t, inner.EnableReverseReplication)
}

func TestWorkflowVDiffCreateRedirectsToShow(t *testing.T) {
	fake := &workflowActionsFakeServer{}
	s := newWorkflowActionsTestServer(t, fake, false)

	form := url.Values{
		"uuid":         {"my-vdiff-uuid"},
		"source_cells": {"zone1, zone2"},
		"tables":       {"users, orders"},
	}
	rec := postShardForm(t, s, workflowActionBase+"/vdiff", form)

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	assert.Contains(t, rec.Header().Get("Location"), "/vdiff/local/show?")
	assert.Contains(t, rec.Header().Get("Location"), "workflow=users_to_sales")
	assert.Contains(t, rec.Header().Get("Location"), "keyspace=sales")
	assert.Contains(t, rec.Header().Get("Location"), "arg=my-vdiff-uuid")

	req := fake.vdiffCreateReq
	require.NotNil(t, req)
	assert.Equal(t, testClusterID, req.ClusterId)

	inner := req.GetRequest()
	assert.Equal(t, "users_to_sales", inner.Workflow)
	assert.Equal(t, "sales", inner.TargetKeyspace)
	assert.Equal(t, "my-vdiff-uuid", inner.Uuid)
	assert.Equal(t, []string{"zone1", "zone2"}, inner.SourceCells)
	assert.Equal(t, []string{"users", "orders"}, inner.Tables)
}

func TestWorkflowVDiffCreateRedirectsUsingReturnedUUID(t *testing.T) {
	fake := &workflowActionsFakeServer{}
	s := newWorkflowActionsTestServer(t, fake, false)

	rec := postShardForm(t, s, workflowActionBase+"/vdiff", url.Values{})

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	assert.Contains(t, rec.Header().Get("Location"), "arg="+generatedVDiffUUID)
}

func TestWorkflowSwitchTrafficDefaultsTabletTypes(t *testing.T) {
	fake := &workflowActionsFakeServer{}
	s := newWorkflowActionsTestServer(t, fake, false)

	rec := postShardForm(t, s, workflowActionBase+"/switch_traffic", url.Values{})

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	inner := fake.workflowSwitchTrafficReq.GetRequest()
	require.NotNil(t, inner)
	assert.Equal(t, []topodatapb.TabletType{
		topodatapb.TabletType_PRIMARY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}, inner.TabletTypes)
}

func TestWorkflowReverseTrafficDefaultsTabletTypes(t *testing.T) {
	fake := &workflowActionsFakeServer{}
	s := newWorkflowActionsTestServer(t, fake, false)

	rec := postShardForm(t, s, workflowActionBase+"/reverse_traffic", url.Values{})

	assert.Equal(t, http.StatusSeeOther, rec.Code)
	inner := fake.workflowSwitchTrafficReq.GetRequest()
	require.NotNil(t, inner)
	assert.Equal(t, []topodatapb.TabletType{
		topodatapb.TabletType_PRIMARY,
		topodatapb.TabletType_REPLICA,
		topodatapb.TabletType_RDONLY,
	}, inner.TabletTypes)
}

func TestWorkflowActionsReadOnly(t *testing.T) {
	actions := []string{"/start", "/stop", "/cancel", "/complete", "/switch_traffic", "/reverse_traffic", "/vdiff"}

	for _, action := range actions {
		t.Run(action, func(t *testing.T) {
			fake := &workflowActionsFakeServer{}
			s := newWorkflowActionsTestServer(t, fake, true)

			rec := postShardForm(t, s, workflowActionBase+action, url.Values{})

			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Nil(t, fake.startWorkflowReq)
			assert.Nil(t, fake.workflowDeleteReq)
		})
	}
}

func TestWorkflowDetailRendersActions(t *testing.T) {
	fake := &workflowActionsFakeServer{}
	s := newWorkflowActionsTestServer(t, fake, false)

	token, rec := renderWithCSRF(t, s, workflowActionBase)
	body := rec.Body.String()
	assert.Contains(t, body, workflowActionBase+"/start")
	assert.Contains(t, body, workflowActionBase+"/switch_traffic")
	assert.Contains(t, body, workflowActionBase+"/complete")
	assert.Contains(t, body, workflowActionBase+"/vdiff")

	// A POST using the exact rendered token/cookie pairing must get past
	// CSRF validation.
	rec = postFormWithCSRF(s, workflowActionBase+"/start", token, url.Values{
		"csrf_token": {token},
	})
	assert.Equal(t, http.StatusSeeOther, rec.Code)
	require.NotNil(t, fake.startWorkflowReq)
}
