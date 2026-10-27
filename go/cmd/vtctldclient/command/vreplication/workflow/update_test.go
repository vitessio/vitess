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

package workflow

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	"vitess.io/vitess/go/vt/vtctl/vtctldclient"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

// capturingWorkflowUpdateClient records the WorkflowUpdate request so tests
// can assert that client-side flags are copied into TabletRequest.
type capturingWorkflowUpdateClient struct {
	vtctldclient.VtctldClient
	req *vtctldatapb.WorkflowUpdateRequest
}

func (c *capturingWorkflowUpdateClient) WorkflowUpdate(ctx context.Context, req *vtctldatapb.WorkflowUpdateRequest, _ ...grpc.CallOption) (*vtctldatapb.WorkflowUpdateResponse, error) {
	c.req = req
	return &vtctldatapb.WorkflowUpdateResponse{
		Summary: "ok",
		Details: []*vtctldatapb.WorkflowUpdateResponse_TabletInfo{
			{Tablet: &topodatapb.TabletAlias{Cell: "zone1", Uid: 200}, Changed: true},
		},
	}, nil
}

func resetCommandUpdateState() {
	baseOptions.Keyspace = ""
	baseOptions.Workflow = ""
	baseOptions.Shards = nil
	updateOptions.Cells = nil
	updateOptions.TabletTypes = nil
	updateOptions.TabletTypesInPreferenceOrder = false
	updateOptions.OnDDL = ""
	updateOptions.ConfigOverrides = nil
}

// TestCommandUpdatePassesShards confirms that `Workflow update --shards`
// copies the shard subset into UpdateVReplicationWorkflowRequest for every
// update option. Without this, the server falls back to all target shards.
func TestCommandUpdatePassesShards(t *testing.T) {
	wantShards := []string{"-80"}
	tests := []struct {
		name  string
		setup func()
		check func(t *testing.T, req *vtctldatapb.WorkflowUpdateRequest)
	}{
		{
			name: "cells",
			setup: func() {
				updateOptions.Cells = []string{"zone1"}
			},
			check: func(t *testing.T, req *vtctldatapb.WorkflowUpdateRequest) {
				t.Helper()
				require.Equal(t, []string{"zone1"}, req.TabletRequest.Cells)
			},
		},
		{
			name: "tablet-types",
			setup: func() {
				updateOptions.TabletTypes = []topodatapb.TabletType{topodatapb.TabletType_PRIMARY}
			},
			check: func(t *testing.T, req *vtctldatapb.WorkflowUpdateRequest) {
				t.Helper()
				require.Equal(t, []topodatapb.TabletType{topodatapb.TabletType_PRIMARY}, req.TabletRequest.TabletTypes)
			},
		},
		{
			name: "on-ddl",
			setup: func() {
				updateOptions.OnDDL = "STOP"
			},
			check: func(t *testing.T, req *vtctldatapb.WorkflowUpdateRequest) {
				t.Helper()
				require.NotNil(t, req.TabletRequest.OnDdl)
				require.Equal(t, binlogdatapb.OnDDLAction_STOP, *req.TabletRequest.OnDdl)
			},
		},
		{
			name: "config-overrides",
			setup: func() {
				updateOptions.ConfigOverrides = []string{"vreplication-heartbeat-update-interval=10"}
			},
			check: func(t *testing.T, req *vtctldatapb.WorkflowUpdateRequest) {
				t.Helper()
				require.Equal(t, map[string]string{"vreplication-heartbeat-update-interval": "10"}, req.TabletRequest.ConfigOverrides)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetCommandUpdateState()
			t.Cleanup(resetCommandUpdateState)

			baseOptions.Keyspace = "customer"
			baseOptions.Workflow = "wf1"
			baseOptions.Shards = append([]string(nil), wantShards...)
			tt.setup()

			fake := &capturingWorkflowUpdateClient{}
			common.SetClient(fake)
			common.SetCommandCtx(t.Context())
			t.Cleanup(func() {
				common.SetClient(nil)
				common.SetCommandCtx(context.Background())
			})

			cmd := &cobra.Command{Use: "update"}
			cmd.Flags().Bool("tablet-types-in-order", true, "")

			// commandUpdate prints JSON to stdout.
			oldStdout := os.Stdout
			r, w, err := os.Pipe()
			require.NoError(t, err)
			os.Stdout = w
			err = commandUpdate(cmd, nil)
			require.NoError(t, w.Close())
			os.Stdout = oldStdout
			_, _ = io.ReadAll(r)
			require.NoError(t, err)

			require.NotNil(t, fake.req)
			require.NotNil(t, fake.req.TabletRequest)
			require.Equal(t, wantShards, fake.req.TabletRequest.Shards, "TabletRequest.Shards must be copied from --shards")
			tt.check(t, fake.req)
		})
	}
}
