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

// TestCommandUpdatePassesShards confirms that commandUpdate copies --shards
// into TabletRequest.Shards. Without this the server updates every target shard.
func TestCommandUpdatePassesShards(t *testing.T) {
	t.Cleanup(func() {
		baseOptions.Keyspace = ""
		baseOptions.Workflow = ""
		baseOptions.Shards = nil
		common.SetClient(nil)
		common.SetCommandCtx(context.Background())
	})

	wantShards := []string{"-80"}
	baseOptions.Keyspace = "customer"
	baseOptions.Workflow = "wf1"
	baseOptions.Shards = wantShards

	fake := &capturingWorkflowUpdateClient{}
	common.SetClient(fake)
	common.SetCommandCtx(t.Context())

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
	require.Equal(t, wantShards, fake.req.TabletRequest.Shards)
}
