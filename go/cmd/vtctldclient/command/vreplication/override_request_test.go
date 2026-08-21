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

package vreplication_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/cmd/vtctldclient/command"
	vtctldclientcommon "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/common"
	_ "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/movetables"
	_ "vitess.io/vitess/go/cmd/vtctldclient/command/vreplication/workflow"
	"vitess.io/vitess/go/vt/vtctl/localvtctldclient"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

type overrideCaptureServer struct {
	vtctlservicepb.UnimplementedVtctldServer

	moveTablesCreateReq *vtctldatapb.MoveTablesCreateRequest
	workflowUpdateReq   *vtctldatapb.WorkflowUpdateRequest
}

func (s *overrideCaptureServer) MoveTablesCreate(_ context.Context, req *vtctldatapb.MoveTablesCreateRequest) (*vtctldatapb.WorkflowStatusResponse, error) {
	s.moveTablesCreateReq = req
	return &vtctldatapb.WorkflowStatusResponse{}, nil
}

func (s *overrideCaptureServer) WorkflowUpdate(_ context.Context, req *vtctldatapb.WorkflowUpdateRequest) (*vtctldatapb.WorkflowUpdateResponse, error) {
	s.workflowUpdateReq = req
	return &vtctldatapb.WorkflowUpdateResponse{}, nil
}

func TestVtctldclientConfigOverrideRequestsIncludeParallelReplicationWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	server := &overrideCaptureServer{}
	localvtctldclient.SetServer(server)

	origArgs := append([]string{}, os.Args...)
	origProtocol := command.VtctldClientProtocol
	t.Cleanup(func() {
		os.Args = append([]string{}, origArgs...)
		command.VtctldClientProtocol = origProtocol
	})

	command.VtctldClientProtocol = "local"
	vtctldclientcommon.SetCommandCtx(ctx)

	t.Run("MoveTablesCreate", func(t *testing.T) {
		os.Args = []string{
			"vtctldclient",
			"--server", "ignored",
			"MoveTables",
			"--workflow", "wf1",
			"--target-keyspace", "target",
			"create",
			"--source-keyspace", "source",
			"--all-tables",
			"--config-overrides", "vreplication-parallel-replication-workers=7",
		}

		err := command.Root.Execute()
		require.NoError(t, err)
		require.NotNil(t, server.moveTablesCreateReq)
		require.NotNil(t, server.moveTablesCreateReq.WorkflowOptions)
		require.Equal(t, "7", server.moveTablesCreateReq.WorkflowOptions.Config["vreplication-parallel-replication-workers"])
	})

	t.Run("WorkflowUpdate", func(t *testing.T) {
		os.Args = []string{
			"vtctldclient",
			"--server", "ignored",
			"Workflow",
			"--keyspace", "target",
			"update",
			"--workflow", "wf1",
			"--config-overrides", "vreplication-parallel-replication-workers=9",
		}

		err := command.Root.Execute()
		require.NoError(t, err)
		require.NotNil(t, server.workflowUpdateReq)
		require.NotNil(t, server.workflowUpdateReq.TabletRequest)
		require.Equal(t, "9", server.workflowUpdateReq.TabletRequest.ConfigOverrides["vreplication-parallel-replication-workers"])
	})
}
