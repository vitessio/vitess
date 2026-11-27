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

package command_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/cmd/vtctldclient/command"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/localvtctldclient"
	"vitess.io/vitess/go/vt/vterrors"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type recordingEmergencyReparentServer struct {
	vtctlservicepb.UnimplementedVtctldServer

	requests []*vtctldatapb.EmergencyReparentShardRequest
}

func (s *recordingEmergencyReparentServer) EmergencyReparentShard(_ context.Context, req *vtctldatapb.EmergencyReparentShardRequest) (*vtctldatapb.EmergencyReparentShardResponse, error) {
	s.requests = append(s.requests, req)

	return &vtctldatapb.EmergencyReparentShardResponse{}, nil
}

func resetEmergencyReparentShardFlags(t *testing.T) {
	t.Helper()

	for _, name := range []string{"allow-split-brain-promotion", "new-primary", "required-position"} {
		flag := command.EmergencyReparentShard.Flags().Lookup(name)
		require.NotNil(t, flag)
		require.NoError(t, flag.Value.Set(flag.DefValue))
		flag.Changed = false
	}
}

// TestERSRequiredPositionFlag checks that --required-position reaches the request unchanged.
func TestERSRequiredPositionFlag(t *testing.T) {
	originalProtocol := command.VtctldClientProtocol
	resetEmergencyReparentShardFlags(t)
	command.VtctldClientProtocol = "local"
	t.Cleanup(func() {
		command.Root.SetArgs(nil)
		command.VtctldClientProtocol = originalProtocol
		resetEmergencyReparentShardFlags(t)
	})

	server := &recordingEmergencyReparentServer{}
	localvtctldclient.SetServer(server)

	const position = "MySQL56/3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"
	command.Root.SetArgs([]string{"EmergencyReparentShard", "--required-position", position, "commerce/0"})

	require.NoError(t, command.Root.Execute())
	require.Len(t, server.requests, 1)
	assert.Equal(t, position, server.requests[0].GetRequiredPosition())
}

func TestERSSplitBrainPromotionFlags(t *testing.T) {
	originalProtocol := command.VtctldClientProtocol
	resetEmergencyReparentShardFlags(t)
	command.VtctldClientProtocol = "local"
	t.Cleanup(func() {
		command.Root.SetArgs(nil)
		command.VtctldClientProtocol = originalProtocol
		resetEmergencyReparentShardFlags(t)
	})

	t.Run("rejects missing new primary before RPC", func(t *testing.T) {
		resetEmergencyReparentShardFlags(t)
		server := &recordingEmergencyReparentServer{}
		localvtctldclient.SetServer(server)
		command.Root.SetArgs([]string{
			"EmergencyReparentShard",
			"--allow-split-brain-promotion",
			"commerce/0",
		})

		err := command.Root.Execute()
		require.ErrorContains(t, err, "--allow-split-brain-promotion requires --new-primary")
		assert.Empty(t, server.requests)
	})

	t.Run("sends override with new primary", func(t *testing.T) {
		resetEmergencyReparentShardFlags(t)
		server := &recordingEmergencyReparentServer{}
		localvtctldclient.SetServer(server)
		command.Root.SetArgs([]string{
			"EmergencyReparentShard",
			"--new-primary", "zone1-0000000100",
			"--allow-split-brain-promotion",
			"commerce/0",
		})

		err := command.Root.Execute()
		require.NoError(t, err)
		require.Len(t, server.requests, 1)
		require.NotNil(t, server.requests[0].GetNewPrimary())
		assert.Equal(t, "zone1", server.requests[0].GetNewPrimary().GetCell())
		assert.Equal(t, uint32(100), server.requests[0].GetNewPrimary().GetUid())
		assert.True(t, server.requests[0].GetAllowSplitBrainPromotion())
	})
}

type recordingPlannedReparentServer struct {
	vtctlservicepb.UnimplementedVtctldServer

	tablets  map[string]*topodatapb.Tablet
	requests []*vtctldatapb.PlannedReparentShardRequest
}

func (s *recordingPlannedReparentServer) GetTablet(_ context.Context, req *vtctldatapb.GetTabletRequest) (*vtctldatapb.GetTabletResponse, error) {
	tablet, ok := s.tablets[topoproto.TabletAliasString(req.GetTabletAlias())]
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_NOT_FOUND, "tablet %s not found", topoproto.TabletAliasString(req.GetTabletAlias()))
	}

	return &vtctldatapb.GetTabletResponse{Tablet: tablet}, nil
}

func (s *recordingPlannedReparentServer) PlannedReparentShard(_ context.Context, req *vtctldatapb.PlannedReparentShardRequest) (*vtctldatapb.PlannedReparentShardResponse, error) {
	s.requests = append(s.requests, req)

	return &vtctldatapb.PlannedReparentShardResponse{}, nil
}

func resetPlannedReparentShardFlags(t *testing.T) {
	t.Helper()

	for _, name := range []string{"new-primary", "avoid-primary"} {
		flag := command.PlannedReparentShard.Flags().Lookup(name)
		require.NotNil(t, flag)
		require.NoError(t, flag.Value.Set(flag.DefValue))
		flag.Changed = false
	}
}

// TestPRSKeyspaceShardIsOptionalWithNewPrimary checks that PlannedReparentShard
// resolves the shard from --new-primary when <keyspace/shard> is omitted, and
// still requires one of the two.
func TestPRSKeyspaceShardIsOptionalWithNewPrimary(t *testing.T) {
	originalProtocol := command.VtctldClientProtocol
	command.VtctldClientProtocol = "local"
	t.Cleanup(func() {
		command.Root.SetArgs(nil)
		command.VtctldClientProtocol = originalProtocol
		resetPlannedReparentShardFlags(t)
	})

	newServer := func() *recordingPlannedReparentServer {
		server := &recordingPlannedReparentServer{
			tablets: map[string]*topodatapb.Tablet{
				"zone1-0000000101": {
					Alias:    &topodatapb.TabletAlias{Cell: "zone1", Uid: 101},
					Keyspace: "commerce",
					Shard:    "-80",
				},
			},
		}
		localvtctldclient.SetServer(server)
		return server
	}

	t.Run("resolves shard from new primary", func(t *testing.T) {
		resetPlannedReparentShardFlags(t)
		server := newServer()
		command.Root.SetArgs([]string{"PlannedReparentShard", "--new-primary", "zone1-0000000101"})

		require.NoError(t, command.Root.Execute())
		require.Len(t, server.requests, 1)
		assert.Equal(t, "commerce", server.requests[0].GetKeyspace())
		assert.Equal(t, "-80", server.requests[0].GetShard())
		assert.Equal(t, uint32(101), server.requests[0].GetNewPrimary().GetUid())
	})

	t.Run("keeps explicit keyspace/shard", func(t *testing.T) {
		resetPlannedReparentShardFlags(t)
		server := newServer()
		command.Root.SetArgs([]string{"PlannedReparentShard", "--avoid-primary", "zone1-0000000100", "commerce/0"})

		require.NoError(t, command.Root.Execute())
		require.Len(t, server.requests, 1)
		assert.Equal(t, "commerce", server.requests[0].GetKeyspace())
		assert.Equal(t, "0", server.requests[0].GetShard())
	})

	t.Run("rejects missing keyspace/shard and new primary", func(t *testing.T) {
		resetPlannedReparentShardFlags(t)
		server := newServer()
		command.Root.SetArgs([]string{"PlannedReparentShard", "--avoid-primary", "zone1-0000000100"})

		err := command.Root.Execute()
		require.ErrorContains(t, err, "requires <keyspace/shard>, --new-primary, or both")
		assert.Empty(t, server.requests)
	})

	t.Run("fails when new primary is unknown", func(t *testing.T) {
		resetPlannedReparentShardFlags(t)
		server := newServer()
		command.Root.SetArgs([]string{"PlannedReparentShard", "--new-primary", "zone1-0000000999"})

		err := command.Root.Execute()
		require.ErrorContains(t, err, "failed to look up the shard of new primary zone1-0000000999")
		assert.Empty(t, server.requests)
	})
}
