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
	"vitess.io/vitess/go/vt/vtctl/localvtctldclient"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
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

	for _, name := range []string{"allow-split-brain-promotion", "new-primary"} {
		flag := command.EmergencyReparentShard.Flags().Lookup(name)
		require.NotNil(t, flag)
		require.NoError(t, flag.Value.Set(flag.DefValue))
		flag.Changed = false
	}
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
