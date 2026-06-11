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

package localvtctldclient

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

type localReplicationSourcePolicyServer struct {
	vtctlservicepb.UnimplementedVtctldServer

	request *vtctldatapb.SetKeyspaceReplicationSourcePolicyRequest
}

func (s *localReplicationSourcePolicyServer) SetKeyspaceReplicationSourcePolicy(ctx context.Context, in *vtctldatapb.SetKeyspaceReplicationSourcePolicyRequest) (*vtctldatapb.SetKeyspaceReplicationSourcePolicyResponse, error) {
	s.request = in
	return &vtctldatapb.SetKeyspaceReplicationSourcePolicyResponse{}, nil
}

func TestSetKeyspaceReplicationSourcePolicy(t *testing.T) {
	server := &localReplicationSourcePolicyServer{}
	client := New(server)

	req := &vtctldatapb.SetKeyspaceReplicationSourcePolicyRequest{Keyspace: "ks"}
	resp, err := client.SetKeyspaceReplicationSourcePolicy(t.Context(), req)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, req, server.request)
}
