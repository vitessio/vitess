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

package command

import (
	"context"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	vtctlservicepb "vitess.io/vitess/go/vt/proto/vtctlservice"
)

type replicationSourcePolicyClient struct {
	vtctlservicepb.VtctldClient

	request *vtctldatapb.SetKeyspaceReplicationSourcePolicyRequest
	err     error
}

func (c *replicationSourcePolicyClient) SetKeyspaceReplicationSourcePolicy(ctx context.Context, in *vtctldatapb.SetKeyspaceReplicationSourcePolicyRequest, opts ...grpc.CallOption) (*vtctldatapb.SetKeyspaceReplicationSourcePolicyResponse, error) {
	c.request = in
	if c.err != nil {
		return nil, c.err
	}
	return &vtctldatapb.SetKeyspaceReplicationSourcePolicyResponse{}, nil
}

func (c *replicationSourcePolicyClient) Close() error {
	return nil
}

func TestCommandSetKeyspaceReplicationSourcePolicy(t *testing.T) {
	origClient := client
	origCommandCtx := commandCtx
	origOptions := setKeyspaceReplicationSourcePolicyOptions
	t.Cleanup(func() {
		client = origClient
		commandCtx = origCommandCtx
		setKeyspaceReplicationSourcePolicyOptions = origOptions
	})

	fake := &replicationSourcePolicyClient{}
	client = fake
	commandCtx = t.Context()
	setKeyspaceReplicationSourcePolicyOptions.RdonlyPolicy = "replica"

	cmd := &cobra.Command{}
	require.NoError(t, cmd.Flags().Parse([]string{"commerce"}))

	err := commandSetKeyspaceReplicationSourcePolicy(cmd, []string{"commerce"})
	require.NoError(t, err)
	require.NotNil(t, fake.request)
	assert.Equal(t, "commerce", fake.request.Keyspace)
	assert.Equal(t, topodatapb.ReplicationSourceConfig_REPLICA, fake.request.RdonlyPolicy)
}

func TestCommandSetKeyspaceReplicationSourcePolicyErrors(t *testing.T) {
	origClient := client
	origCommandCtx := commandCtx
	origOptions := setKeyspaceReplicationSourcePolicyOptions
	t.Cleanup(func() {
		client = origClient
		commandCtx = origCommandCtx
		setKeyspaceReplicationSourcePolicyOptions = origOptions
	})

	tests := []struct {
		name         string
		rdonlyPolicy string
		client       *replicationSourcePolicyClient
		wantErr      string
	}{
		{
			name:         "invalid policy",
			rdonlyPolicy: "primary",
			client:       &replicationSourcePolicyClient{},
			wantErr:      `invalid --rdonly-policy "primary"`,
		},
		{
			name:         "client error",
			rdonlyPolicy: "replica",
			client:       &replicationSourcePolicyClient{err: assert.AnError},
			wantErr:      assert.AnError.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client = tt.client
			commandCtx = t.Context()
			setKeyspaceReplicationSourcePolicyOptions.RdonlyPolicy = tt.rdonlyPolicy

			cmd := &cobra.Command{}
			require.NoError(t, cmd.Flags().Parse([]string{"commerce"}))

			err := commandSetKeyspaceReplicationSourcePolicy(cmd, []string{"commerce"})
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestParseRdonlyReplicationSourcePolicy(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    topodatapb.ReplicationSourceConfig_RdonlyReplicationSourcePolicy
		wantErr string
	}{
		{
			name:  "empty",
			value: "",
			want:  topodatapb.ReplicationSourceConfig_UNSPECIFIED,
		},
		{
			name:  "unspecified",
			value: " unspecified ",
			want:  topodatapb.ReplicationSourceConfig_UNSPECIFIED,
		},
		{
			name:  "replica",
			value: "REPLICA",
			want:  topodatapb.ReplicationSourceConfig_REPLICA,
		},
		{
			name:    "invalid",
			value:   "primary",
			wantErr: `invalid --rdonly-policy "primary"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseRdonlyReplicationSourcePolicy(tt.value)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
