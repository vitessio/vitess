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

package vtadmin

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
	"vitess.io/vitess/go/vt/vtadmin/rbac"
	vtadmintestutil "vitess.io/vitess/go/vt/vtadmin/testutil"
	"vitess.io/vitess/go/vt/vtadmin/vtctldclient/fakevtctldclient"
	"vitess.io/vitess/go/vt/vtenv"
)

func TestVDiffCreateRequiresPutAction(t *testing.T) {
	t.Parallel()

	opts := Options{
		RBAC: &rbac.Config{
			Rules: []*struct {
				Resource string
				Actions  []string
				Subjects []string
				Clusters []string
			}{
				{
					Resource: "Cluster",
					Actions:  []string{"get"},
					Subjects: []string{"user:readonly"},
					Clusters: []string{"*"},
				},
				{
					Resource: "Cluster",
					Actions:  []string{"put"},
					Subjects: []string{"user:allowed"},
					Clusters: []string{"*"},
				},
			},
		},
	}
	require.NoError(t, opts.RBAC.Reify())

	fake := &fakevtctldclient.VtctldClient{}
	api := NewAPI(vtenv.NewTestEnv(), vtadmintestutil.BuildClusters(t, vtadmintestutil.TestClusterConfig{
		Cluster:      &vtadminpb.Cluster{Id: "c1", Name: "cluster1"},
		VtctldClient: fake,
	}), opts)
	t.Cleanup(func() {
		assert.NoError(t, api.Close())
	})

	t.Run("read-only actor is not permitted", func(t *testing.T) {
		ctx := rbac.NewContext(t.Context(), &rbac.Actor{Name: "readonly"})
		resp, err := api.VDiffCreate(ctx, &vtadminpb.VDiffCreateRequest{
			ClusterId: "c1",
			Request:   &vtctldatapb.VDiffCreateRequest{Workflow: "wf", TargetKeyspace: "ks"},
		})
		require.ErrorContains(t, err, "unauthorized")
		assert.Nil(t, resp)
		assert.Nil(t, fake.LastVDiffCreateRequest)
	})

	t.Run("actor with put action is permitted", func(t *testing.T) {
		ctx := rbac.NewContext(t.Context(), &rbac.Actor{Name: "allowed"})
		resp, err := api.VDiffCreate(ctx, &vtadminpb.VDiffCreateRequest{
			ClusterId: "c1",
			Request:   &vtctldatapb.VDiffCreateRequest{Workflow: "wf", TargetKeyspace: "ks", Uuid: "explicit-uuid"},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, "explicit-uuid", resp.GetUUID())
	})
}

func TestVDiffCreatePreservesExplicitOptions(t *testing.T) {
	t.Parallel()

	fake := &fakevtctldclient.VtctldClient{}
	api := NewAPI(vtenv.NewTestEnv(), vtadmintestutil.BuildClusters(t, vtadmintestutil.TestClusterConfig{
		Cluster:      &vtadminpb.Cluster{Id: "c1", Name: "cluster1"},
		VtctldClient: fake,
	}), Options{})
	t.Cleanup(func() {
		assert.NoError(t, api.Close())
	})

	resp, err := api.VDiffCreate(t.Context(), &vtadminpb.VDiffCreateRequest{
		ClusterId: "c1",
		Request: &vtctldatapb.VDiffCreateRequest{
			Workflow:       "wf",
			TargetKeyspace: "ks",
			Uuid:           "kept-uuid",
			AutoRetry:      false,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, "kept-uuid", resp.GetUUID())

	got := fake.LastVDiffCreateRequest
	require.NotNil(t, got)
	assert.Equal(t, "kept-uuid", got.Uuid)
	assert.False(t, got.AutoRetry)
}

func TestReloadSchemaShardForwardsKeyspaceAndShard(t *testing.T) {
	t.Parallel()

	api := NewAPI(vtenv.NewTestEnv(), vtadmintestutil.BuildClusters(t, vtadmintestutil.TestClusterConfig{
		Cluster: &vtadminpb.Cluster{Id: "c1", Name: "cluster1"},
		VtctldClient: &fakevtctldclient.VtctldClient{
			ReloadSchemaShardResults: map[string]struct {
				Response *vtctldatapb.ReloadSchemaShardResponse
				Error    error
			}{
				"commerce/0": {
					Response: &vtctldatapb.ReloadSchemaShardResponse{},
				},
			},
		},
	}), Options{})
	t.Cleanup(func() {
		assert.NoError(t, api.Close())
	})

	resp, err := api.ReloadSchemaShard(t.Context(), &vtadminpb.ReloadSchemaShardRequest{
		ClusterId: "c1",
		Keyspace:  "commerce",
		Shard:     "0",
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
}

func TestReloadSchemaShardRequiresReloadAction(t *testing.T) {
	t.Parallel()

	opts := Options{
		RBAC: &rbac.Config{
			Rules: []*struct {
				Resource string
				Actions  []string
				Subjects []string
				Clusters []string
			}{
				{
					Resource: "Schema",
					Actions:  []string{"get"},
					Subjects: []string{"user:readonly"},
					Clusters: []string{"*"},
				},
				{
					Resource: "Schema",
					Actions:  []string{"reload"},
					Subjects: []string{"user:allowed"},
					Clusters: []string{"*"},
				},
			},
		},
	}
	require.NoError(t, opts.RBAC.Reify())

	api := NewAPI(vtenv.NewTestEnv(), vtadmintestutil.BuildClusters(t, vtadmintestutil.TestClusterConfig{
		Cluster: &vtadminpb.Cluster{Id: "c1", Name: "cluster1"},
		VtctldClient: &fakevtctldclient.VtctldClient{
			ReloadSchemaShardResults: map[string]struct {
				Response *vtctldatapb.ReloadSchemaShardResponse
				Error    error
			}{
				"commerce/0": {
					Response: &vtctldatapb.ReloadSchemaShardResponse{},
				},
			},
		},
	}), opts)
	t.Cleanup(func() {
		assert.NoError(t, api.Close())
	})

	t.Run("read-only actor is not permitted", func(t *testing.T) {
		t.Parallel()

		ctx := rbac.NewContext(t.Context(), &rbac.Actor{Name: "readonly"})
		resp, err := api.ReloadSchemaShard(ctx, &vtadminpb.ReloadSchemaShardRequest{
			ClusterId: "c1",
			Keyspace:  "commerce",
			Shard:     "0",
		})
		require.ErrorContains(t, err, "unauthorized")
		assert.Nil(t, resp)
	})

	t.Run("actor with reload action is permitted", func(t *testing.T) {
		t.Parallel()

		ctx := rbac.NewContext(t.Context(), &rbac.Actor{Name: "allowed"})
		resp, err := api.ReloadSchemaShard(ctx, &vtadminpb.ReloadSchemaShardRequest{
			ClusterId: "c1",
			Keyspace:  "commerce",
			Shard:     "0",
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}
