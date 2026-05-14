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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type topologyFakeServer struct {
	fakeVTAdminServer
	topologyPathRequest              *vtadminpb.GetTopologyPathRequest
	shardReplicationPositionsRequest *vtadminpb.GetShardReplicationPositionsRequest
	getTopologyPathNil               bool
}

func (f *topologyFakeServer) GetTopologyPath(ctx context.Context, req *vtadminpb.GetTopologyPathRequest) (*vtctldatapb.GetTopologyPathResponse, error) {
	f.topologyPathRequest = req
	if f.getTopologyPathNil {
		return nil, nil
	}
	return &vtctldatapb.GetTopologyPathResponse{
		Cell: &vtctldatapb.TopologyCell{
			Name:     "zone1",
			Path:     "/vitess",
			Children: []string{"keyspaces"},
		},
	}, nil
}

func (f *topologyFakeServer) GetShardReplicationPositions(ctx context.Context, req *vtadminpb.GetShardReplicationPositionsRequest) (*vtadminpb.GetShardReplicationPositionsResponse, error) {
	f.shardReplicationPositionsRequest = req
	return &vtadminpb.GetShardReplicationPositionsResponse{
		ReplicationPositions: []*vtadminpb.ClusterShardReplicationPosition{
			{
				Cluster:      &vtadminpb.Cluster{Id: "local", Name: "Local"},
				Keyspace:     "commerce",
				Shard:        "0",
				PositionInfo: &vtctldatapb.ShardReplicationPositionsResponse{},
			},
		},
	}, nil
}

func TestTopologyPathPageCallsServerAndRendersResponse(t *testing.T) {
	fake := &topologyFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/topology?cluster_id=local&path=/vitess", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.topologyPathRequest)
	assert.Equal(t, "local", fake.topologyPathRequest.GetClusterId())
	assert.Equal(t, "/vitess", fake.topologyPathRequest.GetPath())
	assert.Contains(t, rec.Body.String(), "Topology")
	assert.Contains(t, rec.Body.String(), "/vitess")
	assert.Contains(t, rec.Body.String(), "keyspaces")
}

func TestTopologyPathPageRequiresClusterID(t *testing.T) {
	fake := &topologyFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/topology", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "cluster_id is required")
	assert.Nil(t, fake.topologyPathRequest)
}

func TestTopologyPathNilResponseReturnsNotFound(t *testing.T) {
	fake := &topologyFakeServer{getTopologyPathNil: true}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/topology?cluster_id=local&path=/vitess", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestShardsPageCallsServerAndRendersRows(t *testing.T) {
	fake := &topologyFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/shards?cluster_id=local&keyspace=commerce&keyspace_shard=commerce/0", nil)
	s.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	require.NotNil(t, fake.shardReplicationPositionsRequest)
	assert.Equal(t, []string{"local"}, fake.shardReplicationPositionsRequest.GetClusterIds())
	assert.Equal(t, []string{"commerce"}, fake.shardReplicationPositionsRequest.GetKeyspaces())
	assert.Equal(t, []string{"commerce/0"}, fake.shardReplicationPositionsRequest.GetKeyspaceShards())
	assert.Contains(t, rec.Body.String(), "local")
	assert.Contains(t, rec.Body.String(), "commerce")
	assert.Contains(t, rec.Body.String(), "0")
}
