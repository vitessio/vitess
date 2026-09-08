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
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

type topologyTreeFakeServer struct {
	fakeVTAdminServer
}

func (f *topologyTreeFakeServer) GetKeyspaces(ctx context.Context, req *vtadminpb.GetKeyspacesRequest) (*vtadminpb.GetKeyspacesResponse, error) {
	if !slices.Contains(req.ClusterIds, "local") && len(req.ClusterIds) > 0 {
		return &vtadminpb.GetKeyspacesResponse{}, nil
	}
	return &vtadminpb.GetKeyspacesResponse{Keyspaces: []*vtadminpb.Keyspace{
		{
			Cluster:  &vtadminpb.Cluster{Id: "local"},
			Keyspace: &vtctldatapb.Keyspace{Name: "commerce"},
			Shards: map[string]*vtctldatapb.Shard{
				"0":   {Name: "0"},
				"-80": {Name: "-80"},
			},
		},
		{
			Cluster:  &vtadminpb.Cluster{Id: "local"},
			Keyspace: &vtctldatapb.Keyspace{Name: "customer"},
			Shards: map[string]*vtctldatapb.Shard{
				"-": {Name: "-"},
			},
		},
	}}, nil
}

func (f *topologyTreeFakeServer) GetTablets(ctx context.Context, req *vtadminpb.GetTabletsRequest) (*vtadminpb.GetTabletsResponse, error) {
	if !slices.Contains(req.ClusterIds, "local") && len(req.ClusterIds) > 0 {
		return &vtadminpb.GetTabletsResponse{}, nil
	}
	tablet := func(cell string, uid uint32, ks, shard string, typ topodatapb.TabletType) *vtadminpb.Tablet {
		return &vtadminpb.Tablet{
			Cluster: &vtadminpb.Cluster{Id: "local"},
			Tablet: &topodatapb.Tablet{
				Alias:    &topodatapb.TabletAlias{Cell: cell, Uid: uid},
				Keyspace: ks,
				Shard:    shard,
				Type:     typ,
			},
		}
	}
	return &vtadminpb.GetTabletsResponse{Tablets: []*vtadminpb.Tablet{
		tablet("zone1", 100, "commerce", "0", topodatapb.TabletType_PRIMARY),
		tablet("zone1", 101, "commerce", "0", topodatapb.TabletType_REPLICA),
		tablet("zone2", 200, "commerce", "-80", topodatapb.TabletType_REPLICA),
	}}, nil
}

func TestTopologyTreeRendersHierarchy(t *testing.T) {
	fake := &topologyTreeFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/topology/local", nil)
	s.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()

	// Cells.
	assert.Contains(t, body, "zone1")
	assert.Contains(t, body, "zone2")
	// Keyspaces.
	assert.Contains(t, body, "commerce")
	assert.Contains(t, body, "customer")
	// Shards.
	assert.Contains(t, body, "-80")
	// Tablet links.
	assert.Contains(t, body, "/tablet/local/zone1-0000000100")
	// Tablet counts.
	assert.Contains(t, body, "2")
}

func TestTopologyTreeUnknownCluster(t *testing.T) {
	fake := &topologyTreeFakeServer{}
	s, err := NewServer(fake, Options{})
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/topology/bogus", nil)
	s.ServeHTTP(rec, req)

	// An unknown cluster simply renders an empty tree with a note.
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "No keyspaces found.")
}
