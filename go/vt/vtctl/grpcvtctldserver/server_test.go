/*
Copyright 2020 The Vitess Authors.

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

package grpcvtctldserver

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func TestFindAllShardsInKeyspace(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	vtctld := NewVtctldServer(ts)

	ks := &vtctldatapb.Keyspace{
		Name:     "testkeyspace",
		Keyspace: &topodatapb.Keyspace{},
	}
	addKeyspace(ctx, t, ts, ks)

	si1, err := ts.GetOrCreateShard(ctx, ks.Name, "-80")
	require.NoError(t, err)
	si2, err := ts.GetOrCreateShard(ctx, ks.Name, "80-")
	require.NoError(t, err)

	resp, err := vtctld.FindAllShardsInKeyspace(ctx, &vtctldatapb.FindAllShardsInKeyspaceRequest{Keyspace: ks.Name})
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	expected := map[string]*vtctldatapb.Shard{
		"-80": {
			Keyspace: ks.Name,
			Name:     "-80",
			Shard:    si1.Shard,
		},
		"80-": {
			Keyspace: ks.Name,
			Name:     "80-",
			Shard:    si2.Shard,
		},
	}

	assert.Equal(t, expected, resp.Shards)

	_, err = vtctld.FindAllShardsInKeyspace(ctx, &vtctldatapb.FindAllShardsInKeyspaceRequest{Keyspace: "nothing"})
	assert.Error(t, err)
}

func TestGetKeyspace(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	vtctld := NewVtctldServer(ts)

	expected := &vtctldatapb.GetKeyspaceResponse{
		Keyspace: &vtctldatapb.Keyspace{
			Name: "testkeyspace",
			Keyspace: &topodatapb.Keyspace{
				ShardingColumnName: "col1",
			},
		},
	}
	addKeyspace(ctx, t, ts, expected.Keyspace)

	ks, err := vtctld.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{Keyspace: expected.Keyspace.Name})
	assert.NoError(t, err)
	assert.Equal(t, expected, ks)

	_, err = vtctld.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{Keyspace: "notfound"})
	assert.Error(t, err)
}

func addKeyspace(ctx context.Context, t *testing.T, ts *topo.Server, ks *vtctldatapb.Keyspace) {
	in := *ks.Keyspace // take a copy to avoid the XXX_ fields changing

	err := ts.CreateKeyspace(ctx, ks.Name, &in)
	require.NoError(t, err)
}

func TestGetKeyspaces(t *testing.T) {
	ctx := context.Background()
	ts, topofactory := memorytopo.NewServerAndFactory("cell1")
	vtctld := NewVtctldServer(ts)

	resp, err := vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	assert.NoError(t, err)
	assert.Empty(t, resp.Keyspaces)

	expected := []*vtctldatapb.Keyspace{
		{
			Name: "ks1",
			Keyspace: &topodatapb.Keyspace{
				ShardingColumnName: "ks1_col1",
			},
		},
		{
			Name: "ks2",
			Keyspace: &topodatapb.Keyspace{
				ShardingColumnName: "ks2_col1",
			},
		},
		{
			Name: "ks3",
			Keyspace: &topodatapb.Keyspace{
				ShardingColumnName: "ks3_col1",
			},
		},
	}
	for _, ks := range expected {
		addKeyspace(ctx, t, ts, ks)
	}

	resp, err = vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	assert.NoError(t, err)
	assert.Equal(t, expected, resp.Keyspaces)

	topofactory.SetError(errors.New("error from toposerver"))

	_, err = vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	assert.Error(t, err)
}
