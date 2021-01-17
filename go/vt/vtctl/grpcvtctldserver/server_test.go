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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"

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
	testutil.AddKeyspace(ctx, t, ts, ks)

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
	testutil.AddKeyspace(ctx, t, ts, expected.Keyspace)

	ks, err := vtctld.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{Keyspace: expected.Keyspace.Name})
	assert.NoError(t, err)
	assert.Equal(t, expected, ks)

	_, err = vtctld.GetKeyspace(ctx, &vtctldatapb.GetKeyspaceRequest{Keyspace: "notfound"})
	assert.Error(t, err)
}

func TestGetCellInfoNames(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1", "cell2", "cell3")
	vtctld := NewVtctldServer(ts)

	resp, err := vtctld.GetCellInfoNames(ctx, &vtctldatapb.GetCellInfoNamesRequest{})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"cell1", "cell2", "cell3"}, resp.Names)

	vtctld.ts = memorytopo.NewServer()

	resp, err = vtctld.GetCellInfoNames(ctx, &vtctldatapb.GetCellInfoNamesRequest{})
	assert.NoError(t, err)
	assert.Empty(t, resp.Names)

	var topofactory *memorytopo.Factory
	vtctld.ts, topofactory = memorytopo.NewServerAndFactory("cell1")

	topofactory.SetError(assert.AnError)
	_, err = vtctld.GetCellInfoNames(ctx, &vtctldatapb.GetCellInfoNamesRequest{})
	assert.Error(t, err)
}

func TestGetCellInfo(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer()
	vtctld := NewVtctldServer(ts)

	expected := &topodatapb.CellInfo{
		ServerAddress: "example.com",
		Root:          "vitess",
	}
	input := *expected // shallow copy
	require.NoError(t, ts.CreateCellInfo(ctx, "cell1", &input))

	resp, err := vtctld.GetCellInfo(ctx, &vtctldatapb.GetCellInfoRequest{Cell: "cell1"})
	assert.NoError(t, err)
	assert.Equal(t, expected, resp.CellInfo)

	_, err = vtctld.GetCellInfo(ctx, &vtctldatapb.GetCellInfoRequest{Cell: "does_not_exist"})
	assert.Error(t, err)

	_, err = vtctld.GetCellInfo(ctx, &vtctldatapb.GetCellInfoRequest{})
	assert.Error(t, err)
}

func TestGetCellsAliases(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("c11", "c12", "c13", "c21", "c22")
	vtctld := NewVtctldServer(ts)

	alias1 := &topodatapb.CellsAlias{
		Cells: []string{"c11", "c12", "c13"},
	}
	alias2 := &topodatapb.CellsAlias{
		Cells: []string{"c21", "c22"},
	}

	for i, alias := range []*topodatapb.CellsAlias{alias1, alias2} {
		input := *alias // shallow copy
		name := fmt.Sprintf("a%d", i+1)

		require.NoError(t, ts.CreateCellsAlias(ctx, name, &input), "cannot create cells alias %d (idx = %d) = %+v", i+1, i, &input)
	}

	expected := map[string]*topodatapb.CellsAlias{
		"a1": alias1,
		"a2": alias2,
	}

	resp, err := vtctld.GetCellsAliases(ctx, &vtctldatapb.GetCellsAliasesRequest{})
	assert.NoError(t, err)
	assert.Equal(t, expected, resp.Aliases)

	ts, topofactory := memorytopo.NewServerAndFactory()
	vtctld.ts = ts

	topofactory.SetError(assert.AnError)
	_, err = vtctld.GetCellsAliases(ctx, &vtctldatapb.GetCellsAliasesRequest{})
	assert.Error(t, err)
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
		testutil.AddKeyspace(ctx, t, ts, ks)
	}

	resp, err = vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	assert.NoError(t, err)
	assert.Equal(t, expected, resp.Keyspaces)

	topofactory.SetError(errors.New("error from toposerver"))

	_, err = vtctld.GetKeyspaces(ctx, &vtctldatapb.GetKeyspacesRequest{})
	assert.Error(t, err)
}
