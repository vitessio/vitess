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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/mysqlctl/backupstorage"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtctl/grpcvtctldserver/testutil"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	mysqlctlpb "vitess.io/vitess/go/vt/proto/mysqlctl"
	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	vtctldatapb "vitess.io/vitess/go/vt/proto/vtctldata"
)

func init() {
	*tmclient.TabletManagerProtocol = testutil.TabletManagerClientProtocol
	*backupstorage.BackupStorageImplementation = testutil.BackupStorageImplementation
}

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

func TestGetBackups(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer()
	vtctld := NewVtctldServer(ts)

	testutil.BackupStorage.Backups = map[string][]string{
		"testkeyspace/-": {"backup1", "backup2"},
	}

	expected := &vtctldatapb.GetBackupsResponse{
		Backups: []*mysqlctlpb.BackupInfo{
			{
				Directory: "testkeyspace/-",
				Name:      "backup1",
			},
			{
				Directory: "testkeyspace/-",
				Name:      "backup2",
			},
		},
	}

	resp, err := vtctld.GetBackups(ctx, &vtctldatapb.GetBackupsRequest{
		Keyspace: "testkeyspace",
		Shard:    "-",
	})
	assert.NoError(t, err)
	assert.Equal(t, expected, resp)

	t.Run("no backupstorage", func(t *testing.T) {
		*backupstorage.BackupStorageImplementation = "doesnotexist"
		defer func() { *backupstorage.BackupStorageImplementation = testutil.BackupStorageImplementation }()

		_, err := vtctld.GetBackups(ctx, &vtctldatapb.GetBackupsRequest{
			Keyspace: "testkeyspace",
			Shard:    "-",
		})
		assert.Error(t, err)
	})

	t.Run("listbackups error", func(t *testing.T) {
		testutil.BackupStorage.ListBackupsError = assert.AnError
		defer func() { testutil.BackupStorage.ListBackupsError = nil }()

		_, err := vtctld.GetBackups(ctx, &vtctldatapb.GetBackupsRequest{
			Keyspace: "testkeyspace",
			Shard:    "-",
		})
		assert.Error(t, err)
	})
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

func TestGetTablet(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("cell1")
	vtctld := NewVtctldServer(ts)

	tablet := &topodatapb.Tablet{
		Alias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
		Hostname: "localhost",
		Keyspace: "testkeyspace",
		Shard:    "-",
		Type:     topodatapb.TabletType_REPLICA,
	}

	testutil.AddTablet(ctx, t, ts, tablet)

	resp, err := vtctld.GetTablet(ctx, &vtctldatapb.GetTabletRequest{
		TabletAlias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  100,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, resp.Tablet, tablet)

	// not found
	_, err = vtctld.GetTablet(ctx, &vtctldatapb.GetTabletRequest{
		TabletAlias: &topodatapb.TabletAlias{
			Cell: "cell1",
			Uid:  101,
		},
	})
	assert.Error(t, err)
}

func TestGetSchema(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("zone1")
	vtctld := NewVtctldServer(ts)

	validAlias := &topodatapb.TabletAlias{
		Cell: "zone1",
		Uid:  100,
	}
	testutil.AddTablet(ctx, t, ts, &topodatapb.Tablet{
		Alias: validAlias,
	})
	otherAlias := &topodatapb.TabletAlias{
		Cell: "zone1",
		Uid:  101,
	}
	testutil.AddTablet(ctx, t, ts, &topodatapb.Tablet{
		Alias: otherAlias,
	})

	// we need to run this on each test case or they will pollute each other
	setupSchema := func() {
		testutil.TabletManagerClient.Schemas[topoproto.TabletAliasString(validAlias)] = &tabletmanagerdatapb.SchemaDefinition{
			DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
			TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
				{
					Name: "t1",
					Schema: `CREATE TABLE t1 (
	id int(11) not null,
	PRIMARY KEY (id)
);`,
					Type:       "BASE",
					Columns:    []string{"id"},
					DataLength: 100,
					RowCount:   50,
					Fields: []*querypb.Field{
						{
							Name: "id",
							Type: querypb.Type_INT32,
						},
					},
				},
			},
		}
	}

	tests := []*struct {
		name      string
		req       *vtctldatapb.GetSchemaRequest
		expected  *vtctldatapb.GetSchemaResponse
		shouldErr bool
	}{
		{
			name: "normal path",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias: validAlias,
			},
			expected: &vtctldatapb.GetSchemaResponse{
				Schema: &tabletmanagerdatapb.SchemaDefinition{
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name: "t1",
							Schema: `CREATE TABLE t1 (
	id int(11) not null,
	PRIMARY KEY (id)
);`,
							Type:       "BASE",
							Columns:    []string{"id"},
							DataLength: 100,
							RowCount:   50,
							Fields: []*querypb.Field{
								{
									Name: "id",
									Type: querypb.Type_INT32,
								},
							},
						},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "table names only",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias:    validAlias,
				TableNamesOnly: true,
			},
			expected: &vtctldatapb.GetSchemaResponse{
				Schema: &tabletmanagerdatapb.SchemaDefinition{
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name: "t1",
						},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "table sizes only",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias:    validAlias,
				TableSizesOnly: true,
			},
			expected: &vtctldatapb.GetSchemaResponse{
				Schema: &tabletmanagerdatapb.SchemaDefinition{
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name:       "t1",
							Type:       "BASE",
							DataLength: 100,
							RowCount:   50,
						},
					},
				},
			},
			shouldErr: false,
		},
		{
			name: "table names take precedence over table sizes",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias:    validAlias,
				TableNamesOnly: true,
				TableSizesOnly: true,
			},
			expected: &vtctldatapb.GetSchemaResponse{
				Schema: &tabletmanagerdatapb.SchemaDefinition{
					DatabaseSchema: "CREATE DATABASE vt_testkeyspace",
					TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
						{
							Name: "t1",
						},
					},
				},
			},
			shouldErr: false,
		},
		// error cases
		{
			name: "no tablet",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias: &topodatapb.TabletAlias{
					Cell: "notfound",
					Uid:  100,
				},
			},
			expected:  nil,
			shouldErr: true,
		},
		{
			name: "no schema",
			req: &vtctldatapb.GetSchemaRequest{
				TabletAlias: otherAlias,
			},
			expected:  nil,
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setupSchema()

			resp, err := vtctld.GetSchema(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tt.expected, resp)
		})
	}
}

func TestGetSrvVSchema(t *testing.T) {
	ctx := context.Background()
	ts, topofactory := memorytopo.NewServerAndFactory("zone1", "zone2")
	vtctld := NewVtctldServer(ts)

	zone1SrvVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"testkeyspace": {
				Sharded:                true,
				RequireExplicitRouting: false,
			},
		},
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{},
		},
	}
	zone2SrvVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"testkeyspace": {
				Sharded:                true,
				RequireExplicitRouting: false,
			},
			"unsharded": {
				Sharded:                false,
				RequireExplicitRouting: false,
			},
		},
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{},
		},
	}

	err := ts.UpdateSrvVSchema(ctx, "zone1", zone1SrvVSchema)
	require.NoError(t, err, "cannot add zone1 srv vschema")
	err = ts.UpdateSrvVSchema(ctx, "zone2", zone2SrvVSchema)
	require.NoError(t, err, "cannot add zone2 srv vschema")

	expected := &vschemapb.SrvVSchema{ // have to copy our structs because of proto marshal artifacts
		Keyspaces: map[string]*vschemapb.Keyspace{
			"testkeyspace": {
				Sharded:                true,
				RequireExplicitRouting: false,
			},
		},
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{},
		},
	}
	resp, err := vtctld.GetSrvVSchema(ctx, &vtctldatapb.GetSrvVSchemaRequest{Cell: "zone1"})
	assert.NoError(t, err)
	assert.Equal(t, expected.Keyspaces, resp.SrvVSchema.Keyspaces, "GetSrvVSchema(zone1) mismatch")
	assert.ElementsMatch(t, expected.RoutingRules.Rules, resp.SrvVSchema.RoutingRules.Rules, "GetSrvVSchema(zone1) rules mismatch")

	expected = &vschemapb.SrvVSchema{ // have to copy our structs because of proto marshal artifacts
		Keyspaces: map[string]*vschemapb.Keyspace{
			"testkeyspace": {
				Sharded:                true,
				RequireExplicitRouting: false,
			},
			"unsharded": {
				Sharded:                false,
				RequireExplicitRouting: false,
			},
		},
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{},
		},
	}
	resp, err = vtctld.GetSrvVSchema(ctx, &vtctldatapb.GetSrvVSchemaRequest{Cell: "zone2"})
	assert.NoError(t, err)
	assert.Equal(t, expected.Keyspaces, resp.SrvVSchema.Keyspaces, "GetSrvVSchema(zone2) mismatch %+v %+v", zone2SrvVSchema.Keyspaces["testkeyspace"], resp.SrvVSchema.Keyspaces["testkeyspace"])
	assert.ElementsMatch(t, expected.RoutingRules.Rules, resp.SrvVSchema.RoutingRules.Rules, "GetSrvVSchema(zone2) rules mismatch")

	resp, err = vtctld.GetSrvVSchema(ctx, &vtctldatapb.GetSrvVSchemaRequest{Cell: "dne"})
	assert.Error(t, err, "GetSrvVSchema(dne)")
	assert.Nil(t, resp, "GetSrvVSchema(dne)")

	topofactory.SetError(assert.AnError)
	_, err = vtctld.GetSrvVSchema(ctx, &vtctldatapb.GetSrvVSchemaRequest{Cell: "zone1"})
	assert.Error(t, err)
}

func TestGetTablets(t *testing.T) {
	tests := []struct {
		name      string
		cells     []string
		tablets   []*topodatapb.Tablet
		req       *vtctldatapb.GetTabletsRequest
		expected  []*topodatapb.Tablet
		shouldErr bool
	}{
		{
			name:      "no tablets",
			cells:     []string{"cell1"},
			tablets:   []*topodatapb.Tablet{},
			req:       &vtctldatapb.GetTabletsRequest{},
			expected:  []*topodatapb.Tablet{},
			shouldErr: false,
		},
		{
			name:  "keyspace and shard filter",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
					Shard:    "-80",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Keyspace: "ks1",
					Shard:    "80-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  102,
					},
					Keyspace: "ks2",
					Shard:    "-",
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Keyspace: "ks1",
				Shard:    "80-",
			},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Keyspace: "ks1",
					Shard:    "80-",
				},
			},
			shouldErr: false,
		},
		{
			name:  "keyspace filter",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Keyspace: "ks1",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  102,
					},
					Keyspace: "otherkeyspace",
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Keyspace: "ks1",
			},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Keyspace: "ks1",
				},
			},
			shouldErr: false,
		},
		{
			name:  "keyspace and shard filter - stale primary",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
					Shard:    "-80",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Keyspace: "ks1",
					Shard:    "80-",
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  102,
					},
					Keyspace:            "ks2",
					Shard:               "-",
					Type:                topodatapb.TabletType_MASTER,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  103,
					},
					Keyspace:            "ks2",
					Shard:               "-",
					Hostname:            "stale.primary",
					Type:                topodatapb.TabletType_MASTER,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 14, 4, 5, 0, time.UTC)),
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Keyspace: "ks2",
				Shard:    "-",
			},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  102,
					},
					Keyspace:            "ks2",
					Shard:               "-",
					Type:                topodatapb.TabletType_MASTER,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  103,
					},
					Keyspace:            "ks2",
					Shard:               "-",
					Hostname:            "stale.primary",
					Type:                topodatapb.TabletType_UNKNOWN,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 14, 4, 5, 0, time.UTC)),
				},
			},
			shouldErr: false,
		},
		{
			name:  "stale primary",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace:            "ks1",
					Shard:               "-",
					Hostname:            "slightly less stale",
					Type:                topodatapb.TabletType_MASTER,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Hostname:            "stale primary",
					Keyspace:            "ks1",
					Shard:               "-",
					Type:                topodatapb.TabletType_MASTER,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 14, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  103,
					},
					Hostname:            "true primary",
					Keyspace:            "ks1",
					Shard:               "-",
					Type:                topodatapb.TabletType_MASTER,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 16, 4, 5, 0, time.UTC)),
				},
			},
			req: &vtctldatapb.GetTabletsRequest{},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace:            "ks1",
					Shard:               "-",
					Hostname:            "slightly less stale",
					Type:                topodatapb.TabletType_UNKNOWN,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 15, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  101,
					},
					Hostname:            "stale primary",
					Keyspace:            "ks1",
					Shard:               "-",
					Type:                topodatapb.TabletType_UNKNOWN,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 14, 4, 5, 0, time.UTC)),
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  103,
					},
					Hostname:            "true primary",
					Keyspace:            "ks1",
					Shard:               "-",
					Type:                topodatapb.TabletType_MASTER,
					MasterTermStartTime: logutil.TimeToProto(time.Date(2006, time.January, 2, 16, 4, 5, 0, time.UTC)),
				},
			},
			shouldErr: false,
		},
		{
			name:    "keyspace and shard filter - error",
			cells:   []string{"cell1"},
			tablets: []*topodatapb.Tablet{},
			req: &vtctldatapb.GetTabletsRequest{
				Keyspace: "ks1",
				Shard:    "-",
			},
			expected:  []*topodatapb.Tablet{},
			shouldErr: true,
		},
		{
			name:  "cells filter",
			cells: []string{"cell1", "cell2", "cell3"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell2",
						Uid:  200,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell3",
						Uid:  300,
					},
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Cells: []string{"cell1", "cell3"},
			},
			expected: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
				},
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell3",
						Uid:  300,
					},
				},
			},
			shouldErr: false,
		},
		{
			name:  "cells filter - error",
			cells: []string{"cell1"},
			tablets: []*topodatapb.Tablet{
				{
					Alias: &topodatapb.TabletAlias{
						Cell: "cell1",
						Uid:  100,
					},
					Keyspace: "ks1",
					Shard:    "-",
				},
			},
			req: &vtctldatapb.GetTabletsRequest{
				Cells: []string{"cell1", "doesnotexist"},
			},
			expected:  []*topodatapb.Tablet{},
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			ts := memorytopo.NewServer(tt.cells...)
			vtctld := NewVtctldServer(ts)

			for _, tablet := range tt.tablets {
				testutil.AddTablet(ctx, t, ts, tablet)
			}

			resp, err := vtctld.GetTablets(ctx, tt.req)
			if tt.shouldErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.ElementsMatch(t, tt.expected, resp.Tablets)
		})
	}
}

func TestGetVSchema(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer("zone1")
	vtctld := NewVtctldServer(ts)

	err := ts.SaveVSchema(ctx, "testkeyspace", &vschemapb.Keyspace{
		Sharded: true,
		Vindexes: map[string]*vschemapb.Vindex{
			"v1": {
				Type: "hash",
			},
		},
	})
	require.NoError(t, err)

	expected := &vtctldatapb.GetVSchemaResponse{
		VSchema: &vschemapb.Keyspace{
			Sharded: true,
			Vindexes: map[string]*vschemapb.Vindex{
				"v1": {
					Type: "hash",
				},
			},
		},
	}

	resp, err := vtctld.GetVSchema(ctx, &vtctldatapb.GetVSchemaRequest{
		Keyspace: "testkeyspace",
	})
	assert.NoError(t, err)
	assert.Equal(t, expected, resp)

	t.Run("not found", func(t *testing.T) {
		_, err := vtctld.GetVSchema(ctx, &vtctldatapb.GetVSchemaRequest{
			Keyspace: "doesnotexist",
		})
		assert.Error(t, err)
	})
}
