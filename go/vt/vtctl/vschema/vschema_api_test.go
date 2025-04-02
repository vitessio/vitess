/*
Copyright 2025 The Vitess Authors.

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

package vschema

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/ptr"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	"vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/proto/vtctldata"
)

func TestCreate(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell")
	defer ts.Close()

	api := NewVSchemaAPI(ts, sqlparser.NewTestParser())

	tests := []struct {
		name            string
		req             *vtctldata.VSchemaCreateRequest
		wantErrContains string
		validate        func(t *testing.T, vsInfo *vschemapb.Keyspace)
	}{
		{
			name: "create keyspace with valid vschema JSON",
			req: &vtctldata.VSchemaCreateRequest{
				VSchemaName: "ks",
				VSchemaJson: `{"tables": {"table1": {}}}`,
				Sharded:     true,
				Draft:       false,
			},
			validate: func(t *testing.T, vsInfo *vschemapb.Keyspace) {
				assert.NotNil(t, vsInfo)
				assert.True(t, vsInfo.Sharded)
				assert.False(t, vsInfo.Draft)
				assert.Contains(t, vsInfo.Tables, "table1")
			},
		},
		{
			name: "create keyspace with empty vschema JSON",
			req: &vtctldata.VSchemaCreateRequest{
				VSchemaName: "ks_empty",
				VSchemaJson: `{}`,
				Sharded:     false,
				Draft:       true,
			},
			validate: func(t *testing.T, vsInfo *vschemapb.Keyspace) {
				assert.NotNil(t, vsInfo)
				assert.False(t, vsInfo.Sharded)
				assert.True(t, vsInfo.Draft)
				assert.Empty(t, vsInfo.Tables)
			},
		},
		{
			name: "invalid vschema JSON",
			req: &vtctldata.VSchemaCreateRequest{
				VSchemaName: "ks_invalid",
				VSchemaJson: `invalid_json`,
			},
			wantErrContains: "unable to unmarshal vschema JSON",
		},
		{
			name: "duplicate keyspace creation",
			req: &vtctldata.VSchemaCreateRequest{
				VSchemaName: "ks",
				VSchemaJson: `{"tables": {"table1": {}}}`,
			},
			wantErrContains: "node already exists",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := api.Create(ctx, tc.req)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
				return
			}

			assert.NoError(t, err)

			vsInfo, err := ts.GetVSchema(ctx, tc.req.VSchemaName)
			require.NoError(t, err)

			if tc.validate != nil {
				tc.validate(t, vsInfo.Keyspace)
			}
		})
	}
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell")
	defer ts.Close()

	ks := "ks"
	err := ts.CreateKeyspace(ctx, ks, &topodatapb.Keyspace{})
	require.NoError(t, err)
	vsInfo := &topo.KeyspaceVSchemaInfo{
		Name: ks,
		Keyspace: &vschemapb.Keyspace{
			Tables: map[string]*vschemapb.Table{
				"t1": {},
				"t2": {},
			},
			// Initially marking this as draft.
			Draft: true,
		},
	}
	err = ts.SaveVSchema(ctx, vsInfo)
	require.NoError(t, err)

	api := NewVSchemaAPI(ts, sqlparser.NewTestParser())
	_, err = api.Get(ctx, &vtctldata.VSchemaGetRequest{
		VSchemaName: "non_existent_ks",
	})
	assert.ErrorContains(t, err, "failed to retrieve vschema")

	_, err = api.Get(ctx, &vtctldata.VSchemaGetRequest{
		VSchemaName: ks,
	})
	assert.ErrorContains(t, err, "draft")

	vschema, err := api.Get(ctx, &vtctldata.VSchemaGetRequest{
		VSchemaName:   ks,
		IncludeDrafts: true,
	})
	assert.NoError(t, err)
	assert.True(t, proto.Equal(vsInfo.Keyspace, vschema), "want: %v, got: %v", vsInfo.Keyspace, vschema)

	// Marking it as ready i.e. non-draft.
	vsInfo.Draft = false

	err = ts.SaveVSchema(ctx, vsInfo)
	require.NoError(t, err)

	vschema, err = api.Get(ctx, &vtctldata.VSchemaGetRequest{
		VSchemaName: ks,
	})
	assert.NoError(t, err)
	assert.True(t, proto.Equal(vsInfo.Keyspace, vschema), "want: %v, got: %v", vsInfo.Keyspace, vschema)

	vschema, err = api.Get(ctx, &vtctldata.VSchemaGetRequest{
		VSchemaName:   ks,
		IncludeDrafts: true,
	})
	assert.NoError(t, err)
	assert.True(t, proto.Equal(vsInfo.Keyspace, vschema), "want: %v, got: %v", vsInfo.Keyspace, vschema)
}

func TestUpdate(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell")
	defer ts.Close()

	ks := "ks"
	err := ts.CreateKeyspace(ctx, ks, &topodatapb.Keyspace{})
	require.NoError(t, err)
	origKs := &vschemapb.Keyspace{}
	err = ts.SaveVSchema(ctx,
		&topo.KeyspaceVSchemaInfo{
			Name:     ks,
			Keyspace: origKs,
		})
	require.NoError(t, err)

	multiTenantKs := "mks"
	err = ts.CreateKeyspace(ctx, multiTenantKs, &topodatapb.Keyspace{})
	require.NoError(t, err)
	origMultiTenantKs := &vschemapb.Keyspace{
		MultiTenantSpec: &vschemapb.MultiTenantSpec{
			TenantIdColumnName: "orig_col_name",
			TenantIdColumnType: query.Type_INT64,
		},
	}
	err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name:     multiTenantKs,
		Keyspace: origMultiTenantKs,
	})
	require.NoError(t, err)
	api := NewVSchemaAPI(ts, sqlparser.NewTestParser())

	tests := []struct {
		name             string
		req              *vtctldata.VSchemaUpdateRequest
		wantErrContains  string
		expectedKeyspace *vschemapb.Keyspace
		// updatesMultiTenantKs should be true if the test case changes
		// `multiTenantKs` vsInfo/vschema, this will set it back to the
		// original vsinfo/vschema state after the test case is run, else `ks`
		// will be set back to it's original vschema after the test is run.
		updatesMultiTenantKs bool
	}{
		{
			name: "non existent ks",
			req: &vtctldata.VSchemaUpdateRequest{
				VSchemaName: "non_existent_ks",
			},
			wantErrContains: "failed to retrieve vschema",
		},
		{
			name: "invalid fkmode",
			req: &vtctldata.VSchemaUpdateRequest{
				VSchemaName:    ks,
				ForeignKeyMode: ptr.Of("invalid_fkmode"),
			},
			wantErrContains: "invalid value provided for foreign key mode",
		},
		{
			name: "update fkmode",
			req: &vtctldata.VSchemaUpdateRequest{
				VSchemaName:    ks,
				ForeignKeyMode: ptr.Of("managed"),
			},
			expectedKeyspace: &vschemapb.Keyspace{
				ForeignKeyMode: vschemapb.Keyspace_managed,
			},
		},
		{
			name: "update draft",
			req: &vtctldata.VSchemaUpdateRequest{
				VSchemaName: ks,
				Draft:       ptr.Of(true),
			},
			expectedKeyspace: &vschemapb.Keyspace{
				Draft: true,
			},
		},
		{
			name: "update sharded",
			req: &vtctldata.VSchemaUpdateRequest{
				VSchemaName: ks,
				Sharded:     ptr.Of(true),
			},
			expectedKeyspace: &vschemapb.Keyspace{
				Sharded: true,
			},
		},
		{
			name: "nothing to update but multi tenant",
			req: &vtctldata.VSchemaUpdateRequest{
				VSchemaName: multiTenantKs,
				MultiTenant: ptr.Of(true),
			},
			wantErrContains: "nothing to update",
		},
		{
			name: "no multi-tenant param provided",
			req: &vtctldata.VSchemaUpdateRequest{
				VSchemaName: ks,
				MultiTenant: ptr.Of(true),
			},
			wantErrContains: "both tenant id column name and column type should be provided",
		},
		{
			name: "only one param, tenant spec nil",
			req: &vtctldata.VSchemaUpdateRequest{
				VSchemaName:        ks,
				MultiTenant:        ptr.Of(true),
				TenantIdColumnName: ptr.Of("col_name"),
			},
			wantErrContains: "both tenant id column name and column type",
		},
		{
			name: "only one param, tenant spec nil",
			req: &vtctldata.VSchemaUpdateRequest{
				VSchemaName:        ks,
				MultiTenant:        ptr.Of(true),
				TenantIdColumnType: ptr.Of("varchar"),
			},
			wantErrContains: "both tenant id column name and column type",
		},
		{
			name: "invalid tenant column type",
			req: &vtctldata.VSchemaUpdateRequest{
				VSchemaName:        ks,
				MultiTenant:        ptr.Of(true),
				TenantIdColumnName: ptr.Of("col_name"),
				TenantIdColumnType: ptr.Of("invalid_type"),
			},
			wantErrContains: "invalid tenant id column type",
		},
		{
			name: "cannot specify tenant id with multi tenant false",
			req: &vtctldata.VSchemaUpdateRequest{
				VSchemaName:        ks,
				MultiTenant:        ptr.Of(false),
				TenantIdColumnName: ptr.Of("col_name"),
				TenantIdColumnType: ptr.Of("invalid_type"),
			},
			wantErrContains: "cannot specify tenant-id-column-name or tenant-id-column-type",
		},
		{
			name: "set new tenant spec",
			req: &vtctldata.VSchemaUpdateRequest{
				VSchemaName:        ks,
				MultiTenant:        ptr.Of(true),
				TenantIdColumnName: ptr.Of("col_name"),
				TenantIdColumnType: ptr.Of("varchar"),
			},
			expectedKeyspace: &vschemapb.Keyspace{
				MultiTenantSpec: &vschemapb.MultiTenantSpec{
					TenantIdColumnName: "col_name",
					TenantIdColumnType: query.Type_VARCHAR,
				},
			},
		},
		{
			name: "update tenant spec, col name",
			req: &vtctldata.VSchemaUpdateRequest{
				VSchemaName:        multiTenantKs,
				MultiTenant:        ptr.Of(true),
				TenantIdColumnName: ptr.Of("col_name"),
			},
			expectedKeyspace: &vschemapb.Keyspace{
				MultiTenantSpec: &vschemapb.MultiTenantSpec{
					TenantIdColumnName: "col_name",
					TenantIdColumnType: query.Type_INT64,
				},
			},
			updatesMultiTenantKs: true,
		},
		{
			name: "remove multi tenant spec",
			req: &vtctldata.VSchemaUpdateRequest{
				VSchemaName: multiTenantKs,
				MultiTenant: ptr.Of(false),
			},
			expectedKeyspace:     &vschemapb.Keyspace{},
			updatesMultiTenantKs: true,
		},
		{
			name: "update tenant spec, col type",
			req: &vtctldata.VSchemaUpdateRequest{
				VSchemaName:        multiTenantKs,
				MultiTenant:        ptr.Of(true),
				TenantIdColumnType: ptr.Of("varchar"),
			},
			expectedKeyspace: &vschemapb.Keyspace{
				MultiTenantSpec: &vschemapb.MultiTenantSpec{
					TenantIdColumnName: "orig_col_name",
					TenantIdColumnType: query.Type_VARCHAR,
				},
			},
			updatesMultiTenantKs: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err = api.Update(ctx, tc.req)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
				return
			}
			assert.NoError(t, err)
			ksInfo, err := ts.GetVSchema(ctx, tc.req.VSchemaName)
			require.NoError(t, err)
			assert.True(t, proto.Equal(tc.expectedKeyspace, ksInfo.Keyspace))

			// Bring back to it's original ksinfo.
			if !tc.updatesMultiTenantKs {
				err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
					Name:     ks,
					Keyspace: origKs,
				})
				require.NoError(t, err)
			} else {
				// Update to original multi-tenant vschema.
				err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
					Name:     multiTenantKs,
					Keyspace: origMultiTenantKs,
				})
				require.NoError(t, err)
			}
		})
	}
}

func TestPublish(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell")
	defer ts.Close()

	ks := "ks"
	err := ts.CreateKeyspace(ctx, ks, &topodatapb.Keyspace{})
	require.NoError(t, err)
	err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name:     ks,
		Keyspace: &vschemapb.Keyspace{},
	})
	require.NoError(t, err)

	api := NewVSchemaAPI(ts, sqlparser.NewTestParser())
	err = api.Publish(ctx, &vtctldata.VSchemaPublishRequest{
		VSchemaName: "non_existent_ks",
	})
	assert.ErrorContains(t, err, "failed to retrieve vschema")

	// Already published.
	err = api.Publish(ctx, &vtctldata.VSchemaPublishRequest{
		VSchemaName: ks,
	})
	assert.ErrorContains(t, err, "already published")

	// Marking it as draft now.
	err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name: ks,
		Keyspace: &vschemapb.Keyspace{
			Draft: true,
		},
	})
	require.NoError(t, err)

	err = api.Publish(ctx, &vtctldata.VSchemaPublishRequest{
		VSchemaName: ks,
	})
	assert.NoError(t, err)

	ksInfo, err := ts.GetVSchema(ctx, ks)
	require.NoError(t, err)

	ksInfo.Draft = false
}

func TestAddVindex(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell")
	defer ts.Close()

	api := NewVSchemaAPI(ts, sqlparser.NewTestParser())

	ks := "ks"
	err := ts.CreateKeyspace(ctx, ks, &topodatapb.Keyspace{})
	require.NoError(t, err)

	err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name: ks,
		Keyspace: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"existing_vindex": {
					Type: "hash",
				},
			},
		},
	})
	require.NoError(t, err)

	tests := []struct {
		name            string
		req             *vtctldata.VSchemaAddVindexRequest
		wantErrContains string
	}{
		{
			name: "non-existent keyspace",
			req: &vtctldata.VSchemaAddVindexRequest{
				VSchemaName: "non_existent_keyspace",
				VindexName:  "new_vindex",
				VindexType:  "hash",
			},
			wantErrContains: "failed to retrieve vschema",
		},
		{
			name: "duplicate vindex",
			req: &vtctldata.VSchemaAddVindexRequest{
				VSchemaName: ks,
				VindexName:  "existing_vindex",
				VindexType:  "hash",
			},
			wantErrContains: "vindex 'existing_vindex' already exists",
		},
		{
			name: "invalid vindex type",
			req: &vtctldata.VSchemaAddVindexRequest{
				VSchemaName: ks,
				VindexName:  "invalid_vindex",
				VindexType:  "invalid_type",
			},
			wantErrContains: "failed to create vindex 'invalid_vindex'",
		},
		{
			name: "successful addition",
			req: &vtctldata.VSchemaAddVindexRequest{
				VSchemaName: ks,
				VindexName:  "new_vindex",
				VindexType:  "hash",
				Params: map[string]string{
					"param1": "value1",
				},
			},
			wantErrContains: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := api.AddVindex(ctx, tc.req)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
				return
			}

			assert.NoError(t, err)

			vsInfo, err := ts.GetVSchema(ctx, tc.req.VSchemaName)
			require.NoError(t, err)

			vindex, ok := vsInfo.Vindexes[tc.req.VindexName]
			assert.True(t, ok, "vindex should exist in the VSchema")
			assert.Equal(t, tc.req.VindexType, vindex.Type)
			assert.Equal(t, tc.req.Params, vindex.Params)
		})
	}
}

func TestRemoveVindex(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell")
	defer ts.Close()

	ks := "ks"
	err := ts.CreateKeyspace(ctx, ks, &topodatapb.Keyspace{})
	require.NoError(t, err)

	err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name: ks,
		Keyspace: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"vindex1": {Type: "hash"},
			},
			Tables: map[string]*vschemapb.Table{
				"table1": {
					ColumnVindexes: []*vschemapb.ColumnVindex{
						{Name: "vindex1", Columns: []string{"col1"}},
					},
				},
				"table2": {
					ColumnVindexes: []*vschemapb.ColumnVindex{
						{Name: "vindex1", Columns: []string{"col2"}},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	api := NewVSchemaAPI(ts, sqlparser.NewTestParser())

	tests := []struct {
		name            string
		req             *vtctldata.VSchemaRemoveVindexRequest
		wantErrContains string
		validate        func(t *testing.T, vsInfo *vschemapb.Keyspace)
	}{
		{
			name: "non-existent keyspace",
			req: &vtctldata.VSchemaRemoveVindexRequest{
				VSchemaName: "non_existent_keyspace",
				VindexName:  "vindex1",
			},
			wantErrContains: "failed to retrieve vschema",
		},
		{
			name: "non-existent vindex",
			req: &vtctldata.VSchemaRemoveVindexRequest{
				VSchemaName: ks,
				VindexName:  "non_existent_vindex",
			},
			wantErrContains: "vindex 'non_existent_vindex' doesn't exist",
		},
		{
			name: "successful removal",
			req: &vtctldata.VSchemaRemoveVindexRequest{
				VSchemaName: ks,
				VindexName:  "vindex1",
			},
			validate: func(t *testing.T, vsInfo *vschemapb.Keyspace) {
				_, exists := vsInfo.Vindexes["vindex1"]
				assert.False(t, exists, "vindex1 should be removed")

				for _, table := range vsInfo.Tables {
					for _, colVindex := range table.ColumnVindexes {
						assert.NotEqual(t, "vindex1", colVindex.Name, "vindex1 should be removed from all tables")
					}
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := api.RemoveVindex(ctx, tc.req)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
				return
			}

			assert.NoError(t, err)

			vsInfo, err := ts.GetVSchema(ctx, tc.req.VSchemaName)
			require.NoError(t, err)

			_, exists := vsInfo.Vindexes[tc.req.VindexName]
			assert.False(t, exists, "vindex1 should be removed")

			for _, table := range vsInfo.Tables {
				for _, colVindex := range table.ColumnVindexes {
					assert.NotEqual(t, tc.req.VindexName, colVindex.Name, "vindex1 should be removed from all tables")
				}
			}
			if tc.validate != nil {
				tc.validate(t, vsInfo.Keyspace)
			}
		})
	}
}

func TestAddLookupVindex(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell")
	defer ts.Close()

	ks := "ks"
	err := ts.CreateKeyspace(ctx, ks, &topodatapb.Keyspace{})
	require.NoError(t, err)

	err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name: ks,
		Keyspace: &vschemapb.Keyspace{
			Tables: map[string]*vschemapb.Table{
				"owner_table": {},
			},
		},
	})
	require.NoError(t, err)

	ks2 := "ks2"
	err = ts.CreateKeyspace(ctx, ks2, &topodatapb.Keyspace{})
	require.NoError(t, err)

	err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name: ks2,
		Keyspace: &vschemapb.Keyspace{
			Tables: map[string]*vschemapb.Table{
				"lookup_table": {
					Type: vindexes.TypeTable,
				},
			},
		},
	})
	require.NoError(t, err)

	api := NewVSchemaAPI(ts, sqlparser.NewTestParser())

	tests := []struct {
		name            string
		req             *vtctldata.VSchemaAddLookupVindexRequest
		wantErrContains string
		validate        func(t *testing.T, vsInfo *vschemapb.Keyspace)
	}{
		{
			name: "non-existent keyspace",
			req: &vtctldata.VSchemaAddLookupVindexRequest{
				VSchemaName:      "non_existent_keyspace",
				VindexName:       "lookup_vindex",
				LookupVindexType: "lookup_hash",
				TableName:        "lookup_table",
				FromColumns:      []string{"col1"},
				Owner:            "owner_table",
			},
			wantErrContains: "failed to retrieve vschema",
		},
		{
			name: "invalid lookup vindex type",
			req: &vtctldata.VSchemaAddLookupVindexRequest{
				VSchemaName:      ks,
				VindexName:       "invalid_lookup_vindex",
				LookupVindexType: "invalid_type",
				TableName:        "ks2.lookup_table",
				FromColumns:      []string{"col1"},
				Owner:            "owner_table",
			},
			wantErrContains: "invalid lookup vindex type",
		},
		{
			name: "no from columns specified",
			req: &vtctldata.VSchemaAddLookupVindexRequest{
				VSchemaName:      ks,
				VindexName:       "lookup_vindex",
				LookupVindexType: "lookup_hash",
				TableName:        "ks2.lookup_table",
				Owner:            "owner_table",
			},
			wantErrContains: "at least 1 column should be specified for lookup vindex",
		},
		{
			name: "invalid lookup table, invalid ks",
			req: &vtctldata.VSchemaAddLookupVindexRequest{
				VSchemaName:      ks,
				VindexName:       "lookup_vindex",
				LookupVindexType: "lookup_hash",
				TableName:        "non_existent_ks.non_existent_table",
				FromColumns:      []string{"col1"},
				Owner:            "owner_table",
			},
			wantErrContains: "invalid lookup table",
		},
		{
			name: "invalid lookup table",
			req: &vtctldata.VSchemaAddLookupVindexRequest{
				VSchemaName:      ks,
				VindexName:       "lookup_vindex",
				LookupVindexType: "lookup_hash",
				TableName:        "ks2.non_existent_table",
				FromColumns:      []string{"col1"},
				Owner:            "owner_table",
			},
			wantErrContains: "invalid lookup table",
		},
		{
			name: "invalid lookup type",
			req: &vtctldata.VSchemaAddLookupVindexRequest{
				VSchemaName:      ks,
				VindexName:       "lookup_vindex",
				LookupVindexType: "lookup_non_existent",
				TableName:        "ks2.lookup_table",
				FromColumns:      []string{"col1"},
				Owner:            "owner_table",
			},
			wantErrContains: "failed to create vindex",
		},
		{
			name: "non-existent owner table",
			req: &vtctldata.VSchemaAddLookupVindexRequest{
				VSchemaName:      ks,
				VindexName:       "lookup_vindex",
				LookupVindexType: "lookup_hash",
				TableName:        "ks2.lookup_table",
				FromColumns:      []string{"col1"},
				Owner:            "non_existent_owner",
			},
			wantErrContains: "table 'non_existent_owner' not found",
		},
		{
			name: "successful addition",
			req: &vtctldata.VSchemaAddLookupVindexRequest{
				VSchemaName:      ks,
				VindexName:       "lookup_vindex",
				LookupVindexType: "lookup_hash",
				TableName:        "ks2.lookup_table",
				FromColumns:      []string{"col1"},
				Owner:            "owner_table",
				IgnoreNulls:      true,
			},
			validate: func(t *testing.T, vsInfo *vschemapb.Keyspace) {
				vindex, ok := vsInfo.Vindexes["lookup_vindex"]
				assert.True(t, ok, "lookup_vindex should exist in the VSchema")
				assert.Equal(t, "lookup_hash", vindex.Type)
				assert.Equal(t, map[string]string{
					"table":        "ks2.lookup_table",
					"from":         "col1",
					"to":           "keyspace_id",
					"ignore_nulls": "true",
				}, vindex.Params)
				assert.Equal(t, "owner_table", vindex.Owner)

				ownerTable, ok := vsInfo.Tables["owner_table"]
				assert.True(t, ok, "owner_table should exist in the VSchema")
				assert.Len(t, ownerTable.ColumnVindexes, 1)
				assert.Equal(t, "lookup_vindex", ownerTable.ColumnVindexes[0].Name)
				assert.Equal(t, []string{"col1"}, ownerTable.ColumnVindexes[0].Columns)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := api.AddLookupVindex(ctx, tc.req)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
				return
			}

			assert.NoError(t, err)

			vsInfo, err := ts.GetVSchema(ctx, tc.req.VSchemaName)
			require.NoError(t, err)

			if tc.validate != nil {
				tc.validate(t, vsInfo.Keyspace)
			}
		})
	}
}

func TestAddTables(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell")
	defer ts.Close()

	ks := "ks"
	err := ts.CreateKeyspace(ctx, ks, &topodatapb.Keyspace{})
	require.NoError(t, err)

	err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name: ks,
		Keyspace: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"primary_vindex": {
					Type: "hash",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"existing_table": {},
			},
		},
	})
	require.NoError(t, err)

	api := NewVSchemaAPI(ts, sqlparser.NewTestParser())

	tests := []struct {
		name            string
		req             *vtctldata.VSchemaAddTablesRequest
		wantErrContains string
		validate        func(t *testing.T, vsInfo *vschemapb.Keyspace)
	}{
		{
			name: "non-existent keyspace",
			req: &vtctldata.VSchemaAddTablesRequest{
				VSchemaName: "non_existent_keyspace",
				Tables:      []string{"new_table"},
			},
			wantErrContains: "failed to retrieve vschema",
		},
		{
			name: "no tables in request",
			req: &vtctldata.VSchemaAddTablesRequest{
				VSchemaName: ks,
			},
			wantErrContains: "no tables found in the request",
		},
		{
			name: "table already exists",
			req: &vtctldata.VSchemaAddTablesRequest{
				VSchemaName: ks,
				Tables:      []string{"existing_table"},
			},
			wantErrContains: "existing_table already exist",
		},
		{
			name: "primary vindex not found",
			req: &vtctldata.VSchemaAddTablesRequest{
				VSchemaName:       ks,
				Tables:            []string{"new_table"},
				PrimaryVindexName: "non_existent_vindex",
			},
			wantErrContains: "vindex 'non_existent_vindex' not found",
		},
		{
			name: "add tables without primary vindex",
			req: &vtctldata.VSchemaAddTablesRequest{
				VSchemaName: ks,
				Tables:      []string{"new_table1", "new_table2"},
			},
			validate: func(t *testing.T, vsInfo *vschemapb.Keyspace) {
				assert.Contains(t, vsInfo.Tables, "new_table1")
				assert.Contains(t, vsInfo.Tables, "new_table2")
				assert.Nil(t, vsInfo.Tables["new_table1"].ColumnVindexes)
				assert.Nil(t, vsInfo.Tables["new_table2"].ColumnVindexes)
			},
		},
		{
			name: "add tables with add all true",
			req: &vtctldata.VSchemaAddTablesRequest{
				VSchemaName:       ks,
				Tables:            []string{"new_table3", "new_table4"},
				PrimaryVindexName: "primary_vindex",
				Columns:           []string{"col1"},
				AddAll:            true,
			},
			validate: func(t *testing.T, vsInfo *vschemapb.Keyspace) {
				for _, tableName := range []string{"new_table3", "new_table4"} {
					assert.Contains(t, vsInfo.Tables, tableName)
					assert.Len(t, vsInfo.Tables[tableName].ColumnVindexes, 1)
					assert.Equal(t, "primary_vindex", vsInfo.Tables[tableName].ColumnVindexes[0].Name)
					assert.Equal(t, []string{"col1"}, vsInfo.Tables[tableName].ColumnVindexes[0].Columns)
				}
			},
		},
		{
			name: "add tables with add all false",
			req: &vtctldata.VSchemaAddTablesRequest{
				VSchemaName:       ks,
				Tables:            []string{"new_table5", "new_table6"},
				PrimaryVindexName: "primary_vindex",
				Columns:           []string{"col1"},
			},
			validate: func(t *testing.T, vsInfo *vschemapb.Keyspace) {
				assert.Contains(t, vsInfo.Tables, "new_table5")
				assert.Contains(t, vsInfo.Tables, "new_table6")
				assert.Len(t, vsInfo.Tables["new_table5"].ColumnVindexes, 1)
				assert.Equal(t, "primary_vindex", vsInfo.Tables["new_table5"].ColumnVindexes[0].Name)
				assert.Equal(t, []string{"col1"}, vsInfo.Tables["new_table5"].ColumnVindexes[0].Columns)
				assert.Nil(t, vsInfo.Tables["new_table6"].ColumnVindexes)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := api.AddTables(ctx, tc.req)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
				return
			}

			assert.NoError(t, err)

			vsInfo, err := ts.GetVSchema(ctx, tc.req.VSchemaName)
			require.NoError(t, err)

			if tc.validate != nil {
				tc.validate(t, vsInfo.Keyspace)
			}
		})
	}
}

func TestRemoveTables(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell")
	defer ts.Close()

	ks := "ks"
	err := ts.CreateKeyspace(ctx, ks, &topodatapb.Keyspace{})
	require.NoError(t, err)

	err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name: ks,
		Keyspace: &vschemapb.Keyspace{
			Tables: map[string]*vschemapb.Table{
				"table1": {},
				"table2": {},
				"table3": {},
			},
		},
	})
	require.NoError(t, err)

	api := NewVSchemaAPI(ts, sqlparser.NewTestParser())

	tests := []struct {
		name            string
		req             *vtctldata.VSchemaRemoveTablesRequest
		wantErrContains string
		validate        func(t *testing.T, vsInfo *vschemapb.Keyspace)
	}{
		{
			name: "non-existent keyspace",
			req: &vtctldata.VSchemaRemoveTablesRequest{
				VSchemaName: "non_existent_keyspace",
				Tables:      []string{"table1"},
			},
			wantErrContains: "failed to retrieve vschema",
		},
		{
			name: "non-existent table",
			req: &vtctldata.VSchemaRemoveTablesRequest{
				VSchemaName: ks,
				Tables:      []string{"non_existent_table"},
			},
			wantErrContains: "table(s) non_existent_table not found",
		},
		{
			name: "remove single table",
			req: &vtctldata.VSchemaRemoveTablesRequest{
				VSchemaName: ks,
				Tables:      []string{"table1"},
			},
			validate: func(t *testing.T, vsInfo *vschemapb.Keyspace) {
				_, exists := vsInfo.Tables["table1"]
				assert.False(t, exists, "table1 should be removed")
				_, exists = vsInfo.Tables["table2"]
				assert.True(t, exists, "table2 should still exist")
				_, exists = vsInfo.Tables["table3"]
				assert.True(t, exists, "table3 should still exist")
			},
		},
		{
			name: "remove multiple tables",
			req: &vtctldata.VSchemaRemoveTablesRequest{
				VSchemaName: ks,
				Tables:      []string{"table2", "table3"},
			},
			validate: func(t *testing.T, vsInfo *vschemapb.Keyspace) {
				_, exists := vsInfo.Tables["table2"]
				assert.False(t, exists, "table2 should be removed")
				_, exists = vsInfo.Tables["table3"]
				assert.False(t, exists, "table3 should be removed")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := api.RemoveTables(ctx, tc.req)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
				return
			}

			assert.NoError(t, err)

			vsInfo, err := ts.GetVSchema(ctx, tc.req.VSchemaName)
			require.NoError(t, err)

			if tc.validate != nil {
				tc.validate(t, vsInfo.Keyspace)
			}
		})
	}
}

func TestSetPrimaryVindex(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell")
	defer ts.Close()

	ks := "ks"
	err := ts.CreateKeyspace(ctx, ks, &topodatapb.Keyspace{})
	require.NoError(t, err)

	err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name: ks,
		Keyspace: &vschemapb.Keyspace{
			Vindexes: map[string]*vschemapb.Vindex{
				"primary_vindex": {
					Type: "hash",
				},
				"vindex2": {
					Type: "hash",
				},
			},
			Tables: map[string]*vschemapb.Table{
				"table1": {},
				"table2": {
					ColumnVindexes: []*vschemapb.ColumnVindex{
						{
							Name:    "vindex2",
							Columns: []string{"col1"},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	api := NewVSchemaAPI(ts, sqlparser.NewTestParser())

	tests := []struct {
		name            string
		req             *vtctldata.VSchemaSetPrimaryVindexRequest
		wantErrContains string
		validate        func(t *testing.T, vsInfo *vschemapb.Keyspace)
	}{
		{
			name: "non-existent keyspace",
			req: &vtctldata.VSchemaSetPrimaryVindexRequest{
				VSchemaName: "non_existent_keyspace",
				Tables:      []string{"table1"},
				VindexName:  "primary_vindex",
				Columns:     []string{"col1"},
			},
			wantErrContains: "failed to retrieve vschema",
		},
		{
			name: "no columns specified",
			req: &vtctldata.VSchemaSetPrimaryVindexRequest{
				VSchemaName: ks,
				Tables:      []string{"table1"},
				VindexName:  "primary_vindex",
			},
			wantErrContains: "at least 1 column should be specified for vindex",
		},
		{
			name: "non-existent table",
			req: &vtctldata.VSchemaSetPrimaryVindexRequest{
				VSchemaName: ks,
				Tables:      []string{"non_existent_table"},
				VindexName:  "primary_vindex",
				Columns:     []string{"col1"},
			},
			wantErrContains: "table(s) non_existent_table not found",
		},
		{
			name: "non-existent vindex",
			req: &vtctldata.VSchemaSetPrimaryVindexRequest{
				VSchemaName: ks,
				Tables:      []string{"table1"},
				VindexName:  "non_existent_vindex",
				Columns:     []string{"col1"},
			},
			wantErrContains: "vindex 'non_existent_vindex' not found",
		},
		{
			name: "set primary vindex for single table",
			req: &vtctldata.VSchemaSetPrimaryVindexRequest{
				VSchemaName: ks,
				Tables:      []string{"table1"},
				VindexName:  "primary_vindex",
				Columns:     []string{"col1"},
			},
			validate: func(t *testing.T, vsInfo *vschemapb.Keyspace) {
				assert.Contains(t, vsInfo.Tables, "table1")
				assert.Len(t, vsInfo.Tables["table1"].ColumnVindexes, 1)
				assert.Equal(t, "primary_vindex", vsInfo.Tables["table1"].ColumnVindexes[0].Name)
				assert.Equal(t, []string{"col1"}, vsInfo.Tables["table1"].ColumnVindexes[0].Columns)
			},
		},
		{
			name: "set primary vindex for multiple tables",
			req: &vtctldata.VSchemaSetPrimaryVindexRequest{
				VSchemaName: ks,
				Tables:      []string{"table1", "table2"},
				VindexName:  "primary_vindex",
				Columns:     []string{"col1"},
			},
			validate: func(t *testing.T, vsInfo *vschemapb.Keyspace) {
				for _, tableName := range []string{"table1", "table2"} {
					assert.Contains(t, vsInfo.Tables, tableName)
					assert.Len(t, vsInfo.Tables[tableName].ColumnVindexes, 1)
					assert.Equal(t, "primary_vindex", vsInfo.Tables[tableName].ColumnVindexes[0].Name)
					assert.Equal(t, []string{"col1"}, vsInfo.Tables[tableName].ColumnVindexes[0].Columns)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := api.SetPrimaryVindex(ctx, tc.req)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
				return
			}

			assert.NoError(t, err)

			vsInfo, err := ts.GetVSchema(ctx, tc.req.VSchemaName)
			require.NoError(t, err)

			if tc.validate != nil {
				tc.validate(t, vsInfo.Keyspace)
			}
		})
	}
}

func TestSetSequence(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell")
	defer ts.Close()

	ks := "ks"
	err := ts.CreateKeyspace(ctx, ks, &topodatapb.Keyspace{})
	require.NoError(t, err)

	err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name: ks,
		Keyspace: &vschemapb.Keyspace{
			Tables: map[string]*vschemapb.Table{
				"table1": {},
			},
		},
	})
	require.NoError(t, err)

	err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name: "unsharded",
		Keyspace: &vschemapb.Keyspace{
			Tables: map[string]*vschemapb.Table{
				"sequence_table": {
					Type: "sequence",
				},
			},
		},
	})
	require.NoError(t, err)

	err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name: "sharded",
		Keyspace: &vschemapb.Keyspace{
			Sharded: true,
			Tables: map[string]*vschemapb.Table{
				"sequence_table": {
					Type: "sequence",
				},
			},
		},
	})
	require.NoError(t, err)

	api := NewVSchemaAPI(ts, sqlparser.NewTestParser())

	tests := []struct {
		name            string
		req             *vtctldata.VSchemaSetSequenceRequest
		wantErrContains string
	}{
		{
			name: "non-existent keyspace",
			req: &vtctldata.VSchemaSetSequenceRequest{
				VSchemaName:    "non_existent_keyspace",
				TableName:      "table1",
				Column:         "id",
				SequenceSource: "unsharded.sequence_table",
			},
			wantErrContains: "failed to retrieve vschema",
		},
		{
			name: "non-existent table",
			req: &vtctldata.VSchemaSetSequenceRequest{
				VSchemaName:    ks,
				TableName:      "non_existent_table",
				Column:         "id",
				SequenceSource: "unsharded.sequence_table",
			},
			wantErrContains: "table 'non_existent_table' not found",
		},
		{
			name: "empty sequence source",
			req: &vtctldata.VSchemaSetSequenceRequest{
				VSchemaName: ks,
				TableName:   "table1",
				Column:      "id",
			},
			wantErrContains: "sequence source cannot be empty",
		},
		{
			name: "empty column name",
			req: &vtctldata.VSchemaSetSequenceRequest{
				VSchemaName:    ks,
				TableName:      "table1",
				SequenceSource: "unsharded.sequence_table",
			},
			wantErrContains: "column name cannot be empty",
		},
		{
			name: "sharded sequence table",
			req: &vtctldata.VSchemaSetSequenceRequest{
				VSchemaName:    ks,
				TableName:      "table1",
				Column:         "id",
				SequenceSource: "sharded.sequence_table",
			},
			wantErrContains: "cannot be sharded",
		},
		{
			name: "valid sequence setup",
			req: &vtctldata.VSchemaSetSequenceRequest{
				VSchemaName:    ks,
				TableName:      "table1",
				Column:         "id",
				SequenceSource: "unsharded.sequence_table",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := api.SetSequence(ctx, tc.req)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
				return
			}

			assert.NoError(t, err)

			vsInfo, err := ts.GetVSchema(ctx, tc.req.VSchemaName)
			require.NoError(t, err)

			table, ok := vsInfo.Tables[tc.req.TableName]
			require.True(t, ok, "table '%s' should exist", tc.req.TableName)

			assert.NotNil(t, table.AutoIncrement)
			assert.Equal(t, "id", table.AutoIncrement.Column)
			assert.Equal(t, "unsharded.sequence_table", table.AutoIncrement.Sequence)
		})
	}
}

func TestSetReference(t *testing.T) {
	ctx := context.Background()
	ts := memorytopo.NewServer(ctx, "cell")
	defer ts.Close()

	sourceKs := "sourceKs"
	ks := "ks"
	err := ts.CreateKeyspace(ctx, sourceKs, &topodatapb.Keyspace{})
	require.NoError(t, err)

	sourceTableName := "t1"
	err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name: sourceKs,
		Keyspace: &vschemapb.Keyspace{
			Tables: map[string]*vschemapb.Table{
				sourceTableName: {},
			},
		},
	})
	require.NoError(t, err)
	err = ts.CreateKeyspace(ctx, ks, &topodatapb.Keyspace{})
	require.NoError(t, err)
	err = ts.SaveVSchema(ctx, &topo.KeyspaceVSchemaInfo{
		Name: ks,
		Keyspace: &vschemapb.Keyspace{
			Tables: map[string]*vschemapb.Table{
				"t2": {},
				"t3": {},
			},
		},
	})
	require.NoError(t, err)

	api := NewVSchemaAPI(ts, sqlparser.NewTestParser())
	err = api.SetReference(ctx, &vtctldata.VSchemaSetReferenceRequest{
		VSchemaName: "non_existent_ks",
		TableName:   "t1",
	})
	assert.ErrorContains(t, err, "failed to retrieve vschema")

	err = api.SetReference(ctx, &vtctldata.VSchemaSetReferenceRequest{
		VSchemaName: ks,
		TableName:   "non_existent_table",
	})
	assert.ErrorContains(t, err, "'non_existent_table' not found")

	err = api.SetReference(ctx, &vtctldata.VSchemaSetReferenceRequest{
		VSchemaName: ks,
		TableName:   "t2",
	})
	assert.NoError(t, err)
	ksInfo, err := ts.GetVSchema(ctx, ks)
	require.NoError(t, err)
	table := ksInfo.Tables["t2"]
	assert.NotNil(t, table)
	assert.Equal(t, vindexes.TypeReference, table.Type)

	err = api.SetReference(ctx, &vtctldata.VSchemaSetReferenceRequest{
		VSchemaName: ks,
		TableName:   "t2",
	})
	assert.ErrorContains(t, err, "already a reference table")

	err = api.SetReference(ctx, &vtctldata.VSchemaSetReferenceRequest{
		VSchemaName: ks,
		TableName:   "t3",
		Source:      fmt.Sprintf("%s.%s", sourceKs, "non_existent_source_table"),
	})
	assert.ErrorContains(t, err, "'non_existent_source_table' not found")

	err = api.SetReference(ctx, &vtctldata.VSchemaSetReferenceRequest{
		VSchemaName: ks,
		TableName:   "t3",
		Source:      fmt.Sprintf("%s.%s", "non_existent_ks", sourceTableName),
	})
	assert.ErrorContains(t, err, "failed to retrieve vschema")

	// Unqualified source should return error
	err = api.SetReference(ctx, &vtctldata.VSchemaSetReferenceRequest{
		VSchemaName: ks,
		TableName:   "t3",
		Source:      "t1",
	})
	assert.ErrorContains(t, err, "invalid reference table source")

	// Qualified source
	source := fmt.Sprintf("%s.%s", sourceKs, sourceTableName)
	err = api.SetReference(ctx, &vtctldata.VSchemaSetReferenceRequest{
		VSchemaName: ks,
		TableName:   "t3",
		Source:      source,
	})
	assert.ErrorContains(t, err, "not a reference table")

	err = api.SetReference(ctx, &vtctldata.VSchemaSetReferenceRequest{
		VSchemaName: sourceKs,
		TableName:   "t1",
	})
	assert.NoError(t, err)

	err = api.SetReference(ctx, &vtctldata.VSchemaSetReferenceRequest{
		VSchemaName: ks,
		TableName:   "t3",
		Source:      source,
	})
	assert.NoError(t, err)
	ksInfo, err = ts.GetVSchema(ctx, ks)
	require.NoError(t, err)
	table = ksInfo.Tables["t3"]
	assert.NotNil(t, table)
	assert.Equal(t, vindexes.TypeReference, table.Type)
	assert.Equal(t, source, table.Source)
}
