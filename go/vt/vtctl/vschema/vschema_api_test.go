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
		name                 string
		req                  *vtctldata.VSchemaUpdateRequest
		wantErrContains      string
		expectedKeyspace     *vschemapb.Keyspace
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

			// Go back to original ksinfo.
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
