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

	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/memorytopo"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

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

	api := NewVSchemaAPI(ts)
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
	assert.ErrorContains(t, err, "failed to parse source")

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
