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

package wrangler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

func TestValidateSchemaShard(t *testing.T) {
	ctx := context.Background()
	sourceShards := []string{"-80", "80-"}
	targetShards := []string{"-40", "40-80", "80-c0", "c0-"}

	tme := newTestShardMigrater(ctx, t, sourceShards, targetShards)

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "not_in_vschema",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}

	// This is the vschema returned by newTestShardMigrater
	schm2 := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:    "t1",
				Columns: []string{"c1"},
			},
			{
				Name:    "t2",
				Columns: []string{"c1"},
			},
			{
				Name:    "t3",
				Columns: []string{"c1"},
			},
		},
	}

	for _, primary := range tme.sourceMasters {
		if primary.Tablet.Shard == "80-" {
			primary.FakeMysqlDaemon.Schema = schm
		} else {
			primary.FakeMysqlDaemon.Schema = schm2
		}
	}

	// Schema Checks
	err := tme.wr.ValidateSchemaShard(ctx, "ks", "-80", nil /*excludeTables*/, true /*includeViews*/, true /*includeVSchema*/)
	require.NoError(t, err)
	shouldErr := tme.wr.ValidateSchemaShard(ctx, "ks", "80-", nil /*excludeTables*/, true /*includeViews*/, true /*includeVSchema*/)
	require.Contains(t, shouldErr.Error(), "ks/80- has tables that are not in the vschema:")

	// VSchema Specific Checks
	err = tme.wr.ValidateVSchema(ctx, "ks", []string{"-80"}, nil /*excludeTables*/, true /*includeViews*/)
	require.NoError(t, err)
	shouldErr = tme.wr.ValidateVSchema(ctx, "ks", []string{"80-"}, nil /*excludeTables*/, true /*includeVoews*/)
	require.Contains(t, shouldErr.Error(), "ks/80- has tables that are not in the vschema:")
}

func TestValidateSchemaKeyspace(t *testing.T) {
	ctx := context.Background()
	sourceShards := []string{"-80", "80-"}
	targetShards := []string{"-40", "40-80", "80-c0", "c0-"}

	tmePass := newTestShardMigrater(ctx, t, sourceShards, targetShards)
	tmeDiffs := newTestShardMigrater(ctx, t, sourceShards, targetShards)

	schm := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{{
			Name:              "not_in_vschema",
			Columns:           []string{"c1", "c2"},
			PrimaryKeyColumns: []string{"c1"},
			Fields:            sqltypes.MakeTestFields("c1|c2", "int64|int64"),
		}},
	}

	// This is the vschema returned by newTestShardMigrater
	sameAsVSchema := &tabletmanagerdatapb.SchemaDefinition{
		TableDefinitions: []*tabletmanagerdatapb.TableDefinition{
			{
				Name:    "t1",
				Columns: []string{"c1"},
			},
			{
				Name:    "t2",
				Columns: []string{"c1"},
			},
			{
				Name:    "t3",
				Columns: []string{"c1"},
			},
		},
	}

	for _, primary := range append(tmePass.sourceMasters, tmePass.targetMasters...) {
		primary.FakeMysqlDaemon.Schema = sameAsVSchema
	}

	for _, primary := range append(tmeDiffs.sourceMasters, tmeDiffs.targetMasters...) {
		primary.FakeMysqlDaemon.Schema = schm
	}

	// Schema Checks
	err := tmePass.wr.ValidateSchemaKeyspace(ctx, "ks", nil /*excludeTables*/, true /*includeViews*/, true /*skipNoMaster*/, true /*includeVSchema*/)
	require.NoError(t, err)
	err = tmePass.wr.ValidateSchemaKeyspace(ctx, "ks", nil /*excludeTables*/, true /*includeViews*/, true /*skipNoMaster*/, false /*includeVSchema*/)
	require.NoError(t, err)
	shouldErr := tmeDiffs.wr.ValidateSchemaKeyspace(ctx, "ks", nil /*excludeTables*/, true /*includeViews*/, true /*skipNoMaster*/, true /*includeVSchema*/)
	require.Error(t, shouldErr)
}
