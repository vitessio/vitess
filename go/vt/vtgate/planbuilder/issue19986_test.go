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

package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vschemawrapper"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// TestIssue19986CrossKeyspaceJoinStarExpansion reproduces issue #19986:
// `SELECT a.* FROM table_a a JOIN table_b b ON ...` over two unsharded
// keyspaces with empty `tables: {}` in vschema and routing rules pointing
// at the per-keyspace tables. The planner relies on the schema tracker to
// supply columns for the routed tables.
//
// Before the fix, the routing rule's BaseTable was the synthesized
// placeholder from the initial buildRoutingRule pass; the schema tracker's
// setColumns produced a separate authoritative BaseTable in ks.Tables, but
// the routing rule kept its stale pointer. Cross-keyspace JOIN with `t.*`
// then failed with VT09015. The fix re-resolves routing rules via
// RebuildRoutingRules after schema-tracker updates so the rule picks up
// whatever is in ks.Tables.
func TestIssue19986CrossKeyspaceJoinStarExpansion(t *testing.T) {
	parser := sqlparser.NewTestParser()

	srcVSchema := &vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"main": {Sharded: false},
			"ks_a": {Sharded: false},
			"ks_b": {Sharded: false},
		},
		RoutingRules: &vschemapb.RoutingRules{
			Rules: []*vschemapb.RoutingRule{
				{FromTable: "table_a", ToTables: []string{"ks_a.table_a"}},
				{FromTable: "table_b", ToTables: []string{"ks_b.table_b"}},
			},
		},
	}

	vschema := vindexes.BuildVSchema(srcVSchema, parser)

	// Simulate what vschema_manager does after the schema tracker reports
	// columns for these tables: place authoritative BaseTables in ks.Tables,
	// then call RebuildRoutingRules so the routing rule's Tables[0] gets
	// re-resolved to those entries.
	populate := func(ks, tbl string, cols []vindexes.Column) {
		t.Helper()
		ksSchema := vschema.Keyspaces[ks]
		require.NotNil(t, ksSchema)
		ksSchema.Tables[tbl] = &vindexes.BaseTable{
			Name:                    sqlparser.NewIdentifierCS(tbl),
			Keyspace:                ksSchema.Keyspace,
			Columns:                 cols,
			ColumnListAuthoritative: true,
		}
	}
	populate("ks_a", "table_a", []vindexes.Column{
		{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_INT64},
		{Name: sqlparser.NewIdentifierCI("fk"), Type: querypb.Type_INT64},
		{Name: sqlparser.NewIdentifierCI("name"), Type: querypb.Type_VARCHAR},
	})
	populate("ks_b", "table_b", []vindexes.Column{
		{Name: sqlparser.NewIdentifierCI("id"), Type: querypb.Type_INT64},
		{Name: sqlparser.NewIdentifierCI("label"), Type: querypb.Type_VARCHAR},
	})
	vindexes.RebuildRoutingRules(srcVSchema, vschema, parser)

	env := vtenv.NewTestEnv()
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)

	queries := []string{
		"select a.* from table_a a limit 1",
		"select b.* from table_b b limit 1",
		"select a.id, b.label from table_a a join table_b b on a.fk = b.id",
		"select a.* from table_a a join table_b b on a.fk = b.id",
	}
	for _, q := range queries {
		t.Run(q, func(t *testing.T) {
			_, err := TestBuilder(q, vw, vw.CurrentDb())
			assert.NoError(t, err, "query failed: %s", q)
		})
	}
}
