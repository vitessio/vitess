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

// TestRoutedCrossKeyspaceJoinStarExpansion exercises the planner against a
// routing-rule-only setup: two unsharded keyspaces with empty `tables: {}`
// in vschema, routing rules redirecting unqualified names at per-keyspace
// tables, and authoritative columns installed afterwards (as the schema
// tracker would in production).
//
// All four query shapes must plan:
//   - `select t.*` against either routed table -- single-Route push-down.
//   - cross-keyspace JOIN with explicit columns -- no `*` expansion needed.
//   - cross-keyspace JOIN with `t.*` qualified to one side -- the planner
//     expands the qualified `*` at vtgate using the routing rule's
//     BaseTable, which has to be authoritative.
//
// We replicate what `vschema_manager` does in production by installing
// authoritative BaseTables into `ks.Tables` (matching `setColumns`) and
// calling `RebuildRoutingRules` so each rule's `Tables[0]` picks up the new
// pointer.
func TestRoutedCrossKeyspaceJoinStarExpansion(t *testing.T) {
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

	// Stand in for the schema tracker: install authoritative BaseTables in
	// `ks.Tables`, then re-resolve routing rules so each rule's `Tables[0]`
	// points at those entries (matching what `buildAndEnhanceVSchema` does
	// once the tracker reports columns).
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
