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
// Before the fix in buildRoutingRule, the routing rule's BaseTable was a
// distinct pointer from ks.Tables[name], so schema-tracker columns landed
// on a different pointer and the routing rule kept a non-authoritative
// placeholder. Cross-keyspace JOIN with `t.*` then failed with VT09015.
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

	// Simulate the schema tracker: mutate the existing BaseTables that
	// buildRoutingRule registered into ks.Tables. With the fix, the routing
	// rule references the same pointer, so this propagates.
	populate := func(ks, tbl string, cols []vindexes.Column) {
		t.Helper()
		bt := vschema.Keyspaces[ks].Tables[tbl]
		require.NotNil(t, bt, "table %s.%s not registered in ks.Tables", ks, tbl)
		bt.Columns = cols
		bt.ColumnListAuthoritative = true
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
