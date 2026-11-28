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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func TestUnionSortKnownNumericAvoidsMetadata(t *testing.T) {
	env := vtenv.NewTestEnv()
	vw, err := vschemawrapper.NewVschemaWrapper(env, loadSchema(t, "vschemas/schema.json", true), TestBuilder)
	require.NoError(t, err)
	q := "select intcol from user union all select col14 from unsharded_fk_allow.u_tbl1 order by intcol limit 10"
	plan, err := TestBuilder(q, vw, vw.CurrentDb())
	require.NoError(t, err)
	sort := engine.Find(func(p engine.Primitive) bool { _, ok := p.(*engine.MemorySort); return ok }, plan.Instructions).(*engine.MemorySort)
	require.NotEqual(t, sqltypes.Unknown, sort.OrderBy[0].Type.Type())
	require.NotNil(t, sort.UpperLimit, "the global count must bound the UNION heap")
	require.Empty(t, sort.CollationQueries, "known numeric UNIONs must avoid runtime collation probes")
}

func TestUnionSortOriginalStatementEligibility(t *testing.T) {
	env := vtenv.NewTestEnv()
	vw, err := vschemawrapper.NewVschemaWrapper(env, loadSchema(t, "vschemas/schema.json", true), TestBuilder)
	require.NoError(t, err)
	for _, q := range []string{"select name,sum(id) from unsharded where name='a'", "select distinct name,id from unsharded"} {
		t.Run(q, func(t *testing.T) {
			stmt, err := env.Parser().Parse(q)
			require.NoError(t, err)
			route := &engine.Route{QueryStatement: stmt, FieldQuery: sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery).WriteNode(stmt).ParsedQuery().Query}
			metadata, err := unionSortCollationQuery(&plancontext.PlanningContext{VSchema: vw}, route, []int{0})
			require.NoError(t, err)
			if q == "select name,sum(id) from unsharded where name='a'" {
				require.Nil(t, metadata, "implicit aggregation must be excluded")
			} else {
				require.NotNil(t, metadata, "DISTINCT is preserved inside the original arm")
			}
		})
	}
}

func TestUnionSortUnknownSharedColumnsStillResolveMetadata(t *testing.T) {
	env := vtenv.NewTestEnv()
	vw, err := vschemawrapper.NewVschemaWrapper(env, loadSchema(t, "vschemas/schema.json", false), TestBuilder)
	require.NoError(t, err)
	plan, err := TestBuilder("select textcol1 as x from user union all select col1 from unsharded_fk_allow.u_tbl1 order by x limit 10", vw, vw.CurrentDb())
	require.NoError(t, err)
	sort := engine.Find(func(p engine.Primitive) bool { _, ok := p.(*engine.MemorySort); return ok }, plan.Instructions).(*engine.MemorySort)
	require.False(t, sort.UnionCollationKnown, "two Unknown IDs do not establish a shared collation")
	require.Len(t, sort.CollationQueries, 2, "each arm must resolve its unknown textual collation")
}

func TestUnionSortExplicitSharedColumnsAvoidMetadata(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", false)
	for _, tc := range []struct{ keyspace, table, column string }{
		{"user", "user", "textcol1"},
		{"unsharded_fk_allow", "u_tbl1", "col1"},
	} {
		table := vschema.Keyspaces[tc.keyspace].Tables[tc.table]
		for i := range table.Columns {
			if table.Columns[i].Name.EqualString(tc.column) {
				table.Columns[i].CollationName = "utf8mb4_general_ci"
			}
		}
	}
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	plan, err := TestBuilder("select textcol1 as x from user union all select col1 from unsharded_fk_allow.u_tbl1 order by x limit 10", vw, vw.CurrentDb())
	require.NoError(t, err)
	sort := engine.Find(func(p engine.Primitive) bool { _, ok := p.(*engine.MemorySort); return ok }, plan.Instructions).(*engine.MemorySort)
	require.True(t, sort.UnionCollationKnown)
	require.Empty(t, sort.CollationQueries)
}

func TestUnionSortKnownColumnsRequiresMatchingCollations(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", false)
	table := vschema.Keyspaces["unsharded_fk_allow"].Tables["u_tbl1"]
	for i := range table.Columns {
		if table.Columns[i].Name.EqualString("col1") {
			table.Columns[i].CollationName = "utf8mb4_bin"
		}
	}
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	plan, err := TestBuilder("select textcol1 as x from user union all select col1 from unsharded_fk_allow.u_tbl1 order by x limit 10", vw, vw.CurrentDb())
	require.NoError(t, err)
	sort := engine.Find(func(p engine.Primitive) bool { _, ok := p.(*engine.MemorySort); return ok }, plan.Instructions).(*engine.MemorySort)
	require.False(t, sort.UnionCollationKnown)
	require.Len(t, sort.CollationQueries, 2, "different known collations need runtime common-collation resolution")
}

func TestUnionSortKnownColumnsRejectsAggregateRoutes(t *testing.T) {
	env := vtenv.NewTestEnv()
	vw, err := vschemawrapper.NewVschemaWrapper(env, loadSchema(t, "vschemas/schema.json", true), TestBuilder)
	require.NoError(t, err)
	left := &engine.Route{QueryStatement: mustParseStatement(t, env, "select textcol1,max(id) from user group by textcol1")}
	right := &engine.Route{QueryStatement: mustParseStatement(t, env, "select col1,max(col14) from unsharded_fk_allow.u_tbl1 group by col1")}
	source := &operators.Union{Selects: [][]sqlparser.SelectExpr{
		{&sqlparser.AliasedExpr{Expr: sqlparser.NewColName("textcol1")}},
		{&sqlparser.AliasedExpr{Expr: sqlparser.NewColName("col1")}},
	}}
	sort := &engine.MemorySort{Input: engine.NewConcatenate([]engine.Primitive{left, right}, nil), OrderBy: nil}
	require.False(t, unionSortKnownColumns(&plancontext.PlanningContext{VSchema: vw}, source, sort), "aggregate arms are excluded from the shared-column shortcut")
}

func mustParseStatement(t *testing.T, env *vtenv.Environment, query string) sqlparser.Statement {
	t.Helper()
	stmt, err := env.Parser().Parse(query)
	require.NoError(t, err)
	return stmt
}

func TestUnionSortAliasAndOrdinalUseSameLocalTypeInference(t *testing.T) {
	env := vtenv.NewTestEnv()
	vw, err := vschemawrapper.NewVschemaWrapper(env, loadSchema(t, "vschemas/schema.json", false), TestBuilder)
	require.NoError(t, err)
	var sorts []*engine.MemorySort
	for _, orderBy := range []string{"x", "1"} {
		query := "select textcol1 as x from user union all select col1 from unsharded_fk_allow.u_tbl1 order by " + orderBy + " limit 10"
		plan, err := TestBuilder(query, vw, vw.CurrentDb())
		require.NoError(t, err)
		sorts = append(sorts, engine.Find(func(p engine.Primitive) bool { _, ok := p.(*engine.MemorySort); return ok }, plan.Instructions).(*engine.MemorySort))
	}
	require.Equal(t, sorts[0].OrderBy[0].Type.Type(), sorts[1].OrderBy[0].Type.Type())
	require.Equal(t, sorts[0].OrderBy[0].Type.Collation(), sorts[1].OrderBy[0].Type.Collation())
	require.Equal(t, sorts[0].UnionCollationKnown, sorts[1].UnionCollationKnown)
	require.Len(t, sorts[0].CollationQueries, len(sorts[1].CollationQueries))
}
