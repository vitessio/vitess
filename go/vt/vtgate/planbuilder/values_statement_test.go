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
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vschemapb "vitess.io/vitess/go/vt/proto/vschema"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type targetDestinationFailingVSchema struct {
	plancontext.VSchema
}

func (targetDestinationFailingVSchema) TargetDestination(string) (key.ShardDestination, *vindexes.Keyspace, topodatapb.TabletType, error) {
	return nil, nil, topodatapb.TabletType_UNKNOWN, errors.New("unexpected TargetDestination fallback")
}

func TestValuesStatementPlanning(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := vindexes.BuildVSchema(&vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"main": {Sharded: false},
		},
	}, sqlparser.NewTestParser())
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["main"].Keyspace

	tests := []struct {
		query          string
		wantFieldQuery string
	}{
		{
			query: "values row(1)",
		},
		{
			query:          "values row('top-level VALUES ORDER BY generated name', 'values row(1) order by column_0', 1) order by column_0",
			wantFieldQuery: "select dt.c0 as column_0, dt.c1 as column_1, dt.c2 as column_2, weight_string(dt.c0) from (values row('top-level VALUES ORDER BY generated name', 'values row(1) order by column_0', 1) limit 0) as dt(c0, c1, c2) where 1 != 1",
		},
		{
			query: "values row('top-level VALUES ORDER BY ordinal', 'values row(1) order by 1', 1) order by 1",
		},
		{
			query: "values row(2, 'b'), row(1, 'a') order by column_0",
		},
		{
			query: "select 1 union values row(1)",
		},
		{
			query: "select * from (select 1) a union values row(1)",
		},
		{
			query: "values row(1) union select 2",
		},
		{
			query: "values row(1) union values row(2)",
		},
		{
			query: "select * from (values row(1) union select 2) as dt",
		},
		{
			query: "select * from (values row(1) union values row(2)) as dt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			var plan *engine.Plan
			require.NotPanics(t, func() {
				plan, err = TestBuilder(tt.query, vw, vw.CurrentDb())
			})
			require.NoError(t, err)
			require.NotNil(t, plan.Instructions)
			if tt.wantFieldQuery != "" {
				prims := collectPrimitives(plan.Instructions)
				route, routeIdx := findPrimitive[*engine.Route](prims)
				require.GreaterOrEqual(t, routeIdx, 0)
				require.Equal(t, tt.wantFieldQuery, route.FieldQuery)
			}
		})
	}
}

// collectPrimitives returns the primitive tree in pre-order (parents before children).
func collectPrimitives(root engine.Primitive) []engine.Primitive {
	prims := []engine.Primitive{root}
	inputs, _ := root.Inputs()
	for _, input := range inputs {
		prims = append(prims, collectPrimitives(input)...)
	}
	return prims
}

func findPrimitive[T engine.Primitive](prims []engine.Primitive) (T, int) {
	for i, prim := range prims {
		if typed, ok := prim.(T); ok {
			return typed, i
		}
	}
	var zero T
	return zero, -1
}

// MySQL silently ignores ORDER BY on a VALUES statement, so the sort has to
// happen at the vtgate level and the query sent to MySQL must not contain
// ORDER BY (or a LIMIT that would cut rows before the sort).
func TestValuesStatementOrderBySortsAtVTGate(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := vindexes.BuildVSchema(&vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"main": {Sharded: false},
		},
	}, sqlparser.NewTestParser())
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["main"].Keyspace

	t.Run("values row(2), row(1) order by column_0", func(t *testing.T) {
		plan, err := TestBuilder("values row(2), row(1) order by column_0", vw, vw.CurrentDb())
		require.NoError(t, err)
		prims := collectPrimitives(plan.Instructions)

		_, sortIdx := findPrimitive[*engine.MemorySort](prims)
		require.GreaterOrEqual(t, sortIdx, 0, "expected a vtgate-side MemorySort in the plan")

		route, routeIdx := findPrimitive[*engine.Route](prims)
		require.GreaterOrEqual(t, routeIdx, 0)
		assert.Contains(t, route.Query, "values row(2), row(1)")
		assert.NotContains(t, route.Query, "order by")
	})

	t.Run("values row(2), row(1) order by column_0 limit 1", func(t *testing.T) {
		plan, err := TestBuilder("values row(2), row(1) order by column_0 limit 1", vw, vw.CurrentDb())
		require.NoError(t, err)
		prims := collectPrimitives(plan.Instructions)

		_, limitIdx := findPrimitive[*engine.Limit](prims)
		require.GreaterOrEqual(t, limitIdx, 0, "expected a vtgate-side Limit in the plan")

		_, sortIdx := findPrimitive[*engine.MemorySort](prims)
		require.GreaterOrEqual(t, sortIdx, 0, "expected a vtgate-side MemorySort in the plan")
		assert.Less(t, limitIdx, sortIdx, "LIMIT must apply after (above) the vtgate sort")

		route, routeIdx := findPrimitive[*engine.Route](prims)
		require.GreaterOrEqual(t, routeIdx, 0)
		assert.Contains(t, route.Query, "values row(2), row(1)")
		assert.NotContains(t, route.Query, "order by")
		assert.NotContains(t, route.Query, "limit")
	})
}

// MySQL ignores ORDER BY inside a derived table without LIMIT and rejects
// ORDER BY expressions on a VALUES statement with an unknown-column error, so
// a derived VALUES table must never be sent to MySQL with its ORDER BY.
func TestDerivedValuesOrderByNotSentToMySQL(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["main"].Keyspace

	queries := []string{
		"select * from (values row(1,2),row(3,4) order by column_0 + column_1 desc) as sub, unsharded",
		"select * from (values row(1,2),row(3,4) order by column_0 + column_1 desc) as sub",
		"select * from (values row(2),row(1) order by column_0 limit 1) as sub",
	}
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			plan, err := TestBuilder(query, vw, vw.CurrentDb())
			require.NoError(t, err)
			route, routeIdx := findPrimitive[*engine.Route](collectPrimitives(plan.Instructions))
			require.GreaterOrEqual(t, routeIdx, 0)
			assert.NotContains(t, route.Query, "order by")
			assert.NotContains(t, route.FieldQuery, "order by")
		})
	}
}

// The unsharded shortcut used to strip ORDER BY from every VALUES statement it
// found, silently dropping the sort for non-derived ordered VALUES (CTE body,
// union arm). Those statements must skip the shortcut so normal planning can
// sort at the vtgate level (MySQL ignores ORDER BY on VALUES).
func TestNonDerivedOrderedValuesSkipUnshardedShortcut(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["main"].Keyspace

	t.Run("with t as (select col from unsharded) values row(2), row(1) order by column_0", func(t *testing.T) {
		plan, err := TestBuilder("with t as (select col from unsharded) values row(2), row(1) order by column_0", vw, vw.CurrentDb())
		require.NoError(t, err)
		prims := collectPrimitives(plan.Instructions)

		_, sortIdx := findPrimitive[*engine.MemorySort](prims)
		require.GreaterOrEqual(t, sortIdx, 0, "expected a vtgate-side MemorySort in the plan")

		route, routeIdx := findPrimitive[*engine.Route](prims)
		require.GreaterOrEqual(t, routeIdx, 0)
		assert.NotContains(t, route.Query, "order by")
	})

	t.Run("(values row(2), row(1) order by column_0 limit 1) union select col from unsharded", func(t *testing.T) {
		plan, err := TestBuilder("(values row(2), row(1) order by column_0 limit 1) union select col from unsharded", vw, vw.CurrentDb())
		require.NoError(t, err)
		prims := collectPrimitives(plan.Instructions)

		limit, limitIdx := findPrimitive[*engine.Limit](prims)
		require.GreaterOrEqual(t, limitIdx, 0, "expected a vtgate-side Limit over the VALUES arm")
		limitPrims := collectPrimitives(limit)

		_, sortIdx := findPrimitive[*engine.MemorySort](limitPrims)
		require.GreaterOrEqual(t, sortIdx, 0, "expected a vtgate-side MemorySort under the Limit")

		route, routeIdx := findPrimitive[*engine.Route](limitPrims)
		require.GreaterOrEqual(t, routeIdx, 0)
		assert.Contains(t, route.Query, "values row(2), row(1)")
		assert.NotContains(t, route.Query, "order by")
		assert.NotContains(t, route.Query, "limit")
	})
}

// Plain unsharded queries must still take the shortcut: a derived ordered
// VALUES table does not disqualify the statement.
func TestDerivedOrderedValuesStillTakeUnshardedShortcut(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["main"].Keyspace

	plan, err := TestBuilder("select * from (values row(2), row(1) order by column_0) as sub, unsharded", vw, vw.CurrentDb())
	require.NoError(t, err)
	route, ok := plan.Instructions.(*engine.Route)
	require.True(t, ok, "expected the plan to be a single Route (unsharded shortcut)")
	assert.NotContains(t, route.Query, "order by")
}

func TestValuesStatementPlanningOrderByGeneratedColumns(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := vindexes.BuildVSchema(&vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"main": {Sharded: false},
		},
	}, sqlparser.NewTestParser())
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["main"].Keyspace

	tests := []struct {
		query         string
		routeQuery    string
		truncateCount int
	}{
		{
			query:         "values row(2, 'b'), row(1, 'a') order by column_0",
			routeQuery:    "select dt.c0 as column_0, dt.c1 as column_1, weight_string(dt.c0) from (values row(2, 'b'), row(1, 'a')) as dt(c0, c1)",
			truncateCount: 2,
		},
		{
			query:         "values row(2, 'b'), row(1, 'a') order by COLUMN_0",
			routeQuery:    "select dt.c0 as column_0, dt.c1 as column_1, weight_string(dt.c0) from (values row(2, 'b'), row(1, 'a')) as dt(c0, c1)",
			truncateCount: 2,
		},
		{
			// The ordering expression is computed over the materialized VALUES
			// columns (dt.cN), not substituted back into the rows, so a volatile
			// expression would be evaluated exactly once.
			query:         "values row('case', 'sql', 1, 10), row('case', 'sql', 2, 20) order by column_2 + column_3 desc",
			routeQuery:    "select dt.c0 as column_0, dt.c1 as column_1, dt.c2 as column_2, dt.c3 as column_3, dt.c2 + dt.c3 as `column_2 + column_3`, weight_string(dt.c2 + dt.c3) from (values row('case', 'sql', 1, 10), row('case', 'sql', 2, 20)) as dt(c0, c1, c2, c3)",
			truncateCount: 4,
		},
		{
			query:         "values row('case', 'sql', 1, 10), row('case', 'sql', 2, 20) order by COLUMN_2 + Column_3 desc",
			routeQuery:    "select dt.c0 as column_0, dt.c1 as column_1, dt.c2 as column_2, dt.c3 as column_3, dt.c2 + dt.c3 as `COLUMN_2 + Column_3`, weight_string(dt.c2 + dt.c3) from (values row('case', 'sql', 1, 10), row('case', 'sql', 2, 20)) as dt(c0, c1, c2, c3)",
			truncateCount: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			plan, err := TestBuilder(tt.query, vw, vw.CurrentDb())
			require.NoError(t, err)
			prims := collectPrimitives(plan.Instructions)

			// the sort happens at the vtgate level; the query sent to MySQL
			// carries neither ORDER BY nor the generated sort column names
			sort, sortIdx := findPrimitive[*engine.MemorySort](prims)
			require.GreaterOrEqual(t, sortIdx, 0, "expected a vtgate-side MemorySort in the plan")
			require.Equal(t, tt.truncateCount, sort.TruncateColumnCount)

			route, routeIdx := findPrimitive[*engine.Route](prims)
			require.GreaterOrEqual(t, routeIdx, 0)
			require.Equal(t, tt.routeQuery, route.Query)
		})
	}
}

// An ordered VALUES arm inside a UNION ALL adds a weight_string helper column
// for the vtgate-side sort. The top-level truncation only reaches the plan
// root, so without a per-arm truncation the MemorySort arm would be one column
// wider than its sibling arm and engine.Concatenate.getFieldTypes would fail at
// execution with "The used SELECT statements have a different number of
// columns". The MemorySort must truncate the helper column away so every
// Concatenate arm produces the same number of columns.
func TestValuesStatementUnionAllOrderedArmTruncatesHelperColumn(t *testing.T) {
	envUnsharded := vtenv.NewTestEnv()
	unsharded := vindexes.BuildVSchema(&vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"main": {Sharded: false},
		},
	}, sqlparser.NewTestParser())
	vwUnsharded, err := vschemawrapper.NewVschemaWrapper(envUnsharded, unsharded, TestBuilder)
	require.NoError(t, err)
	vwUnsharded.Keyspace = unsharded.Keyspaces["main"].Keyspace

	envSharded := vtenv.NewTestEnv()
	sharded := loadSchema(t, "vschemas/schema.json", true)
	vwSharded, err := vschemawrapper.NewVschemaWrapper(envSharded, sharded, TestBuilder)
	require.NoError(t, err)
	vwSharded.Keyspace = sharded.Keyspaces["user"].Keyspace

	tests := []struct {
		query string
		vw    *vschemawrapper.VSchemaWrapper
	}{
		{query: "(values row(2),row(1) order by column_0) union all select id from unsharded", vw: vwUnsharded},
		{query: "values row(1) union all (values row(2),row(1) order by column_0)", vw: vwUnsharded},
		{query: "select 1 union all (values row(2),row(1) order by column_0)", vw: vwUnsharded},
		{query: "(values row(2),row(1) order by column_0) union all values row(3)", vw: vwUnsharded},
		{query: "(values row(2),row(1) order by column_0 limit 1) union all select id from unsharded", vw: vwUnsharded},
		{query: "(values row(2),row(1) order by column_0) union all select id from user", vw: vwSharded},
		{query: "values row(1) union all (values row(2),row(1) order by column_0)", vw: vwSharded},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			plan, err := TestBuilder(tt.query, tt.vw, tt.vw.CurrentDb())
			require.NoError(t, err)
			prims := collectPrimitives(plan.Instructions)

			_, concatIdx := findPrimitive[*engine.Concatenate](prims)
			require.GreaterOrEqual(t, concatIdx, 0, "expected a vtgate-side Concatenate")

			sort, sortIdx := findPrimitive[*engine.MemorySort](prims)
			require.GreaterOrEqual(t, sortIdx, 0, "expected a vtgate-side MemorySort for the ordered VALUES arm")
			require.Equal(t, 1, sort.TruncateColumnCount,
				"the ordered VALUES arm must truncate its weight_string helper column so the Concatenate arms line up")
		})
	}
}

// MySQL ignores ORDER BY on a VALUES statement, so vtgate sorts the rows with a
// MemorySort. When the ordering expression is volatile (e.g. rand()), the sort
// key and the returned column must come from a SINGLE evaluation. The ordering
// helper must therefore be computed OVER the materialized VALUES columns
// (dt.cN), not substituted back into the VALUES rows: widening the rows to
// `values row(rand(), rand() + 0)` re-evaluates rand(), so MemorySort would sort
// by a different value than the one returned.
func TestValuesStatementOrderByVolatileExprEvaluatedOnce(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := vindexes.BuildVSchema(&vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"main": {Sharded: false},
		},
	}, sqlparser.NewTestParser())
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["main"].Keyspace

	plan, err := TestBuilder("values row(rand()), row(rand()) order by column_0 + 0", vw, vw.CurrentDb())
	require.NoError(t, err)
	prims := collectPrimitives(plan.Instructions)

	_, sortIdx := findPrimitive[*engine.MemorySort](prims)
	require.GreaterOrEqual(t, sortIdx, 0, "expected a vtgate-side MemorySort in the plan")

	route, routeIdx := findPrimitive[*engine.Route](prims)
	require.GreaterOrEqual(t, routeIdx, 0)

	// the VALUES rows must be materialized exactly as written: a single rand()
	// per row, wrapped in a derived table so the ordering helper can reference
	// the materialized column.
	assert.Contains(t, route.Query, "(values row(rand()), row(rand()))",
		"the VALUES rows must not be widened with a substituted ordering expression")
	assert.NotContains(t, route.Query, "rand() + 0",
		"the ordering expression must not be substituted back into the VALUES rows (would re-evaluate rand())")
	// the ordering helper (and its weight_string) is computed over the
	// materialized derived-table column dt.c0, so it derives from the same
	// single evaluation as the returned column_0.
	assert.Contains(t, route.Query, "dt.c0 + 0",
		"the ordering helper must be computed over the materialized VALUES column")
}

func TestValuesStatementPlanningIsIdempotentOnSameAST(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := vindexes.BuildVSchema(&vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"main": {Sharded: false},
		},
	}, sqlparser.NewTestParser())
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["main"].Keyspace

	query := "values row('case', 'sql', 1, 10), row('case', 'sql', 2, 20) order by column_2 + column_3 desc"
	stmt, known, err := sqlparser.NewTestParser().Parse2(query)
	require.NoError(t, err)
	reservedVars := sqlparser.NewReservedVars("vtg", known)
	result, err := sqlparser.Normalize(stmt, reservedVars, map[string]*querypb.BindVariable{}, false, vw.CurrentDb(), sqlparser.SQLSelectLimitUnset, "", nil, vw.GetForeignKeyChecksState(), vw)
	require.NoError(t, err)

	astBefore := sqlparser.String(result.AST)

	buildPlan := func() string {
		plan, err := BuildFromStmt(t.Context(), query, result.AST, reservedVars, vw, result.BindVarNeeds, staticConfig{})
		require.NoError(t, err)
		desc, err := json.Marshal(engine.PrimitiveToPlanDescription(plan.Instructions, nil))
		require.NoError(t, err)
		return string(desc)
	}

	first := buildPlan()
	second := buildPlan()
	assert.Equal(t, first, second)
	assert.Equal(t, astBefore, sqlparser.String(result.AST))
}

func TestStatementHasValuesSubquery(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{query: "values row((select 1)) union values row(1)", want: true},
		{query: "values row(1) union values row((select 1))", want: true},
		{query: "values row((select 1))", want: true},
		{query: "values row(1)", want: false},
		{query: "values row(1) union values row(2)", want: false},
		{query: "select * from (values row(1)) as dt", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			stmt, err := sqlparser.NewTestParser().Parse(tt.query)
			require.NoError(t, err)
			assert.Equal(t, tt.want, statementHasValuesSubquery(stmt))
		})
	}
}

func TestValuesStatementPlanningRejectsSubqueries(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := vindexes.BuildVSchema(&vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"main": {Sharded: false},
		},
	}, sqlparser.NewTestParser())
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["main"].Keyspace

	tests := []struct {
		query string
		err   string
	}{
		{
			query: "values row((select sku from product limit 1))",
			err:   "VT12001: unsupported: subqueries in VALUES statements",
		},
		{
			query: "select * from (values row((select sku from product limit 1))) as dt",
			err:   "VT12001: unsupported: subqueries in VALUES statements",
		},
		{
			query: "select (values row(1))",
			err:   "VT12001: unsupported: VALUES statements in subqueries",
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			_, err := TestBuilder(tt.query, vw, vw.CurrentDb())
			require.ErrorContains(t, err, tt.err)
		})
	}
}

func TestValuesStatementPlanningRejectsRaggedRows(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := vindexes.BuildVSchema(&vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"main": {Sharded: false},
		},
	}, sqlparser.NewTestParser())
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["main"].Keyspace

	tests := []string{
		"values row(1, 2), row(3)",
		"values row(1, 2), row(3) order by column_1 + 1",
	}

	for _, query := range tests {
		t.Run(query, func(t *testing.T) {
			require.NotPanics(t, func() {
				_, err = TestBuilder(query, vw, vw.CurrentDb())
			})
			require.ErrorContains(t, err, "The used SELECT statements have a different number of columns: 2, 1")
		})
	}
}

func TestExplainValuesStatementPlanning(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := vindexes.BuildVSchema(&vschemapb.SrvVSchema{
		Keyspaces: map[string]*vschemapb.Keyspace{
			"main": {Sharded: false},
		},
	}, sqlparser.NewTestParser())
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["main"].Keyspace

	stmt, bindVars, err := sqlparser.NewTestParser().Parse2("explain values row(1)")
	require.NoError(t, err)

	reservedVars := sqlparser.NewReservedVars("vtg", bindVars)
	plan, err := buildExplainStmtPlan(stmt, reservedVars, targetDestinationFailingVSchema{VSchema: vw})
	require.NoError(t, err)
	send, ok := plan.primitive.(*engine.Send)
	require.True(t, ok)
	require.Equal(t, "main", send.Keyspace.Name)
	_, ok = send.TargetDestination.(key.DestinationAnyShard)
	require.True(t, ok)
	require.Equal(t, "explain values row(1)", send.Query)
}

func TestValuesStatementUnionAgainstShardedTable(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["user"].Keyspace

	t.Run("select id from user union values row(1)", func(t *testing.T) {
		// UNION DISTINCT may merge the VALUES row into the scatter route:
		// every shard returns the row, and the vtgate-side Distinct dedups it.
		plan, err := TestBuilder("select id from user union values row(1)", vw, vw.CurrentDb())
		require.NoError(t, err)

		distinct, ok := plan.Instructions.(*engine.Distinct)
		require.True(t, ok, "expected a vtgate-side Distinct on top of the plan")
		route, routeIdx := findPrimitive[*engine.Route](collectPrimitives(distinct))
		require.GreaterOrEqual(t, routeIdx, 0)
		assert.Equal(t, engine.Scatter, route.Opcode)
	})

	t.Run("select id from user union all values row(1)", func(t *testing.T) {
		// UNION ALL must NOT merge the VALUES row into the scatter route:
		// there is no dedup, so a merged route would return the row once per
		// shard. The VALUES arm has to stay in its own single-shard route.
		plan, err := TestBuilder("select id from user union all values row(1)", vw, vw.CurrentDb())
		require.NoError(t, err)
		prims := collectPrimitives(plan.Instructions)

		_, concatIdx := findPrimitive[*engine.Concatenate](prims)
		require.GreaterOrEqual(t, concatIdx, 0, "expected a vtgate-side Concatenate, not a merged route")

		var scatterRoutes, valuesRoutes []*engine.Route
		for _, prim := range prims {
			route, ok := prim.(*engine.Route)
			if !ok {
				continue
			}
			if route.Opcode == engine.Scatter {
				scatterRoutes = append(scatterRoutes, route)
			}
			if strings.Contains(route.Query, "values row(1)") {
				valuesRoutes = append(valuesRoutes, route)
			}
		}

		require.Len(t, scatterRoutes, 1)
		assert.NotContains(t, scatterRoutes[0].Query, "values", "VALUES must not be sent to every shard")
		require.Len(t, valuesRoutes, 1)
		assert.True(t, valuesRoutes[0].Opcode.IsSingleShard(),
			"the VALUES arm must target a single shard, got opcode %s", valuesRoutes[0].Opcode.String())
	})
}

// A VALUES arm that hides behind a derived table or behind an already-merged
// intermediate Route (whose Source is a Union) must still be kept out of a
// multi-shard route under UNION ALL, or its rows would be returned once per
// shard.
func TestValuesStatementUnionAllDeepGuard(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["user"].Keyspace

	assertValuesArmNotScattered := func(t *testing.T, query string) {
		plan, err := TestBuilder(query, vw, vw.CurrentDb())
		require.NoError(t, err)
		prims := collectPrimitives(plan.Instructions)

		_, concatIdx := findPrimitive[*engine.Concatenate](prims)
		require.GreaterOrEqual(t, concatIdx, 0, "expected a vtgate-side Concatenate, not a merged route")

		var scatterRoutes, valuesRoutes []*engine.Route
		for _, prim := range prims {
			route, ok := prim.(*engine.Route)
			if !ok {
				continue
			}
			if route.Opcode == engine.Scatter {
				scatterRoutes = append(scatterRoutes, route)
			}
			if strings.Contains(route.Query, "values row(1)") {
				valuesRoutes = append(valuesRoutes, route)
			}
		}

		require.Len(t, scatterRoutes, 1)
		assert.NotContains(t, scatterRoutes[0].Query, "values", "VALUES must not be sent to every shard")
		require.Len(t, valuesRoutes, 1)
		assert.True(t, valuesRoutes[0].Opcode.IsSingleShard(),
			"the VALUES arm must target a single shard, got opcode %s", valuesRoutes[0].Opcode.String())
	}

	t.Run("derived VALUES arm", func(t *testing.T) {
		// The VALUES statement is wrapped in a derived table, so
		// valuesStatementUnderRoute reports false; the deep guard must still
		// see it and keep it out of the scatter route.
		assertValuesArmNotScattered(t, "select * from (values row(1),row(2)) as v(a) union all select id from user")
	})

	t.Run("VALUES arm merged into an intermediate route", func(t *testing.T) {
		// values row(1) first merges with the dual "select 2" into a Route
		// whose Source is a Union; the deep guard must still find the VALUES
		// statement inside that Union before the merge into the scatter.
		assertValuesArmNotScattered(t, "values row(1) union all select 2 union all select id from user")
	})

	t.Run("VALUES joined into a scattered arm still merges", func(t *testing.T) {
		// Here the VALUES row is joined with a scattered table, so its arm is
		// itself a scatter route. Per-shard semantics are correct, so the guard
		// must NOT fire and both scatter arms merge into a single route.
		plan, err := TestBuilder("select u.id from user u join (values row(1)) v union all select id from user", vw, vw.CurrentDb())
		require.NoError(t, err)
		route, ok := plan.Instructions.(*engine.Route)
		require.True(t, ok, "expected the two scatter arms to merge into a single Route")
		assert.Equal(t, engine.Scatter, route.Opcode)
	})
}

// Merging a VALUES arm into a single-shard route is safe even for UNION ALL:
// only one shard returns the row, so no duplicates can appear.
func TestValuesStatementUnionAllStaysMergedOnSingleShard(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	vw.Keyspace = vschema.Keyspaces["main"].Keyspace

	tests := []struct {
		query     string
		wantQuery string
	}{
		{
			query:     "values row(1) union all values row(2)",
			wantQuery: "values row(1) union all values row(2)",
		},
		{
			query:     "select id from unsharded union all values row(1)",
			wantQuery: "select id from unsharded union all values row(1)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			plan, err := TestBuilder(tt.query, vw, vw.CurrentDb())
			require.NoError(t, err)
			route, ok := plan.Instructions.(*engine.Route)
			require.True(t, ok, "expected the plan to be a single merged Route")
			assert.True(t, route.Opcode.IsSingleShard())
			assert.Equal(t, tt.wantQuery, route.Query)
		})
	}
}
