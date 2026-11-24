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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func TestUnionSortCollationQueries(t *testing.T) {
	for _, query := range []string{
		"select 'All' as name from unsharded union all select name from user order by name",
		"select name from user union all select 'All' as name from unsharded order by name desc limit 2, 5",
		"select name, id from user union all select name, id from unsharded order by name, id limit 5",
		"select name from user where id in (1, 2) union all select name from unsharded order by name",
		"select name from (select 'All' as name from unsharded union all select name from user) t order by name",
		"select id, name from (select name, id from user union all select name, id from unsharded) t order by name, id",
	} {
		t.Run(query, func(t *testing.T) {
			env := vtenv.NewTestEnv()
			vschema := loadSchema(t, "vschemas/schema.json", true)
			vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
			require.NoError(t, err)
			plan, err := TestBuilder(query, vw, vw.CurrentDb())
			require.NoError(t, err)
			primitive := engine.Find(func(p engine.Primitive) bool { _, ok := p.(*engine.MemorySort); return ok }, plan.Instructions)
			require.NotNil(t, primitive)
			sort := primitive.(*engine.MemorySort)
			union, ok := sort.Input.(*engine.Concatenate)
			require.True(t, ok)
			require.Len(t, sort.CollationQueries, len(union.Sources))
			for i, input := range union.Sources {
				route := input.(*engine.Route)
				metadata := sort.CollationQueries[i].(*engine.Route)
				assert.Equal(t, route.RoutingParameters, metadata.RoutingParameters, "keep the arm's destinations")
				assert.NotContains(t, route.Query, ":__upper_limit", "resolve the global comparator before truncating arms")
				assert.Contains(t, metadata.Query, "limit 0")
				assert.Equal(t, 2*len(sort.OrderBy), strings.Count(metadata.Query, "(select __union_sort.c"))
				assert.Contains(t, metadata.Query, "coercibility(")
				assert.False(t, metadata.NoRoutesSpecialHandling, "empty metadata routes must retain the shard-affine GetFields fallback")
				assert.NotContains(t, metadata.Query, "::__vals", "metadata must work even when IN routing has no destinations")
				stmt, err := env.Parser().Parse(metadata.Query)
				require.NoError(t, err)
				assert.Equal(t, 2*len(sort.OrderBy), stmt.(sqlparser.SelectStatement).GetColumnCount())
			}
		})
	}
}

func TestUnionSortCollationQueryWrappers(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	for _, query := range []string{
		"select 'All' as name from unsharded union all select name from music where id = 1 order by name limit 1",
		"select 'All' as name from unsharded union all (select name from user order by name limit 2) order by name limit 1",
	} {
		t.Run(query, func(t *testing.T) {
			plan, err := TestBuilder(query, vw, vw.CurrentDb())
			require.NoError(t, err)
			primitive := engine.Find(func(p engine.Primitive) bool { _, ok := p.(*engine.MemorySort); return ok }, plan.Instructions)
			require.NotNil(t, primitive)
			sort := primitive.(*engine.MemorySort)
			require.Len(t, sort.CollationQueries, 2)
			union := sort.Input.(*engine.Concatenate)
			switch arm := union.Sources[1].(type) {
			case *engine.VindexLookup:
				metadata, ok := sort.CollationQueries[1].(*engine.VindexLookup)
				require.True(t, ok, "lookup retains ownership of its destination")
				assert.Same(t, arm.Lookup, metadata.Lookup)
				assert.NotSame(t, arm.SendTo, metadata.SendTo)
				assert.Contains(t, metadata.SendTo.Query, "coercibility(")
				assert.NotContains(t, arm.SendTo.Query, "coercibility(")
			case *engine.Limit:
				assert.Equal(t, "2", sqlparser.String(arm.Count), "keep arm-local row selection")
				_, ok := sort.CollationQueries[1].(*engine.Route)
				assert.True(t, ok, "metadata is not truncated by the arm's LIMIT")
			default:
				t.Fatalf("test did not produce a wrapped arm: %T", arm)
			}
		})
	}

	route := &engine.Route{FieldQuery: "select name, id from unsharded where 1 != 1"}
	projection := &engine.SimpleProjection{Cols: []int{1, 0}, Input: route}
	metadata, err := unionSortCollationQuery(&plancontext.PlanningContext{VSchema: vw}, projection, []int{0})
	require.NoError(t, err)
	require.NotNil(t, metadata)
	assert.Contains(t, metadata.(*engine.Route).Query, "select __union_sort.c1 from", "map projected offsets back to route columns")
}

func TestUnionSortMetadataStatement(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	statement, err := env.Parser().Parse("select /*+ SET_VAR(sql_mode='NO_ENGINE_SUBSTITUTION') SET_VAR(sort_buffer_size=16384) NO_MERGE(t) */ name from unsharded t for update")
	require.NoError(t, err)
	route := &engine.Route{
		QueryStatement: statement,
		FieldQuery:     "select /*+ SET_VAR(sql_mode='NO_ENGINE_SUBSTITUTION') SET_VAR(sort_buffer_size=16384) NO_MERGE(t) */ name from unsharded t where 1 != 1",
	}
	query, err := unionSortCollationQuery(&plancontext.PlanningContext{VSchema: vw}, route, []int{0})
	require.NoError(t, err)
	metadata := query.(*engine.Route)
	parsed, err := env.Parser().Parse(metadata.Query)
	require.NoError(t, err)
	outer := parsed.(*sqlparser.Select)
	assert.Equal(t, "'NO_ENGINE_SUBSTITUTION'", outer.GetParsedComments().GetMySQLSetVarValue("sql_mode"))
	assert.Equal(t, "16384", outer.GetParsedComments().GetMySQLSetVarValue("sort_buffer_size"))
	assert.NotContains(t, sqlparser.String(outer.Comments), "NO_MERGE", "table-specific hints stay in their query block")
	assert.Contains(t, metadata.Query, "NO_MERGE(t)")
	assert.Equal(t, metadata.Query, sqlparser.String(metadata.QueryStatement), "replica warming must use the metadata statement")
	assert.Equal(t, sqlparser.NoLock, metadata.QueryStatement.(*sqlparser.Select).Lock)
	assert.Equal(t, sqlparser.ForUpdateLock, statement.(*sqlparser.Select).Lock, "leave the data route untouched")
}

func TestUnionSortExcludesRowShapingRoutes(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)
	for _, query := range []string{
		"select name from unsharded group by id",
		"select distinct name from unsharded",
		"select name from (select name from unsharded group by id) t",
		"select name from unsharded union all select name from user",
	} {
		t.Run(query, func(t *testing.T) {
			route := &engine.Route{FieldQuery: query}
			metadata, err := unionSortCollationQuery(&plancontext.PlanningContext{VSchema: vw}, route, []int{0})
			require.NoError(t, err)
			assert.Nil(t, metadata, "avoid probes and added keys that alter grouping or duplicate equality")
			assert.Equal(t, query, route.FieldQuery)
		})
	}
}
