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

	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// A reference table on the preserved side of an outer join is only merged into the shards when
// the route is single-shard, because a multi-shard route returns each unmatched preserved row
// once per shard. These tests cover what has to hold for the rows of such a route: it must run
// even when the routing resolves to no destination, and no later merge may widen it again.

func planReferenceOuterJoin(t *testing.T, query string) engine.Primitive {
	t.Helper()

	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(vtenv.NewTestEnv(), vschema, TestBuilder)
	require.NoError(t, err)

	plan, err := TestBuilder(query, vw, vw.CurrentDb())
	require.NoError(t, err)
	return plan.Instructions
}

func TestReferencePreservedByOuterJoinRunsWithoutADestination(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"select ref_with_source.col from ref_with_source left join (select * from `user` where id = :id) as u on ref_with_source.col = u.col")

	route, ok := primitive.(*engine.Route)
	require.True(t, ok, "the join is expected to be merged into a single route, got %T", primitive)
	require.Equal(t, engine.EqualUnique, route.Opcode)
	require.True(t, route.NoRoutesSpecialHandling,
		"a route that owes the preserved reference rows cannot return an empty result when the routing finds no destination")
}

func TestReferencePreservedByOuterJoinRunsWithoutADestinationAfterAnotherMerge(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"select ref_with_source.col from ref_with_source left join (select * from `user` where id = :id) as u on ref_with_source.col = u.col join ref_with_source as r2 on r2.col = ref_with_source.col")

	route, ok := primitive.(*engine.Route)
	require.True(t, ok, "the joins are expected to be merged into a single route, got %T", primitive)
	require.Equal(t, engine.EqualUnique, route.Opcode)
	require.True(t, route.NoRoutesSpecialHandling,
		"merging the route with another one must not drop what the outer join owes the preserved rows")
}

func TestReferencePreservedByOuterJoinRunsWithoutADestinationAfterASubqueryMerge(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"select ref_with_source.col from ref_with_source left join (select * from `user` where id = :id) as u on ref_with_source.col = u.col where exists (select 1 from ref_with_source as r2)")

	route, ok := primitive.(*engine.Route)
	require.True(t, ok, "the subquery is expected to be merged into the route, got %T", primitive)
	require.Equal(t, engine.EqualUnique, route.Opcode)
	require.True(t, route.NoRoutesSpecialHandling,
		"merging a subquery into the route must not drop what the outer join owes the preserved rows")
}

func TestReferencePreservedByOuterJoinIsNotWidenedByAUnion(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"select ref_with_source.col from ref_with_source left join (select * from `user` where id = :id) as u on ref_with_source.col = u.col union all select col from `user`")

	require.IsType(t, &engine.Concatenate{}, primitive,
		"merging the route into the scatter branch would return each unmatched preserved row once per shard")
}

// A dual on the preserved side owes its row for the same reason a reference table does, and its
// routing reports the same opcode.
func TestDualPreservedByOuterJoinRunsWithoutADestination(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"select 1 from (select 1 from dual) as d left join (select * from `user` where id = :id) as u on u.col = 1")

	route, ok := primitive.(*engine.Route)
	require.True(t, ok, "the join is expected to be merged into a single route, got %T", primitive)
	require.True(t, route.NoRoutesSpecialHandling,
		"the preserved dual row cannot go missing because the other side routes nowhere")
}

// Both sides of the outer join can be reference tables, which routes to any single shard. That
// route is single-shard, so nothing blocks it on its own, but a later merge can still widen it.
func TestReferenceJoinedToReferenceIsNotWidenedByAUnion(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"select r1.col from ref_with_source as r1 left join ref as r2 on r1.col = r2.col union all select col from `user`")

	require.IsType(t, &engine.Concatenate{}, primitive,
		"merging the reference route into the scatter branch would return each unmatched r1 row once per shard")
}
