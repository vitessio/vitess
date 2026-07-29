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

// A refused merge has to leave the plan as it was. The CTE merger rewrites the recursion definition
// as it goes, so the routing is checked before any of that: a rejection afterwards leaves the CTE
// marked as merged, and the term query then reads the CTE as a table instead of taking the value
// from the bind variable.
func TestRefusedCTEMergeLeavesAWorkingPlan(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"with recursive c as (select r1.col as col from ref_with_source as r1 left join ref as r2 on r1.col = r2.col union all select u.col from `user` as u join c on u.col = c.col) select col from c")

	cte, ok := primitive.(*engine.RecurseCTE)
	require.True(t, ok, "the scatter term cannot be merged into the reference seed, got %T", primitive)
	term, ok := cte.Term.(*engine.Route)
	require.True(t, ok, "expected the recursive term to be a route, got %T", cte.Term)
	require.Equal(t, "select u.col from `user` as u where u.col = :c_col", term.Query)
}

// Refusing the merge after the routing has been reset must not leave the inputs behind changed:
// the reset rewrites a ShardedRouting in place, and the merge can be sharing that object with the
// route the plan falls back to.
func TestRefusedJoinMergeLeavesTheProbeSingleShard(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"select r1.col from ref_with_source as r1 left join ref as r2 on r1.col = r2.col join `user` as u on u.id = r1.col")

	join, ok := primitive.(*engine.Join)
	require.True(t, ok, "the reference outer join cannot be merged into the shards, got %T", primitive)
	probe, ok := join.Right.(*engine.Route)
	require.True(t, ok, "expected the probed side to be a route, got %T", join.Right)
	require.Equal(t, engine.EqualUnique, probe.Opcode,
		"the probe is routed by the value coming from the left, so it reads one shard per row, not every shard")
}

// A merged CTE carries the invariant on to whatever merges next, so the seed keeps owing its
// preserved rows once the recursion is behind it.
func TestReferenceRowsSurviveIntoAMergedCTE(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"with recursive c as (select r1.col from ref_with_source as r1 left join ref as r2 on r1.col = r2.col union all select col + 1 from c where col < 5) select col from c union all select col from `user`")

	require.IsType(t, &engine.Concatenate{}, primitive,
		"merging the CTE route into the scatter branch would return each unmatched r1 row once per shard")
}

// Only the recursive term's routing is the one rebuilt from a predicate that gets restored, so a
// seed that is single-shard on its own keeps its merge. Rejecting it does more than cost RPCs: the
// recursion counts every frontier row against its step limit, which the merged route does not.
func TestReferenceRowsAreMergedIntoACTERoutedByItsSeed(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"with recursive c as (select r.col as col from ref_with_source as r left join (select * from `user` where id = 1) as u on r.col = u.col union all select col + 1 from c where col < 5) select col from c")

	route, ok := primitive.(*engine.Route)
	require.True(t, ok, "the seed already reads one shard, so the recursion belongs on it, got %T", primitive)
	require.Equal(t, engine.EqualUnique, route.Opcode)
}

// The recursion's own predicate can make the term look single-shard, and the CTE merger restores it
// to its cross-table shape without recomputing the routing afterwards.
func TestReferenceRowsAreNotMergedIntoACTERoutedByTheRecursion(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"with recursive c as (select r1.col as col from ref_with_source as r1 left join ref as r2 on r1.col = r2.col union all select u.col from `user` as u join c on u.id = c.col) select col from c")

	require.IsType(t, &engine.RecurseCTE{}, primitive,
		"the merged route would be routed by a predicate that no longer exists once the recursion is restored")
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

// A multi-table DML whose only target is the sharded side never reads the rows the outer join
// preserves: an unmatched reference row has no row to write to, so a route that reads it once per
// shard writes nothing extra. The merge has to stay, or these statements need schema tracking to
// be planned at all - the fallback reads the rows to write them back by primary key. These two
// build the plan without it, which is what an operator without schema tracking has.
func TestMultiTableUpdateThroughAReferenceOuterJoinIsStillMerged(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"update ref r left join `user` u on r.col = u.col set u.foo = 4")

	upd, ok := primitive.(*engine.Update)
	require.True(t, ok, "the update is expected to be sent to the shards as it is, got %T", primitive)
	require.Equal(t, engine.Scatter, upd.Opcode)
	require.Equal(t, "update ref as r left join `user` as u on r.col = u.col set u.foo = 4", upd.Query)
}

func TestMultiTableDeleteThroughAReferenceOuterJoinIsStillMerged(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"delete u from ref r left join `user` u on r.col = u.col")

	del, ok := primitive.(*engine.Delete)
	require.True(t, ok, "the delete is expected to be sent to the shards as it is, got %T", primitive)
	require.Equal(t, engine.Scatter, del.Opcode)
	require.Equal(t, "delete u from ref as r left join `user` as u on r.col = u.col", del.Query)
}

// The reference table can be the target itself, and then every shard has a physical copy to delete.
func TestDeleteOfThePreservedReferenceTableIsStillMerged(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"delete r from ref r left join `user` u on r.col = u.col")

	del, ok := primitive.(*engine.Delete)
	require.True(t, ok, "the delete is expected to be sent to the shards as it is, got %T", primitive)
	require.Equal(t, engine.Scatter, del.Opcode)
}

// Both sides of the outer join can be reference tables, which routes to any single shard. That
// route is single-shard, so nothing blocks it on its own, but a later merge can still widen it.
func TestReferenceJoinedToReferenceIsNotWidenedByAUnion(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"select r1.col from ref_with_source as r1 left join ref as r2 on r1.col = r2.col union all select col from `user`")

	require.IsType(t, &engine.Concatenate{}, primitive,
		"merging the reference route into the scatter branch would return each unmatched r1 row once per shard")
}
