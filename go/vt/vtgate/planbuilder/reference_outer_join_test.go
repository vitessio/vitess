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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// A reference table on the preserved side of an outer join is only merged with other reference
// tables, because a route that reads more than one shard returns each unmatched preserved row once
// per shard. These tests cover where such a route can and cannot be merged afterwards.

func planReferenceOuterJoin(t *testing.T, query string) engine.Primitive {
	t.Helper()

	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(vtenv.NewTestEnv(), vschema, TestBuilder)
	require.NoError(t, err)

	plan, err := TestBuilder(query, vw, vw.CurrentDb())
	require.NoError(t, err)
	return plan.Instructions
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

// A refused merge must leave the probed side as it was. The preserved rows have to stay on the
// preserved side of the outer join with `user`, or the merge is a safe one and nothing is refused.
func TestRefusedJoinMergeLeavesTheProbeSingleShard(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"select r1.col from ref_with_source as r1 left join ref as r2 on r1.col = r2.col left join `user` as u on u.id = r1.col")

	join, ok := primitive.(*engine.Join)
	require.True(t, ok, "the reference outer join cannot be merged into the shards, got %T", primitive)
	probe, ok := join.Right.(*engine.Route)
	require.True(t, ok, "expected the probed side to be a route, got %T", join.Right)
	require.Equal(t, engine.EqualUnique, probe.Opcode,
		"the probe is routed by the value coming from the left, so it reads one shard per row, not every shard")
}

// The recursion's own predicate can make the term look single-shard, and the CTE merger restores it
// to its cross-table shape without recomputing the routing afterwards.
func TestReferenceRowsAreNotMergedIntoACTERoutedByTheRecursion(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"with recursive c as (select r1.col as col from ref_with_source as r1 left join ref as r2 on r1.col = r2.col union all select u.col from `user` as u join c on u.id = c.col) select col from c")

	require.IsType(t, &engine.RecurseCTE{}, primitive,
		"the merged route would be routed by a predicate that no longer exists once the recursion is restored")
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

// The reference table can be the target itself. Nothing in this statement depends on the other
// side, so every shard deletes its whole copy, which is what the join asks for.
func TestDeleteOfThePreservedReferenceTableIsStillMerged(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"delete r from ref r left join `user` u on r.col = u.col")

	del, ok := primitive.(*engine.Delete)
	require.True(t, ok, "the delete is expected to be sent to the shards as it is, got %T", primitive)
	require.Equal(t, engine.Scatter, del.Opcode)
}

// The preserved rows can come from a route built before the DML target joins it, and the target is
// still what this join writes through: an unmatched reference row has no `user` row to update.
func TestMultiTableUpdateThroughANestedReferenceOuterJoinIsStillMerged(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"update ref r1 left join ref r2 on r1.col = r2.col join `user` u on u.col = r1.col set u.foo = 4")

	upd, ok := primitive.(*engine.Update)
	require.True(t, ok, "the update is expected to be sent to the shards as it is, got %T", primitive)
	require.Equal(t, engine.Scatter, upd.Opcode)
}

// A subquery's rows are consumed by its predicate instead of returned, so a wider route cannot
// duplicate them into the result: each outer row lives on one shard and asks the question there.
func TestPreservedRowsConsumedByASubqueryDoNotBlockAScatterOuter(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"select u.id from `user` u where exists (select 1 from ref r1 left join ref r2 on r1.col = r2.col where r1.col = u.col)")

	route, ok := primitive.(*engine.Route)
	require.True(t, ok, "the subquery is expected to be merged into the scatter, got %T", primitive)
	require.Equal(t, engine.Scatter, route.Opcode)
}

// Pairing the preserved rows with the rows of a sharded table pins each pairing to the shard that
// row lives on, so the route can read every shard again. Whatever the outer join kept now only
// comes back next to a `user` row, once.
func TestPreservedRowsJoinedToAShardedTableAreMergedIntoTheScatter(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"select u.id, t.col from `user` u join (select r1.col from ref r1 left join ref r2 on r1.col = r2.col) as t on u.col = t.col")

	route, ok := primitive.(*engine.Route)
	require.True(t, ok, "the reference join is expected to be merged into the scatter, got %T", primitive)
	require.Equal(t, engine.Scatter, route.Opcode)
}

func TestPreservedRowsOnTheNullExtendedSideAreMergedIntoTheScatter(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"select u.id, t.col from `user` u left join (select r1.col from ref r1 left join ref r2 on r1.col = r2.col) as t on u.col = t.col")

	route, ok := primitive.(*engine.Route)
	require.True(t, ok, "the reference join is expected to be merged into the scatter, got %T", primitive)
	require.Equal(t, engine.Scatter, route.Opcode)
}

// A DML that has to read its rows first, here because of the LIMIT, reads them through a SELECT
// and writes them back by primary key. That SELECT is a read like any other: a scatter would
// return each unmatched reference row once per shard and test `u.id is null` against one shard's
// users at a time.
func TestReferenceRowsReadByADMLWithInputAreNotMergedIntoTheScatter(t *testing.T) {
	vschema := loadSchema(t, "vschemas/schema.json", true)
	id := sqlparser.Columns{sqlparser.NewIdentifierCI("id")}
	vschema.Keyspaces["user"].Tables["ref_with_source"].PrimaryKey = id
	vschema.Keyspaces["main"].Tables["source_of_ref"].PrimaryKey = id
	vw, err := vschemawrapper.NewVschemaWrapper(vtenv.NewTestEnv(), vschema, TestBuilder)
	require.NoError(t, err)

	plan, err := TestBuilder("update ref_with_source r left join `user` u on r.col = u.col set r.foo = 1 where u.id is null limit 5", vw, vw.CurrentDb())
	require.NoError(t, err)

	dml, ok := plan.Instructions.(*engine.DMLWithInput)
	require.True(t, ok, "the rows are expected to be read before they are written, got %T", plan.Instructions)
	limit, ok := dml.Input.(*engine.Limit)
	require.True(t, ok, "the LIMIT is expected to stay above the join, got %T", dml.Input)
	filter, ok := limit.Input.(*engine.Filter)
	require.True(t, ok, "`u.id is null` is expected to be tested against every shard's users, got %T", limit.Input)
	require.IsType(t, &engine.Join{}, filter.Input,
		"merging the reference route into the scatter would read each unmatched r row once per shard")
}

// Every row of the recursive term is paired with a row of the CTE, which the seed placed on one
// shard, so what the term preserved comes back once from a sharded seed's route.
func TestReferenceRowsInARecursiveTermAreMergedIntoAShardedSeed(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"with recursive c as (select id as col from `user` union all select r.col from ref as r left join ref as r2 on r.col = r2.col join c on c.col = r.col where r.col < 5) select col from c")

	route, ok := primitive.(*engine.Route)
	require.True(t, ok, "the recursion is expected to be merged into the seed's scatter, got %T", primitive)
	require.Equal(t, engine.Scatter, route.Opcode)
}

// The rows the inner outer join preserved reach the join with `user` on its preserved side, and
// that join is the row source of an update that only writes `user`.
func TestMultiTableUpdateThroughANestedReferenceLeftJoinIsStillMerged(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"update ref r1 left join ref r2 on r1.col = r2.col left join `user` u on u.col = r1.col set u.foo = 4")

	upd, ok := primitive.(*engine.Update)
	require.True(t, ok, "the update is expected to be sent to the shards as it is, got %T", primitive)
	require.Equal(t, engine.Scatter, upd.Opcode)
}

// A delete with more than one target reads its rows through a SELECT and deletes them by primary
// key. A scatter would test `u.id is null` against one shard's users at a time and delete a
// reference row that another shard has a match for.
func TestReferenceRowsReadByAMultiTargetDeleteAreNotMergedIntoTheScatter(t *testing.T) {
	vschema := loadSchema(t, "vschemas/schema.json", true)
	id := sqlparser.Columns{sqlparser.NewIdentifierCI("id")}
	vschema.Keyspaces["user"].Tables["ref"].PrimaryKey = id
	vschema.Keyspaces["user"].Tables["user"].PrimaryKey = id
	vw, err := vschemawrapper.NewVschemaWrapper(vtenv.NewTestEnv(), vschema, TestBuilder)
	require.NoError(t, err)

	plan, err := TestBuilder("delete r, u from ref r left join `user` u on r.col = u.col where u.id is null", vw, vw.CurrentDb())
	require.NoError(t, err)

	dml, ok := plan.Instructions.(*engine.DMLWithInput)
	require.True(t, ok, "the rows are expected to be read before they are deleted, got %T", plan.Instructions)
	filter, ok := dml.Input.(*engine.Filter)
	require.True(t, ok, "`u.id is null` is expected to be tested against every shard's users, got %T", dml.Input)
	require.IsType(t, &engine.Join{}, filter.Input,
		"merging the reference route into the scatter would read each unmatched r row once per shard")
}

// A subquery that is not a top level predicate keeps the outer routing, and the merged route still
// owes the rows the outer join preserved: the later IN cannot route it to the one shard of `user`.
func TestPreservedRowsSurviveANonTopLevelSubqueryMerge(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"select r1.col from ref r1 left join ref r2 on r1.col = r2.col where (r1.col = 1 or exists (select 1 from ref r3)) and r1.col in (select u.col from `user` u where u.id = 5)")

	sq, ok := primitive.(*engine.UncorrelatedSubquery)
	require.True(t, ok, "the IN subquery is expected to stay apart from the reference route, got %T", primitive)
	outer, ok := sq.Outer.(*engine.Route)
	require.True(t, ok, "expected the outer side to be a route, got %T", sq.Outer)
	require.Equal(t, engine.Reference, outer.Opcode)
}

// A top level subquery merged into the reference route keeps the rows the outer join preserved
// owed, so the later IN cannot route the route to the one shard of `user`.
func TestPreservedRowsSurviveATopLevelSubqueryMerge(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"select r1.col from ref r1 left join ref r2 on r1.col = r2.col where r1.col in (select r3.col from ref r3) and r1.col in (select u.col from `user` u where u.id = 5)")

	sq, ok := primitive.(*engine.UncorrelatedSubquery)
	require.True(t, ok, "the IN subquery on `user` is expected to stay apart from the reference route, got %T", primitive)
	outer, ok := sq.Outer.(*engine.Route)
	require.True(t, ok, "expected the outer side to be a route, got %T", sq.Outer)
	require.Equal(t, engine.Reference, outer.Opcode)
}

// A merged CTE still owes the rows its seed preserved to whatever merges with it next.
func TestPreservedRowsSurviveIntoAMergedCTE(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"with recursive c as (select r1.col as col from ref r1 left join ref r2 on r1.col = r2.col union all select col + 1 from c where col < 5) select col from c where col in (select u.col from `user` u where u.id = 5)")

	sq, ok := primitive.(*engine.UncorrelatedSubquery)
	require.True(t, ok, "the IN subquery on `user` is expected to stay apart from the CTE route, got %T", primitive)
	outer, ok := sq.Outer.(*engine.Route)
	require.True(t, ok, "expected the outer side to be a route, got %T", sq.Outer)
	require.Equal(t, engine.Reference, outer.Opcode)
}

// An inner join with another reference table keeps the rows the outer join preserved on a reference
// route, whichever side of it they come from, so a sharded recursive term cannot widen that route.
func TestPreservedRowsSurviveAnInnerJoinWithAReferenceTable(t *testing.T) {
	for _, seed := range []string{
		"select r1.col as col from ref r1 left join ref r2 on r1.col = r2.col join ref r3 on r3.col = r1.col",
		"select r1.col as col from ref r3 join (ref r1 left join ref r2 on r1.col = r2.col) on r3.col = r1.col",
	} {
		t.Run(seed, func(t *testing.T) {
			primitive := planReferenceOuterJoin(t,
				"with recursive c as ("+seed+" union all select u.col from `user` u join c on u.col = c.col where u.col < 5) select col from c")

			require.IsType(t, &engine.RecurseCTE{}, primitive,
				"merging the term into the seed would return each unmatched r1 row once per shard")
		})
	}
}

// An UPDATE that sets a column from another table reads its rows through a SELECT and writes them
// back by primary key. A scatter would test `user.id is null` against one shard's users at a time
// and update a reference row that another shard has a match for.
func TestReferenceRowsReadByAnUpdateFromAnotherTableAreNotMergedIntoTheScatter(t *testing.T) {
	vschema := loadSchema(t, "vschemas/schema.json", true)
	id := sqlparser.Columns{sqlparser.NewIdentifierCI("id")}
	vschema.Keyspaces["user"].Tables["ref_with_source"].PrimaryKey = id
	vschema.Keyspaces["main"].Tables["source_of_ref"].PrimaryKey = id
	vw, err := vschemawrapper.NewVschemaWrapper(vtenv.NewTestEnv(), vschema, TestBuilder)
	require.NoError(t, err)

	plan, err := TestBuilder("update ref_with_source left join `user` on ref_with_source.col = `user`.col set ref_with_source.foo = ref_with_source.foo + coalesce(`user`.foo, 1) where `user`.id is null", vw, vw.CurrentDb())
	require.NoError(t, err)

	dml, ok := plan.Instructions.(*engine.DMLWithInput)
	require.True(t, ok, "the rows are expected to be read before they are written, got %T", plan.Instructions)
	proj, ok := dml.Input.(*engine.SimpleProjection)
	require.True(t, ok, "expected the input to project the columns the update needs, got %T", dml.Input)
	filter, ok := proj.Input.(*engine.Filter)
	require.True(t, ok, "`user.id is null` is expected to be tested against every shard's users, got %T", proj.Input)
	require.IsType(t, &engine.Join{}, filter.Input,
		"merging the reference route into the scatter would read each unmatched ref_with_source row once per shard")
}

// A recursive term that keeps its own preserved rows next to a reference seed is still a reference
// route, so a later join with a sharded table cannot route the CTE to a single shard.
func TestPreservedRowsOfARecursiveTermSurviveIntoAMergedCTE(t *testing.T) {
	primitive := planReferenceOuterJoin(t,
		"with recursive c as (select col from ref union all select r1.col from ref r1 left join ref r2 on r1.col = r2.col join c on c.col = r1.col where r1.col < 5) select col from c where exists (select 1 from `user` u where u.id = c.col)")

	require.IsType(t, &engine.SemiJoin{}, primitive,
		"merging the CTE into the single shard of `user` would drop the rows the term preserved")
}
