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

// TestReferencePreservedByOuterJoinRunsEvenWithoutADestination covers the merge that keeps a
// reference table on the preserved side of an outer join in a single route: the routing has to
// be single-shard for the merge to happen, but a single-shard opcode can still resolve to no
// shard at all, and then the merged query would run nowhere and drop the preserved rows.
func TestReferencePreservedByOuterJoinRunsEvenWithoutADestination(t *testing.T) {
	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(vtenv.NewTestEnv(), vschema, TestBuilder)
	require.NoError(t, err)

	plan, err := TestBuilder(
		"select ref_with_source.col from ref_with_source left join (select * from `user` where id = :id) as u on ref_with_source.col = u.col",
		vw, vw.CurrentDb())
	require.NoError(t, err)

	route, ok := plan.Instructions.(*engine.Route)
	require.True(t, ok, "the join is expected to be merged into a single route, got %T", plan.Instructions)
	require.Equal(t, engine.EqualUnique, route.Opcode)
	require.True(t, route.NoRoutesSpecialHandling,
		"a route that has to produce the preserved reference rows cannot return an empty result when the routing finds no destination")
}
