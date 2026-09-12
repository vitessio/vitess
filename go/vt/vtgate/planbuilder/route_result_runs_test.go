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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/test/vschemawrapper"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// TestRouteShardResultIsSorted proves only routes whose shard query carries the
// required ordering are marked for result-run merging.
func TestRouteShardResultIsSorted(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)

	orderedPlan, err := TestBuilder("select id from user order by id", vw, vw.CurrentDb())
	require.NoError(t, err)
	orderedRoute := findRoute(t, orderedPlan.Instructions)
	require.True(t, orderedRoute.ShardResultIsSorted)
	require.NotEmpty(t, orderedRoute.OrderBy)
	require.Contains(t, strings.ToLower(orderedRoute.Query), "order by")
	statement, ok := orderedRoute.QueryStatement.(sqlparser.SelectStatement)
	require.True(t, ok)
	require.Len(t, statement.GetOrderBy(), len(orderedRoute.OrderBy))

	unorderedPlan, err := TestBuilder("select id from user", vw, vw.CurrentDb())
	require.NoError(t, err)
	unorderedRoute := findRoute(t, unorderedPlan.Instructions)
	require.False(t, unorderedRoute.ShardResultIsSorted)

	randomPlan, err := TestBuilder("select id from user order by rand(), id", vw, vw.CurrentDb())
	require.NoError(t, err)
	randomRoute := findRoute(t, randomPlan.Instructions)
	require.NotEmpty(t, randomRoute.OrderBy)
	require.False(t, randomRoute.ShardResultIsSorted)
	randomStatement, ok := randomRoute.QueryStatement.(sqlparser.SelectStatement)
	require.True(t, ok)
	require.Greater(t, len(randomStatement.GetOrderBy()), len(randomRoute.OrderBy))
}

func findRoute(t *testing.T, primitive engine.Primitive) *engine.Route {
	t.Helper()
	found := engine.Find(func(primitive engine.Primitive) bool {
		_, ok := primitive.(*engine.Route)
		return ok
	}, primitive)
	route, ok := found.(*engine.Route)
	require.True(t, ok)
	return route
}
