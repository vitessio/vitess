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
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// TestPlanCachedSizeExcludesSharedObjects verifies that the size of a plan, which is
// what the plan cache weighs it by, does not include the process-wide collation
// environment. Plans only reference that object, so counting it makes plans that
// reference it several times (such as an ordered aggregate) look orders of magnitude
// larger than they are, and evicts plans from the cache that would have fit.
func TestPlanCachedSizeExcludesSharedObjects(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)

	// The ordered aggregate holds the collation environment in its aggregate
	// params and in its grouping columns.
	plan, err := TestBuilder("select col, count(*) from user group by col order by col", vw, vw.CurrentDb())
	require.NoError(t, err)

	// The collation environment alone is ~58 KB and was counted three times for
	// this plan. The plan itself is a few KB.
	const collationEnvSize = 50_000
	size := plan.CachedSize(true)
	assert.Less(t, size, int64(collationEnvSize), "plan size must not include the shared collation environment")
}

// TestRoutePlanDoesNotRetainAST verifies that a route keeps only the query strings
// it needs to execute, and not the AST they were rendered from, which is about
// as large as the route itself. For warming reads, the query without its lock
// clause is rendered when the plan is built.
func TestRoutePlanDoesNotRetainAST(t *testing.T) {
	env := vtenv.NewTestEnv()
	vschema := loadSchema(t, "vschemas/schema.json", true)
	vw, err := vschemawrapper.NewVschemaWrapper(env, vschema, TestBuilder)
	require.NoError(t, err)

	plan, err := TestBuilder("select id from user where id = 5", vw, vw.CurrentDb())
	require.NoError(t, err)
	route, ok := plan.Instructions.(*engine.Route)
	require.True(t, ok, "expected a Route, got %T", plan.Instructions)
	assert.Empty(t, route.WarmingReadQuery)
	// A single route with an equality predicate was 1504 bytes when it kept its AST,
	// and is 832 bytes without it.
	assert.Less(t, plan.CachedSize(true), int64(1200))

	plan, err = TestBuilder("select id from user where id = 5 for update", vw, vw.CurrentDb())
	require.NoError(t, err)
	route, ok = plan.Instructions.(*engine.Route)
	require.True(t, ok, "expected a Route, got %T", plan.Instructions)
	assert.Equal(t, "select id from `user` where id = 5", route.WarmingReadQuery)
	assert.Equal(t, "select id from `user` where id = 5 for update", route.Query)
}
