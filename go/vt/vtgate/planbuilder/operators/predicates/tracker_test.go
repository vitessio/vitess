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

package predicates

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func parseExpr(t *testing.T, in string) sqlparser.Expr {
	t.Helper()
	expr, err := sqlparser.NewTestParser().ParseExpr(in)
	require.NoError(t, err)
	return expr
}

// TestSkipCascadesToDerivedPredicates covers the shape a UNION produces: one predicate split
// into a copy per branch. Once the outer query takes ownership of the original, none of the
// copies may render, or the query is left referencing join bind variables nothing binds.
func TestSkipCascadesToDerivedPredicates(t *testing.T) {
	tracker := NewTracker()
	original := tracker.NewJoinPredicate(parseExpr(t, ":user_id = sub.c"))
	first := tracker.NewDerivedJoinPredicate(original.ID, parseExpr(t, ":user_id = 1"))
	second := tracker.NewDerivedJoinPredicate(original.ID, parseExpr(t, ":user_id = 2"))

	tracker.Skip(original.ID)

	assert.Nil(t, original.Current())
	assert.Nil(t, first.Current())
	assert.Nil(t, second.Current())
}

// TestSkipCascadesThroughNestedDerivedPredicates covers a UNION nested inside another UNION,
// which splits an already-split predicate again. The copies then form a chain rather than a
// flat list, so skipping has to follow it all the way down.
func TestSkipCascadesThroughNestedDerivedPredicates(t *testing.T) {
	tracker := NewTracker()
	original := tracker.NewJoinPredicate(parseExpr(t, ":user_id = sub.c"))
	branch := tracker.NewDerivedJoinPredicate(original.ID, parseExpr(t, ":user_id = i1.c"))
	leaf := tracker.NewDerivedJoinPredicate(branch.ID, parseExpr(t, ":user_id = 1"))

	tracker.Skip(original.ID)

	assert.Nil(t, original.Current())
	assert.Nil(t, branch.Current())
	assert.Nil(t, leaf.Current())
}

// TestSkipDerivedPredicateDoesNotAffectItsOrigin checks that the cascade only runs downwards.
// Skipping one branch's copy says nothing about the original or about the sibling branches.
func TestSkipDerivedPredicateDoesNotAffectItsOrigin(t *testing.T) {
	tracker := NewTracker()
	original := tracker.NewJoinPredicate(parseExpr(t, ":user_id = sub.c"))
	first := tracker.NewDerivedJoinPredicate(original.ID, parseExpr(t, ":user_id = 1"))
	second := tracker.NewDerivedJoinPredicate(original.ID, parseExpr(t, ":user_id = 2"))

	tracker.Skip(first.ID)

	assert.Nil(t, first.Current())
	assert.Equal(t, ":user_id = sub.c", sqlparser.String(original.Current()))
	assert.Equal(t, ":user_id = 2", sqlparser.String(second.Current()))
}

// TestSetDoesNotCascadeToDerivedPredicates pins a deliberate asymmetry with Skip. Set restores
// the original, outer-scope shape of a predicate - `user.id = sub.c` - which references a column
// that is not in scope inside a UNION branch. Each copy has been rewritten to the expressions of
// the branch it was pushed into, and has to keep that rewrite.
func TestSetDoesNotCascadeToDerivedPredicates(t *testing.T) {
	tracker := NewTracker()
	original := tracker.NewJoinPredicate(parseExpr(t, ":user_id = sub.c"))
	branch := tracker.NewDerivedJoinPredicate(original.ID, parseExpr(t, ":user_id = 1"))

	tracker.Set(original.ID, parseExpr(t, "user.id = sub.c"))

	assert.Equal(t, "`user`.id = sub.c", sqlparser.String(original.Current()))
	assert.Equal(t, ":user_id = 1", sqlparser.String(branch.Current()))
}
