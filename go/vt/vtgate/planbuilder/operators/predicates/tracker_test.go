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

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestSkipWithDescendantsCascadesToChildren(t *testing.T) {
	tracker := NewTracker()
	parent := tracker.NewJoinPredicate(sqlparser.NewIntLiteral("1"))
	childA := tracker.NewChildJoinPredicate(parent, sqlparser.NewIntLiteral("2"))
	childB := tracker.NewChildJoinPredicate(parent, sqlparser.NewIntLiteral("3"))

	tracker.SkipWithDescendants(parent.ID)

	require.Nil(t, parent.Current())
	require.Nil(t, childA.Current())
	require.Nil(t, childB.Current())
}

func TestSkipWithDescendantsCascadesTransitively(t *testing.T) {
	tracker := NewTracker()
	parent := tracker.NewJoinPredicate(sqlparser.NewIntLiteral("1"))
	child := tracker.NewChildJoinPredicate(parent, sqlparser.NewIntLiteral("2"))
	grandchild := tracker.NewChildJoinPredicate(child, sqlparser.NewIntLiteral("3"))

	tracker.SkipWithDescendants(parent.ID)

	require.Nil(t, parent.Current())
	require.Nil(t, child.Current())
	require.Nil(t, grandchild.Current())
}

func TestSkipWithDescendantsLeavesUnrelatedPredicates(t *testing.T) {
	tracker := NewTracker()
	parent := tracker.NewJoinPredicate(sqlparser.NewIntLiteral("1"))
	child := tracker.NewChildJoinPredicate(parent, sqlparser.NewIntLiteral("2"))
	other := tracker.NewJoinPredicate(sqlparser.NewIntLiteral("3"))

	tracker.SkipWithDescendants(parent.ID)

	require.Nil(t, parent.Current())
	require.Nil(t, child.Current())
	require.NotNil(t, other.Current())
}

func TestDescendantIDsReturnsTransitiveClosure(t *testing.T) {
	tracker := NewTracker()
	parent := tracker.NewJoinPredicate(sqlparser.NewIntLiteral("1"))
	childA := tracker.NewChildJoinPredicate(parent, sqlparser.NewIntLiteral("2"))
	childB := tracker.NewChildJoinPredicate(parent, sqlparser.NewIntLiteral("3"))
	grandchild := tracker.NewChildJoinPredicate(childA, sqlparser.NewIntLiteral("4"))

	require.ElementsMatch(t, []ID{childA.ID, childB.ID, grandchild.ID}, tracker.DescendantIDs(parent.ID))
	require.Empty(t, tracker.DescendantIDs(childB.ID))
}
