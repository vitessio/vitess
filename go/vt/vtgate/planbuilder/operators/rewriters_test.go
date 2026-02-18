/*
Copyright 2025 The Vitess Authors.

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

package operators

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// TestTopDown_RevisitsReplacedOperators verifies that when an operator is replaced
// during TopDown traversal, the new operator gets re-visited.
//
// This test demonstrates the bug fix where TopDown now re-visits operators that are returned
// as replacements. Without the fix, if a visitor returns a different operator, that new
// operator would be inserted into the tree but never visited itself.
//
// Test scenario:
//  1. Create a simple operator
//  2. First visit returns a different operator (simulating what happens in offset planning)
//  3. Verify that the replacement operator is also visited
//
// Without the fix, the second operator would never be visited because TopDown would just
// replace the first with the second and descend into the second's inputs without visiting
// the second itself.
func TestTopDown_RevisitsReplacedOperators(t *testing.T) {
	visited := map[semantics.TableSet]bool{}

	// Create two simple operators to track visits
	id0 := semantics.SingleTableSet(0)
	id1 := semantics.SingleTableSet(1)
	op1 := &fakeOp{id: id0}
	op2 := &fakeOp{id: id1}

	// Create visitor that replaces op1 with op2 and marks each operator as visited

	visitor := func(in Operator, _ semantics.TableSet, _ bool) (Operator, *ApplyResult) {
		visited[TableID(in)] = true
		if in == op1 {
			return op2, Rewrote("replaced operator")
		}
		return in, NoRewrite
	}

	// Run TopDown traversal
	result := TopDown(op1, func(op Operator) semantics.TableSet {
		return semantics.EmptyTableSet()
	}, visitor, func(Operator) VisitRule {
		return VisitChildren
	})

	// Verify both operators were visited
	assert.True(t, visited[id0], "first operator should have been visited")
	assert.True(t, visited[id1], "second operator (replacement) should have been visited")
	assert.Equal(t, op2, result, "result should be the second operator")
}
