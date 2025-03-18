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

package semantics

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
)

var comp = sqlparser.Equals.Expr

const (
	A = "colA"
	B = "colB"
	C = "colC"
	D = "colD"
	X = "colX"
	Y = "colY"
)

func TestTransitiveClosures_SimpleAddAndGet(t *testing.T) {
	// Simple test to add an equality and fetch the set.
	tc := NewTransitiveClosures()

	// Basic expressions (pretend these are valid map keys).
	exprA := sqlparser.NewColName(A)
	exprB := sqlparser.NewColName(B)

	// Add an equality between exprA and exprB.
	tc.Add(exprA, exprB, comp)

	// Now fetching either expression should return a slice containing both.
	gotA := tc.Get(exprA, comp)
	gotB := tc.Get(exprB, comp)

	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprB}, gotA, "exprA's set must have A and B")
	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprB}, gotB, "exprB's set must have A and B")
}

func TestTransitiveClosures_MultipleMerges(t *testing.T) {
	// Test multiple merges of equivalence sets.
	tc := NewTransitiveClosures()

	// We'll connect colA=colB, colC=colD, colX=colY, then merge colC=colX and colB=colC
	exprA := sqlparser.NewColName(A)
	exprB := sqlparser.NewColName(B)
	exprC := sqlparser.NewColName(C)
	exprD := sqlparser.NewColName(D)
	exprX := sqlparser.NewColName(X)
	exprY := sqlparser.NewColName(Y)

	tc.Add(exprA, exprB, comp) // A==B
	tc.Add(exprC, exprD, comp) // C==D
	tc.Add(exprX, exprY, comp) // X==Y
	// Merge the first two sets
	tc.Add(exprA, exprC, comp) // A==C => A==B==C==D

	// Finally merge in the last
	tc.Add(exprC, exprX, comp) // C==X => A==B==C==D==X==Y

	// All of A,B,C,X,Y should be in the same set
	setA := tc.Get(exprA, comp)
	setC := tc.Get(exprC, comp)
	setX := tc.Get(exprX, comp)
	expected := []sqlparser.Expr{exprA, exprB, exprC, exprD, exprX, exprY}

	assert.ElementsMatch(t, expected, setA, "A's set should contain A,B,C,D,X,Y")
	assert.ElementsMatch(t, expected, setC, "C's set should contain A,B,C,D,X,Y")
	assert.ElementsMatchf(t, expected, setX, "X's set should contain A,B,C,D,X,Y: %s", sqlparser.SliceString(setX))
}

func TestTransitiveClosures_NotMapKeys(t *testing.T) {
	tc := NewTransitiveClosures()

	// ValTuple is not a valid map key
	notMapKeyExpr := sqlparser.ValTuple{
		sqlparser.NewIntLiteral("1"),
		sqlparser.NewIntLiteral("2"),
	}

	// Another expression that might not be a valid map key
	notMapKeyExpr2 := sqlparser.ValTuple{
		sqlparser.NewIntLiteral("4"),
		sqlparser.NewIntLiteral("5"),
	}

	tc.Add(notMapKeyExpr, notMapKeyExpr2, comp)

	// We expect them to be in the same equivalence set
	got1 := tc.Get(notMapKeyExpr, comp)
	got2 := tc.Get(notMapKeyExpr2, comp)

	assert.ElementsMatch(t, []sqlparser.Expr{notMapKeyExpr, notMapKeyExpr2}, got1)
	assert.ElementsMatch(t, []sqlparser.Expr{notMapKeyExpr, notMapKeyExpr2}, got2)
}

func TestTransitiveClosures_MixedMapKeyValidity(t *testing.T) {
	tc := NewTransitiveClosures()

	// exprA is valid map key
	exprA := sqlparser.NewColName(A)

	// ValTuple is not a valid map key
	exprB := sqlparser.ValTuple{
		sqlparser.NewIntLiteral("4"),
		sqlparser.NewIntLiteral("5"),
	}

	tc.Add(exprA, exprB, comp)

	// Both should be in the same set
	gotA := tc.Get(exprA, comp)
	gotF := tc.Get(exprB, comp)

	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprB}, gotA)
	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprB}, gotF)
}

func TestTransitiveClosures_DuplicateAdds(t *testing.T) {
	tc := NewTransitiveClosures()

	exprA := sqlparser.NewColName(A)
	exprB := sqlparser.NewColName(B)

	tc.Add(exprA, exprB, comp)
	tc.Add(exprA, exprB, comp) // Duplicate Add should not cause problems.

	// They should remain in the same set with no duplicates in Get()
	got := tc.Get(exprA, comp)
	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprB}, got)
}

func TestTransitiveClosures_MergeNonMapKeyExpressions(t *testing.T) {
	tc := NewTransitiveClosures()

	// This test specifically ensures setID updates properly for notMapKeys
	exprA := tuple(1, 2)
	exprB := tuple(3, 4)
	exprC := tuple(5, 6)
	exprD := tuple(5, 6)

	// Add (A == B), then unify B with C => should produce A==B==C
	tc.Add(exprA, exprB, comp)
	tc.Add(exprC, exprD, comp)
	tc.Add(exprA, exprD, comp)

	gotA := tc.Get(exprA, comp)
	gotB := tc.Get(exprB, comp)
	gotC := tc.Get(exprC, comp)
	gotD := tc.Get(exprD, comp)

	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprB, exprC, exprD}, gotA)
	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprB, exprC, exprD}, gotB)
	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprB, exprC, exprD}, gotC)
	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprB, exprC, exprD}, gotD)
}

func TestTransitiveClosures_StructuralMatchInMapKeys(t *testing.T) {
	tc := NewTransitiveClosures()

	// Two colNames with the same name but different pointer references
	exprPtr1 := sqlparser.NewColName("same")
	exprPtr2 := sqlparser.NewColName("same")

	// Even though the AST pointers differ, structurally they are the same colName
	// If ValidAsMapKey(expr) => pointer-based map usage might skip exprPtr2.
	// The code attempts to do a structural match fallback in `Get()`.
	tc.Add(exprPtr1, exprPtr2, comp)

	got1 := tc.Get(exprPtr1, comp)
	got2 := tc.Get(exprPtr2, comp)

	// Both sets should contain the same references, though how you unify them
	// depends on your map fallback logic. Minimally, we expect a single equivalence set.
	assert.ElementsMatch(t, got1, got2, "Structurally identical expressions should unify")
	assert.Len(t, got1, 2, "Should have exactly two expressions in the set")
}

func tuple(i ...int) sqlparser.Expr {
	elements := slice.Map(i, func(i int) sqlparser.Expr {
		return sqlparser.NewIntLiteral(strconv.Itoa(i))
	})
	return sqlparser.ValTuple(elements)
}
