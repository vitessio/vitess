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
	gotA := get(tc, exprA)
	gotB := get(tc, exprB)

	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprA, exprB}, gotA, "exprA's set must have A and B")
	assert.ElementsMatch(t, []sqlparser.Expr{exprB, exprA, exprB}, gotB, "exprB's set must have A and B")
}

func TestTransitiveClosures_NotMergingUnrelatedEqualities(t *testing.T) {
	// Test that unrelated equalities are not merged together
	tc := NewTransitiveClosures()

	// We add X and Y, which will be the first ID
	exprX := sqlparser.NewColName(X)
	exprY := sqlparser.NewColName(Y)
	tc.Add(exprX, exprY, comp) // X==Y

	// Then we add A, B, C which are all equal and should have the second ID
	exprA := sqlparser.NewColName(A)
	exprB := sqlparser.NewColName(B)
	exprC := sqlparser.NewColName(C)
	tc.Add(exprA, exprB, comp) // A==B
	tc.Add(exprB, exprC, comp) // B==C => A==B==C

	// ID: 0 => X==Y
	// ID: 1 => A==B==C

	gotX := get(tc, exprX)
	gotY := get(tc, exprY)
	assert.ElementsMatch(t, []sqlparser.Expr{exprX, exprX, exprY}, gotX, "exprX's set must have X and Y")
	assert.ElementsMatch(t, []sqlparser.Expr{exprY, exprX, exprY}, gotY, "exprX's set must have X and Y")

	gotA := get(tc, exprA)
	gotB := get(tc, exprB)
	gotC := get(tc, exprC)
	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprA, exprB, exprC}, gotA, "exprA's set must have A and B and C")
	assert.ElementsMatch(t, []sqlparser.Expr{exprB, exprA, exprB, exprC}, gotB, "exprB's set must have A and B and C")
	assert.ElementsMatch(t, []sqlparser.Expr{exprC, exprA, exprB, exprC}, gotC, "exprC's set must have A and B and C")
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
	setA := get(tc, exprA)
	setC := get(tc, exprC)
	setX := get(tc, exprX)
	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprA, exprB, exprC, exprD, exprX, exprY}, setA, "A's set should contain A,B,C,D,X,Y")
	assert.ElementsMatch(t, []sqlparser.Expr{exprC, exprA, exprB, exprC, exprD, exprX, exprY}, setC, "C's set should contain A,B,C,D,X,Y")
	assert.ElementsMatchf(t, []sqlparser.Expr{exprX, exprA, exprB, exprC, exprD, exprX, exprY}, setX, "X's set should contain A,B,C,D,X,Y: %s", sqlparser.SliceString(setX))
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
	got1 := get(tc, notMapKeyExpr)
	got2 := get(tc, notMapKeyExpr2)

	assert.ElementsMatch(t, []sqlparser.Expr{notMapKeyExpr, notMapKeyExpr, notMapKeyExpr2}, got1)
	assert.ElementsMatch(t, []sqlparser.Expr{notMapKeyExpr2, notMapKeyExpr, notMapKeyExpr2}, got2)
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
	gotA := get(tc, exprA)
	gotB := get(tc, exprB)

	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprA, exprB}, gotA)
	assert.ElementsMatch(t, []sqlparser.Expr{exprB, exprA, exprB}, gotB)
}

func TestTransitiveClosures_DuplicateAdds(t *testing.T) {
	tc := NewTransitiveClosures()

	exprA := sqlparser.NewColName(A)
	exprB := sqlparser.NewColName(B)

	tc.Add(exprA, exprB, comp)
	tc.Add(exprA, exprB, comp) // Duplicate Add should not cause problems.

	// They should remain in the same set with no duplicates in Get()
	got := get(tc, exprA)
	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprA, exprB}, got)
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

	gotA := get(tc, exprA)
	gotB := get(tc, exprB)
	gotC := get(tc, exprC)
	gotD := get(tc, exprD)

	assert.ElementsMatch(t, []sqlparser.Expr{exprA, exprA, exprB, exprC, exprD}, gotA)
	assert.ElementsMatch(t, []sqlparser.Expr{exprB, exprA, exprB, exprC, exprD}, gotB)
	assert.ElementsMatch(t, []sqlparser.Expr{exprC, exprA, exprB, exprC, exprD}, gotC)
	assert.ElementsMatch(t, []sqlparser.Expr{exprD, exprA, exprB, exprC, exprD}, gotD)
}

func get(tc *TransitiveClosures, expr sqlparser.Expr) (res []sqlparser.Expr) {
	_ = tc.Foreach(expr, comp, func(expr sqlparser.Expr) error {
		res = append(res, expr)
		return nil
	})
	return
}

func tuple(i ...int) sqlparser.Expr {
	elements := slice.Map(i, func(i int) sqlparser.Expr {
		return sqlparser.NewIntLiteral(strconv.Itoa(i))
	})
	return sqlparser.ValTuple(elements)
}
