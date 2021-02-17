/*
Copyright 2021 The Vitess Authors.

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

package integration_test

import (
	"reflect"
	"testing"

	"vitess.io/vitess/go/test/utils"
	"vitess.io/vitess/go/tools/asthelpergen/integration"

	"github.com/stretchr/testify/assert"
)

func TestVisit(t *testing.T) {
	one := &integration.LiteralInt{1}
	minusOne := &integration.UnaryMinus{Val: one}
	foo := &integration.LiteralString{"foo"}
	plus := &integration.Plus{Left: minusOne, Right: foo}

	preOrder, postOrder := testVisitOrder(plus)

	assert.Equal(t, []integration.AST{plus, minusOne, one, foo}, preOrder, "pre-order wrong")
	assert.Equal(t, []integration.AST{one, minusOne, foo, plus}, postOrder, "post-order wrong")
}

func TestVisitWSlice(t *testing.T) {
	int1 := &integration.LiteralInt{1}
	int2 := &integration.LiteralInt{2}
	slice := &integration.Array{
		Values: []integration.AST{int1, int2},
		Stuff:  []int{1, 2, 3},
	}
	foo := &integration.LiteralString{"foo"}
	plus := &integration.Plus{Left: slice, Right: foo}

	preOrder, postOrder := testVisitOrder(plus)

	assert.Equal(t, []integration.AST{plus, slice, int1, int2, foo}, preOrder, "pre-order wrong")
	assert.Equal(t, []integration.AST{int1, int2, slice, foo, plus}, postOrder, "post-order wrong")
}

func testVisitOrder(plus integration.AST) ([]integration.AST, []integration.AST) {
	var preOrder, postOrder []integration.AST

	integration.Rewrite(plus,
		func(cursor *integration.Cursor) bool {
			preOrder = append(preOrder, cursor.Node())
			return true
		},
		func(cursor *integration.Cursor) bool {
			postOrder = append(postOrder, cursor.Node())
			return true
		})

	return preOrder, postOrder
}

func TestDeepEqualsWorksForAST(t *testing.T) {
	one := &integration.LiteralInt{1}
	two := &integration.LiteralInt{2}
	plus := &integration.Plus{Left: one, Right: two}
	oneB := &integration.LiteralInt{1}
	twoB := &integration.LiteralInt{2}
	plusB := &integration.Plus{Left: oneB, Right: twoB}

	if !reflect.DeepEqual(plus, plusB) {
		t.Fatalf("oh noes")
	}
}

func TestReplace(t *testing.T) {
	one := &integration.LiteralInt{1}
	two := &integration.LiteralInt{2}
	plus := &integration.Plus{Left: one, Right: two}
	four := &integration.LiteralInt{4}
	expected := &integration.Plus{Left: two, Right: four}

	result := integration.Rewrite(plus, func(cursor *integration.Cursor) bool {
		switch n := cursor.Node().(type) {
		case *integration.LiteralInt:
			newNode := &integration.LiteralInt{Val: n.Val * 2}
			cursor.Replace(newNode)
		}
		return true
	}, nil)

	utils.MustMatch(t, expected, result)
}

func TestReplaceInSlice(t *testing.T) {
	one := &integration.LiteralInt{1}
	two := &integration.LiteralInt{2}
	three := &integration.LiteralInt{3}
	array := &integration.Array{Values: []integration.AST{one, two, three}}
	string2 := &integration.LiteralString{"two"}

	result := integration.Rewrite(array, func(cursor *integration.Cursor) bool {
		switch n := cursor.Node().(type) {
		case *integration.LiteralInt:
			if n.Val == 2 {
				cursor.Replace(string2)
			}
		}
		return true
	}, nil)

	expected := &integration.Array{Values: []integration.AST{one, string2, three}}
	utils.MustMatch(t, expected, result)
}

func TestReplaceValue(t *testing.T) {
	plus := &integration.Plus{
		Left:  &integration.StructHolder{&integration.LiteralInt{1}},
		Right: &integration.LiteralInt{2},
	}

	result := integration.Rewrite(plus, func(cursor *integration.Cursor) bool {
		switch n := cursor.Node().(type) {
		case *integration.LiteralInt:
			if n.Val == 1 {
				t.Logf("reached %T", n)
				cursor.Replace(&integration.LiteralInt{3})
			}
		}
		return true
	}, nil)

	expected := &integration.Plus{
		Left:  &integration.StructHolder{&integration.LiteralInt{3}},
		Right: &integration.LiteralInt{2},
	}

	utils.MustMatch(t, expected, result)
}
