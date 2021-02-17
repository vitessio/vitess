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

package integration

import (
	"reflect"
	"testing"

	"vitess.io/vitess/go/test/utils"

	"github.com/stretchr/testify/assert"
)

func TestVisit(t *testing.T) {
	one := &LiteralInt{1}
	minusOne := &UnaryMinus{Val: one}
	foo := LiteralString{"foo"}
	plus := &Plus{Left: minusOne, Right: foo}

	preOrder, postOrder := testVisitOrder(plus)

	assert.Equal(t, []AST{plus, minusOne, one, foo}, preOrder, "pre-order wrong")
	assert.Equal(t, []AST{one, minusOne, foo, plus}, postOrder, "post-order wrong")
}

func TestVisitWSlice(t *testing.T) {
	int1 := &LiteralInt{1}
	int2 := &LiteralInt{2}
	slice := &Array{
		Values: []AST{int1, int2},
		Stuff:  []int{1, 2, 3},
	}
	foo := LiteralString{"foo"}
	plus := &Plus{Left: slice, Right: foo}

	preOrder, postOrder := testVisitOrder(plus)

	assert.Equal(t, []AST{plus, slice, int1, int2, foo}, preOrder, "pre-order wrong")
	assert.Equal(t, []AST{int1, int2, slice, foo, plus}, postOrder, "post-order wrong")
}

func testVisitOrder(plus AST) ([]AST, []AST) {
	var preOrder, postOrder []AST

	Rewrite(plus,
		func(cursor *Cursor) bool {
			preOrder = append(preOrder, cursor.Node())
			return true
		},
		func(cursor *Cursor) bool {
			postOrder = append(postOrder, cursor.Node())
			return true
		})

	return preOrder, postOrder
}

func TestDeepEqualsWorksForAST(t *testing.T) {
	one := &LiteralInt{1}
	two := &LiteralInt{2}
	plus := &Plus{Left: one, Right: two}
	oneB := &LiteralInt{1}
	twoB := &LiteralInt{2}
	plusB := &Plus{Left: oneB, Right: twoB}

	if !reflect.DeepEqual(plus, plusB) {
		t.Fatalf("oh noes")
	}
}

func TestReplace(t *testing.T) {
	one := &LiteralInt{1}
	two := &LiteralInt{2}
	plus := &Plus{Left: one, Right: two}
	four := &LiteralInt{4}
	expected := &Plus{Left: two, Right: four}

	result := Rewrite(plus, func(cursor *Cursor) bool {
		switch n := cursor.Node().(type) {
		case *LiteralInt:
			newNode := &LiteralInt{Val: n.Val * 2}
			cursor.Replace(newNode)
		}
		return true
	}, nil)

	utils.MustMatch(t, expected, result)
}
func TestReplaceInSlice(t *testing.T) {
	one := &LiteralInt{1}
	two := &LiteralInt{2}
	three := &LiteralInt{3}
	array := &Array{Values: []AST{one, two, three}}
	string2 := &LiteralString{"two"}

	result := Rewrite(array, func(cursor *Cursor) bool {
		switch n := cursor.Node().(type) {
		case *LiteralInt:
			if n.Val == 2 {
				cursor.Replace(string2)
			}
		}
		return true
	}, nil)

	expected := &Array{Values: []AST{one, string2, three}}
	utils.MustMatch(t, expected, result)
}

func TestReplaceStructHolder(t *testing.T) {
	one := &LiteralInt{1}
	root := StructHolder{one}

	defer func() {
		if r := recover(); r != nil {
			// we expect the rewriter to panic
			assert.Equal(t, "StructHolder Val", r)
		}
	}()

	Rewrite(root, func(cursor *Cursor) bool {
		switch cursor.Node().(type) {
		case *LiteralInt:
			// here we are trying to change the value of one of the fields of the value, and that is not allowed
			cursor.Replace(nil)
		}
		return true
	}, nil)
	t.Fatal("Should have panicked")
}
