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

func testVisitOrder(plus AST) ([]AST, []AST) {
	var preOrder, postOrder []AST

	a := &application{
		pre: func(cursor *Cursor) bool {
			preOrder = append(preOrder, cursor.node)
			return true
		},
		post: func(cursor *Cursor) bool {
			postOrder = append(postOrder, cursor.node)
			return true
		},
		cursor: Cursor{},
	}

	a.apply(nil, plus, nil)
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

	parent := &struct{ AST }{plus}

	a := &application{
		pre: func(cursor *Cursor) bool {
			switch n := cursor.node.(type) {
			case *LiteralInt:
				newNode := &LiteralInt{Val: n.Val * 2}
				cursor.replacer(newNode, cursor.parent)
			}
			return true
		},
		post:   nil,
		cursor: Cursor{},
	}

	a.apply(parent, plus, nil)

	utils.MustMatch(t, expected, parent.AST)
}
