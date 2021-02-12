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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestVisit(t *testing.T) {
	one := &LiteralInt{1}
	two := &LiteralInt{1}
	plus := &Plus{Left: one, Right: two}

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

	// visit
	a.apply(nil, plus, nil)

	assert.Equal(t, []AST{plus, one, two}, preOrder, "pre-order wrong")
	assert.Equal(t, []AST{one, two, plus}, postOrder, "post-order wrong")

}
