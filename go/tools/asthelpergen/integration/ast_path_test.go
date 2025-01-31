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

package integration

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestWalkAllPartsOfAST(t *testing.T) {
	sliceContainer := &RefSliceContainer{
		something:                 12,
		ASTElements:               []AST{},
		NotASTElements:            []int{1, 2},
		ASTImplementationElements: []*Leaf{{v: 1}, {v: 2}},
	}

	for i := range 300 {
		sliceContainer.ASTImplementationElements = append(sliceContainer.ASTImplementationElements, &Leaf{v: i})
	}

	ast := &RefContainer{
		ASTType:               sliceContainer,
		NotASTType:            2,
		ASTImplementationType: &Leaf{v: 3},
	}

	v := make(map[ASTPath]AST)

	RewriteWithPaths(ast, func(c *Cursor) bool {
		node := c.Node()
		if !reflect.TypeOf(node).Comparable() {
			return true
		}
		current := c.current
		v[current] = node
		return true
	}, nil)

	fmt.Println("walked all parts of AST")

	assert.NotEmpty(t, v)

	for path, n1 := range v {
		s := path.DebugString()
		fmt.Println(s)

		n2 := WalkASTPath(ast, path)
		assert.Equal(t, n1, n2)
	}
}
