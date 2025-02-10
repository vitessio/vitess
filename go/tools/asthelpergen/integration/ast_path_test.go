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

	for i := range 20 {
		sliceContainer.ASTImplementationElements = append(sliceContainer.ASTImplementationElements, &Leaf{v: i})
	}

	ast := &RefContainer{
		ASTType:               sliceContainer,
		NotASTType:            2,
		ASTImplementationType: &Leaf{v: 3},
	}

	RewriteWithPaths(ast, func(c *Cursor) bool {
		node := c.Node()
		if !reflect.TypeOf(node).Comparable() {
			return true
		}
		fmt.Println(ASTPath(c.current.ToPath()).DebugString())
		return true
	}, nil)
}
