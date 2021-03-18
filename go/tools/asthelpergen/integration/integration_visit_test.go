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
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type testVisitor struct {
	seen []AST
}

func (tv *testVisitor) visit(node AST) (bool, error) {
	tv.seen = append(tv.seen, node)
	return true, nil
}

func TestVisitRefContainer(t *testing.T) {
	leaf1 := &Leaf{1}
	leaf2 := &Leaf{2}
	container := &RefContainer{ASTType: leaf1, ASTImplementationType: leaf2}
	containerContainer := &RefContainer{ASTType: container}

	tv := &testVisitor{}

	require.NoError(t,
		VisitAST(containerContainer, tv.visit))

	tv.assertVisitOrder(t, []AST{
		containerContainer,
		container,
		leaf1,
		leaf2,
	})
}

func TestVisitValueContainer(t *testing.T) {
	leaf1 := &Leaf{1}
	leaf2 := &Leaf{2}
	container := ValueContainer{ASTType: leaf1, ASTImplementationType: leaf2}
	containerContainer := ValueContainer{ASTType: container}

	tv := &testVisitor{}

	require.NoError(t,
		VisitAST(containerContainer, tv.visit))

	expected := []AST{
		containerContainer,
		container,
		leaf1,
		leaf2,
	}
	tv.assertVisitOrder(t, expected)
}

func TestVisitRefSliceContainer(t *testing.T) {
	leaf1 := &Leaf{1}
	leaf2 := &Leaf{2}
	leaf3 := &Leaf{3}
	leaf4 := &Leaf{4}
	container := &RefSliceContainer{ASTElements: []AST{leaf1, leaf2}, ASTImplementationElements: []*Leaf{leaf3, leaf4}}
	containerContainer := &RefSliceContainer{ASTElements: []AST{container}}

	tv := &testVisitor{}

	require.NoError(t,
		VisitAST(containerContainer, tv.visit))

	tv.assertVisitOrder(t, []AST{
		containerContainer,
		container,
		leaf1,
		leaf2,
		leaf3,
		leaf4,
	})
}

func TestVisitValueSliceContainer(t *testing.T) {
	leaf1 := &Leaf{1}
	leaf2 := &Leaf{2}
	leaf3 := &Leaf{3}
	leaf4 := &Leaf{4}
	container := ValueSliceContainer{ASTElements: []AST{leaf1, leaf2}, ASTImplementationElements: []*Leaf{leaf3, leaf4}}
	containerContainer := ValueSliceContainer{ASTElements: []AST{container}}

	tv := &testVisitor{}

	require.NoError(t,
		VisitAST(containerContainer, tv.visit))

	tv.assertVisitOrder(t, []AST{
		containerContainer,
		container,
		leaf1,
		leaf2,
		leaf3,
		leaf4,
	})
}

func TestVisitInterfaceSlice(t *testing.T) {
	leaf1 := &Leaf{2}
	astType := &RefContainer{NotASTType: 12}
	implementationType := &Leaf{2}

	leaf2 := &Leaf{3}
	refContainer := &RefContainer{
		ASTType:               astType,
		ASTImplementationType: implementationType,
	}
	ast := InterfaceSlice{
		refContainer,
		leaf1,
		leaf2,
	}

	tv := &testVisitor{}

	require.NoError(t,
		VisitAST(ast, tv.visit))

	tv.assertVisitOrder(t, []AST{
		ast,
		refContainer,
		astType,
		implementationType,
		leaf1,
		leaf2,
	})
}

func (tv *testVisitor) assertVisitOrder(t *testing.T, expected []AST) {
	t.Helper()
	var lines []string
	failed := false
	expectedSize := len(expected)
	for i, step := range tv.seen {
		if expectedSize <= i {
			t.Errorf("❌️ - Expected less elements %v", tv.seen[i:])
			break
		} else {
			e := expected[i]
			if reflect.DeepEqual(e, step) {
				a := "✔️ - " + e.String()
				if failed {
					fmt.Println(a)
				} else {
					lines = append(lines, a)
				}
			} else {
				if !failed {
					// first error we see.
					failed = true
					for _, line := range lines {
						fmt.Println(line)
					}
				}
				t.Errorf("❌️ - Expected: %s Got: %s\n", e.String(), step.String())
			}
		}
	}
	walkSize := len(tv.seen)
	if expectedSize > walkSize {
		t.Errorf("❌️ - Expected more elements %v", expected[walkSize:])
	}
}
