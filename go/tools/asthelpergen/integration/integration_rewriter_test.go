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

	"github.com/stretchr/testify/assert"
)

func TestRewriteVisitRefContainer(t *testing.T) {
	leaf1 := &Leaf{1}
	leaf2 := &Leaf{2}
	container := &RefContainer{ASTType: leaf1, ASTImplementationType: leaf2}
	containerContainer := &RefContainer{ASTType: container}

	tv := &rewriteTestVisitor{}

	_ = Rewrite(containerContainer, tv.pre, tv.post)

	expected := []step{
		Pre{containerContainer},
		Pre{container},
		Pre{leaf1},
		Post{leaf1},
		Pre{leaf2},
		Post{leaf2},
		Post{container},
		Post{containerContainer},
	}
	tv.assertEquals(t, expected)
}

func TestRewriteVisitValueContainer(t *testing.T) {
	leaf1 := &Leaf{1}
	leaf2 := &Leaf{2}
	container := ValueContainer{ASTType: leaf1, ASTImplementationType: leaf2}
	containerContainer := ValueContainer{ASTType: container}

	tv := &rewriteTestVisitor{}

	_ = Rewrite(containerContainer, tv.pre, tv.post)

	expected := []step{
		Pre{containerContainer},
		Pre{container},
		Pre{leaf1},
		Post{leaf1},
		Pre{leaf2},
		Post{leaf2},
		Post{container},
		Post{containerContainer},
	}
	tv.assertEquals(t, expected)
}

func TestRewriteVisitRefSliceContainer(t *testing.T) {
	leaf1 := &Leaf{1}
	leaf2 := &Leaf{2}
	leaf3 := &Leaf{3}
	leaf4 := &Leaf{4}
	container := &RefSliceContainer{ASTElements: []AST{leaf1, leaf2}, ASTImplementationElements: []*Leaf{leaf3, leaf4}}
	containerContainer := &RefSliceContainer{ASTElements: []AST{container}}

	tv := &rewriteTestVisitor{}

	_ = Rewrite(containerContainer, tv.pre, tv.post)

	tv.assertEquals(t, []step{
		Pre{containerContainer},
		Pre{container},
		Pre{leaf1},
		Post{leaf1},
		Pre{leaf2},
		Post{leaf2},
		Pre{leaf3},
		Post{leaf3},
		Pre{leaf4},
		Post{leaf4},
		Post{container},
		Post{containerContainer},
	})
}

func TestRewriteVisitValueSliceContainer(t *testing.T) {
	leaf1 := &Leaf{1}
	leaf2 := &Leaf{2}
	leaf3 := &Leaf{3}
	leaf4 := &Leaf{4}
	container := ValueSliceContainer{ASTElements: []AST{leaf1, leaf2}, ASTImplementationElements: []*Leaf{leaf3, leaf4}}
	containerContainer := ValueSliceContainer{ASTElements: []AST{container}}

	tv := &rewriteTestVisitor{}

	_ = Rewrite(containerContainer, tv.pre, tv.post)

	tv.assertEquals(t, []step{
		Pre{containerContainer},
		Pre{container},
		Pre{leaf1},
		Post{leaf1},
		Pre{leaf2},
		Post{leaf2},
		Pre{leaf3},
		Post{leaf3},
		Pre{leaf4},
		Post{leaf4},
		Post{container},
		Post{containerContainer},
	})
}

func TestRewriteVisitInterfaceSlice(t *testing.T) {
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

	tv := &rewriteTestVisitor{}

	_ = Rewrite(ast, tv.pre, tv.post)

	tv.assertEquals(t, []step{
		Pre{ast},
		Pre{refContainer},
		Pre{astType},
		Post{astType},
		Pre{implementationType},
		Post{implementationType},
		Post{refContainer},
		Pre{leaf1},
		Post{leaf1},
		Pre{leaf2},
		Post{leaf2},
		Post{ast},
	})
}

func TestRewriteVisitRefContainerReplace(t *testing.T) {
	ast := &RefContainer{
		ASTType:               &RefContainer{NotASTType: 12},
		ASTImplementationType: &Leaf{2},
	}

	// rewrite field of type AST
	_ = Rewrite(ast, func(cursor *Cursor) bool {
		leaf, ok := cursor.node.(*RefContainer)
		if ok && leaf.NotASTType == 12 {
			cursor.Replace(&Leaf{99})
		}
		return true
	}, nil)

	assert.Equal(t, &RefContainer{
		ASTType:               &Leaf{99},
		ASTImplementationType: &Leaf{2},
	}, ast)

	_ = Rewrite(ast, rewriteLeaf(2, 55), nil)

	assert.Equal(t, &RefContainer{
		ASTType:               &Leaf{99},
		ASTImplementationType: &Leaf{55},
	}, ast)
}

func TestRewriteVisitValueContainerReplace(t *testing.T) {

	ast := ValueContainer{
		ASTType:               ValueContainer{NotASTType: 12},
		ASTImplementationType: &Leaf{2},
	}

	defer func() {
		if r := recover(); r != nil {
			require.Equal(t, "[BUG] tried to replace 'ASTType' on 'ValueContainer'", r)
		}
	}()
	_ = Rewrite(ast, func(cursor *Cursor) bool {
		leaf, ok := cursor.node.(ValueContainer)
		if ok && leaf.NotASTType == 12 {
			cursor.Replace(&Leaf{99})
		}
		return true
	}, nil)

}

func TestRewriteVisitValueContainerReplace2(t *testing.T) {
	ast := ValueContainer{
		ASTType:               ValueContainer{NotASTType: 12},
		ASTImplementationType: &Leaf{2},
	}

	defer func() {
		if r := recover(); r != nil {
			require.Equal(t, "[BUG] tried to replace 'ASTImplementationType' on 'ValueContainer'", r)
		}
	}()
	_ = Rewrite(ast, rewriteLeaf(2, 10), nil)
}

func TestRewriteVisitRefContainerPreOrPostOnly(t *testing.T) {
	leaf1 := &Leaf{1}
	leaf2 := &Leaf{2}
	container := &RefContainer{ASTType: leaf1, ASTImplementationType: leaf2}
	containerContainer := &RefContainer{ASTType: container}

	tv := &rewriteTestVisitor{}

	_ = Rewrite(containerContainer, tv.pre, nil)
	tv.assertEquals(t, []step{
		Pre{containerContainer},
		Pre{container},
		Pre{leaf1},
		Pre{leaf2},
	})

	tv = &rewriteTestVisitor{}
	_ = Rewrite(containerContainer, nil, tv.post)
	tv.assertEquals(t, []step{
		Post{leaf1},
		Post{leaf2},
		Post{container},
		Post{containerContainer},
	})
}

func rewriteLeaf(from, to int) func(*Cursor) bool {
	return func(cursor *Cursor) bool {
		leaf, ok := cursor.node.(*Leaf)
		if ok && leaf.v == from {
			cursor.Replace(&Leaf{to})
		}
		return true
	}
}

func TestRefSliceContainerReplace(t *testing.T) {
	ast := &RefSliceContainer{
		ASTElements:               []AST{&Leaf{1}, &Leaf{2}},
		ASTImplementationElements: []*Leaf{{3}, {4}},
	}

	_ = Rewrite(ast, rewriteLeaf(2, 42), nil)

	assert.Equal(t, &RefSliceContainer{
		ASTElements:               []AST{&Leaf{1}, &Leaf{42}},
		ASTImplementationElements: []*Leaf{{3}, {4}},
	}, ast)

	_ = Rewrite(ast, rewriteLeaf(3, 88), nil)

	assert.Equal(t, &RefSliceContainer{
		ASTElements:               []AST{&Leaf{1}, &Leaf{42}},
		ASTImplementationElements: []*Leaf{{88}, {4}},
	}, ast)
}

type step interface {
	String() string
}
type Pre struct {
	el AST
}

func (r Pre) String() string {
	return fmt.Sprintf("Pre(%s)", r.el.String())
}
func (r Post) String() string {
	return fmt.Sprintf("Post(%s)", r.el.String())
}

type Post struct {
	el AST
}

type rewriteTestVisitor struct {
	walk []step
}

func (tv *rewriteTestVisitor) pre(cursor *Cursor) bool {
	tv.walk = append(tv.walk, Pre{el: cursor.Node()})
	return true
}
func (tv *rewriteTestVisitor) post(cursor *Cursor) bool {
	tv.walk = append(tv.walk, Post{el: cursor.Node()})
	return true
}
func (tv *rewriteTestVisitor) assertEquals(t *testing.T, expected []step) {
	t.Helper()
	var lines []string
	error := false
	expectedSize := len(expected)
	for i, step := range tv.walk {
		if expectedSize <= i {
			t.Errorf("❌️ - Expected less elements %v", tv.walk[i:])
			break
		} else {
			e := expected[i]
			if reflect.DeepEqual(e, step) {
				a := "✔️ - " + e.String()
				if error {
					fmt.Println(a)
				} else {
					lines = append(lines, a)
				}
			} else {
				if !error {
					// first error we see.
					error = true
					for _, line := range lines {
						fmt.Println(line)
					}
				}
				t.Errorf("❌️ - Expected: %s Got: %s\n", e.String(), step.String())
			}
		}
	}
	walkSize := len(tv.walk)
	if expectedSize > walkSize {
		t.Errorf("❌️ - Expected more elements %v", expected[walkSize:])
	}

}
