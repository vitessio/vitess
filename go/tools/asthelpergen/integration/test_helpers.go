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
	"strings"
)

// ast type helpers

func sliceStringAST(els ...AST) string {
	result := make([]string, len(els))
	for i, el := range els {
		result[i] = el.String()
	}
	return strings.Join(result, ", ")
}
func sliceStringLeaf(els ...*Leaf) string {
	result := make([]string, len(els))
	for i, el := range els {
		result[i] = el.String()
	}
	return strings.Join(result, ", ")
}

// the methods below are what the generated code expected to be there in the package

// ApplyFunc is apply function
type ApplyFunc func(*Cursor) bool

// Cursor is cursor
type Cursor struct {
	parent   AST
	replacer replacerFunc
	node     AST
	// marks that the node has been replaced, and the new node should be visited
	revisit bool
}

// Node returns the current Node.
func (c *Cursor) Node() AST { return c.node }

// Parent returns the parent of the current Node.
func (c *Cursor) Parent() AST { return c.parent }

// Replace replaces the current node in the parent field with this new object. The user needs to make sure to not
// replace the object with something of the wrong type, or the visitor will panic.
func (c *Cursor) Replace(newNode AST) {
	c.replacer(newNode, c.parent)
	c.node = newNode
}

// ReplaceAndRevisit replaces the current node in the parent field with this new object.
// When used, this will abort the visitation of the current node - no post or children visited,
// and the new node visited.
func (c *Cursor) ReplaceAndRevisit(newNode AST) {
	switch newNode.(type) {
	case InterfaceSlice:
	default:
		// We need to add support to the generated code for when to look at the revisit flag. At the moment it is only
		// there for slices of AST implementations
		panic("no support added for this type yet")
	}

	c.replacer(newNode, c.parent)
	c.node = newNode
	c.revisit = true
}

type replacerFunc func(newNode, parent AST)

// Rewrite is the api.
func Rewrite(node AST, pre, post ApplyFunc) AST {
	outer := &struct{ AST }{node}

	a := &application{
		pre:  pre,
		post: post,
	}

	a.rewriteAST(outer, node, func(newNode, parent AST) {
		outer.AST = newNode
	})

	return outer.AST
}

type (
	cow struct {
		pre    func(node, parent AST) bool
		post   func(cursor *cursor)
		cloned func(old, new AST)
		cursor cursor
	}
	cursor struct {
		stop bool
	}
)

func (c *cow) postVisit(a, b AST, d bool) (AST, bool) {
	return a, d
}
