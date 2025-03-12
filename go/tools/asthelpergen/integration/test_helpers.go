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
	"encoding/binary"
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser/pathbuilder"
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
	current *pathbuilder.ASTPathBuilder
}

// Node returns the current Node.
func (c *Cursor) Node() AST { return c.node }

// Path returns the current path of the Node.
func (c *Cursor) Path() ASTPath { return ASTPath(c.current.ToPath()) }

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

func RewriteWithPaths(node AST, pre, post ApplyFunc) AST {
	outer := &struct{ AST }{node}

	a := &application{
		pre:          pre,
		post:         post,
		collectPaths: true,
	}
	a.cur.current = pathbuilder.NewASTPathBuilder()

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
		stop           bool
		node, replaced AST
	}
)

func (c *cow) postVisit(a, b AST, d bool) (AST, bool) {
	c.cursor.node = a
	c.cursor.replaced = nil
	c.post(&c.cursor)
	if c.cursor.replaced != nil {
		return c.cursor.replaced, true
	}

	return a, d
}

func (path ASTPath) DebugString() string {
	var sb strings.Builder

	remaining := []byte(path)
	stepCount := 0

	for len(remaining) >= 2 {
		// Read the step code (2 bytes)
		stepVal := binary.BigEndian.Uint16(remaining[:2])
		remaining = remaining[2:]

		step := ASTStep(stepVal)
		stepStr := step.DebugString() // e.g. "CaseExprWhens8" or "CaseExprWhens32"

		// If this isn't the very first step in the path, prepend a separator
		if stepCount > 0 {
			sb.WriteString("->")
		}
		stepCount++

		// Write the step name
		sb.WriteString(stepStr)

		// Check suffix to see if we need to read an offset
		switch {
		case strings.HasSuffix(stepStr, "Offset"):
			if len(remaining) < 1 {
				sb.WriteString("(ERR-no-offset-byte)")
				return sb.String()
			}
			offset, readBytes := binary.Varint(remaining)
			remaining = remaining[readBytes:]
			sb.WriteString(fmt.Sprintf("(%d)", offset))
		}
	}

	// If there's leftover data that doesn't fit into 2 (or more) bytes, you could note it:
	if len(remaining) != 0 {
		sb.WriteString("->(ERR-unaligned-extra-bytes)")
	}

	return sb.String()
}

func CopyOnRewrite(
	node AST,
	pre func(node, parent AST) bool,
	post func(cursor *cursor),
	cloned func(before, after AST),
) AST {
	cow := cow{pre: pre, post: post, cursor: cursor{}, cloned: cloned}
	out, _ := cow.copyOnRewriteAST(node, nil)
	return out
}
