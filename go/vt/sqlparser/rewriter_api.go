/*
Copyright 2019 The Vitess Authors.

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

package sqlparser

import "vitess.io/vitess/go/vt/sqlparser/pathbuilder"

// Rewrite traverses a syntax tree recursively, starting with root,
// and calling pre and post for each node as described below.
// Rewrite returns the syntax tree, possibly modified.
//
// If pre is not nil, it is called for each node before the node's
// children are traversed (pre-order). If pre returns false, no
// children are traversed, and post is not called for that node.
//
// If post is not nil, and a prior call of pre didn't return false,
// post is called for each node after its children are traversed
// (post-order). If post returns false, traversal is terminated and
// Apply returns immediately.
//
// Only fields that refer to AST nodes are considered children;
// i.e., fields of basic types (strings, []byte, etc.) are ignored.
func Rewrite(node SQLNode, pre, post ApplyFunc) (result SQLNode) {
	return rewriteNode(node, pre, post, false)
}

func rewriteNode(node SQLNode, pre ApplyFunc, post ApplyFunc, collectPaths bool) SQLNode {
	parent := &RootNode{node}

	// this is the root-replacer, used when the user replaces the root of the ast
	replacer := func(newNode SQLNode, _ SQLNode) {
		parent.SQLNode = newNode
	}

	a := &application{
		pre:          pre,
		post:         post,
		collectPaths: collectPaths,
	}

	if collectPaths {
		a.cur.current = pathbuilder.NewASTPathBuilder()
	}

	a.rewriteSQLNode(parent, node, replacer)

	return parent.SQLNode
}

func RewriteWithPath(node SQLNode, pre, post ApplyFunc) (result SQLNode) {
	return rewriteNode(node, pre, post, true)
}

// SafeRewrite does not allow replacing nodes on the down walk of the tree walking
// Long term this is the only Rewrite functionality we want
func SafeRewrite(
	node SQLNode,
	shouldVisitChildren func(node SQLNode, parent SQLNode) bool,
	up ApplyFunc,
) SQLNode {
	var pre func(cursor *Cursor) bool
	if shouldVisitChildren != nil {
		pre = func(cursor *Cursor) bool {
			visitChildren := shouldVisitChildren(cursor.Node(), cursor.Parent())
			if !visitChildren && up != nil {
				// this gives the up-function a chance to do work on this node even if we are not visiting the children
				// unfortunately, if the `up` function also returns false for this node, we won't abort the rest of the
				// tree walking. This is a temporary limitation, and will be fixed when we generated the correct code
				up(cursor)
			}
			return visitChildren
		}
	}
	return Rewrite(node, pre, up)
}

// RootNode is the root node of the AST when rewriting. It is the first element of the tree.
type RootNode struct {
	SQLNode
}

// An ApplyFunc is invoked by Rewrite for each node n, even if n is nil,
// before and/or after the node's children, using a Cursor describing
// the current node and providing operations on it.
//
// The return value of ApplyFunc controls the syntax tree traversal.
// See Rewrite for details.
type ApplyFunc func(*Cursor) bool

// A Cursor describes a node encountered during Apply.
// Information about the node and its parent is available
// from the Node and Parent methods.
type Cursor struct {
	parent   SQLNode
	replacer replacerFunc
	node     SQLNode

	// marks that the node has been replaced, and the new node should be visited
	revisit bool
	current *pathbuilder.ASTPathBuilder
}

// Visitable is the interface that needs to be implemented by all nodes that live outside the `sqlparser` package,
// in order to visit/rewrite/copy_on_rewrite these nodes.
type Visitable interface {
	SQLNode
	VisitThis() SQLNode
	Clone(inner SQLNode) SQLNode
}

// Node returns the current Node.
func (c *Cursor) Node() SQLNode { return c.node }

// Parent returns the parent of the current Node.
func (c *Cursor) Parent() SQLNode { return c.parent }

// Replace replaces the current node in the parent field with this new object. The use needs to make sure to not
// replace the object with something of the wrong type, or the visitor will panic.
func (c *Cursor) Replace(newNode SQLNode) {
	c.replacer(newNode, c.parent)
	c.node = newNode
}

// ReplaceAndRevisit replaces the current node in the parent field with this new object.
// When used, this will abort the visitation of the current node - no post or children visited,
// and the new node visited.
func (c *Cursor) ReplaceAndRevisit(newNode SQLNode) {
	c.replacer(newNode, c.parent)
	c.node = newNode
	c.revisit = true
}

// Path returns the current path that got us to the current location in the AST
// Only works if the AST walk was configured to collect path as walking
func (c *Cursor) Path() ASTPath {
	return ASTPath(c.current.ToPath())
}

type replacerFunc func(newNode, parent SQLNode)

// application carries all the shared data so we can pass it around cheaply.
type application struct {
	pre, post    ApplyFunc
	cur          Cursor
	collectPaths bool
}
