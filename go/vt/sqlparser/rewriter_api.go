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

// The rewriter was heavily inspired by https://github.com/golang/tools/blob/master/go/ast/astutil/rewrite.go

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
	parent := &RootNode{node}

	// this is the root-replacer, used when the user replaces the root of the ast
	replacer := func(newNode SQLNode, _ SQLNode) {
		parent.SQLNode = newNode
	}

	a := &application{
		pre:  pre,
		post: post,
	}

	a.rewriteSQLNode(parent, node, replacer)

	return parent.SQLNode
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

// ReplacerF returns a replace func that will work even when the cursor has moved to a different node.
func (c *Cursor) ReplacerF() func(newNode SQLNode) {
	replacer := c.replacer
	parent := c.parent
	return func(newNode SQLNode) {
		replacer(newNode, parent)
	}
}

// ReplaceAndRevisit replaces the current node in the parent field with this new object.
// When used, this will abort the visitation of the current node - no post or children visited,
// and the new node visited.
func (c *Cursor) ReplaceAndRevisit(newNode SQLNode) {
	switch newNode.(type) {
	case SelectExprs:
	default:
		// We need to add support to the generated code for when to look at the revisit flag. At the moment it is only
		// there for slices of SQLNode implementations
		panic("no support added for this type yet")
	}

	c.replacer(newNode, c.parent)
	c.node = newNode
	c.revisit = true
}

type replacerFunc func(newNode, parent SQLNode)

// application carries all the shared data so we can pass it around cheaply.
type application struct {
	pre, post ApplyFunc
	cur       Cursor
}
