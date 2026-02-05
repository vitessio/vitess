/*
Copyright 2023 The Vitess Authors.

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

// CopyOnRewrite traverses a syntax tree recursively, starting with root,
// and calling pre and post for each node as described below.
// Rewrite returns a syntax tree, where some nodes can be shared with the
// original syntax tree.
//
// If pre is not nil, it is called for each node before the node's
// children are traversed (pre-order). If pre returns false, no
// children are traversed, but post is still called for that node.
//
// If post is not nil, and a prior call of pre didn't return false,
// post is called for each node after its children are traversed
// (post-order).
//
// In the post call, the cursor can be used to abort the current
// traversal altogether.
//
// Also in the post call, a user can replace the current node.
// When a node is replaced, all the ancestors of the node are cloned,
// so that the original syntax tree remains untouched
//
// The `cloned` function will be called for all nodes that are cloned
// or replaced, to give the user a chance to copy any metadata that
// needs copying.
//
// Only fields that refer to AST nodes are considered children;
// i.e., fields of basic types (strings, []byte, etc.) are ignored.
func CopyOnRewrite(
	node SQLNode,
	pre func(node, parent SQLNode) bool,
	post func(cursor *CopyOnWriteCursor),
	cloned func(before, after SQLNode),
) SQLNode {
	cow := cow{pre: pre, post: post, cursor: CopyOnWriteCursor{}, cloned: cloned}
	out, _ := cow.copyOnRewriteSQLNode(node, nil)
	return out
}

// StopTreeWalk aborts the current tree walking. No more nodes will be visited, and the rewriter will exit out early
func (c *CopyOnWriteCursor) StopTreeWalk() {
	c.stop = true
}

// Node returns the current node we are visiting
func (c *CopyOnWriteCursor) Node() SQLNode {
	return c.node
}

// Parent returns the parent of the current node.
// Note: This is the parent before any changes have been done - the parent in the output might be a copy of this
func (c *CopyOnWriteCursor) Parent() SQLNode {
	return c.parent
}

// Replace replaces the current node with the given node.
// Note: If you try to set an invalid type on a field, the field will end up with a nil and no error will be reported.
func (c *CopyOnWriteCursor) Replace(n SQLNode) {
	c.replaced = n
}

func (c *cow) postVisit(node, parent SQLNode, changed bool) (SQLNode, bool) {
	c.cursor.node = node
	c.cursor.parent = parent
	c.cursor.replaced = nil
	c.post(&c.cursor)
	if c.cursor.replaced != nil {
		if c.cloned != nil {
			c.cloned(node, c.cursor.replaced)
		}
		return c.cursor.replaced, true
	}
	return node, changed
}

type (
	CopyOnWriteCursor struct {
		node     SQLNode
		parent   SQLNode
		replaced SQLNode
		stop     bool
	}
	cow struct {
		pre    func(node, parent SQLNode) bool
		post   func(cursor *CopyOnWriteCursor)
		cloned func(old, new SQLNode)
		cursor CopyOnWriteCursor
	}
)
