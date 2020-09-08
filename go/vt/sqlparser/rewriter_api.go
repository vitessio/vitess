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
//
// The visitation order between fields is alphabetical.
//
// If all you need is to visit all the nodes of the AST, and you
// don't need the Parent() or Replace() functions, Walk() is slightly faster
func Rewrite(node SQLNode, pre, post ApplyFunc) (result SQLNode) {
	parent := &struct{ SQLNode }{node}
	defer func() {
		if r := recover(); r != nil && r != abort {
			panic(r)
		}
		result = parent.SQLNode
	}()

	a := &application{
		pre:    pre,
		post:   post,
		cursor: Cursor{},
	}

	// this is the root-replacer, used when the user replaces the root of the ast
	replacer := func(newNode SQLNode, _ SQLNode) {
		parent.SQLNode = newNode
	}

	a.apply(parent, node, replacer)

	return parent.SQLNode
}

// An ApplyFunc is invoked by Rewrite for each node n, even if n is nil,
// before and/or after the node's children, using a Cursor describing
// the current node and providing operations on it.
//
// The return value of ApplyFunc controls the syntax tree traversal.
// See Rewrite for details.
type ApplyFunc func(*Cursor) bool

var abort = new(int) // singleton, to signal termination of Apply

// A Cursor describes a node encountered during Apply.
// Information about the node and its parent is available
// from the Node and Parent methods.
type Cursor struct {
	parent   SQLNode
	replacer replacerFunc
	node     SQLNode
}

// Node returns the current Node.
func (c *Cursor) Node() SQLNode { return c.node }

// Parent returns the parent of the current Node.
func (c *Cursor) Parent() SQLNode { return c.parent }

// Replace replaces the current node in the parent field with this new object. The use needs to make sure to not
// replace the object with something of the wrong type, or the visitor will panic.
func (c *Cursor) Replace(newNode SQLNode) {
	c.replacer(newNode, c.parent)
}

// Walk calls visit on every node.
// If visit returns true, the underlying nodes
// are also visited. If it returns an error, walking
// is interrupted, and the error is returned.
func Walk(visit Visit, nodes ...SQLNode) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		switch n := r.(type) {
		case errorOccured:
			err = n.err
		default:
			panic(r)
		}
	}()

	for _, node := range nodes {
		__walk(visit, node)
	}
	return err
}

// Visit defines the signature of a function that
// can be used to visit all nodes of a parse tree.
type Visit func(node SQLNode) (kontinue bool, err error)
