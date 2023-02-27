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

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyOnRewrite(t *testing.T) {
	// rewrite an expression without changing the original
	expr, err := ParseExpr("a = b")
	require.NoError(t, err)
	out := CopyOnRewrite(expr, nil, func(cursor *CopyOnWriteCursor) {
		col, ok := cursor.Node().(*ColName)
		if !ok {
			return
		}
		if col.Name.EqualString("a") {
			cursor.Replace(NewIntLiteral("1"))
		}
	}, nil)

	assert.Equal(t, "a = b", String(expr))
	assert.Equal(t, "1 = b", String(out))
}

func TestCopyOnRewriteDeeper(t *testing.T) {
	// rewrite an expression without changing the original. the changed happens deep in the syntax tree,
	// here we are testing that all ancestors up to the root are cloned correctly
	expr, err := ParseExpr("a + b * c = 12")
	require.NoError(t, err)
	var path []string
	out := CopyOnRewrite(expr, nil, func(cursor *CopyOnWriteCursor) {
		col, ok := cursor.Node().(*ColName)
		if !ok {
			return
		}
		if col.Name.EqualString("c") {
			cursor.Replace(NewIntLiteral("1"))
		}
	}, func(before, _ SQLNode) {
		path = append(path, String(before))
	})

	assert.Equal(t, "a + b * c = 12", String(expr))
	assert.Equal(t, "a + b * 1 = 12", String(out))

	expected := []string{ // this are all the nodes that we need to clone when changing the `c` node
		"c",
		"b * c",
		"a + b * c",
		"a + b * c = 12",
	}
	assert.Equal(t, expected, path)
}

func TestDontCopyWithoutRewrite(t *testing.T) {
	// when no rewriting happens, we want the original back
	expr, err := ParseExpr("a = b")
	require.NoError(t, err)
	out := CopyOnRewrite(expr, nil, func(cursor *CopyOnWriteCursor) {}, nil)

	assert.Same(t, expr, out)
}

func TestStopTreeWalk(t *testing.T) {
	// stop walking down part of the AST
	original := "a = b + c"
	expr, err := ParseExpr(original)
	require.NoError(t, err)
	out := CopyOnRewrite(expr, func(node, parent SQLNode) bool {
		_, ok := node.(*BinaryExpr)
		return !ok
	}, func(cursor *CopyOnWriteCursor) {
		col, ok := cursor.Node().(*ColName)
		if !ok {
			return
		}

		cursor.Replace(NewStrLiteral(col.Name.String()))
	}, nil)

	assert.Equal(t, original, String(expr))
	assert.Equal(t, "'a' = b + c", String(out)) // b + c are unchanged since they are under the + (*BinaryExpr)
}

func TestStopTreeWalkButStillVisit(t *testing.T) {
	// here we are asserting that even when we stop at the binary expression, we still visit it in the post visitor
	original := "1337 = b + c"
	expr, err := ParseExpr(original)
	require.NoError(t, err)
	out := CopyOnRewrite(expr, func(node, parent SQLNode) bool {
		_, ok := node.(*BinaryExpr)
		return !ok
	}, func(cursor *CopyOnWriteCursor) {
		switch cursor.Node().(type) {
		case *BinaryExpr:
			cursor.Replace(NewStrLiteral("johnny was here"))
		case *ColName:
			t.Errorf("should not visit ColName in the post")
		}
	}, nil)

	assert.Equal(t, original, String(expr))
	assert.Equal(t, "1337 = 'johnny was here'", String(out)) // b + c are replaced
}
