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

type replaceArrayValues int

func (r *replaceArrayValues) replace(newNode, parent AST) {
	parent.(*Array).Values[int(*r)] = newNode.(AST)
}
func (r *replaceArrayValues) inc() {
	*r++
}
func replacePlusLeft(newNode, parent AST) {
	parent.(*Plus).Left = newNode.(AST)
}
func replacePlusRight(newNode, parent AST) {
	parent.(*Plus).Right = newNode.(AST)
}
func replaceStructHolderVal(newNode, parent AST) {
	parent.(*StructHolder).Val = newNode.(AST)
}
func replaceUnaryMinusVal(newNode, parent AST) {
	parent.(*UnaryMinus).Val = newNode.(*LiteralInt)
}
func (a *application) apply(parent, node AST, replacer replacerFunc) {
	if node == nil || isNilValue(node) {
		return
	}
	saved := a.cursor
	a.cursor.replacer = replacer
	a.cursor.node = node
	a.cursor.parent = parent
	if a.pre != nil && !a.pre(&a.cursor) {
		a.cursor = saved
		return
	}
	switch n := node.(type) {
	case *Array:
		replacerValues := replaceArrayValues(0)
		replacerValuesB := &replacerValues
		for _, item := range n.Values {
			a.apply(node, item, replacerValuesB.replace)
			replacerValuesB.inc()
		}
	case *LiteralInt:
	case *LiteralString:
	case *Plus:
		a.apply(node, n.Left, replacePlusLeft)
		a.apply(node, n.Right, replacePlusRight)
	case StructHolder:
		a.apply(&n, n.Val, replaceStructHolderVal)
	case *UnaryMinus:
		a.apply(node, n.Val, replaceUnaryMinusVal)
	}
	if a.post != nil && !a.post(&a.cursor) {
		panic(abort)
	}
	a.cursor = saved
}
