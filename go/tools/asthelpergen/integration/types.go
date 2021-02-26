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

//nolint
package integration

import (
	"fmt"
	"reflect"
	"strings"
)

/*
These types are used to test the rewriter generator against these types.
To recreate them, just run:

go run go/tools/asthelpergen -in ./go/tools/asthelpergen/integration -iface vitess.io/vitess/go/tools/asthelpergen/integration.AST
*/
// AST is the interface all interface types implement
type AST interface {
	String() string
}

// Empty struct impl of the iface
type Leaf struct {
	v int
}

func (l *Leaf) String() string {
	if l == nil {
		return "nil"
	}
	return fmt.Sprintf("Leaf(%d)", l.v)
}

// Container implements the interface ByRef
type RefContainer struct {
	ASTType               AST
	NotASTType            int
	ASTImplementationType *Leaf
}

func (r *RefContainer) String() string {
	if r == nil {
		return "nil"
	}
	asttype := ""
	if r.ASTType == nil {
		asttype = "nil"
	} else {
		asttype = r.ASTType.String()
	}
	return fmt.Sprintf("RefContainer{%s, %d, %s}", asttype, r.NotASTType, r.ASTImplementationType.String())
}

// Container implements the interface ByRef
type RefSliceContainer struct {
	ASTElements               []AST
	NotASTElements            []int
	ASTImplementationElements []*Leaf
}

func (r *RefSliceContainer) String() string {
	return fmt.Sprintf("RefSliceContainer{%s, %s, %s}", sliceStringAST(r.ASTElements...), "r.NotASTType", sliceStringLeaf(r.ASTImplementationElements...))
}

// Container implements the interface ByValue
type ValueContainer struct {
	ASTType               AST
	NotASTType            int
	ASTImplementationType *Leaf
}

func (r ValueContainer) String() string {
	return fmt.Sprintf("ValueContainer{%s, %d, %s}", r.ASTType.String(), r.NotASTType, r.ASTImplementationType.String())
}

// Container implements the interface ByValue
type ValueSliceContainer struct {
	ASTElements               []AST
	NotASTElements            []int
	ASTImplementationElements []*Leaf
}

func (r ValueSliceContainer) String() string {
	return fmt.Sprintf("ValueSliceContainer{%s, %s, %s}", sliceStringAST(r.ASTElements...), "r.NotASTType", sliceStringLeaf(r.ASTImplementationElements...))
}

// We need to support these types - a slice of AST elements can implement the interface
type InterfaceSlice []AST

func (r InterfaceSlice) String() string {
	var elements []string
	for _, el := range r {
		elements = append(elements, el.String())
	}

	return strings.Join(elements, ", ")
}

// We need to support these types - a slice of AST elements can implement the interface
type Bytes []byte

func (r Bytes) String() string {
	return string(r)
}

type LeafSlice []*Leaf

func (r LeafSlice) String() string {
	var elements []string
	for _, el := range r {
		elements = append(elements, el.String())
	}
	return strings.Join(elements, ", ")
}

// We want to support all types that are used as field types, which can include interfaces.
// Example would be sqlparser.Expr that implements sqlparser.SQLNode
type SubIface interface {
	AST
	iface()
}

type SubImpl struct {
	inner SubIface
}

func (r *SubImpl) String() string {
	return "SubImpl"
}
func (r *SubImpl) iface() {}

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

type application struct {
	pre, post ApplyFunc
	cursor    Cursor
}

type ApplyFunc func(*Cursor) bool

type Cursor struct {
	parent   AST
	replacer replacerFunc
	node     AST
}

// Node returns the current Node.
func (c *Cursor) Node() AST { return c.node }

// Parent returns the parent of the current Node.
func (c *Cursor) Parent() AST { return c.parent }

// Replace replaces the current node in the parent field with this new object. The use needs to make sure to not
// replace the object with something of the wrong type, or the visitor will panic.
func (c *Cursor) Replace(newNode AST) {
	c.replacer(newNode, c.parent)
	c.node = newNode
}

type replacerFunc func(newNode, parent AST)

func isNilValue(i interface{}) bool {
	valueOf := reflect.ValueOf(i)
	kind := valueOf.Kind()
	isNullable := kind == reflect.Ptr || kind == reflect.Array || kind == reflect.Slice
	return isNullable && valueOf.IsNil()
}

var abort = new(int) // singleton, to signal termination of Apply

func Rewrite(node AST, pre, post ApplyFunc) (result AST) {
	parent := &struct{ AST }{node}

	a := &application{
		pre:    pre,
		post:   post,
		cursor: Cursor{},
	}

	a.apply(parent.AST, node, nil)
	return parent.AST
}

func replacePanic(msg string) func(newNode, parent AST) {
	return func(newNode, parent AST) {
		panic("Tried replacing a field of a value type. This is not supported. " + msg)
	}
}
