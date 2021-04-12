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
	"strings"
)

/*
These types are used to test the rewriter generator against these types.
To recreate them, just run:

go run go/tools/asthelpergen -in ./go/tools/asthelpergen/integration -iface vitess.io/vitess/go/tools/asthelpergen/integration.AST -except "*NoCloneType"
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
	var astType = ""
	if r.ASTType == nil {
		astType = "nil"
	} else {
		astType = r.ASTType.String()
	}
	return fmt.Sprintf("RefContainer{%s, %d, %s}", astType, r.NotASTType, r.ASTImplementationType.String())
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

	return "[" + strings.Join(elements, ", ") + "]"
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

type BasicType int

func (r BasicType) String() string {
	return fmt.Sprintf("int(%d)", r)
}

const (
	// these consts are here to try to trick the generator
	thisIsNotAType  BasicType = 1
	thisIsNotAType2 BasicType = 2
)

// We want to support all types that are used as field types, which can include interfaces.
// Example would be sqlparser.Expr that implements sqlparser.SQLNode
type SubIface interface {
	AST
	iface()
}

type SubImpl struct {
	inner SubIface
	field *bool
}

func (r *SubImpl) String() string {
	return "SubImpl"
}
func (r *SubImpl) iface() {}

type InterfaceContainer struct {
	v interface{}
}

func (r InterfaceContainer) String() string {
	return fmt.Sprintf("%v", r.v)
}

type NoCloneType struct {
	v int
}

func (r *NoCloneType) String() string {
	return fmt.Sprintf("NoClone(%d)", r.v)
}

type Visit func(node AST) (bool, error)

type application struct {
	pre, post ApplyFunc
	cur       Cursor
}
