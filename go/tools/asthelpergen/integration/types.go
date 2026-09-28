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
	"fmt"
	"strings"
)

//go:generate go run ../main --in . --iface vitess.io/vitess/go/tools/asthelpergen/integration.AST --clone_exclude "*NoCloneType"

type (
	// AST is the interface all interface types implement
	AST interface {
		String() string
	}
	// Empty struct impl of the iface
	Leaf struct {
		v int
	}

	// Options have been added to test the behaviour
	// of a struct that doesn't implement the AST interface
	// but includes a field that does.
	Options struct {
		a int
		b string
		l *Leaf
	}

	// Container implements the interface ByRef
	RefContainer struct {
		ASTType               AST
		NotASTType            int
		Opts                  []*Options
		ASTImplementationType *Leaf
	}
	// Container implements the interface ByRef
	RefSliceContainer struct {
		something                 int // want a non-AST field first
		ASTElements               []AST
		NotASTElements            []int
		ASTImplementationElements []*Leaf
	}
	// Container implements the interface ByValue
	ValueContainer struct {
		ASTType               AST
		NotASTType            int
		ASTImplementationType *Leaf
	}
	// Container implements the interface ByValue
	ValueSliceContainer struct {
		ASTElements               []AST
		NotASTElements            []int
		ASTImplementationElements LeafSlice
	}
	// We need to support these types - a slice of AST elements can implement the interface
	InterfaceSlice []AST
	// We need to support these types - a slice of AST elements can implement the interface
	Bytes       []byte
	LeafSlice   []*Leaf
	BasicType   int
	NoCloneType struct {
		v int
	}
	// We want to support all types that are used as field types, which can include interfaces.
	// Example would be sqlparser.Expr that implements sqlparser.SQLNode
	SubIface interface {
		AST
		iface()
	}
	SubImpl struct {
		inner SubIface
		field *bool
	}
	InterfaceContainer struct {
		v any
	}

	Visitable interface {
		AST
		VisitThis() AST
		Clone(inner AST) AST
	}
)

func (l *Leaf) String() string {
	if l == nil {
		return "nil"
	}
	return fmt.Sprintf("Leaf(%d)", l.v)
}

func (r *RefContainer) String() string {
	if r == nil {
		return "nil"
	}
	var astType string
	if r.ASTType == nil {
		astType = "nil"
	} else {
		astType = r.ASTType.String()
	}
	return fmt.Sprintf("RefContainer{%s, %d, %s}", astType, r.NotASTType, r.ASTImplementationType.String())
}

func (r *RefSliceContainer) String() string {
	return fmt.Sprintf("RefSliceContainer{%s, %s, %s}", sliceStringAST(r.ASTElements...), "r.NotASTType", sliceStringLeaf(r.ASTImplementationElements...))
}

func (r ValueContainer) String() string {
	return fmt.Sprintf("ValueContainer{%s, %d, %s}", r.ASTType.String(), r.NotASTType, r.ASTImplementationType.String())
}

func (r ValueSliceContainer) String() string {
	return fmt.Sprintf("ValueSliceContainer{%s, %s, %s}", sliceStringAST(r.ASTElements...), "r.NotASTType", sliceStringLeaf(r.ASTImplementationElements...))
}

func (r InterfaceSlice) String() string {
	var elements []string
	for _, el := range r {
		elements = append(elements, el.String())
	}

	return "[" + strings.Join(elements, ", ") + "]"
}

func (r Bytes) String() string {
	return string(r)
}

func (r LeafSlice) String() string {
	var elements []string
	for _, el := range r {
		elements = append(elements, el.String())
	}
	return strings.Join(elements, ", ")
}

func (r BasicType) String() string {
	return fmt.Sprintf("int(%d)", r)
}

const (
	// these consts are here to try to trick the generator
	thisIsNotAType  BasicType = 1
	thisIsNotAType2 BasicType = 2
)

func (r *SubImpl) String() string {
	return "SubImpl"
}
func (r *SubImpl) iface() {}

func (r InterfaceContainer) String() string {
	return fmt.Sprintf("%v", r.v)
}

func (r *NoCloneType) String() string {
	return fmt.Sprintf("NoClone(%d)", r.v)
}

type Visit func(node AST) (bool, error)

type application struct {
	pre, post    ApplyFunc
	cur          Cursor
	collectPaths bool
}

var Equals = &Comparator{}
