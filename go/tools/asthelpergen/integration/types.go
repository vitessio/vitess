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

import "reflect"

/*
These types are used to test the rewriter generator against these types.
To recreate them, just run:

go run go/tools/asthelpergen -in ./go/tools/asthelpergen/integration -iface vitess.io/vitess/go/tools/asthelpergen/integration.AST
*/
type (
	AST interface {
		i()
	}

	Plus struct {
		Left, Right AST
	}

	Array struct {
		Values []AST
		Stuff  []int
	}

	UnaryMinus struct {
		Val *LiteralInt
	}

	LiteralInt struct {
		Val int
	}

	LiteralString struct {
		Val string
	}

	StructHolder struct {
		Val AST
	}

	//ArrayDef []AST
)

func (*Plus) i() {}

func (*Array) i()         {}
func (*UnaryMinus) i()    {}
func (*LiteralInt) i()    {}
func (*LiteralString) i() {}
func (*StructHolder) i()  {}

//func (ArrayDef) i()     {}

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

type replacerFunc func(newNode, parent AST)

func isNilValue(i interface{}) bool {
	valueOf := reflect.ValueOf(i)
	kind := valueOf.Kind()
	isNullable := kind == reflect.Ptr || kind == reflect.Array || kind == reflect.Slice
	return isNullable && valueOf.IsNil()
}

var abort = new(int) // singleton, to signal termination of Apply
