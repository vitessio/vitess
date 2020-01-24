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

package visitorgen

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimplestAst(t *testing.T) {
	/*
		type NodeInterface interface {
			iNode()
		}

		type NodeStruct struct {}

		func (*NodeStruct) iNode{}
	*/
	input := &SourceFile{
		lines: []Sast{
			&InterfaceDeclaration{
				name:  "NodeInterface",
				block: "// an interface lives here"},
			&StructDeclaration{
				name:   "NodeStruct",
				fields: []*Field{}},
			&FuncDeclaration{
				receiver: &Field{
					name: "",
					typ:  &Ref{&TypeString{"NodeStruct"}},
				},
				name:      "iNode",
				block:     "",
				arguments: []*Field{}},
		},
	}

	expected := &SourceInformation{
		interestingTypes: map[string]Type{
			"*NodeStruct": &Ref{&TypeString{"NodeStruct"}}},
		structs: map[string]*StructDeclaration{
			"NodeStruct": {
				name:   "NodeStruct",
				fields: []*Field{}}},
	}

	assert.Equal(t, expected.String(), Transform(input).String())
}

func TestAstWithArray(t *testing.T) {
	/*
		type NodeInterface interface {
			iNode()
		}

		func (*NodeArray) iNode{}

		type NodeArray []NodeInterface
	*/
	input := &SourceFile{
		lines: []Sast{
			&InterfaceDeclaration{
				name: "NodeInterface"},
			&TypeAlias{
				name: "NodeArray",
				typ:  &Array{&TypeString{"NodeInterface"}},
			},
			&FuncDeclaration{
				receiver: &Field{
					name: "",
					typ:  &Ref{&TypeString{"NodeArray"}},
				},
				name:      "iNode",
				block:     "",
				arguments: []*Field{}},
		},
	}

	expected := &SourceInformation{
		interestingTypes: map[string]Type{
			"*NodeArray": &Ref{&TypeString{"NodeArray"}}},
		structs: map[string]*StructDeclaration{},
		typeAliases: map[string]*TypeAlias{
			"NodeArray": {
				name: "NodeArray",
				typ:  &Array{&TypeString{"NodeInterface"}},
			},
		},
	}

	result := Transform(input)

	assert.Equal(t, expected.String(), result.String())
}
