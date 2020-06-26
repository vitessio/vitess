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

func TestEmptyStructVisitor(t *testing.T) {
	/*
		type Node interface{}
		type Struct struct {}
		func (*Struct) iNode() {}
	*/

	input := &SourceInformation{
		interestingTypes: map[string]Type{
			"*Struct": &Ref{&TypeString{"Struct"}},
		},
		interfaces: map[string]bool{
			"Node": true,
		},
		structs: map[string]*StructDeclaration{
			"Struct": {name: "Struct", fields: []*Field{}},
		},
		typeAliases: map[string]*TypeAlias{},
	}

	result := ToVisitorPlan(input)

	expected := &VisitorPlan{
		Switches: []*SwitchCase{{
			Type:   &Ref{&TypeString{"Struct"}},
			Fields: []VisitorItem{},
		}},
	}

	assert.Equal(t, expected.String(), result.String())
}

func TestStructWithSqlNodeField(t *testing.T) {
	/*
			type Node interface{}
			type Struct struct {
				Field Node
		    }
			func (*Struct) iNode() {}
	*/
	input := &SourceInformation{
		interestingTypes: map[string]Type{
			"*Struct": &Ref{&TypeString{"Struct"}},
		},
		interfaces: map[string]bool{
			"Node": true,
		},
		structs: map[string]*StructDeclaration{
			"Struct": {name: "Struct", fields: []*Field{
				{name: "Field", typ: &TypeString{"Node"}},
			}},
		},
		typeAliases: map[string]*TypeAlias{},
	}

	result := ToVisitorPlan(input)

	expected := &VisitorPlan{
		Switches: []*SwitchCase{{
			Type: &Ref{&TypeString{"Struct"}},
			Fields: []VisitorItem{&SingleFieldItem{
				StructType: &Ref{&TypeString{"Struct"}},
				FieldType:  &TypeString{"Node"},
				FieldName:  "Field",
			}},
		}},
	}

	assert.Equal(t, expected.String(), result.String())
}

func TestStructWithStringField2(t *testing.T) {
	/*
			type Node interface{}
			type Struct struct {
				Field Node
		    }
			func (*Struct) iNode() {}
	*/

	input := &SourceInformation{
		interestingTypes: map[string]Type{
			"*Struct": &Ref{&TypeString{"Struct"}},
		},
		interfaces: map[string]bool{
			"Node": true,
		},
		structs: map[string]*StructDeclaration{
			"Struct": {name: "Struct", fields: []*Field{
				{name: "Field", typ: &TypeString{"string"}},
			}},
		},
		typeAliases: map[string]*TypeAlias{},
	}

	result := ToVisitorPlan(input)

	expected := &VisitorPlan{
		Switches: []*SwitchCase{{
			Type:   &Ref{&TypeString{"Struct"}},
			Fields: []VisitorItem{},
		}},
	}

	assert.Equal(t, expected.String(), result.String())
}

func TestArrayAsSqlNode(t *testing.T) {
	/*
		type NodeInterface interface {
			iNode()
		}

		func (*NodeArray) iNode{}

		type NodeArray []NodeInterface
	*/

	input := &SourceInformation{
		interfaces: map[string]bool{"NodeInterface": true},
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

	result := ToVisitorPlan(input)

	expected := &VisitorPlan{
		Switches: []*SwitchCase{{
			Type: &Ref{&TypeString{"NodeArray"}},
			Fields: []VisitorItem{&ArrayItem{
				StructType: &Ref{&TypeString{"NodeArray"}},
				ItemType:   &TypeString{"NodeInterface"},
			}},
		}},
	}

	assert.Equal(t, expected.String(), result.String())
}

func TestStructWithStructField(t *testing.T) {
	/*
			type Node interface{}
			type Struct struct {
				Field *Struct
		    }
			func (*Struct) iNode() {}
	*/

	input := &SourceInformation{
		interestingTypes: map[string]Type{
			"*Struct": &Ref{&TypeString{"Struct"}}},
		structs: map[string]*StructDeclaration{
			"Struct": {name: "Struct", fields: []*Field{
				{name: "Field", typ: &Ref{&TypeString{"Struct"}}},
			}},
		},
		typeAliases: map[string]*TypeAlias{},
	}

	result := ToVisitorPlan(input)

	expected := &VisitorPlan{
		Switches: []*SwitchCase{{
			Type: &Ref{&TypeString{"Struct"}},
			Fields: []VisitorItem{&SingleFieldItem{
				StructType: &Ref{&TypeString{"Struct"}},
				FieldType:  &Ref{&TypeString{"Struct"}},
				FieldName:  "Field",
			}},
		}},
	}

	assert.Equal(t, expected.String(), result.String())
}

func TestStructWithArrayOfNodes(t *testing.T) {
	/*
		type NodeInterface interface {}
		type Struct struct {
			Items []NodeInterface
		}

		func (*Struct) iNode{}
	*/

	input := &SourceInformation{
		interfaces: map[string]bool{
			"NodeInterface": true,
		},
		interestingTypes: map[string]Type{
			"*Struct": &Ref{&TypeString{"Struct"}}},
		structs: map[string]*StructDeclaration{
			"Struct": {name: "Struct", fields: []*Field{
				{name: "Items", typ: &Array{&TypeString{"NodeInterface"}}},
			}},
		},
		typeAliases: map[string]*TypeAlias{},
	}

	result := ToVisitorPlan(input)

	expected := &VisitorPlan{
		Switches: []*SwitchCase{{
			Type: &Ref{&TypeString{"Struct"}},
			Fields: []VisitorItem{&ArrayFieldItem{
				StructType: &Ref{&TypeString{"Struct"}},
				ItemType:   &TypeString{"NodeInterface"},
				FieldName:  "Items",
			}},
		}},
	}

	assert.Equal(t, expected.String(), result.String())
}

func TestStructWithArrayOfStrings(t *testing.T) {
	/*
		type NodeInterface interface {}
		type Struct struct {
			Items []string
		}

		func (*Struct) iNode{}
	*/

	input := &SourceInformation{
		interfaces: map[string]bool{
			"NodeInterface": true,
		},
		interestingTypes: map[string]Type{
			"*Struct": &Ref{&TypeString{"Struct"}}},
		structs: map[string]*StructDeclaration{
			"Struct": {name: "Struct", fields: []*Field{
				{name: "Items", typ: &Array{&TypeString{"string"}}},
			}},
		},
		typeAliases: map[string]*TypeAlias{},
	}

	result := ToVisitorPlan(input)

	expected := &VisitorPlan{
		Switches: []*SwitchCase{{
			Type:   &Ref{&TypeString{"Struct"}},
			Fields: []VisitorItem{},
		}},
	}

	assert.Equal(t, expected.String(), result.String())
}

func TestArrayOfStringsThatImplementSQLNode(t *testing.T) {
	/*
		type NodeInterface interface {}
		type Struct []string
		func (Struct) iNode{}
	*/

	input := &SourceInformation{
		interfaces:       map[string]bool{"NodeInterface": true},
		interestingTypes: map[string]Type{"Struct": &Ref{&TypeString{"Struct"}}},
		structs:          map[string]*StructDeclaration{},
		typeAliases: map[string]*TypeAlias{
			"Struct": {
				name: "Struct",
				typ:  &Array{&TypeString{"string"}},
			},
		},
	}

	result := ToVisitorPlan(input)

	expected := &VisitorPlan{
		Switches: []*SwitchCase{{
			Type:   &Ref{&TypeString{"Struct"}},
			Fields: []VisitorItem{},
		}},
	}

	assert.Equal(t, expected.String(), result.String())
}

func TestSortingOfOutputs(t *testing.T) {
	/*
		type NodeInterface interface {}
		type AStruct struct {
			AField NodeInterface
			BField NodeInterface
		}
		type BStruct struct {
			CField NodeInterface
		}
		func (*AStruct) iNode{}
		func (*BStruct) iNode{}
	*/

	input := &SourceInformation{
		interfaces: map[string]bool{"NodeInterface": true},
		interestingTypes: map[string]Type{
			"AStruct": &Ref{&TypeString{"AStruct"}},
			"BStruct": &Ref{&TypeString{"BStruct"}},
		},
		structs: map[string]*StructDeclaration{
			"AStruct": {name: "AStruct", fields: []*Field{
				{name: "BField", typ: &TypeString{"NodeInterface"}},
				{name: "AField", typ: &TypeString{"NodeInterface"}},
			}},
			"BStruct": {name: "BStruct", fields: []*Field{
				{name: "CField", typ: &TypeString{"NodeInterface"}},
			}},
		},
		typeAliases: map[string]*TypeAlias{},
	}

	result := ToVisitorPlan(input)

	expected := &VisitorPlan{
		Switches: []*SwitchCase{
			{Type: &Ref{&TypeString{"AStruct"}},
				Fields: []VisitorItem{
					&SingleFieldItem{
						StructType: &Ref{&TypeString{"AStruct"}},
						FieldType:  &TypeString{"NodeInterface"},
						FieldName:  "AField",
					},
					&SingleFieldItem{
						StructType: &Ref{&TypeString{"AStruct"}},
						FieldType:  &TypeString{"NodeInterface"},
						FieldName:  "BField",
					}}},
			{Type: &Ref{&TypeString{"BStruct"}},
				Fields: []VisitorItem{
					&SingleFieldItem{
						StructType: &Ref{&TypeString{"BStruct"}},
						FieldType:  &TypeString{"NodeInterface"},
						FieldName:  "CField",
					}}}},
	}
	assert.Equal(t, expected.String(), result.String())
}

func TestAliasOfAlias(t *testing.T) {
	/*
		type NodeInterface interface {
			iNode()
		}

		type NodeArray    []NodeInterface
		type AliasOfAlias NodeArray

		func (NodeArray) 	iNode{}
		func (AliasOfAlias) iNode{}
	*/

	input := &SourceInformation{
		interfaces: map[string]bool{"NodeInterface": true},
		interestingTypes: map[string]Type{
			"NodeArray":    &TypeString{"NodeArray"},
			"AliasOfAlias": &TypeString{"AliasOfAlias"},
		},
		structs: map[string]*StructDeclaration{},
		typeAliases: map[string]*TypeAlias{
			"NodeArray": {
				name: "NodeArray",
				typ:  &Array{&TypeString{"NodeInterface"}},
			},
			"AliasOfAlias": {
				name: "NodeArray",
				typ:  &TypeString{"NodeArray"},
			},
		},
	}

	result := ToVisitorPlan(input)

	expected := &VisitorPlan{
		Switches: []*SwitchCase{
			{Type: &TypeString{"AliasOfAlias"},
				Fields: []VisitorItem{&ArrayItem{
					StructType: &TypeString{"AliasOfAlias"},
					ItemType:   &TypeString{"NodeInterface"},
				}},
			},
			{Type: &TypeString{"NodeArray"},
				Fields: []VisitorItem{&ArrayItem{
					StructType: &TypeString{"NodeArray"},
					ItemType:   &TypeString{"NodeInterface"},
				}},
			}},
	}
	assert.Equal(t, expected.String(), result.String())
}
