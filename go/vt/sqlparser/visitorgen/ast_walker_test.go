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
	"go/parser"
	"go/token"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func TestSingleInterface(t *testing.T) {
	input := `
package sqlparser

type Nodeiface interface {
	iNode()
}
`

	fset := token.NewFileSet()
	ast, err := parser.ParseFile(fset, "ast.go", input, 0)
	require.NoError(t, err)

	result := Walk(ast)
	expected := SourceFile{
		lines: []Sast{&InterfaceDeclaration{
			name:  "Nodeiface",
			block: "",
		}},
	}
	assert.Equal(t, expected.String(), result.String())
}

func TestEmptyStruct(t *testing.T) {
	input := `
package sqlparser

type Empty struct {}
`

	fset := token.NewFileSet()
	ast, err := parser.ParseFile(fset, "ast.go", input, 0)
	require.NoError(t, err)

	result := Walk(ast)
	expected := SourceFile{
		lines: []Sast{&StructDeclaration{
			name:   "Empty",
			fields: []*Field{},
		}},
	}
	assert.Equal(t, expected.String(), result.String())
}

func TestStructWithStringField(t *testing.T) {
	input := `
package sqlparser

type Struct struct {
	field string
}
`

	fset := token.NewFileSet()
	ast, err := parser.ParseFile(fset, "ast.go", input, 0)
	require.NoError(t, err)

	result := Walk(ast)
	expected := SourceFile{
		lines: []Sast{&StructDeclaration{
			name: "Struct",
			fields: []*Field{{
				name: "field",
				typ:  &TypeString{typName: "string"},
			}},
		}},
	}
	assert.Equal(t, expected.String(), result.String())
}

func TestStructWithDifferentTypes(t *testing.T) {
	input := `
package sqlparser

type Struct struct {
	field      string
    reference  *string
    array      []string
    arrayOfRef []*string
}
`

	fset := token.NewFileSet()
	ast, err := parser.ParseFile(fset, "ast.go", input, 0)
	require.NoError(t, err)

	result := Walk(ast)
	expected := SourceFile{
		lines: []Sast{&StructDeclaration{
			name: "Struct",
			fields: []*Field{{
				name: "field",
				typ:  &TypeString{typName: "string"},
			}, {
				name: "reference",
				typ:  &Ref{&TypeString{typName: "string"}},
			}, {
				name: "array",
				typ:  &Array{&TypeString{typName: "string"}},
			}, {
				name: "arrayOfRef",
				typ:  &Array{&Ref{&TypeString{typName: "string"}}},
			}},
		}},
	}
	assert.Equal(t, expected.String(), result.String())
}

func TestStructWithTwoStringFieldInOneLine(t *testing.T) {
	input := `
package sqlparser

type Struct struct {
	left, right string
}
`

	fset := token.NewFileSet()
	ast, err := parser.ParseFile(fset, "ast.go", input, 0)
	require.NoError(t, err)

	result := Walk(ast)
	expected := SourceFile{
		lines: []Sast{&StructDeclaration{
			name: "Struct",
			fields: []*Field{{
				name: "left",
				typ:  &TypeString{typName: "string"},
			}, {
				name: "right",
				typ:  &TypeString{typName: "string"},
			}},
		}},
	}
	assert.Equal(t, expected.String(), result.String())
}

func TestStructWithSingleMethod(t *testing.T) {
	input := `
package sqlparser

type Empty struct {}

func (*Empty) method() {}
`

	fset := token.NewFileSet()
	ast, err := parser.ParseFile(fset, "ast.go", input, 0)
	require.NoError(t, err)

	result := Walk(ast)
	expected := SourceFile{
		lines: []Sast{
			&StructDeclaration{
				name:   "Empty",
				fields: []*Field{}},
			&FuncDeclaration{
				receiver: &Field{
					name: "",
					typ:  &Ref{&TypeString{"Empty"}},
				},
				name:      "method",
				block:     "",
				arguments: []*Field{},
			},
		},
	}
	assert.Equal(t, expected.String(), result.String())
}

func TestSingleArrayType(t *testing.T) {
	input := `
package sqlparser

type Strings []string
`

	fset := token.NewFileSet()
	ast, err := parser.ParseFile(fset, "ast.go", input, 0)
	require.NoError(t, err)

	result := Walk(ast)
	expected := SourceFile{
		lines: []Sast{&TypeAlias{
			name: "Strings",
			typ:  &Array{&TypeString{"string"}},
		}},
	}
	assert.Equal(t, expected.String(), result.String())
}

func TestSingleTypeAlias(t *testing.T) {
	input := `
package sqlparser

type String string
`

	fset := token.NewFileSet()
	ast, err := parser.ParseFile(fset, "ast.go", input, 0)
	require.NoError(t, err)

	result := Walk(ast)
	expected := SourceFile{
		lines: []Sast{&TypeAlias{
			name: "String",
			typ:  &TypeString{"string"},
		}},
	}
	assert.Equal(t, expected.String(), result.String())
}
