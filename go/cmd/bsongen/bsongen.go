// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"text/template"

	"github.com/youtube/vitess/go/testfiles"
)

type TypeInfo struct {
	Package string
	Name    string
	Var     string
	Fields  []FieldInfo
}

type FieldInfo struct {
	Name string
	Type string
}

func FindType(file *ast.File, name string) (*TypeInfo, error) {
	typeInfo := &TypeInfo{
		Package: file.Name.Name,
	}
	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		if genDecl.Tok != token.TYPE {
			continue
		}
		if len(genDecl.Specs) != 1 {
			continue
		}
		typeSpec, ok := genDecl.Specs[0].(*ast.TypeSpec)
		if !ok {
			continue
		}
		if typeSpec.Name.Name != name {
			continue
		}
		typeInfo.Name = name
		typeInfo.Var = strings.ToLower(name[:1]) + name[1:]
		structType, ok := typeSpec.Type.(*ast.StructType)
		if !ok {
			return nil, fmt.Errorf("%s is not a struct", name)
		}
		fields, err := BuildFields(structType)
		if err != nil {
			return nil, err
		}
		typeInfo.Fields = fields
		return typeInfo, nil
	}
	return nil, fmt.Errorf("%s not found", name)
}

func BuildFields(structType *ast.StructType) ([]FieldInfo, error) {
	fieldInfo := make([]FieldInfo, 0, 8)
	for _, field := range structType.Fields.List {
		ident, ok := field.Type.(*ast.Ident)
		if !ok {
			return nil, fmt.Errorf("%s is not a simple type", field.Names)
		}
		if ident.Name != "int64" {
			return nil, fmt.Errorf("%s is not a recognized type", ident.Name)
		}
		for _, name := range field.Names {
			fieldInfo = append(fieldInfo, FieldInfo{Name: name.Name, Type: ident.Name})
		}
	}
	return fieldInfo, nil
}

func main() {
	input := testfiles.Locate("bson_test/simple_type.go")
	b, err := ioutil.ReadFile(input)
	if err != nil {
		log.Fatal(err)
	}
	src := string(b)

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", src, 0)
	if err != nil {
		log.Fatal(err)
	}
	ast.Print(fset, f)
	typeInfo, err := FindType(f, "MyType")
	if err != nil {
		fmt.Println(err)
		return
	}
	generator.Execute(os.Stdout, typeInfo)
}

var generator = template.Must(template.New("Generator").Parse(`{{$Top := .}}
// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package {{.Package}}

func ({{.Var}} *{{.Name}}) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)
	{{range .Fields}}
	bson.EncodeInt64(buf, "{{.Name}}", {{$Top.Var}}.{{.Name}})
	{{end}}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

func ({{.Var}} *{{.Name}}) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		key := bson.ReadCString(buf)
		switch key {
{{range .Fields}}		case "{{.Name}}":
			{{$Top.Var}}.{{.Name}} = DecondeInt64(buf, kind){{end}}
		default:
			bson.Skip(buf, kind)
		}
		kind = bson.NextByte(buf)
	}
}
`))
