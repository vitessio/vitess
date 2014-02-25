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

var (
	encoderMap = map[string]string{
		"float64": "EncodeFloat64",
		"string":  "EncodeString",
		"bool":    "EncodeBool",
		"int64":   "EncodeInt64",
		"int32":   "EncodeInt32",
		"int":     "EncodeInt",
		"uint64":  "EncodeUint64",
		"uint32":  "EncodeUint32",
		"uint":    "EncodeUint",
		"[]byte":  "EncodeBinary",
	}
	decoderMap = map[string]string{
		"float64": "DecodeFloat64",
		"string":  "DecodeString",
		"bool":    "DecodeBool",
		"int64":   "DecodeInt64",
		"int32":   "DecodeInt32",
		"int":     "DecodeInt",
		"uint64":  "DecodeUint64",
		"uint32":  "DecodeUint32",
		"uint":    "DecodeUint",
		"[]byte":  "DecodeBinary",
	}
)

type TypeInfo struct {
	Package string
	Name    string
	Var     string
	Fields  []*FieldInfo
}

type FieldInfo struct {
	Tag      string
	Name     string
	Type     string
	Subfield *FieldInfo
}

func (f *FieldInfo) IsPointer() bool {
	return f.Type == "*"
}

func (f *FieldInfo) Encoder() string {
	return encoderMap[f.Type]
}

func (f *FieldInfo) Decoder() string {
	return decoderMap[f.Type]
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
		fields, err := buildFields(structType, typeInfo.Var)
		if err != nil {
			return nil, err
		}
		typeInfo.Fields = fields
		return typeInfo, nil
	}
	return nil, fmt.Errorf("%s not found", name)
}

func buildFields(structType *ast.StructType, varName string) ([]*FieldInfo, error) {
	fields := make([]*FieldInfo, 0, 8)
	for _, field := range structType.Fields.List {
		for _, name := range field.Names {
			fullName := varName + "." + name.Name
			fieldInfo, err := buildField(field.Type, name.Name, fullName)
			if err != nil {
				return nil, err
			}
			fields = append(fields, fieldInfo)
		}
	}
	return fields, nil
}

func buildField(fieldType ast.Expr, tag, name string) (*FieldInfo, error) {
	switch ident := fieldType.(type) {
	case *ast.Ident:
		if encoderMap[ident.Name] == "" {
			return nil, fmt.Errorf("%s is not a recognized type", ident.Name)
		}
		return &FieldInfo{Tag: tag, Name: name, Type: ident.Name}, nil
	case *ast.ArrayType:
		if ident.Len != nil {
			goto notSimple
		}
		innerIdent, ok := ident.Elt.(*ast.Ident)
		if !ok {
			goto notSimple
		}
		if innerIdent.Name != "byte" {
			goto notSimple
		}
		return &FieldInfo{Tag: tag, Name: name, Type: "[]byte"}, nil
	case *ast.StarExpr:
		subfield, err := buildField(ident.X, tag, "*"+name)
		if err != nil {
			return nil, err
		}
		return &FieldInfo{Tag: tag, Name: name, Type: "*", Subfield: subfield}, nil
	}
notSimple:
	return nil, fmt.Errorf("%v is not a simple type", fieldType)
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
	generator.ExecuteTemplate(os.Stdout, "Body", typeInfo)
}

var generator = template.Must(template.New("Generator").Parse(`
{{define "SimpleEncoder"}}bson.{{.Encoder}}(buf, "{{.Tag}}", {{.Name}}){{end}}

{{define "StarEncoder"}}if {{.Name}} == nil {
		bson.EncodePrefix(buf, bson.Null, "{{.Tag}}")
	} else {
		{{template "Encoder" .Subfield}}
	}{{end}}

{{define "Encoder"}}{{if .IsPointer}}{{template "StarEncoder" .}}{{else}}{{template "SimpleEncoder" .}}{{end}}{{end}}

{{define "Body"}}// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package {{.Package}}

import (
	"bytes"

	"github.com/youtube/vitess/go/bson"
	"github.com/youtube/vitess/go/bytes2"
)

// MarshalBson bson-encodes {{.Name}}.
func ({{.Var}} *{{.Name}}) MarshalBson(buf *bytes2.ChunkedWriter) {
	lenWriter := bson.NewLenWriter(buf)

	{{range .Fields}}{{template "Encoder" .}}
	{{end}}
	buf.WriteByte(0)
	lenWriter.RecordLen()
}

// UnmarshalBson bson-decodes into {{.Name}}.
func ({{.Var}} *{{.Name}}) UnmarshalBson(buf *bytes.Buffer) {
	bson.Next(buf, 4)

	kind := bson.NextByte(buf)
	for kind != bson.EOO {
		switch bson.ReadCString(buf) {
{{range .Fields}}		case "{{.Tag}}":
			{{.Name}} = bson.{{.Decoder}}(buf, kind)
{{end}}		default:
			bson.Skip(buf, kind)
		}
		kind = bson.NextByte(buf)
	}
}
{{end}}`))
