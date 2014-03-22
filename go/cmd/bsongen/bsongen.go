// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"text/template"
)

var (
	filename = flag.String("file", "", "input file name")
	typename = flag.String("type", "", "type to generate code for")
	outfile  = flag.String("o", "", "output file name, default stdout")
	counter  = 0
)

func main() {
	flag.Parse()
	if *filename == "" || *typename == "" {
		flag.PrintDefaults()
		return
	}
	b, err := ioutil.ReadFile(*filename)
	if err != nil {
		log.Fatal(err)
	}
	out, err := generateCode(string(b), *typename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		return
	}
	fout := os.Stdout
	if *outfile != "" {
		fout, err = os.Create(*outfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			return
		}
		defer fout.Close()
	}
	fmt.Fprintf(fout, "%s", out)
}

var (
	encoderMap = map[string]string{
		"string":      "EncodeString",
		"[]byte":      "EncodeBinary",
		"int64":       "EncodeInt64",
		"int32":       "EncodeInt32",
		"int":         "EncodeInt",
		"uint64":      "EncodeUint64",
		"uint32":      "EncodeUint32",
		"uint":        "EncodeUint",
		"float64":     "EncodeFloat64",
		"bool":        "EncodeBool",
		"interface{}": "EncodeInterface",
		"time.Time":   "EncodeTime",
	}
	decoderMap = map[string]string{
		"string":      "DecodeString",
		"[]byte":      "DecodeBinary",
		"int64":       "DecodeInt64",
		"int32":       "DecodeInt32",
		"int":         "DecodeInt",
		"uint64":      "DecodeUint64",
		"uint32":      "DecodeUint32",
		"uint":        "DecodeUint",
		"float64":     "DecodeFloat64",
		"bool":        "DecodeBool",
		"interface{}": "DecodeInterface",
		"time.Time":   "DecodeTime",
	}
)

type TypeInfo struct {
	Package string
	Imports []string
	Name    string
	Var     string
	Fields  []*FieldInfo
}

type FieldInfo struct {
	Tag      string
	Name     string
	typ      string
	Subfield *FieldInfo
}

func (f *FieldInfo) IsPointer() bool {
	return f.typ == "*"
}

func (f *FieldInfo) IsSlice() bool {
	return f.typ == "[]"
}

func (f *FieldInfo) IsMap() bool {
	return f.typ == "map[string]"
}

func (f *FieldInfo) IsCustom() bool {
	if f.IsPointer() || f.IsSlice() || f.IsMap() {
		return false
	}
	return encoderMap[f.typ] == ""
}

func (f *FieldInfo) Encoder() string {
	return encoderMap[f.typ]
}

func (f *FieldInfo) Decoder() string {
	return decoderMap[f.typ]
}

func (f *FieldInfo) NewType() string {
	if f.typ != "*" {
		return ""
	}
	typ := ""
	for field := f.Subfield; field != nil; field = field.Subfield {
		typ += field.typ
	}
	return typ
}

func (f *FieldInfo) Type() string {
	typ := f.typ
	for field := f.Subfield; field != nil; field = field.Subfield {
		typ += field.typ
	}
	return typ
}

func findType(file *ast.File, name string) (*TypeInfo, error) {
	typeInfo := &TypeInfo{
		Package: file.Name.Name,
	}
	for _, decl := range file.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		if genDecl.Tok == token.IMPORT {
			typeInfo.Imports = append(typeInfo.Imports, buildImports(genDecl.Specs)...)
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

func buildImports(importSpecs []ast.Spec) (imports []string) {
	for _, spec := range importSpecs {
		importSpec, ok := spec.(*ast.ImportSpec)
		if !ok {
			continue
		}
		var str string
		if importSpec.Name == nil {
			str = importSpec.Path.Value
		} else {
			str = importSpec.Name.Name + " " + importSpec.Path.Value
		}
		imports = append(imports, str)
	}
	return imports
}

func buildFields(structType *ast.StructType, varName string) (fields []*FieldInfo, err error) {
	for _, field := range structType.Fields.List {
		if field.Names == nil {
			return nil, fmt.Errorf("anonymous embeds not supported: %#v", field.Type)
		}
		for _, name := range field.Names {
			fullName := varName + "." + name.Name
			fieldInfo, err := buildField(field.Type, "\""+name.Name+"\"", fullName)
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
		return &FieldInfo{Tag: tag, Name: name, typ: ident.Name}, nil
	case *ast.InterfaceType:
		if ident.Methods.List != nil {
			goto notSimple
		}
		return &FieldInfo{Tag: tag, Name: name, typ: "interface{}"}, nil
	case *ast.ArrayType:
		if ident.Len != nil {
			goto notSimple
		}
		innerIdent, ok := ident.Elt.(*ast.Ident)
		if ok && innerIdent.Name == "byte" {
			return &FieldInfo{Tag: tag, Name: name, typ: "[]byte"}, nil
		}
		subfield, err := buildField(ident.Elt, "bson.Itoa(_i)", newVarName())
		if err != nil {
			return nil, err
		}
		return &FieldInfo{Tag: tag, Name: name, typ: "[]", Subfield: subfield}, nil
	case *ast.StarExpr:
		subfield, err := buildField(ident.X, tag, "(*"+name+")")
		if err != nil {
			return nil, err
		}
		return &FieldInfo{Tag: tag, Name: name, typ: "*", Subfield: subfield}, nil
	case *ast.MapType:
		key, ok := ident.Key.(*ast.Ident)
		if !ok || key.Name != "string" {
			goto notSimple
		}
		subfield, err := buildField(ident.Value, "_k", newVarName())
		if err != nil {
			return nil, err
		}
		return &FieldInfo{Tag: tag, Name: name, typ: "map[string]", Subfield: subfield}, nil
	case *ast.SelectorExpr:
		pkg, ok := ident.X.(*ast.Ident)
		if !ok {
			goto notSimple
		}
		return &FieldInfo{Tag: tag, Name: name, typ: pkg.Name + "." + ident.Sel.Name}, nil
	}
notSimple:
	return nil, fmt.Errorf("%#v is not a simple type", fieldType)
}

func newVarName() string {
	counter++
	return fmt.Sprintf("_v%d", counter)
}

func generateCode(in string, typename string) (out []byte, err error) {
	raw, err := generateRawCode(in, typename)
	if err != nil {
		return nil, err
	}
	return formatCode(raw)
}

func generateRawCode(in string, typename string) (out []byte, err error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", in, 0)
	if err != nil {
		return nil, err
	}
	//ast.Print(fset, f)
	typeInfo, err := findType(f, typename)
	if err != nil {
		return nil, err
	}
	buf := bytes.NewBuffer(nil)
	err = generator.ExecuteTemplate(buf, "Body", typeInfo)
	if err != nil {
		return nil, err
	}
	return formatCode(buf.Bytes())
}

func formatCode(in []byte) (out []byte, err error) {
	cmd := exec.Command("goimports")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	err = cmd.Start()
	if err != nil {
		return nil, err
	}
	defer cmd.Wait()
	go func() {
		bytes.NewBuffer(in).WriteTo(stdin)
		stdin.Close()
	}()
	b, err := ioutil.ReadAll(stdout)
	if err != nil {
		return nil, err
	}
	return b, nil
}

var generator = template.Must(template.New("Generator").Parse(`
{{define "SimpleEncoder"}}bson.{{.Encoder}}(buf, {{.Tag}}, {{.Name}}){{end}}

{{define "CustomEncoder"}}{{.Name}}.MarshalBson(buf, {{.Tag}}){{end}}

{{define "StarEncoder"}}// {{.Type}}
if {{.Name}} == nil {
	bson.EncodePrefix(buf, bson.Null, {{.Tag}})
} else {
	{{template "Encoder" .Subfield}}
}{{end}}

{{define "SliceEncoder"}}// {{.Type}}
{
	bson.EncodePrefix(buf, bson.Array, {{.Tag}})
	lenWriter := bson.NewLenWriter(buf)
	for _i, {{.Subfield.Name}} := range {{.Name}} {
		{{template "Encoder" .Subfield}}
	}
	lenWriter.Close()
}{{end}}

{{define "MapEncoder"}}// {{.Type}}
{
	bson.EncodePrefix(buf, bson.Object, {{.Tag}})
	lenWriter := bson.NewLenWriter(buf)
	for _k, {{.Subfield.Name}} := range {{.Name}} {
		{{template "Encoder" .Subfield}}
	}
	lenWriter.Close()
}{{end}}

{{define "Encoder"}}{{if .IsPointer}}{{template "StarEncoder" .}}{{else if .IsSlice}}{{template "SliceEncoder" .}}{{else if .IsMap}}{{template "MapEncoder" .}}{{else if .IsCustom}}{{template "CustomEncoder" .}}{{else}}{{template "SimpleEncoder" .}}{{end}}{{end}}

{{define "SimpleDecoder"}}{{.Name}} = bson.{{.Decoder}}(buf, kind){{end}}

{{define "CustomDecoder"}}{{.Name}}.UnmarshalBson(buf, kind){{end}}

{{define "StarDecoder"}}// {{.Type}}
if kind != bson.Null {
	{{.Name}} = new({{.NewType}})
	{{template "Decoder" .Subfield}}
}{{end}}

{{define "SliceDecoder"}}// {{.Type}}
if kind != bson.Null {
	if kind != bson.Array {
		panic(bson.NewBsonError("unexpected kind %v for {{.Name}}", kind))
	}
	bson.Next(buf, 4)
	{{.Name}} = make({{.Type}}, 0, 8)
	for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
		bson.SkipIndex(buf)
		var {{.Subfield.Name}} {{.Subfield.Type}}
		{{template "Decoder" .Subfield}}
		{{.Name}} = append({{.Name}}, {{.Subfield.Name}})
	}
}{{end}}

{{define "MapDecoder"}}// {{.Type}}
if kind != bson.Null {
	if kind != bson.Object {
		panic(bson.NewBsonError("unexpected kind %v for {{.Name}}", kind))
	}
	bson.Next(buf, 4)
	{{.Name}} = make({{.Type}})
	for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
		_k := bson.ReadCString(buf)
		var {{.Subfield.Name}} {{.Subfield.Type}}
		{{template "Decoder" .Subfield}}
		{{.Name}}[_k] = {{.Subfield.Name}}
	}
}{{end}}

{{define "Decoder"}}{{if .IsPointer}}{{template "StarDecoder" .}}{{else if .IsSlice}}{{template "SliceDecoder" .}}{{else if .IsMap}}{{template "MapDecoder" .}}{{else if .IsCustom}}{{template "CustomDecoder" .}}{{else}}{{template "SimpleDecoder" .}}{{end}}{{end}}

{{define "Body"}}// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package {{.Package}}

// DO NOT EDIT.
// FILE GENERATED BY BSONGEN.

import (
{{range .Imports}}	{{.}}
{{end}}
)

// MarshalBson bson-encodes {{.Name}}.
func ({{.Var}} *{{.Name}}) MarshalBson(buf *bytes2.ChunkedWriter, key string) {
	bson.EncodeOptionalPrefix(buf, bson.Object, key)
	lenWriter := bson.NewLenWriter(buf)

{{range .Fields}}	{{template "Encoder" .}}
{{end}}
	lenWriter.Close()
}

// UnmarshalBson bson-decodes into {{.Name}}.
func ({{.Var}} *{{.Name}}) UnmarshalBson(buf *bytes.Buffer, kind byte) {
	switch kind {
	case bson.EOO, bson.Object:
		// valid
	case bson.Null:
		return
	default:
		panic(bson.NewBsonError("unexpected kind %v for {{.Name}}", kind))
	}
	bson.Next(buf, 4)

	for kind := bson.NextByte(buf); kind != bson.EOO; kind = bson.NextByte(buf) {
		switch bson.ReadCString(buf) {
{{range .Fields}}		case {{.Tag}}:
			{{template "Decoder" .}}
{{end}}		default:
			bson.Skip(buf, kind)
		}
	}
}
{{end}}`))
