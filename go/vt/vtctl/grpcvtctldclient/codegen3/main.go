/*
Copyright 2020 The Vitess Authors.

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

package main

import (
	"errors"
	"flag"
	"fmt"
	"go/types"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"

	"golang.org/x/tools/go/packages"
)

func main() { // nolint:funlen
	source := flag.String("source", "../../proto/vtctlservice", "source package")
	typeName := flag.String("type", "VtctldClient", "interface type to implement")
	implType := flag.String("impl", "gRPCVtctldClient", "type implementing the interface")
	pkgName := flag.String("targetpkg", "grpcvtctldclient", "package name to generate code for")
	out := flag.String("out", "", "output destination. leave empty to use stdout")

	flag.Parse()

	if *source == "" {
		panic("-source cannot be empty")
	}

	if *typeName == "" {
		panic("-type cannot be empty")
	}

	if *implType == "" {
		panic("-impl cannot be empty")
	}

	if *pkgName == "" {
		panic("-targetpkg cannot be empty")
	}

	var output io.Writer = os.Stdout

	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			panic(err)
		}

		defer f.Close()
		output = f
	}

	pkgs, err := packages.Load(&packages.Config{
		Mode: packages.NeedTypes | packages.NeedSyntax | packages.NeedTypesInfo,
	}, *source)
	if err != nil {
		panic(err)
	}

	if len(pkgs) != 1 {
		panic("must specify exactly one package")
	}

	pkg := pkgs[0]
	if len(pkg.Errors) > 0 {
		var err error

		for _, e := range pkg.Errors {
			switch err {
			case nil:
				err = fmt.Errorf("errors loading package %s: %s", *source, e.Error())
			default:
				err = fmt.Errorf("%w; %s", err, e.Error())
			}
		}

		panic(err)
	}

	obj := pkg.Types.Scope().Lookup(*typeName)
	if obj == nil {
		panic(fmt.Sprintf("no symbol %s found in package %s", *typeName, *source))
	}

	var (
		iface *types.Interface
		ok    bool
	)

	switch t := obj.Type().(type) {
	case *types.Named:
		iface, ok = t.Underlying().(*types.Interface)
		if !ok {
			panic(fmt.Sprintf("symbol %s in package %s was not an interface but %T", *typeName, *source, t.Underlying()))
		}
	case *types.Interface:
		iface = t
	default:
		panic(fmt.Sprintf("symbol %s in package %s was not an interface but %T", *typeName, *source, t))
	}

	imports := map[string]string{
		"context": "context",
	}
	importNames := []string{}
	funcs := make(map[string]*Func, iface.NumExplicitMethods())
	funcNames := make([]string, iface.NumExplicitMethods())

	for i := 0; i < iface.NumExplicitMethods(); i++ {
		m := iface.ExplicitMethod(i)
		funcNames[i] = m.Name()

		sig, ok := m.Type().(*types.Signature)
		if !ok {
			panic(fmt.Sprintf("could not derive signature from method %s, have %T", m.FullName(), m.Type()))
		}

		if sig.Params().Len() != 3 {
			panic(fmt.Sprintf("all methods in a grpc client interface should have exactly 3 params; found\n=> %s", sig))
		}

		if sig.Results().Len() != 2 {
			panic(fmt.Sprintf("all methods in a grpc client interface should have exactly 2 results; found\n=> %s", sig))
		}

		f := &Func{
			Name: m.Name(),
		}
		funcs[f.Name] = f

		fmt.Println(sig.Params().String())

		// The first parameter is always context.Context. The third parameter is
		// always a ...grpc.CallOption.
		param := sig.Params().At(1)

		localType, localImport, pkgPath, err := extractLocalPointerType(param)
		if err != nil {
			panic(err)
		}

		f.Param.Name = param.Name()
		f.Param.Type = "*" + localImport + "." + localType

		if _, ok := imports[localImport]; !ok {
			importNames = append(importNames, localImport)
		}

		imports[localImport] = pkgPath

		// TODO: check which grpc lib CallOption is imported from in this
		// interface; it could be either google.golang.org/grpc or
		// github.com/golang/protobuf/grpc.

		// The second result is always error.
		result := sig.Results().At(0)

		localType, localImport, pkgPath, err = extractLocalPointerType(result) // TODO: does not work for streaming rpcs
		if err != nil {
			panic(err)
		}

		f.Result.Name = result.Name()
		f.Result.Type = "*" + localImport + "." + localType

		if _, ok := imports[localImport]; !ok {
			importNames = append(importNames, localImport)
		}

		imports[localImport] = pkgPath
	}

	sort.Strings(importNames)
	sort.Strings(funcNames)

	def := &ClientInterfaceDef{
		PackageName: *pkgName,
		Type:        *implType,
	}

	for _, name := range importNames {
		imp := &Import{
			Path: imports[name],
		}

		if filepath.Base(imp.Path) != name {
			imp.Alias = name
		}

		def.Imports = append(def.Imports, imp)
	}

	for _, name := range funcNames {
		def.Methods = append(def.Methods, funcs[name])
	}

	if err := tmpl.Execute(output, def); err != nil {
		panic(err)
	}
}

type ClientInterfaceDef struct {
	PackageName string
	Type        string
	Imports     []*Import
	Methods     []*Func
}

type Import struct {
	Alias string
	Path  string
}

type Func struct {
	Name   string
	Param  Param
	Result Param
}

type Param struct {
	Name string
	// locally-qualified type, e.g. "grpc.CallOption", and not "google.golang.org/grpc.CallOption".
	Type string
}

var vitessProtoRegexp = regexp.MustCompile(`^vitess.io.*/proto/.*`)

func rewriteProtoImports(pkg *types.Package) string {
	if vitessProtoRegexp.MatchString(pkg.Path()) {
		return pkg.Name() + "pb"
	}

	return pkg.Name()
}

func extractLocalPointerType(v *types.Var) (string, string, string, error) {
	ptr, ok := v.Type().(*types.Pointer)
	if !ok {
		return "", "", "", errors.New("TODO: provide details")
	}

	typ, ok := ptr.Elem().(*types.Named)
	if !ok {
		return "", "", "", errors.New("TODO: provide details")
	}

	return typ.Obj().Name(), rewriteProtoImports(typ.Obj().Pkg()), typ.Obj().Pkg().Path(), nil
}
