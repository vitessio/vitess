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

package main

import (
	"errors"
	"fmt"
	"go/types"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"

	"github.com/spf13/pflag"
	"golang.org/x/tools/go/packages"
)

func main() { // nolint:funlen
	source := pflag.String("source", "../../proto/vtctlservice", "source package")
	typeName := pflag.String("type", "VtctldClient", "interface type to implement")
	implType := pflag.String("impl", "gRPCVtctldClient", "type implementing the interface")
	pkgName := pflag.String("targetpkg", "grpcvtctldclient", "package name to generate code for")
	local := pflag.Bool("local", false, "generate a local, in-process client rather than a grpcclient")
	out := pflag.String("out", "", "output destination. leave empty to use stdout")

	pflag.Parse()

	if *source == "" {
		panic("--source cannot be empty")
	}

	if *typeName == "" {
		panic("--type cannot be empty")
	}

	if *implType == "" {
		panic("--impl cannot be empty")
	}

	if *pkgName == "" {
		panic("--targetpkg cannot be empty")
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

	pkg, err := loadPackage(*source)
	if err != nil {
		panic(err)
	}

	iface, err := extractSourceInterface(pkg, *typeName)
	if err != nil {
		panic(fmt.Errorf("error getting %s in %s: %w", *typeName, *source, err))
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

		// (TODO|@amason): check which grpc lib CallOption is imported from in
		// this interface; it could be either google.golang.org/grpc or
		// github.com/golang/protobuf/grpc, although in vitess we currently
		// always use the former.

		// In the case of unary RPCs, the first result is a Pointer. In the case
		// of streaming RPCs, it is a Named type whose underlying type is an
		// Interface.
		//
		// The second result is always error.
		result := sig.Results().At(0)
		switch result.Type().(type) {
		case *types.Pointer:
			localType, localImport, pkgPath, err = extractLocalPointerType(result)
		case *types.Named:
			switch result.Type().Underlying().(type) {
			case *types.Interface:
				f.IsStreaming = true
				localType, localImport, pkgPath, err = extractLocalNamedType(result)
				if err == nil && *local {
					// We need to get the pointer type returned by `stream.Recv()`
					// in the local case for the stream adapter.
					var recvType, recvImport, recvPkgPath string
					recvType, recvImport, recvPkgPath, err = extractRecvType(result)
					if err == nil {
						f.StreamMessage = buildParam("stream", recvImport, recvType, true)
						importNames = addImport(recvImport, recvPkgPath, importNames, imports)
					}
				}
			default:
				err = fmt.Errorf("expected either pointer (for unary) or named interface (for streaming) rpc result type, got %T", result.Type().Underlying())
			}
		default:
			err = fmt.Errorf("expected either pointer (for unary) or named interface (for streaming) rpc result type, got %T", result.Type())
		}

		if err != nil {
			panic(err)
		}

		f.Result = buildParam(result.Name(), localImport, localType, !f.IsStreaming)
		importNames = addImport(localImport, pkgPath, importNames, imports)
	}

	sort.Strings(importNames)
	sort.Strings(funcNames)

	def := &ClientInterfaceDef{
		PackageName: *pkgName,
		Type:        *implType,
		ClientName:  "grpcvtctldclient",
	}

	if *local {
		def.ClientName = "localvtctldclient"
		def.Local = true
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

// ClientInterfaceDef is a struct providing enough information to generate an
// implementation of a gRPC Client interface.
type ClientInterfaceDef struct {
	PackageName string
	Type        string
	Imports     []*Import
	Methods     []*Func
	Local       bool
	ClientName  string
}

// NeedsGRPCShim returns true if the generated client code needs the internal
// grpcshim imported. Currently this is true if the client is Local and has any
// methods that are streaming RPCs.
func (def *ClientInterfaceDef) NeedsGRPCShim() bool {
	if !def.Local {
		return false
	}

	for _, m := range def.Methods {
		if m.IsStreaming {
			return true
		}
	}

	return false
}

// Import contains the meta information about a Go import.
type Import struct {
	Alias string
	Path  string
}

// Func is the variable part of a gRPC client interface method (i.e. not the
// context or dialopts arguments, or the error part of the result tuple).
type Func struct {
	Name          string
	Param         Param
	Result        Param
	IsStreaming   bool
	StreamMessage Param
}

// Param represents an element of either a parameter list or result list. It
// contains an optional name, and a package-local type. This struct exists
// purely to power template execution, which is why the Type field is simply a
// bare string.
type Param struct {
	Name string
	// locally-qualified type, e.g. "grpc.CallOption", and not "google.golang.org/grpc.CallOption".
	Type string
}

func buildParam(name string, localImport string, localType string, isPointer bool) Param {
	p := Param{
		Name: name,
		Type: fmt.Sprintf("%s.%s", localImport, localType),
	}

	if isPointer {
		p.Type = "*" + p.Type
	}

	return p
}

func addImport(localImport string, pkgPath string, importNames []string, imports map[string]string) []string {
	if _, ok := imports[localImport]; !ok {
		importNames = append(importNames, localImport)
	}

	imports[localImport] = pkgPath
	return importNames
}

func loadPackage(source string) (*packages.Package, error) {
	pkgs, err := packages.Load(&packages.Config{
		Mode: packages.NeedTypes | packages.NeedSyntax | packages.NeedTypesInfo,
	}, source)
	if err != nil {
		return nil, err
	}

	if len(pkgs) != 1 {
		return nil, errors.New("must specify exactly one package")
	}

	pkg := pkgs[0]
	if len(pkg.Errors) > 0 {
		var err error

		for _, e := range pkg.Errors {
			switch err {
			case nil:
				err = fmt.Errorf("errors loading package %s: %s", source, e.Error())
			default:
				err = fmt.Errorf("%w; %s", err, e.Error())
			}
		}

		return nil, err
	}

	return pkg, nil
}

func extractSourceInterface(pkg *packages.Package, name string) (*types.Interface, error) {
	obj := pkg.Types.Scope().Lookup(name)
	if obj == nil {
		return nil, fmt.Errorf("no symbol found with name %s", name)
	}

	switch t := obj.Type().(type) {
	case *types.Named:
		iface, ok := t.Underlying().(*types.Interface)
		if !ok {
			return nil, fmt.Errorf("symbol %s was not an interface but %T", name, t.Underlying())
		}

		return iface, nil
	case *types.Interface:
		return t, nil
	}

	return nil, fmt.Errorf("symbol %s was not an interface but %T", name, obj.Type())
}

var vitessProtoRegexp = regexp.MustCompile(`^vitess.io.*/proto/.*`)

func rewriteProtoImports(pkg *types.Package) string {
	if vitessProtoRegexp.MatchString(pkg.Path()) {
		return pkg.Name() + "pb"
	}

	return pkg.Name()
}

func extractLocalNamedType(v *types.Var) (name string, localImport string, pkgPath string, err error) {
	named, ok := v.Type().(*types.Named)
	if !ok {
		return "", "", "", fmt.Errorf("expected a named type for %s, got %v", v.Name(), v.Type())
	}

	name = named.Obj().Name()
	localImport = rewriteProtoImports(named.Obj().Pkg())
	pkgPath = named.Obj().Pkg().Path()

	return name, localImport, pkgPath, nil
}

func extractLocalPointerType(v *types.Var) (name string, localImport string, pkgPath string, err error) {
	ptr, ok := v.Type().(*types.Pointer)
	if !ok {
		return "", "", "", fmt.Errorf("expected a pointer type for %s, got %v", v.Name(), v.Type())
	}

	typ, ok := ptr.Elem().(*types.Named)
	if !ok {
		return "", "", "", fmt.Errorf("expected an underlying named type for %s, got %v", v.Name(), ptr.Elem())
	}

	name = typ.Obj().Name()
	localImport = rewriteProtoImports(typ.Obj().Pkg())
	pkgPath = typ.Obj().Pkg().Path()

	return name, localImport, pkgPath, nil
}

func extractRecvType(v *types.Var) (name string, localImport string, pkgPath string, err error) {
	named, ok := v.Type().(*types.Named)
	if !ok {
		return "", "", "", fmt.Errorf("expected a named type for %s, got %v", v.Name(), v.Type())
	}

	iface, ok := named.Underlying().(*types.Interface)
	if !ok {
		return "", "", "", fmt.Errorf("expected %s to name an interface type, got %v", v.Name(), named.Underlying())
	}

	for i := 0; i < iface.NumExplicitMethods(); i++ {
		m := iface.ExplicitMethod(i)
		if m.Name() != "Recv" {
			continue
		}

		sig, ok := m.Type().(*types.Signature)
		if !ok {
			return "", "", "", fmt.Errorf("%s.Recv should have type Signature; got %v", v.Name(), m.Type())
		}

		if sig.Results().Len() != 2 {
			return "", "", "", fmt.Errorf("%s.Recv should return two values, not %d", v.Name(), sig.Results().Len())
		}

		return extractLocalPointerType(sig.Results().At(0))
	}

	return "", "", "", fmt.Errorf("interface %s has no explicit method named Recv", named.Obj().Name())
}
