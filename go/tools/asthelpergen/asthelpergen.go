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
	"bytes"
	"flag"
	"fmt"
	"go/types"
	"io/ioutil"
	"log"
	"path"
	"strings"

	"github.com/dave/jennifer/jen"
	"golang.org/x/tools/go/packages"
)

const licenseFileHeader = `Copyright 2021 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.`

type generator interface {
	visitStruct(t types.Type, stroct *types.Struct) error
	visitInterface(t types.Type, iface *types.Interface) error
	visitSlice(t types.Type, slice *types.Slice) error
	createFile(pkgName string) (string, *jen.File)
}

// astHelperGen finds implementations of the given interface,
// and uses the supplied `generator`s to produce the output code
type astHelperGen struct {
	DebugTypes bool
	mod        *packages.Module
	sizes      types.Sizes
	namedIface *types.Named
	iface      *types.Interface
	gens       []generator
}

func newGenerator(mod *packages.Module, sizes types.Sizes, named *types.Named, generators ...generator) *astHelperGen {
	return &astHelperGen{
		DebugTypes: true,
		mod:        mod,
		sizes:      sizes,
		namedIface: named,
		iface:      named.Underlying().(*types.Interface),
		gens:       generators,
	}
}

func findImplementations(scope *types.Scope, iff *types.Interface, impl func(types.Type) error) error {
	for _, name := range scope.Names() {
		obj := scope.Lookup(name)
		if _, ok := obj.(*types.TypeName); !ok {
			continue
		}
		baseType := obj.Type()
		if types.Implements(baseType, iff) {
			err := impl(baseType)
			if err != nil {
				return err
			}
			continue
		}
		pointerT := types.NewPointer(baseType)
		if types.Implements(pointerT, iff) {
			err := impl(pointerT)
			if err != nil {
				return err
			}
			continue
		}
	}
	return nil
}

func (gen *astHelperGen) visitStruct(t types.Type, stroct *types.Struct) error {
	for _, g := range gen.gens {
		err := g.visitStruct(t, stroct)
		if err != nil {
			return err
		}
	}
	return nil
}

func (gen *astHelperGen) visitSlice(t types.Type, slice *types.Slice) error {
	for _, g := range gen.gens {
		err := g.visitSlice(t, slice)
		if err != nil {
			return err
		}
	}
	return nil
}

func (gen *astHelperGen) visitInterface(t types.Type, iface *types.Interface) error {
	for _, g := range gen.gens {
		err := g.visitInterface(t, iface)
		if err != nil {
			return err
		}
	}
	return nil
}

// GenerateCode is the main loop where we build up the code per file.
func (gen *astHelperGen) GenerateCode() (map[string]*jen.File, error) {
	pkg := gen.namedIface.Obj().Pkg()
	iface, ok := gen.iface.Underlying().(*types.Interface)
	if !ok {
		return nil, fmt.Errorf("expected interface, but got %T", gen.iface)
	}

	err := findImplementations(pkg.Scope(), iface, func(t types.Type) error {
		switch n := t.Underlying().(type) {
		case *types.Struct:
			return gen.visitStruct(t, n)
		case *types.Slice:
			return gen.visitSlice(t, n)
		case *types.Pointer:
			strct, isStrct := n.Elem().Underlying().(*types.Struct)
			if isStrct {
				return gen.visitStruct(t, strct)
			}
		case *types.Interface:
			return gen.visitInterface(t, n)
		default:
			// do nothing
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	result := map[string]*jen.File{}
	for _, g := range gen.gens {
		file, code := g.createFile(pkg.Name())
		fullPath := path.Join(gen.mod.Dir, strings.TrimPrefix(pkg.Path(), gen.mod.Path), file)
		result[fullPath] = code
	}

	return result, nil
}

type typePaths []string

func (t *typePaths) String() string {
	return fmt.Sprintf("%v", *t)
}

func (t *typePaths) Set(path string) error {
	*t = append(*t, path)
	return nil
}

func main() {
	var patterns typePaths
	var generate, except string
	var verify bool

	flag.Var(&patterns, "in", "Go packages to load the generator")
	flag.StringVar(&generate, "iface", "", "Root interface generate rewriter for")
	flag.BoolVar(&verify, "verify", false, "ensure that the generated files are correct")
	flag.StringVar(&except, "except", "", "don't deep clone these types")
	flag.Parse()

	result, err := GenerateASTHelpers(patterns, generate, except)
	if err != nil {
		log.Fatal(err)
	}

	if verify {
		for _, err := range VerifyFilesOnDisk(result) {
			log.Fatal(err)
		}
		log.Printf("%d files OK", len(result))
	} else {
		for fullPath, file := range result {
			if err := file.Save(fullPath); err != nil {
				log.Fatalf("failed to save file to '%s': %v", fullPath, err)
			}
			log.Printf("saved '%s'", fullPath)
		}
	}
}

// VerifyFilesOnDisk compares the generated results from the codegen against the files that
// currently exist on disk and returns any mismatches
func VerifyFilesOnDisk(result map[string]*jen.File) (errors []error) {
	for fullPath, file := range result {
		existing, err := ioutil.ReadFile(fullPath)
		if err != nil {
			errors = append(errors, fmt.Errorf("missing file on disk: %s (%w)", fullPath, err))
			continue
		}

		var buf bytes.Buffer
		if err := file.Render(&buf); err != nil {
			errors = append(errors, fmt.Errorf("render error for '%s': %w", fullPath, err))
			continue
		}

		if !bytes.Equal(existing, buf.Bytes()) {
			errors = append(errors, fmt.Errorf("'%s' has changed", fullPath))
			continue
		}
	}
	return errors
}

// GenerateASTHelpers loads the input code, constructs the necessary generators,
// and generates the rewriter and clone methods for the AST
func GenerateASTHelpers(packagePatterns []string, rootIface, exceptCloneType string) (map[string]*jen.File, error) {
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesSizes | packages.NeedTypesInfo | packages.NeedDeps | packages.NeedImports | packages.NeedModule,
		Logf: log.Printf,
	}, packagePatterns...)

	if err != nil {
		return nil, err
	}

	scopes := make(map[string]*types.Scope)
	for _, pkg := range loaded {
		scopes[pkg.PkgPath] = pkg.Types.Scope()
	}

	pos := strings.LastIndexByte(rootIface, '.')
	if pos < 0 {
		return nil, fmt.Errorf("unexpected input type: %s", rootIface)
	}

	pkgname := rootIface[:pos]
	typename := rootIface[pos+1:]

	scope := scopes[pkgname]
	if scope == nil {
		return nil, fmt.Errorf("no scope found for type '%s'", rootIface)
	}

	tt := scope.Lookup(typename)
	if tt == nil {
		return nil, fmt.Errorf("no type called '%s' found in '%s'", typename, pkgname)
	}

	nt := tt.Type().(*types.Named)

	iface := nt.Underlying().(*types.Interface)

	interestingType := func(t types.Type) bool {
		return types.Implements(t, iface)
	}
	rewriter := newRewriterGen(interestingType, nt.Obj().Name())
	clone := newCloneGen(iface, scope, exceptCloneType)

	generator := newGenerator(loaded[0].Module, loaded[0].TypesSizes, nt, rewriter, clone)
	it, err := generator.GenerateCode()
	if err != nil {
		return nil, err
	}

	return it, nil
}
