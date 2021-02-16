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

type astHelperGen struct {
	DebugTypes bool
	mod        *packages.Module
	sizes      types.Sizes
	namedIface *types.Named
	iface      *types.Interface
}

func newGenerator(mod *packages.Module, sizes types.Sizes, named *types.Named) *astHelperGen {
	return &astHelperGen{
		DebugTypes: true,
		mod:        mod,
		sizes:      sizes,
		namedIface: named,
		iface:      named.Underlying().(*types.Interface),
	}
}

func findImplementations(scope *types.Scope, iff *types.Interface, impl func(types.Type) error) error {
	for _, name := range scope.Names() {
		obj := scope.Lookup(name)
		baseType := obj.Type()
		if types.Implements(baseType, iff) {
			err := impl(baseType)
			if err != nil {
				return err
			}
		}
		pointerT := types.NewPointer(baseType)
		if types.Implements(pointerT, iff) {
			err := impl(pointerT)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (gen *astHelperGen) doIt() (map[string]*jen.File, error) {
	pkg := gen.namedIface.Obj().Pkg()

	rewriter := newRewriterGen(func(t types.Type) bool {
		return types.Implements(t, gen.iface)
	}, gen.namedIface.Obj().Name())

	iface, ok := gen.iface.Underlying().(*types.Interface)
	if !ok {
		return nil, fmt.Errorf("expected interface, but got %T", gen.iface)
	}

	err := findImplementations(pkg.Scope(), iface, func(t types.Type) error {
		switch n := t.Underlying().(type) {
		//case *types.Struct:
		//	named := t.(*types.Named)
		//	return rewriter.visitStruct(t, types.TypeString(t, noQualifier), named.Obj().Name(), n)
		case *types.Pointer:
			strct := n.Elem().Underlying().(*types.Struct)
			named := t.(*types.Pointer).Elem().(*types.Named)
			return rewriter.visitStruct(t, types.TypeString(t, noQualifier), named.Obj().Name(), strct)
		case *types.Interface:

		default:
			fmt.Printf("unknown %T\n", t)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	result := map[string]*jen.File{}
	fullPath := path.Join(gen.mod.Dir, strings.TrimPrefix(pkg.Path(), gen.mod.Path), "rewriter.go")
	result[fullPath] = rewriter.createFile(pkg.Name())

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
	var generate string
	var verify bool

	flag.Var(&patterns, "in", "Go packages to load the generator")
	flag.StringVar(&generate, "iface", "", "Root interface generate rewriter for")
	flag.BoolVar(&verify, "verify", false, "ensure that the generated files are correct")
	flag.Parse()

	result, err := GenerateASTHelpers(patterns, generate)
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

// GenerateASTHelpers generates the auxiliary code that implements CachedSize helper methods
// for all the types listed in typePatterns
func GenerateASTHelpers(packagePatterns []string, rootIface string) (map[string]*jen.File, error) {
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

	generator := newGenerator(loaded[0].Module, loaded[0].TypesSizes, nt)
	it, err := generator.doIt()
	if err != nil {
		return nil, err
	}

	return it, nil
}
