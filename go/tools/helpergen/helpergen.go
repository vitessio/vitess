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

type helpergen struct {
	DebugTypes bool
	mod        *packages.Module
	sizes      types.Sizes
	codegen    map[string]*codeFile
	known      map[*types.Named]*typeState
	out        output
}

type output interface {
	finalizeFile(file *codeFile, out *jen.File)
	implForStruct(file *codeFile, name *types.TypeName, st *types.Struct, sizes types.Sizes, debugTypes bool) (jen.Code, codeFlag)
	fileName() string
	isEmpty(file *codeFile) bool
}

type typeLookup interface {
	getKnownType(named *types.Named) *typeState
}

type codeFlag uint32

const (
	codeWithInterface = 1 << 0
	codeWithUnsafe    = 1 << 1
)

type codeImpl struct {
	name  string
	flags codeFlag
	code  jen.Code
}

type codeFile struct {
	pkg   string
	state interface{}
}

type typeState struct {
	generated bool
	local     bool
	pod       bool // struct with only primitives
}

func newHelperGen(mod *packages.Module, sizes types.Sizes, outputs output) *helpergen {
	return &helpergen{
		DebugTypes: true,
		mod:        mod,
		sizes:      sizes,
		known:      make(map[*types.Named]*typeState),
		codegen:    make(map[string]*codeFile),
		out:        outputs,
	}
}

func isPod(tt types.Type) bool {
	switch tt := tt.(type) {
	case *types.Struct:
		for i := 0; i < tt.NumFields(); i++ {
			if !isPod(tt.Field(i).Type()) {
				return false
			}
		}
		return true

	case *types.Basic:
		switch tt.Kind() {
		case types.String, types.UnsafePointer:
			return false
		}
		return true

	default:
		return false
	}
}

func (gen *helpergen) getKnownType(named *types.Named) *typeState {
	ts := gen.known[named]
	if ts == nil {
		local := strings.HasPrefix(named.Obj().Pkg().Path(), gen.mod.Path)
		ts = &typeState{
			local: local,
			pod:   isPod(named.Underlying()),
		}
		gen.known[named] = ts
	}
	return ts
}

func (gen *helpergen) generateType(pkg *types.Package, file *codeFile, named *types.Named) {
	ts := gen.getKnownType(named)
	if ts.generated {
		return
	}
	log.Printf("generating for type %s", named.String())
	ts.generated = true

	switch tt := named.Underlying().(type) {
	case *types.Struct:
		gen.out.implForStruct(file, named.Obj(), tt, gen.sizes, gen.DebugTypes)

	case *types.Interface:
		findImplementations(pkg.Scope(), tt, func(tt types.Type) {
			if _, isStruct := tt.Underlying().(*types.Struct); isStruct {
				gen.generateType(pkg, file, tt.(*types.Named))
			}
		})
	default:
		// no-op
	}
}

func (gen *helpergen) generateKnownType(named *types.Named) {
	pkgInfo := named.Obj().Pkg()
	file := gen.codegen[pkgInfo.Path()]
	if file == nil {
		file = &codeFile{pkg: pkgInfo.Name()}
		gen.codegen[pkgInfo.Path()] = file
	}

	gen.generateType(pkgInfo, file, named)
}

func findImplementations(scope *types.Scope, iff *types.Interface, impl func(types.Type)) {
	for _, name := range scope.Names() {
		obj := scope.Lookup(name)
		baseType := obj.Type()
		if types.Implements(baseType, iff) || types.Implements(types.NewPointer(baseType), iff) {
			impl(baseType)
		}
	}
}

func (gen *helpergen) generateKnownInterface(pkg *types.Package, iff *types.Interface) {
	findImplementations(pkg.Scope(), iff, func(tt types.Type) {
		if named, ok := tt.(*types.Named); ok {
			gen.generateKnownType(named)
		}
	})
}

func (gen *helpergen) finalize() map[string]*jen.File {
	var complete bool

	for !complete {
		complete = true
		for tt, ts := range gen.known {
			isComplex := !ts.pod
			notYetGenerated := !ts.generated
			if ts.local && isComplex && notYetGenerated {
				gen.generateKnownType(tt)
				complete = false
			}
		}
	}

	outputFiles := make(map[string]*jen.File)

	for pkg, file := range gen.codegen {
		if gen.out.isEmpty(file) {
			continue
		}
		if !strings.HasPrefix(pkg, gen.mod.Path) {
			log.Printf("failed to generate code for foreign package '%s'", pkg)
			log.Printf("DEBUG:\n%#v", file)
			continue
		}

		out := jen.NewFile(file.pkg)
		out.HeaderComment(licenseFileHeader)
		out.HeaderComment("Code generated by Sizegen. DO NOT EDIT.")

		gen.out.finalizeFile(file, out)

		fullPath := path.Join(gen.mod.Dir, strings.TrimPrefix(pkg, gen.mod.Path), gen.out.fileName())
		outputFiles[fullPath] = out
	}

	return outputFiles
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
	var generate typePaths
	var verify bool

	flag.Var(&patterns, "in", "Go packages to load the generator")
	flag.Var(&generate, "gen", "Typename of the Go struct to generate size info for")
	flag.BoolVar(&verify, "verify", false, "ensure that the generated files are correct")
	flag.Parse()

	if len(patterns) == 0 || len(generate) == 0 {
		log.Fatalf("not enough arguments")
	}

	result, err := GenerateHelpers(patterns, generate, newCachedSize)
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
				log.Fatalf("filed to save file to '%s': %v", fullPath, err)
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

type outputCreator func(typeLookup) output

// GenerateHelpers generates the auxiliary code that implements helper methods
// for all the types listed in typePatterns
func GenerateHelpers(packagePatterns []string, typePatterns []string, creator outputCreator) (map[string]*jen.File, error) {
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesSizes | packages.NeedTypesInfo | packages.NeedDeps | packages.NeedImports | packages.NeedModule,
		Logf: log.Printf,
	}, packagePatterns...)

	if err != nil {
		return nil, err
	}

	helpergen := newHelperGen(loaded[0].Module, loaded[0].TypesSizes, nil)
	helpergen.out = creator(helpergen)

	scopes := make(map[string]*types.Scope)
	for _, pkg := range loaded {
		scopes[pkg.PkgPath] = pkg.Types.Scope()
	}

	for _, pattern := range typePatterns {
		pos := strings.LastIndexByte(pattern, '.')
		if pos < 0 {
			return nil, fmt.Errorf("unexpected input type: %s", pattern)
		}

		pkgname := pattern[:pos]
		typename := pattern[pos+1:]

		scope := scopes[pkgname]
		if scope == nil {
			return nil, fmt.Errorf("no scope found for type '%s'", pattern)
		}

		if typename == "*" {
			for _, name := range scope.Names() {
				helpergen.generateKnownType(scope.Lookup(name).Type().(*types.Named))
			}
		} else {
			tt := scope.Lookup(typename)
			if tt == nil {
				return nil, fmt.Errorf("no type called '%s' found in '%s'", typename, pkgname)
			}

			helpergen.generateKnownType(tt.Type().(*types.Named))
		}
	}

	return helpergen.finalize(), nil
}
