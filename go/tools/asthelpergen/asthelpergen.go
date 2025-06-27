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

// Package asthelpergen provides code generation for AST (Abstract Syntax Tree) helper methods.
//
// This package automatically generates helper methods for AST nodes including:
//   - Deep cloning (Clone methods)
//   - Equality comparison (Equals methods)
//   - Visitor pattern support (Visit methods)
//   - AST rewriting/transformation (Rewrite methods)
//   - Path enumeration for navigation
//   - Copy-on-write functionality
//
// The generator works by discovering all types that implement a root interface and
// then generating the appropriate helper methods for each type using a plugin architecture.
//
// Usage:
//
//	result, err := asthelpergen.GenerateASTHelpers(&asthelpergen.Options{
//	    Packages:      []string{"./mypackage"},
//	    RootInterface: "github.com/example/mypackage.MyASTInterface",
//	})
//
// The generated code follows Go conventions and includes proper error handling,
// nil checks, and type safety.
package asthelpergen

import (
	"bytes"
	"fmt"
	"go/types"
	"os"
	"path"
	"strings"

	"github.com/dave/jennifer/jen"
	"golang.org/x/tools/go/packages"

	"vitess.io/vitess/go/textutil"
	"vitess.io/vitess/go/tools/codegen"
)

const (
	// Common constants used across generators
	visitableName = "Visitable"
	anyTypeName   = "any"

	// License header for generated files
	licenseFileHeader = `Copyright 2025 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.`
)

type (
	// generatorSPI provides services to individual generators during code generation.
	// It acts as a service provider interface, giving generators access to type discovery
	// and scope information needed for generating helper methods.
	generatorSPI interface {
		// addType adds a newly discovered type to the processing queue
		addType(t types.Type)
		// scope returns the type scope for finding implementations
		scope() *types.Scope
		// findImplementations finds all types that implement the given interface
		findImplementations(iff *types.Interface, impl func(types.Type) error) error
		// iface returns the root interface that all nodes are expected to implement
		iface() *types.Interface
	}

	// generator defines the interface that all specialized generators must implement.
	// Each generator handles specific types of Go constructs (structs, interfaces, etc.)
	// and produces the appropriate helper methods for those types.
	generator interface {
		// genFile generates the final output file for this generator
		genFile(generatorSPI) (string, *jen.File)
		// interfaceMethod handles interface types with type switching logic
		interfaceMethod(t types.Type, iface *types.Interface, spi generatorSPI) error
		// structMethod handles struct types with field iteration
		structMethod(t types.Type, strct *types.Struct, spi generatorSPI) error
		// ptrToStructMethod handles pointer-to-struct types
		ptrToStructMethod(t types.Type, strct *types.Struct, spi generatorSPI) error
		// ptrToBasicMethod handles pointer-to-basic types (e.g., *int, *string)
		ptrToBasicMethod(t types.Type, basic *types.Basic, spi generatorSPI) error
		// sliceMethod handles slice types with element processing
		sliceMethod(t types.Type, slice *types.Slice, spi generatorSPI) error
		// basicMethod handles basic types (int, string, bool, etc.)
		basicMethod(t types.Type, basic *types.Basic, spi generatorSPI) error
	}

	// astHelperGen is the main orchestrator that coordinates the code generation process.
	// It discovers implementations of a root interface and uses multiple specialized
	// generators to produce helper methods for all discovered types.
	astHelperGen struct {
		// DebugTypes enables debug output for type processing
		DebugTypes bool
		// mod is the Go module information for path resolution
		mod *packages.Module
		// sizes provides platform-specific type size information
		sizes types.Sizes
		// namedIface is the root interface type for which helpers are generated
		namedIface *types.Named
		// _iface is the underlying interface type
		_iface *types.Interface
		// gens is the list of specialized generators (clone, equals, visit, etc.)
		gens []generator

		// _scope is the type scope for finding implementations
		_scope *types.Scope
		// todo is the queue of types that need to be processed
		todo []types.Type
	}
)

func (gen *astHelperGen) iface() *types.Interface {
	return gen._iface
}

// newGenerator creates a new AST helper generator with the specified configuration.
//
// Parameters:
//   - mod: Go module information for path resolution
//   - sizes: Platform-specific type size information
//   - named: The root interface type for which helpers will be generated
//   - generators: Specialized generators for different helper types (clone, equals, etc.)
//
// Returns a configured astHelperGen ready to process types and generate code.
func newGenerator(mod *packages.Module, sizes types.Sizes, named *types.Named, generators ...generator) *astHelperGen {
	return &astHelperGen{
		DebugTypes: true,
		mod:        mod,
		sizes:      sizes,
		namedIface: named,
		_iface:     named.Underlying().(*types.Interface),
		gens:       generators,
	}
}

func findImplementations(scope *types.Scope, iff *types.Interface, impl func(types.Type) error) error {
	const onlyReferences = false

	for _, name := range scope.Names() {
		obj := scope.Lookup(name)
		if _, ok := obj.(*types.TypeName); !ok {
			continue
		}
		baseType := obj.Type()
		if types.Implements(baseType, iff) {
			if onlyReferences {
				switch tt := baseType.Underlying().(type) {
				case *types.Interface:
					// This is OK; interfaces are references
				default:
					return fmt.Errorf("interface %s implemented by %s (%s as %T) without ptr", iff.String(), baseType, tt.String(), tt)
				}
			}
			if types.TypeString(baseType, noQualifier) == visitableName {
				// skip the visitable interface
				continue
			}
			if err := impl(baseType); err != nil {
				return err
			}
			continue
		}
		pointerT := types.NewPointer(baseType)
		if types.Implements(pointerT, iff) {
			if err := impl(pointerT); err != nil {
				return err
			}
			continue
		}
	}
	return nil
}

func (gen *astHelperGen) findImplementations(iff *types.Interface, impl func(types.Type) error) error {
	return findImplementations(gen._scope, iff, impl)
}

// GenerateCode is the main loop where we build up the code per file.
func (gen *astHelperGen) GenerateCode() (map[string]*jen.File, error) {
	pkg := gen.namedIface.Obj().Pkg()

	gen._scope = pkg.Scope()
	gen.todo = append(gen.todo, gen.namedIface)
	jenFiles, err := gen.createFiles()
	if err != nil {
		return nil, err
	}

	result := map[string]*jen.File{}
	for fName, genFile := range jenFiles {
		fullPath := path.Join(gen.mod.Dir, strings.TrimPrefix(pkg.Path(), gen.mod.Path), fName)
		result[fullPath] = genFile
	}

	return result, nil
}

// VerifyFilesOnDisk compares the generated results from the codegen against the files that
// currently exist on disk and returns any mismatches
func VerifyFilesOnDisk(result map[string]*jen.File) (errors []error) {
	for fullPath, file := range result {
		existing, err := os.ReadFile(fullPath)
		if err != nil {
			errors = append(errors, fmt.Errorf("missing file on disk: %s (%w)", fullPath, err))
			continue
		}

		genFile, err := codegen.FormatJenFile(file)
		if err != nil {
			errors = append(errors, fmt.Errorf("goimport error: %w", err))
			continue
		}

		if !bytes.Equal(existing, genFile) {
			errors = append(errors, fmt.Errorf("'%s' has changed", fullPath))
			continue
		}
	}
	return errors
}

// Options configures the AST helper generation process.
type Options struct {
	// Packages specifies the Go packages to analyze for AST types.
	// Can be package paths like "./mypackage" or import paths like "github.com/example/ast".
	Packages []string

	// RootInterface is the fully qualified name of the root interface that all AST nodes implement.
	// Format: "package.path.InterfaceName" (e.g., "github.com/example/ast.Node")
	RootInterface string

	// Clone configures the clone generator options
	Clone CloneOptions

	// Equals configures the equality comparison generator options
	Equals EqualsOptions
}

// GenerateASTHelpers is the main entry point for generating AST helper methods.
//
// It loads the specified packages, analyzes the types that implement the root interface,
// and generates comprehensive helper methods including clone, equals, visit, rewrite,
// path enumeration, and copy-on-write functionality.
//
// The function returns a map where keys are file paths and values are the generated
// Go source files. The caller is responsible for writing these files to disk.
//
// Returns an error if package loading fails, the root interface cannot be found,
// or code generation encounters any issues.
func GenerateASTHelpers(options *Options) (map[string]*jen.File, error) {
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesSizes | packages.NeedTypesInfo | packages.NeedDeps | packages.NeedImports | packages.NeedModule,
	}, options.Packages...)

	if err != nil {
		return nil, fmt.Errorf("failed to load packages: %w", err)
	}

	if err := codegen.CheckErrors(loaded, codegen.GeneratedInSqlparser); err != nil {
		return nil, err
	}

	scopes := make(map[string]*types.Scope)
	for _, pkg := range loaded {
		scopes[pkg.PkgPath] = pkg.Types.Scope()
	}

	tt, err := findTypeObject(options.RootInterface, scopes)
	if err != nil {
		return nil, err
	}

	nt := tt.Type().(*types.Named)
	pName := nt.Obj().Pkg().Name()
	ifaceName := types.TypeString(nt, noQualifier)
	generator := newGenerator(loaded[0].Module, loaded[0].TypesSizes, nt,
		newEqualsGen(pName, &options.Equals),
		newCloneGen(pName, &options.Clone),
		newPathGen(pName, ifaceName),
		newVisitGen(pName, ifaceName),
		newRewriterGen(pName, ifaceName),
		newCOWGen(pName, nt),
	)

	it, err := generator.GenerateCode()
	if err != nil {
		return nil, err
	}

	return it, nil
}

// findTypeObject finds the types.Object for the given interface from the given scopes.
func findTypeObject(interfaceToFind string, scopes map[string]*types.Scope) (types.Object, error) {
	pos := strings.LastIndexByte(interfaceToFind, '.')
	if pos < 0 {
		return nil, fmt.Errorf("unexpected input type: %s", interfaceToFind)
	}

	pkgname := interfaceToFind[:pos]
	typename := interfaceToFind[pos+1:]

	scope := scopes[pkgname]
	if scope == nil {
		return nil, fmt.Errorf("no scope found for type '%s'", interfaceToFind)
	}

	tt := scope.Lookup(typename)
	if tt == nil {
		return nil, fmt.Errorf("no type called '%s' found in '%s'", typename, pkgname)
	}
	return tt, nil
}

var _ generatorSPI = (*astHelperGen)(nil)

func (gen *astHelperGen) scope() *types.Scope {
	return gen._scope
}

func (gen *astHelperGen) addType(t types.Type) {
	gen.todo = append(gen.todo, t)
}

func (gen *astHelperGen) createFiles() (map[string]*jen.File, error) {
	if err := gen.processTypeQueue(); err != nil {
		return nil, err
	}
	return gen.generateOutputFiles(), nil
}

// processTypeQueue processes all types in the todo queue with all generators
func (gen *astHelperGen) processTypeQueue() error {
	alreadyDone := map[string]bool{}
	for len(gen.todo) > 0 {
		t := gen.todo[0]
		typeName := printableTypeName(t)
		gen.todo = gen.todo[1:]

		if alreadyDone[typeName] {
			continue
		}

		if err := gen.processTypeWithGenerators(t); err != nil {
			return fmt.Errorf("failed to process type %s: %w", typeName, err)
		}
		alreadyDone[typeName] = true
	}
	return nil
}

// processTypeWithGenerators dispatches a type to all generators based on its underlying type
func (gen *astHelperGen) processTypeWithGenerators(t types.Type) error {
	underlying := t.Underlying()
	typeName := printableTypeName(t)

	for _, g := range gen.gens {
		var err error
		switch underlying := underlying.(type) {
		case *types.Interface:
			err = g.interfaceMethod(t, underlying, gen)
		case *types.Slice:
			err = g.sliceMethod(t, underlying, gen)
		case *types.Struct:
			err = g.structMethod(t, underlying, gen)
		case *types.Pointer:
			err = gen.handlePointerType(t, underlying, g)
		case *types.Basic:
			err = g.basicMethod(t, underlying, gen)
		default:
			return fmt.Errorf("don't know how to handle type %s %T", typeName, underlying)
		}
		if err != nil {
			return fmt.Errorf("generator failed for type %s: %w", typeName, err)
		}
	}
	return nil
}

// handlePointerType handles pointer types by dispatching to the appropriate method
func (gen *astHelperGen) handlePointerType(t types.Type, ptr *types.Pointer, g generator) error {
	ptrToType := ptr.Elem().Underlying()
	switch ptrToType := ptrToType.(type) {
	case *types.Struct:
		return g.ptrToStructMethod(t, ptrToType, gen)
	case *types.Basic:
		return g.ptrToBasicMethod(t, ptrToType, gen)
	default:
		return fmt.Errorf("unsupported pointer type %T", ptrToType)
	}
}

// generateOutputFiles collects the generated files from all generators
func (gen *astHelperGen) generateOutputFiles() map[string]*jen.File {
	result := map[string]*jen.File{}
	for _, g := range gen.gens {
		fName, jenFile := g.genFile(gen)
		result[fName] = jenFile
	}
	return result
}

// noQualifier is used to print types without package qualifiers
var noQualifier = func(*types.Package) string { return "" }

// printableTypeName returns a string that can be used as a valid golang identifier
func printableTypeName(t types.Type) string {
	switch t := t.(type) {
	case *types.Pointer:
		return "RefOf" + printableTypeName(t.Elem())
	case *types.Slice:
		return "SliceOf" + printableTypeName(t.Elem())
	case *types.Named:
		return t.Obj().Name()
	case *types.Basic:
		return textutil.Title(t.Name())
	case *types.Interface:
		return t.String()
	default:
		panic(fmt.Sprintf("unknown type %T %v", t, t))
	}
}
