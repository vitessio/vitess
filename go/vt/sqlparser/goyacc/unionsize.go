package main

import (
	"fmt"
	"go/types"
	"os"
	"path/filepath"

	"golang.org/x/tools/go/packages"
)

// inferUnionDataWords loads the package containing the grammar, looks up
// each type declared in %struct and %union, and returns the number of
// pointer-sized words needed to hold the largest one.
//
// This allows goyacc to automatically size the data array without a
// manual --union-data-words flag.
func inferUnionDataWords() (int, error) {
	// Collect all type names from the grammar's %struct/%union declarations.
	var typeNames []string
	for _, gt := range gotypes {
		typeNames = append(typeNames, gt.typename)
	}
	if len(typeNames) == 0 {
		return 0, nil
	}

	// Determine the package path from the output file location.
	absOutput, err := filepath.Abs(oflag)
	if err != nil {
		return 0, fmt.Errorf("inferUnionDataWords: %w", err)
	}
	pkgDir := filepath.Dir(absOutput)

	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesSizes,
		Dir:  pkgDir,
	}, ".")
	if err != nil {
		return 0, fmt.Errorf("inferUnionDataWords: failed to load package: %w", err)
	}
	// Ignore errors — the package may not fully compile (gram.go is being regenerated).
	if len(loaded) == 0 {
		return 0, fmt.Errorf("inferUnionDataWords: %s", "no packages loaded")
	}
	pkg := loaded[0]
	if pkg.Types == nil {
		return 0, fmt.Errorf("inferUnionDataWords: %s", "package types not available (package may have errors)")
	}

	sizes := pkg.TypesSizes
	scope := pkg.Types.Scope()
	ptrSize := sizes.Sizeof(types.Typ[types.UnsafePointer])

	var maxSize int64
	for _, name := range typeNames {
		obj := scope.Lookup(name)
		if obj == nil {
			// Might be a built-in (string, bool, int) or a pointer/slice type.
			t, err := evalType(pkg.Types, name)
			if err != nil {
				fmt.Fprintf(os.Stderr, "inferUnionDataWords: cannot resolve type %q: %v\n", name, err)
				continue
			}
			s := sizes.Sizeof(t)
			if s > maxSize {
				maxSize = s
			}
			continue
		}
		s := sizes.Sizeof(obj.Type())
		if s > maxSize {
			maxSize = s
		}
	}

	if maxSize == 0 {
		return 0, fmt.Errorf("inferUnionDataWords: %s", "could not determine any type sizes")
	}

	words := (maxSize + ptrSize - 1) / ptrSize
	return int(words), nil
}

// evalType parses a Go type expression like "*String", "[]Expr", "bool"
// in the context of the given package.
func evalType(pkg *types.Package, expr string) (types.Type, error) {
	// Handle pointer types
	if len(expr) > 0 && expr[0] == '*' {
		inner, err := evalType(pkg, expr[1:])
		if err != nil {
			return nil, err
		}
		return types.NewPointer(inner), nil
	}

	// Handle slice types
	if len(expr) > 2 && expr[0] == '[' && expr[1] == ']' {
		inner, err := evalType(pkg, expr[2:])
		if err != nil {
			return nil, err
		}
		return types.NewSlice(inner), nil
	}

	// Built-in types
	switch expr {
	case "string":
		return types.Typ[types.String], nil
	case "bool":
		return types.Typ[types.Bool], nil
	case "int":
		return types.Typ[types.Int], nil
	case "int64":
		return types.Typ[types.Int64], nil
	case "byte":
		return types.Typ[types.Byte], nil
	case "struct{}":
		return types.NewStruct(nil, nil), nil
	}

	// Look up in package scope
	obj := pkg.Scope().Lookup(expr)
	if obj == nil {
		return nil, fmt.Errorf("type %q not found in package %s", expr, pkg.Name())
	}
	return obj.Type(), nil
}
