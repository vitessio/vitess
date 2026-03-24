package main

import (
	"fmt"
	"go/types"
	"os"
	"path/filepath"
	"sort"

	"golang.org/x/tools/go/packages"
)

// unionTypeInfo holds the inferred size and pointer layout of a union member.
type unionTypeInfo struct {
	words    int   // total size in pointer-sized words
	ptrWords []int // which word offsets contain pointers (sorted)
}

// inferUnionLayout loads the package containing the grammar, inspects
// each type declared in %union, and returns:
//   - dataWords: number of uintptr words needed to hold the largest member
//   - maxPtrs: maximum number of pointer words across all members
//   - typeLayouts: per-member-name layout info (pointer word offsets)
func inferUnionLayout() (dataWords int, maxPtrs int, typeLayouts map[string]*unionTypeInfo, err error) {
	var typeNames []string
	for _, gt := range gotypes {
		typeNames = append(typeNames, gt.typename)
	}
	if len(typeNames) == 0 {
		return 0, 0, nil, nil
	}

	absOutput, err := filepath.Abs(oflag)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("inferUnionLayout: %w", err)
	}
	pkgDir := filepath.Dir(absOutput)

	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesSizes,
		Dir:  pkgDir,
	}, ".")
	if err != nil {
		return 0, 0, nil, fmt.Errorf("inferUnionLayout: failed to load package: %w", err)
	}
	if len(loaded) == 0 {
		return 0, 0, nil, fmt.Errorf("inferUnionLayout: %s", "no packages loaded")
	}
	pkg := loaded[0]
	if pkg.Types == nil {
		return 0, 0, nil, fmt.Errorf("inferUnionLayout: %s", "package types not available")
	}

	sizes := pkg.TypesSizes
	ptrSize := sizes.Sizeof(types.Typ[types.UnsafePointer])

	typeLayouts = make(map[string]*unionTypeInfo)

	for member, gt := range gotypes {
		t := resolveType(pkg.Types, gt.typename)
		if t == nil {
			fmt.Fprintf(os.Stderr, "inferUnionLayout: cannot resolve type %q\n", gt.typename)
			continue
		}

		sz := sizes.Sizeof(t)
		words := int((sz + ptrSize - 1) / ptrSize)
		if words > dataWords {
			dataWords = words
		}

		ptrs := pointerWords(t, sizes, 0)
		sort.Ints(ptrs)
		if len(ptrs) > maxPtrs {
			maxPtrs = len(ptrs)
		}

		typeLayouts[member] = &unionTypeInfo{
			words:    words,
			ptrWords: ptrs,
		}
	}

	if dataWords == 0 {
		return 0, 0, nil, fmt.Errorf("inferUnionLayout: %s", "could not determine any type sizes")
	}

	return dataWords, maxPtrs, typeLayouts, nil
}

// resolveType looks up a type name in the package scope, handling
// built-in types, pointers, and slices.
func resolveType(pkg *types.Package, expr string) types.Type {
	t, err := evalType(pkg, expr)
	if err != nil {
		return nil
	}
	return t
}

// pointerWords returns the word offsets (at ptrSize granularity) that
// contain pointer values for the given type. This is used to determine
// which words from the uintptr data array need to be copied to the
// unsafe.Pointer keepalive array.
func pointerWords(t types.Type, sizes types.Sizes, baseOffset int64) []int {
	ptrSize := sizes.Sizeof(types.Typ[types.UnsafePointer])

	switch t := t.Underlying().(type) {
	case *types.Pointer:
		// A pointer is one word, always a pointer.
		return []int{int(baseOffset / ptrSize)}

	case *types.Interface:
		// An interface is two words: type pointer + data pointer.
		return []int{
			int(baseOffset / ptrSize),
			int(baseOffset/ptrSize) + 1,
		}

	case *types.Slice:
		// A slice is three words: data pointer, len, cap.
		// Only the first word is a pointer.
		return []int{int(baseOffset / ptrSize)}

	case *types.Basic:
		switch t.Kind() {
		case types.String:
			// String is two words: data pointer + length.
			// Only the first word is a pointer.
			return []int{int(baseOffset / ptrSize)}
		case types.UnsafePointer:
			return []int{int(baseOffset / ptrSize)}
		default:
			// Numeric, bool, etc. — no pointers.
			return nil
		}

	case *types.Map, *types.Chan, *types.Signature:
		// Maps, channels, and function values are single pointers.
		return []int{int(baseOffset / ptrSize)}

	case *types.Struct:
		fieldOffsets := sizes.Offsetsof(structFields(t))
		var ptrs []int
		for i := range t.NumFields() {
			ptrs = append(ptrs, pointerWords(t.Field(i).Type(), sizes, baseOffset+fieldOffsets[i])...)
		}
		return ptrs

	case *types.Array:
		elemSize := sizes.Sizeof(t.Elem())
		var ptrs []int
		for i := range t.Len() {
			ptrs = append(ptrs, pointerWords(t.Elem(), sizes, baseOffset+int64(i)*elemSize)...)
		}
		return ptrs

	default:
		return nil
	}
}

// structFields extracts the field list from a struct type for use with Offsetsof.
func structFields(s *types.Struct) []*types.Var {
	fields := make([]*types.Var, s.NumFields())
	for i := range s.NumFields() {
		fields[i] = s.Field(i)
	}
	return fields
}

// evalType parses a Go type expression like "*String", "[]Expr", "bool"
// in the context of the given package.
func evalType(pkg *types.Package, expr string) (types.Type, error) {
	if len(expr) > 0 && expr[0] == '*' {
		inner, err := evalType(pkg, expr[1:])
		if err != nil {
			return nil, err
		}
		return types.NewPointer(inner), nil
	}

	if len(expr) > 2 && expr[0] == '[' && expr[1] == ']' {
		inner, err := evalType(pkg, expr[2:])
		if err != nil {
			return nil, err
		}
		return types.NewSlice(inner), nil
	}

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

	obj := pkg.Scope().Lookup(expr)
	if obj == nil {
		return nil, fmt.Errorf("type %q not found in package %s", expr, pkg.Name())
	}
	return obj.Type(), nil
}
