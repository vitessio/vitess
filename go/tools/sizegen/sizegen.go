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
	"fmt"
	"go/types"
	"log"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/dave/jennifer/jen"
	"github.com/spf13/pflag"
	"golang.org/x/tools/go/packages"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/tools/codegen"
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

type sizegen struct {
	DebugTypes bool
	mod        *packages.Module
	sizes      types.Sizes
	codegen    map[string]*codeFile
	known      map[*types.Named]*typeState
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
	impls []codeImpl
}

type typeState struct {
	generated bool
	local     bool
	pod       bool // struct with only primitives
}

func newSizegen(mod *packages.Module, sizes types.Sizes) *sizegen {
	return &sizegen{
		DebugTypes: true,
		mod:        mod,
		sizes:      sizes,
		known:      make(map[*types.Named]*typeState),
		codegen:    make(map[string]*codeFile),
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
	case *types.Named:
		return isPod(tt.Underlying())
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

func (sizegen *sizegen) getKnownType(named *types.Named) *typeState {
	ts := sizegen.known[named]
	if ts == nil {
		pkg := named.Obj().Pkg()
		local := pkg != nil && strings.HasPrefix(pkg.Path(), sizegen.mod.Path)
		ts = &typeState{
			local: local,
			pod:   isPod(named.Underlying()),
		}
		sizegen.known[named] = ts
	}
	return ts
}

func (sizegen *sizegen) generateType(pkg *types.Package, file *codeFile, named *types.Named) {
	ts := sizegen.getKnownType(named)
	if ts.generated {
		return
	}
	ts.generated = true

	if named.Obj().Pkg() != pkg {
		panic("trying to define external type in wrong package")
	}

	switch tt := named.Underlying().(type) {
	case *types.Struct:
		if impl, flag := sizegen.sizeImplForStruct(named.Obj(), tt); impl != nil {
			file.impls = append(file.impls, codeImpl{
				code:  impl,
				name:  named.String(),
				flags: flag,
			})
		}
	case *types.Interface:
		findImplementations(pkg.Scope(), tt, func(tt types.Type) {
			if _, isStruct := tt.Underlying().(*types.Struct); isStruct {
				sizegen.generateKnownType(tt.(*types.Named))
			}
		})
	default:
		// no-op
	}
}

func (sizegen *sizegen) generateKnownType(named *types.Named) {
	pkgInfo := named.Obj().Pkg()
	file := sizegen.codegen[pkgInfo.Path()]
	if file == nil {
		file = &codeFile{pkg: pkgInfo.Name()}
		sizegen.codegen[pkgInfo.Path()] = file
	}

	sizegen.generateType(pkgInfo, file, named)
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

func (sizegen *sizegen) finalize() map[string]*jen.File {
	var complete bool

	for !complete {
		complete = true
		for tt, ts := range sizegen.known {
			isComplex := !ts.pod
			notYetGenerated := !ts.generated
			if ts.local && isComplex && notYetGenerated {
				sizegen.generateKnownType(tt)
				complete = false
			}
		}
	}

	outputFiles := make(map[string]*jen.File)

	for pkg, file := range sizegen.codegen {
		if len(file.impls) == 0 {
			continue
		}
		if !strings.HasPrefix(pkg, sizegen.mod.Path) {
			log.Printf("failed to generate code for foreign package '%s'", pkg)
			log.Printf("DEBUG:\n%#v", file)
			continue
		}

		sort.Slice(file.impls, func(i, j int) bool {
			return strings.Compare(file.impls[i].name, file.impls[j].name) < 0
		})

		out := jen.NewFile(file.pkg)
		out.HeaderComment(licenseFileHeader)
		out.HeaderComment("Code generated by Sizegen. DO NOT EDIT.")

		for _, impl := range file.impls {
			if impl.flags&codeWithInterface != 0 {
				out.Add(jen.Type().Id("cachedObject").InterfaceFunc(func(i *jen.Group) {
					i.Id("CachedSize").Params(jen.Id("alloc").Id("bool")).Int64()
				}))
				break
			}
		}

		for _, impl := range file.impls {
			if impl.flags&codeWithUnsafe != 0 {
				out.Commentf("//go:nocheckptr")
			}
			out.Add(impl.code)
		}

		fullPath := path.Join(sizegen.mod.Dir, strings.TrimPrefix(pkg, sizegen.mod.Path), "cached_size.go")
		outputFiles[fullPath] = out
	}

	return outputFiles
}

func (sizegen *sizegen) sizeImplForStruct(name *types.TypeName, st *types.Struct) (jen.Code, codeFlag) {
	if sizegen.sizes.Sizeof(st) == 0 {
		return nil, 0
	}

	var stmt []jen.Code
	var funcFlags codeFlag
	for i := 0; i < st.NumFields(); i++ {
		field := st.Field(i)
		fieldType := field.Type()
		fieldName := jen.Id("cached").Dot(field.Name())

		fieldStmt, flag := sizegen.sizeStmtForType(fieldName, fieldType, false)
		if fieldStmt != nil {
			if sizegen.DebugTypes {
				stmt = append(stmt, jen.Commentf("%s", field.String()))
			}
			stmt = append(stmt, fieldStmt)
		}
		funcFlags |= flag
	}

	f := jen.Func()
	f.Params(jen.Id("cached").Op("*").Id(name.Name()))
	f.Id("CachedSize").Params(jen.Id("alloc").Id("bool")).Int64()
	f.BlockFunc(func(b *jen.Group) {
		b.Add(jen.If(jen.Id("cached").Op("==").Nil()).Block(jen.Return(jen.Lit(int64(0)))))
		b.Add(jen.Id("size").Op(":=").Lit(int64(0)))
		b.Add(jen.If(jen.Id("alloc")).Block(
			jen.Id("size").Op("+=").Lit(hack.RuntimeAllocSize(sizegen.sizes.Sizeof(st))),
		))
		for _, s := range stmt {
			b.Add(s)
		}
		b.Add(jen.Return(jen.Id("size")))
	})
	return f, funcFlags
}

func (sizegen *sizegen) sizeStmtForMap(fieldName *jen.Statement, m *types.Map) []jen.Code {
	const bucketCnt = 8
	const sizeofHmap = int64(6 * 8)

	/*
		type bmap struct {
			// tophash generally contains the top byte of the hash value
			// for each key in this bucket. If tophash[0] < minTopHash,
			// tophash[0] is a bucket evacuation state instead.
			tophash [bucketCnt]uint8
			// Followed by bucketCnt keys and then bucketCnt elems.
			// NOTE: packing all the keys together and then all the elems together makes the
			// code a bit more complicated than alternating key/elem/key/elem/... but it allows
			// us to eliminate padding which would be needed for, e.g., map[int64]int8.
			// Followed by an overflow pointer.
		}
	*/
	sizeOfBucket := int(
		bucketCnt + // tophash
			bucketCnt*sizegen.sizes.Sizeof(m.Key()) +
			bucketCnt*sizegen.sizes.Sizeof(m.Elem()) +
			8, // overflow pointer
	)

	return []jen.Code{
		jen.Id("size").Op("+=").Lit(hack.RuntimeAllocSize(sizeofHmap)),

		jen.Id("hmap").Op(":=").Qual("reflect", "ValueOf").Call(fieldName),

		jen.Id("numBuckets").Op(":=").Id("int").Call(
			jen.Qual("math", "Pow").Call(jen.Lit(2), jen.Id("float64").Call(
				jen.Parens(jen.Op("*").Parens(jen.Op("*").Id("uint8")).Call(
					jen.Qual("unsafe", "Pointer").Call(jen.Id("hmap").Dot("Pointer").Call().
						Op("+").Id("uintptr").Call(jen.Lit(9)))))))),

		jen.Id("numOldBuckets").Op(":=").Parens(jen.Op("*").Parens(jen.Op("*").Id("uint16")).Call(
			jen.Qual("unsafe", "Pointer").Call(
				jen.Id("hmap").Dot("Pointer").Call().Op("+").Id("uintptr").Call(jen.Lit(10))))),

		jen.Id("size").Op("+=").Do(mallocsize(jen.Int64().Call(jen.Id("numOldBuckets").Op("*").Lit(sizeOfBucket)))),

		jen.If(jen.Id("len").Call(fieldName).Op(">").Lit(0).Op("||").Id("numBuckets").Op(">").Lit(1)).Block(
			jen.Id("size").Op("+=").Do(mallocsize(jen.Int64().Call(jen.Id("numBuckets").Op("*").Lit(sizeOfBucket))))),
	}
}

func mallocsize(sizeStmt *jen.Statement) func(*jen.Statement) {
	return func(parent *jen.Statement) {
		parent.Qual("vitess.io/vitess/go/hack", "RuntimeAllocSize").Call(sizeStmt)
	}
}

func (sizegen *sizegen) sizeStmtForArray(stmt []jen.Code, fieldName *jen.Statement, elemT types.Type) ([]jen.Code, codeFlag) {
	var flag codeFlag

	switch sizegen.sizes.Sizeof(elemT) {
	case 0:
		return nil, 0

	case 1:
		stmt = append(stmt, jen.Id("size").Op("+=").Do(mallocsize(jen.Int64().Call(jen.Cap(fieldName)))))

	default:
		var nested jen.Code
		nested, flag = sizegen.sizeStmtForType(jen.Id("elem"), elemT, false)

		stmt = append(stmt,
			jen.Id("size").
				Op("+=").
				Do(mallocsize(jen.Int64().Call(jen.Cap(fieldName)).
					Op("*").
					Lit(sizegen.sizes.Sizeof(elemT))),
				))

		if nested != nil {
			stmt = append(stmt, jen.For(jen.List(jen.Id("_"), jen.Id("elem")).Op(":=").Range().Add(fieldName)).Block(nested))
		}
	}

	return stmt, flag
}

func (sizegen *sizegen) sizeStmtForType(fieldName *jen.Statement, field types.Type, alloc bool) (jen.Code, codeFlag) {
	if sizegen.sizes.Sizeof(field) == 0 {
		return nil, 0
	}

	switch node := field.(type) {
	case *types.Slice:
		var cond *jen.Statement
		var stmt []jen.Code
		var flag codeFlag

		if alloc {
			cond = jen.If(fieldName.Clone().Op("!=").Nil())
			fieldName = jen.Op("*").Add(fieldName)
			stmt = append(stmt, jen.Id("size").Op("+=").Lit(hack.RuntimeAllocSize(8*3)))
		}

		stmt, flag = sizegen.sizeStmtForArray(stmt, fieldName, node.Elem())
		if cond != nil {
			return cond.Block(stmt...), flag
		}
		return jen.Block(stmt...), flag

	case *types.Array:
		if alloc {
			cond := jen.If(fieldName.Clone().Op("!=").Nil())
			fieldName = jen.Op("*").Add(fieldName)

			stmt, flag := sizegen.sizeStmtForArray(nil, fieldName, node.Elem())
			return cond.Block(stmt...), flag
		}

		elemT := node.Elem()
		if sizegen.sizes.Sizeof(elemT) > 1 {
			nested, flag := sizegen.sizeStmtForType(jen.Id("elem"), elemT, false)
			if nested != nil {
				return jen.For(jen.List(jen.Id("_"), jen.Id("elem")).Op(":=").Range().Add(fieldName)).Block(nested), flag
			}
		}
		return nil, 0

	case *types.Map:
		keySize, keyFlag := sizegen.sizeStmtForType(jen.Id("k"), node.Key(), false)
		valSize, valFlag := sizegen.sizeStmtForType(jen.Id("v"), node.Elem(), false)

		return jen.If(fieldName.Clone().Op("!=").Nil()).BlockFunc(func(block *jen.Group) {
			for _, stmt := range sizegen.sizeStmtForMap(fieldName, node) {
				block.Add(stmt)
			}

			var forLoopVars []jen.Code
			switch {
			case keySize != nil && valSize != nil:
				forLoopVars = []jen.Code{jen.Id("k"), jen.Id("v")}
			case keySize == nil && valSize != nil:
				forLoopVars = []jen.Code{jen.Id("_"), jen.Id("v")}
			case keySize != nil && valSize == nil:
				forLoopVars = []jen.Code{jen.Id("k")}
			case keySize == nil && valSize == nil:
				return
			}

			block.Add(jen.For(jen.List(forLoopVars...).Op(":=").Range().Add(fieldName))).BlockFunc(func(b *jen.Group) {
				if keySize != nil {
					b.Add(keySize)
				}
				if valSize != nil {
					b.Add(valSize)
				}
			})
		}), codeWithUnsafe | keyFlag | valFlag

	case *types.Pointer:
		return sizegen.sizeStmtForType(fieldName, node.Elem(), true)

	case *types.Named:
		ts := sizegen.getKnownType(node)
		if ts.pod || !ts.local {
			if alloc {
				if !ts.local {
					log.Printf("WARNING: size of external type %s cannot be fully calculated", node)
				}
				return jen.If(fieldName.Clone().Op("!=").Nil()).Block(
					jen.Id("size").Op("+=").Do(mallocsize(jen.Lit(sizegen.sizes.Sizeof(node.Underlying())))),
				), 0
			}
			return nil, 0
		}
		return sizegen.sizeStmtForType(fieldName, node.Underlying(), alloc)

	case *types.Interface:
		if node.Empty() {
			return nil, 0
		}
		return jen.If(
			jen.List(
				jen.Id("cc"), jen.Id("ok")).
				Op(":=").
				Add(fieldName.Clone().Assert(jen.Id("cachedObject"))),
			jen.Id("ok"),
		).Block(
			jen.Id("size").
				Op("+=").
				Id("cc").
				Dot("CachedSize").
				Call(jen.True()),
		), codeWithInterface

	case *types.Struct:
		return jen.Id("size").Op("+=").Add(fieldName.Clone().Dot("CachedSize").Call(jen.Lit(alloc))), 0

	case *types.Basic:
		if !alloc {
			if node.Info()&types.IsString != 0 {
				return jen.Id("size").Op("+=").Do(mallocsize(jen.Int64().Call(jen.Len(fieldName)))), 0
			}
			return nil, 0
		}
		return jen.Id("size").Op("+=").Do(mallocsize(jen.Lit(sizegen.sizes.Sizeof(node)))), 0

	case *types.Signature:
		// assume that function pointers do not allocate (although they might, if they're closures)
		return nil, 0

	default:
		log.Printf("unhandled type: %T", node)
		return nil, 0
	}
}

var defaultGenTypes = []string{
	"vitess.io/vitess/go/pools/smartconnpool.Setting",
	"vitess.io/vitess/go/vt/schema.DDLStrategySetting",
	"vitess.io/vitess/go/vt/vtgate/engine.Plan",
	"vitess.io/vitess/go/vt/vttablet/tabletserver.TabletPlan",
	"vitess.io/vitess/go/sqltypes.Result",
}

func main() {
	var (
		patterns, generate []string
		verify             bool
	)

	pflag.StringSliceVar(&patterns, "in", []string{`./go/...`}, "Go packages to load the generator")
	pflag.StringSliceVar(&generate, "gen", defaultGenTypes, "Typename of the Go struct to generate size info for")
	pflag.BoolVar(&verify, "verify", false, "ensure that the generated files are correct")
	pflag.Parse()

	result, err := GenerateSizeHelpers(patterns, generate)
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
			if err := codegen.SaveJenFile(fullPath, file); err != nil {
				log.Fatal(err)
			}
		}
	}
}

// VerifyFilesOnDisk compares the generated results from the codegen against the files that
// currently exist on disk and returns any mismatches. All the files generated by jennifer
// are formatted using the goimports command. Any difference in the imports will also make
// this test fail.
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

// GenerateSizeHelpers generates the auxiliary code that implements CachedSize helper methods
// for all the types listed in typePatterns
func GenerateSizeHelpers(packagePatterns []string, typePatterns []string) (map[string]*jen.File, error) {
	loaded, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedTypes | packages.NeedTypesSizes | packages.NeedTypesInfo | packages.NeedDeps | packages.NeedImports | packages.NeedModule,
	}, packagePatterns...)

	if err != nil {
		return nil, err
	}

	if err := codegen.CheckErrors(loaded, func(filename string) bool { return filename == "cached_size.go" }); err != nil {
		return nil, err
	}

	sizegen := newSizegen(loaded[0].Module, loaded[0].TypesSizes)

	scopes := make(map[string]*types.Scope)
	for _, pkg := range loaded {
		scopes[pkg.PkgPath] = pkg.Types.Scope()
	}

	for _, gen := range typePatterns {
		pos := strings.LastIndexByte(gen, '.')
		if pos < 0 {
			return nil, fmt.Errorf("unexpected input type: %s", gen)
		}

		pkgname := gen[:pos]
		typename := gen[pos+1:]

		scope := scopes[pkgname]
		if scope == nil {
			return nil, fmt.Errorf("no scope found for type '%s'", gen)
		}

		if typename == "*" {
			for _, name := range scope.Names() {
				sizegen.generateKnownType(scope.Lookup(name).Type().(*types.Named))
			}
		} else {
			tt := scope.Lookup(typename)
			if tt == nil {
				return nil, fmt.Errorf("no type called '%s' found in '%s'", typename, pkgname)
			}

			sizegen.generateKnownType(tt.Type().(*types.Named))
		}
	}

	return sizegen.finalize(), nil
}
