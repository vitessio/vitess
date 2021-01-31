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
	"go/types"
	"log"
	"sort"
	"strings"

	"github.com/dave/jennifer/jen"
)

type cachedSize struct {
	lookup typeLookup
}

func (s *cachedSize) fileName() string {
	return "cached_size.go"
}

func (s *cachedSize) finalizeFile(file *codeFile, out *jen.File) {
	impls := file.state.([]codeImpl)

	sort.Slice(impls, func(i, j int) bool {
		return strings.Compare(impls[i].name, impls[j].name) < 0
	})

	for _, impl := range impls {
		if impl.flags&codeWithInterface != 0 {
			out.Add(jen.Type().Id("cachedObject").InterfaceFunc(func(i *jen.Group) {
				i.Id("CachedSize").Params(jen.Id("alloc").Id("bool")).Int64()
			}))
			break
		}
	}

	for _, impl := range impls {
		if impl.flags&codeWithUnsafe != 0 {
			out.Commentf("//go:nocheckptr")
		}
		out.Add(impl.code)
	}
}

func (s *cachedSize) stmtForMap(fieldName *jen.Statement, m *types.Map, sizes types.Sizes) []jen.Code {
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
			bucketCnt*sizes.Sizeof(m.Key()) +
			bucketCnt*sizes.Sizeof(m.Elem()) +
			8, // overflow pointer
	)

	return []jen.Code{
		jen.Id("size").Op("+=").Lit(sizeofHmap),

		jen.Id("hmap").Op(":=").Qual("reflect", "ValueOf").Call(fieldName),

		jen.Id("numBuckets").Op(":=").Id("int").Call(
			jen.Qual("math", "Pow").Call(jen.Lit(2), jen.Id("float64").Call(
				jen.Parens(jen.Op("*").Parens(jen.Op("*").Id("uint8")).Call(
					jen.Qual("unsafe", "Pointer").Call(jen.Id("hmap").Dot("Pointer").Call().
						Op("+").Id("uintptr").Call(jen.Lit(9)))))))),

		jen.Id("numOldBuckets").Op(":=").Parens(jen.Op("*").Parens(jen.Op("*").Id("uint16")).Call(
			jen.Qual("unsafe", "Pointer").Call(
				jen.Id("hmap").Dot("Pointer").Call().Op("+").Id("uintptr").Call(jen.Lit(10))))),

		jen.Id("size").Op("+=").Id("int64").Call(jen.Id("numOldBuckets").Op("*").Lit(sizeOfBucket)),

		jen.If(jen.Id("len").Call(fieldName).Op(">").Lit(0).Op("||").Id("numBuckets").Op(">").Lit(1)).Block(
			jen.Id("size").Op("+=").Id("int64").Call(
				jen.Id("numBuckets").Op("*").Lit(sizeOfBucket))),
	}
}

func (s *cachedSize) stmtForType(fieldName *jen.Statement, field types.Type, alloc bool, sizes types.Sizes) (jen.Code, codeFlag) {
	if sizes.Sizeof(field) == 0 {
		return nil, 0
	}

	switch node := field.(type) {
	case *types.Slice:
		elemT := node.Elem()
		elemSize := sizes.Sizeof(elemT)

		switch elemSize {
		case 0:
			return nil, 0

		case 1:
			return jen.Id("size").Op("+=").Int64().Call(jen.Cap(fieldName)), 0

		default:
			stmt, flag := s.stmtForType(jen.Id("elem"), elemT, false, sizes)
			return jen.BlockFunc(func(b *jen.Group) {
				b.Add(
					jen.Id("size").
						Op("+=").
						Int64().Call(jen.Cap(fieldName)).
						Op("*").
						Lit(sizes.Sizeof(elemT)))

				if stmt != nil {
					b.Add(jen.For(jen.List(jen.Id("_"), jen.Id("elem")).Op(":=").Range().Add(fieldName)).Block(stmt))
				}
			}), flag
		}

	case *types.Map:
		keySize, keyFlag := s.stmtForType(jen.Id("k"), node.Key(), false, sizes)
		valSize, valFlag := s.stmtForType(jen.Id("v"), node.Elem(), false, sizes)

		return jen.If(fieldName.Clone().Op("!=").Nil()).BlockFunc(func(block *jen.Group) {
			for _, stmt := range s.stmtForMap(fieldName, node, sizes) {
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
		return s.stmtForType(fieldName, node.Elem(), true, sizes)

	case *types.Named:
		ts := s.lookup.getKnownType(node)
		if ts.pod || !ts.local {
			if alloc {
				if !ts.local {
					log.Printf("WARNING: size of external type %s cannot be fully calculated", node)
				}
				return jen.If(fieldName.Clone().Op("!=").Nil()).Block(
					jen.Id("size").Op("+=").Lit(sizes.Sizeof(node.Underlying())),
				), 0
			}
			return nil, 0
		}
		return s.stmtForType(fieldName, node.Underlying(), alloc, sizes)

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
				return jen.Id("size").Op("+=").Int64().Call(jen.Len(fieldName)), 0
			}
			return nil, 0
		}
		return jen.Id("size").Op("+=").Lit(sizes.Sizeof(node)), 0
	default:
		log.Printf("unhandled type: %T", node)
		return nil, 0
	}
}

func (s *cachedSize) implForStruct(file *codeFile, name *types.TypeName, st *types.Struct, sizes types.Sizes, debugTypes bool) (jen.Code, codeFlag) {
	if sizes.Sizeof(st) == 0 {
		return nil, 0
	}

	var stmt []jen.Code
	var funcFlags codeFlag
	for i := 0; i < st.NumFields(); i++ {
		field := st.Field(i)
		fieldType := field.Type()
		fieldName := jen.Id("cached").Dot(field.Name())

		fieldStmt, flag := s.stmtForType(fieldName, fieldType, false, sizes)
		if fieldStmt != nil {
			if debugTypes {
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
			jen.Id("size").Op("+=").Lit(sizes.Sizeof(st)),
		))
		for _, s := range stmt {
			b.Add(s)
		}
		b.Add(jen.Return(jen.Id("size")))
	})

	var impls []codeImpl
	if file.state != nil {
		impls = file.state.([]codeImpl)
	}

	impls = append(impls, codeImpl{
		code:  f,
		name:  name.String(),
		flags: funcFlags,
	})
	file.state = impls

	return f, funcFlags
}

func (s *cachedSize) isEmpty(file *codeFile) bool {
	return file.state == nil || len(file.state.([]codeImpl)) == 0
}

func newCachedSize(lookup typeLookup) output {
	return &cachedSize{lookup: lookup}
}

var _ output = (*cachedSize)(nil)
