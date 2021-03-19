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

package asthelpergen

import (
	"fmt"
	"go/types"

	"github.com/dave/jennifer/jen"
)

const rewriteName = "rewrite"

type rewriteGen struct{}

var _ generator2 = (*rewriteGen)(nil)

func (e rewriteGen) interfaceMethod(t types.Type, iface *types.Interface, spi generatorSPI) error {
	if !shouldAdd(t, spi.iface()) {
		return nil
	}
	/*
		func VisitAST(in AST) (bool, error) {
			if in == nil {
				return false, nil
			}
			switch a := inA.(type) {
			case *SubImpl:
				return VisitSubImpl(a, b)
			default:
				return false, nil
			}
		}
	*/
	stmts := []jen.Code{
		jen.If(jen.Id("node == nil").Block(returnNil())),
	}

	var cases []jen.Code
	_ = spi.findImplementations(iface, func(t types.Type) error {
		if _, ok := t.Underlying().(*types.Interface); ok {
			return nil
		}
		typeString := types.TypeString(t, noQualifier)
		funcName := rewriteName + printableTypeName(t)
		spi.addType(t)
		caseBlock := jen.Case(jen.Id(typeString)).Block(
			jen.Return(jen.Id(funcName).Call(jen.Id("parent, node, replacer, pre, post"))),
		)
		cases = append(cases, caseBlock)
		return nil
	})

	cases = append(cases,
		jen.Default().Block(
			jen.Comment("this should never happen"),
			returnNil(),
		))

	stmts = append(stmts, jen.Switch(jen.Id("node := node.(type)").Block(
		cases...,
	)))

	rewriteFunc(t, stmts, spi)
	return nil
}

func (e rewriteGen) structMethod(t types.Type, strct *types.Struct, spi generatorSPI) error {
	if !shouldAdd(t, spi.iface()) {
		return nil
	}

	/*
	 */

	stmts := rewriteAllStructFields(t, strct, spi, true)
	rewriteFunc(t, stmts, spi)

	return nil
}

func (e rewriteGen) ptrToStructMethod(t types.Type, strct *types.Struct, spi generatorSPI) error {
	if !shouldAdd(t, spi.iface()) {
		return nil
	}

	stmts := []jen.Code{
		/*
			if node == nil { return nil }
		*/
		jen.If(jen.Id("node == nil").Block(returnNil())),

		/*
			cur := Cursor{
				parent:   parent,
				replacer: replacer,
				node:     node,
			}
		*/
		jen.Id("cur := Cursor").Values(
			jen.Dict{
				jen.Id("parent"):   jen.Id("parent"),
				jen.Id("replacer"): jen.Id("replacer"),
				jen.Id("node"):     jen.Id("node"),
			}),

		/*
			if !pre(&cur) {
				return nil
			}
		*/
		jen.If(jen.Id("!pre(&cur)")).Block(returnNil()),
	}

	stmts = append(stmts, rewriteAllStructFields(t, strct, spi, false)...)

	stmts = append(stmts,
		jen.If(jen.Id("!post").Call(jen.Id("&cur"))).Block(jen.Return(jen.Id("abortE"))),
		returnNil(),
	)
	rewriteFunc(t, stmts, spi)

	return nil
}

func (e rewriteGen) ptrToBasicMethod(t types.Type, _ *types.Basic, spi generatorSPI) error {
	if !shouldAdd(t, spi.iface()) {
		return nil
	}

	/*
	 */

	stmts := []jen.Code{
		jen.Comment("ptrToStructMethod"),
	}
	rewriteFunc(t, stmts, spi)

	return nil
}

func (e rewriteGen) sliceMethod(t types.Type, slice *types.Slice, spi generatorSPI) error {
	if !shouldAdd(t, spi.iface()) {
		return nil
	}

	/*
	 */

	stmts := []jen.Code{
		jen.Comment("ptrToStructMethod"),
	}
	rewriteFunc(t, stmts, spi)

	return nil
}

func (e rewriteGen) ptrToOtherMethod(t types.Type, _ *types.Pointer, spi generatorSPI) error {
	if !shouldAdd(t, spi.iface()) {
		return nil
	}

	/*
	 */

	stmts := []jen.Code{
		jen.Comment("ptrToStructMethod"),
	}
	rewriteFunc(t, stmts, spi)

	return nil
}

func (e rewriteGen) basicMethod(t types.Type, basic *types.Basic, spi generatorSPI) error {
	if !shouldAdd(t, spi.iface()) {
		return nil
	}

	/*
	 */

	stmts := []jen.Code{
		jen.Comment("ptrToStructMethod"),
	}
	rewriteFunc(t, stmts, spi)

	return nil
}

func (e rewriteGen) visitNoChildren(t types.Type, spi generatorSPI) error {
	if !shouldAdd(t, spi.iface()) {
		return nil
	}

	/*
	 */

	stmts := []jen.Code{
		jen.Comment("ptrToStructMethod"),
	}
	rewriteFunc(t, stmts, spi)

	return nil
}

func rewriteFunc(t types.Type, stmts []jen.Code, spi generatorSPI) {

	/*
		func (a *application) rewriteNodeType(parent AST, node NodeType, replacer replacerFunc) {
	*/

	typeString := types.TypeString(t, noQualifier)
	funcName := fmt.Sprintf("%s%s", rewriteName, printableTypeName(t))
	code := jen.Func().Id(funcName).Params(
		jen.Id(fmt.Sprintf("parent AST, node %s, replacer replacerFunc, pre, post ApplyFunc", typeString)),
	).Error().
		Block(stmts...)

	spi.addFunc(funcName, rewrite, code)
}

func rewriteAllStructFields(t types.Type, strct *types.Struct, spi generatorSPI, fail bool) []jen.Code {
	//	_, ok := t.Underlying().(*types.Pointer)

	/*
		if errF := rewriteAST(node, node.ASTType, func(newNode, parent AST) {
			err = vterrors.New(vtrpcpb.Code_INTERNAL, "[BUG] tried to replace '%s' on '%s'")
		}, pre, post); errF != nil {
			return errF
		}

	*/
	var output []jen.Code
	for i := 0; i < strct.NumFields(); i++ {
		field := strct.Field(i)
		if types.Implements(field.Type(), spi.iface()) {
			spi.addType(field.Type())
			output = append(output, rewriteChild(t, field.Type(), jen.Id("node").Dot(field.Name()), jen.Dot(field.Name())))
			continue
		}
		slice, isSlice := field.Type().(*types.Slice)
		if isSlice && types.Implements(slice.Elem(), spi.iface()) {
			elem := slice.Elem()
			spi.addType(elem)
			output = append(output,
				jen.For(jen.Id("i, el := range node."+field.Name())).
					Block(rewriteChild(t, elem, jen.Id("el"), jen.Dot(field.Name()).Index(jen.Id("i")))))
		}
	}
	return output
}

func failReplacer(t types.Type, f *types.Var) jen.Code {

}

func rewriteChild(t, field types.Type, param jen.Code, replace jen.Code) jen.Code {
	/*
		    if errF := rewriteAST(node, node.ASTType, func(newNode, parent AST) {
				parent.(*RefContainer).ASTType = newNode.(AST)
			}, pre, post); errF != nil {
				return errF
			}

				if errF := rewriteAST(node, el, func(newNode, parent AST) {
					parent.(*RefSliceContainer).ASTElements[i] = newNode.(AST)
				}, pre, post); errF != nil {
					return errF
				}

	*/
	funcName := rewriteName + printableTypeName(field)
	funcBlock := jen.Func().Call(jen.Id("newNode, parent AST")).
		Block(jen.Id("parent").
			Assert(jen.Id(types.TypeString(t, noQualifier))).
			Add(replace).
			Op("=").
			Id("newNode").Assert(jen.Id(types.TypeString(field, noQualifier))))

	rewriteField := jen.If(
		jen.Id("errF := ").Id(funcName).Call(
			jen.Id("node"),
			param,
			funcBlock,
			jen.Id("pre"),
			jen.Id("post")),
		jen.Id("errF != nil").Block(jen.Return(jen.Id("errF"))))

	return rewriteField
}
