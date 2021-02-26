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
	"fmt"
	"go/types"

	"vitess.io/vitess/go/vt/log"

	"github.com/dave/jennifer/jen"
)

type cloneGen struct {
	methods []jen.Code
	iface   *types.Interface
	scope   *types.Scope
	todo    []types.Type
}

var _ generator = (*cloneGen)(nil)

func newCloneGen(iface *types.Interface, scope *types.Scope) *cloneGen {
	return &cloneGen{
		iface: iface,
		scope: scope,
	}
}

func createTypeString(t types.Type) string {
	switch t := t.(type) {
	case *types.Pointer:
		return "&" + printableTypeName(t.Elem())
	case *types.Named:
		return t.Obj().Name()
	case *types.Basic:
		return t.Name()
	default:
		panic(fmt.Sprintf("unknown type %T", t))
	}
}

func (c *cloneGen) visitStruct(types.Type, *types.Struct) error {
	return nil
}

func (c *cloneGen) visitSlice(types.Type, *types.Slice) error {
	return nil
}

const cloneName = "Clone"

// readValueOfType produces code to read the expression of type `t`, and adds the type to the todo-list
func (c *cloneGen) readValueOfType(t types.Type, expr jen.Code) jen.Code {
	switch t.Underlying().(type) {
	case *types.Basic:
		return expr
	case *types.Interface:
		if types.TypeString(t, noQualifier) == "interface{}" {
			// these fields have to be taken care of manually
			return expr
		}
	}
	c.todo = append(c.todo, t)
	return jen.Id(cloneName + printableTypeName(t)).Call(expr)
}

func (c *cloneGen) makeStructCloneMethod(t types.Type, stroct *types.Struct) error {
	createType := createTypeString(t)
	receiveType := types.TypeString(t, noQualifier)

	var stmts []jen.Code

	values := make(jen.Dict)
	for i := 0; i < stroct.NumFields(); i++ {
		field := stroct.Field(i)
		if field.Name() == "_" {
			continue
		}
		id := jen.Id(field.Name())
		switch field.Type().(type) {
		case *types.Basic:
			// v: n.v
			values[id] = jen.Id("n").Dot(field.Name())

		default:
			// v: CloneType(n.Field)
			values[id] = c.readValueOfType(field.Type(), jen.Id("n").Dot(field.Name()))
		}
	}
	stmts = append(stmts, jen.Return(jen.Id(createType).Values(values)))

	c.methods = append(c.methods,
		jen.Func().Id("Clone"+printableTypeName(t)).Call(jen.Id("n").Id(receiveType)).Id(receiveType).Block(
			stmts...,
		))
	return nil
}

func ifNilReturnNil(id string) *jen.Statement {
	return jen.If(jen.Id(id).Op("==").Nil()).Block(jen.Return(jen.Nil()))
}

func (c *cloneGen) makeSliceCloneMethod(t types.Type, slice *types.Slice) error {
	typeString := types.TypeString(t, noQualifier)

	//func (n Bytes) Clone() Bytes {
	name := printableTypeName(t)
	x := jen.Func().Id(cloneName+name).Call(jen.Id("n").Id(typeString)).Id(typeString).Block(
		//	res := make(Bytes, len(n))
		jen.Id("res").Op(":=").Id("make").Call(jen.Id(typeString), jen.Id("len").Call(jen.Id("n"))),
		c.copySliceElement(slice.Elem()),
		//	return res
		jen.Return(jen.Id("res")),
	)

	c.methods = append(c.methods, x)
	return nil
}

func (c *cloneGen) copySliceElement(elType types.Type) jen.Code {
	_, isBasic := elType.Underlying().(*types.Basic)
	if isBasic {
		//	copy(res, n)
		return jen.Id("copy").Call(jen.Id("res"), jen.Id("n"))
	}

	//for i, x := range n {
	//	res[i] = CloneAST(x)
	//}
	c.todo = append(c.todo, elType)
	return jen.For(jen.List(jen.Id("i"), jen.Id("x"))).Op(":=").Range().Id("n").Block(
		jen.Id("res").Index(jen.Id("i")).Op("=").Add(c.readValueOfType(elType, jen.Id("x"))),
	)
}

func (c *cloneGen) visitInterface(t types.Type, _ *types.Interface) error {
	c.todo = append(c.todo, t)
	return nil
}

func (c *cloneGen) makeInterfaceCloneMethod(t types.Type, iface *types.Interface) error {

	//func CloneAST(in AST) AST {
	//	if in == nil {
	//	return nil
	//}
	//	switch in := in.(type) {
	//case *RefContainer:
	//	return in.CloneRefOfRefContainer()
	//}
	//	// this should never happen
	//	return nil
	//}

	typeString := types.TypeString(t, noQualifier)
	typeName := printableTypeName(t)

	stmts := []jen.Code{ifNilReturnNil("in")}

	var cases []jen.Code
	_ = findImplementations(c.scope, iface, func(t types.Type) error {
		typeString := types.TypeString(t, noQualifier)

		// case Type: return CloneType(in)
		block := jen.Case(jen.Id(typeString)).Block(jen.Return(c.readValueOfType(t, jen.Id("in"))))
		switch t := t.(type) {
		case *types.Pointer:
			_, isIface := t.Elem().(*types.Interface)
			if !isIface {
				cases = append(cases, block)
			}

		case *types.Named:
			_, isIface := t.Underlying().(*types.Interface)
			if !isIface {
				cases = append(cases, block)
			}

		default:
			log.Errorf("unexpected type encountered: %s", typeString)
		}

		return nil
	})

	cases = append(cases,
		jen.Default().Block(
			jen.Comment("this should never happen"),
			jen.Return(jen.Nil()),
		))

	//	switch n := node.(type) {
	stmts = append(stmts, jen.Switch(jen.Id("in").Op(":=").Id("in").Assert(jen.Id("type")).Block(
		cases...,
	)))

	funcDecl := jen.Func().Id(cloneName + typeName).Call(jen.Id("in").Id(typeString)).Id(typeString).Block(stmts...)
	c.methods = append(c.methods, funcDecl)
	return nil
}

func (c *cloneGen) makePtrCloneMethod(t types.Type, ptr *types.Pointer) error {
	receiveType := types.TypeString(t, noQualifier)

	c.methods = append(c.methods,
		jen.Func().Id("Clone"+printableTypeName(t)).Call(jen.Id("n").Id(receiveType)).Id(receiveType).Block(
			ifNilReturnNil("n"),
			jen.Id("out").Op(":=").Add(c.readValueOfType(ptr.Elem(), jen.Op("*").Id("n"))),
			jen.Return(jen.Op("&").Id("out")),
		))
	return nil
}

func (c *cloneGen) createFile(pkgName string) (string, *jen.File) {
	out := jen.NewFile(pkgName)
	out.HeaderComment(licenseFileHeader)
	out.HeaderComment("Code generated by ASTHelperGen. DO NOT EDIT.")
	alreadDone := map[string]bool{}
	for len(c.todo) > 0 {
		t := c.todo[0]
		underlying := t.Underlying()
		typeName := printableTypeName(t)
		c.todo = c.todo[1:]

		if alreadDone[typeName] {
			continue
		}

		if c.tryInterface(underlying, t) ||
			c.trySlice(underlying, t) ||
			c.tryStruct(underlying, t) ||
			c.tryPtr(underlying, t) {
			alreadDone[typeName] = true
			continue
		}

		log.Errorf("don't know how to handle %s %T", typeName, underlying)
	}

	for _, method := range c.methods {
		out.Add(method)
	}

	return "clone.go", out
}

func (c *cloneGen) tryStruct(underlying, t types.Type) bool {
	strct, ok := underlying.(*types.Struct)
	if !ok {
		return false
	}

	err := c.makeStructCloneMethod(t, strct)
	if err != nil {
		panic(err) // todo
	}
	return true
}
func (c *cloneGen) tryPtr(underlying, t types.Type) bool {
	ptr, ok := underlying.(*types.Pointer)
	if !ok {
		return false
	}

	err := c.makePtrCloneMethod(t, ptr)
	if err != nil {
		panic(err) // todo
	}
	return true
}
func (c *cloneGen) tryInterface(underlying, t types.Type) bool {
	iface, ok := underlying.(*types.Interface)
	if !ok {
		return false
	}

	err := c.makeInterfaceCloneMethod(t, iface)
	if err != nil {
		panic(err) // todo
	}
	return true
}
func (c *cloneGen) trySlice(underlying, t types.Type) bool {
	slice, ok := underlying.(*types.Slice)
	if !ok {
		return false
	}

	err := c.makeSliceCloneMethod(t, slice)
	if err != nil {
		panic(err) // todo
	}
	return true
}
