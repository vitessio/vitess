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

// cloneGen creates the deep clone methods for the AST. It works by discovering the types that it needs to support,
// starting from a root interface type. While creating the clone method for this root interface, more types that need
// to be cloned are discovered. This continues type by type until all necessary types have been traversed.
type cloneGen struct {
	methods    []jen.Code
	iface      *types.Interface
	scope      *types.Scope
	todo       []types.Type
	exceptType string
}

var _ generator = (*cloneGen)(nil)

func newCloneGen(iface *types.Interface, scope *types.Scope, exceptType string) *cloneGen {
	return &cloneGen{
		iface:      iface,
		scope:      scope,
		exceptType: exceptType,
	}
}

func (c *cloneGen) visitStruct(types.Type, *types.Struct) error {
	return nil
}

func (c *cloneGen) visitSlice(types.Type, *types.Slice) error {
	return nil
}

func (c *cloneGen) visitInterface(t types.Type, _ *types.Interface) error {
	c.todo = append(c.todo, t)
	return nil
}

const cloneName = "Clone"

func (c *cloneGen) addFunc(name string, code jen.Code) {
	c.methods = append(c.methods, jen.Comment(name+" creates a deep clone of the input."), code)
}

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

func (c *cloneGen) makeStructCloneMethod(t types.Type) error {
	receiveType := types.TypeString(t, noQualifier)
	funcName := "Clone" + printableTypeName(t)
	c.addFunc(funcName,
		jen.Func().Id(funcName).Call(jen.Id("n").Id(receiveType)).Id(receiveType).Block(
			jen.Return(jen.Op("*").Add(c.readValueOfType(types.NewPointer(t), jen.Op("&").Id("n")))),
		))
	return nil
}

func (c *cloneGen) makeSliceCloneMethod(t types.Type, slice *types.Slice) error {
	typeString := types.TypeString(t, noQualifier)
	name := printableTypeName(t)
	funcName := cloneName + name

	c.addFunc(funcName,
		//func (n Bytes) Clone() Bytes {
		jen.Func().Id(funcName).Call(jen.Id("n").Id(typeString)).Id(typeString).Block(
			//	res := make(Bytes, len(n))
			jen.Id("res").Op(":=").Id("make").Call(jen.Id(typeString), jen.Lit(0), jen.Id("len").Call(jen.Id("n"))),
			c.copySliceElement(slice.Elem()),
			//	return res
			jen.Return(jen.Id("res")),
		))
	return nil
}

func (c *cloneGen) copySliceElement(elType types.Type) jen.Code {
	if isBasic(elType) {
		//	copy(res, n)
		return jen.Id("copy").Call(jen.Id("res"), jen.Id("n"))
	}

	//for _, x := range n {
	//  res = append(res, CloneAST(x))
	//}
	c.todo = append(c.todo, elType)
	return jen.For(jen.List(jen.Op("_"), jen.Id("x"))).Op(":=").Range().Id("n").Block(
		jen.Id("res").Op("=").Id("append").Call(jen.Id("res"), c.readValueOfType(elType, jen.Id("x"))),
	)
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

	funcName := cloneName + typeName
	funcDecl := jen.Func().Id(funcName).Call(jen.Id("in").Id(typeString)).Id(typeString).Block(stmts...)
	c.addFunc(funcName, funcDecl)
	return nil
}

func (c *cloneGen) makePtrCloneMethod(t types.Type, ptr *types.Pointer) error {
	receiveType := types.TypeString(t, noQualifier)

	funcName := "Clone" + printableTypeName(t)
	c.addFunc(funcName,
		jen.Func().Id(funcName).Call(jen.Id("n").Id(receiveType)).Id(receiveType).Block(
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
	alreadyDone := map[string]bool{}
	for len(c.todo) > 0 {
		t := c.todo[0]
		underlying := t.Underlying()
		typeName := printableTypeName(t)
		c.todo = c.todo[1:]

		if alreadyDone[typeName] {
			continue
		}

		if c.tryInterface(underlying, t) ||
			c.trySlice(underlying, t) ||
			c.tryStruct(underlying, t) ||
			c.tryPtr(underlying, t) {
			alreadyDone[typeName] = true
			continue
		}

		log.Errorf("don't know how to handle %s %T", typeName, underlying)
	}

	for _, method := range c.methods {
		out.Add(method)
	}

	return "clone.go", out
}

func ifNilReturnNil(id string) *jen.Statement {
	return jen.If(jen.Id(id).Op("==").Nil()).Block(jen.Return(jen.Nil()))
}

func isBasic(t types.Type) bool {
	_, x := t.Underlying().(*types.Basic)
	return x
}

func (c *cloneGen) tryStruct(underlying, t types.Type) bool {
	_, ok := underlying.(*types.Struct)
	if !ok {
		return false
	}

	err := c.makeStructCloneMethod(t)
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

	if strct, isStruct := ptr.Elem().Underlying().(*types.Struct); isStruct {
		c.makePtrToStructCloneMethod(t, strct)
		return true
	}

	err := c.makePtrCloneMethod(t, ptr)
	if err != nil {
		panic(err) // todo
	}
	return true
}

func (c *cloneGen) makePtrToStructCloneMethod(t types.Type, strct *types.Struct) {
	receiveType := types.TypeString(t, noQualifier)
	funcName := "Clone" + printableTypeName(t)

	//func CloneRefOfType(n *Type) *Type
	funcDeclaration := jen.Func().Id(funcName).Call(jen.Id("n").Id(receiveType)).Id(receiveType)

	if receiveType == c.exceptType {
		c.addFunc(funcName, funcDeclaration.Block(
			jen.Return(jen.Id("n")),
		))
		return
	}

	var fields []jen.Code
	for i := 0; i < strct.NumFields(); i++ {
		field := strct.Field(i)
		if isBasic(field.Type()) || field.Name() == "_" {
			continue
		}
		// out.Field = CloneType(n.Field)
		fields = append(fields,
			jen.Id("out").Dot(field.Name()).Op("=").Add(c.readValueOfType(field.Type(), jen.Id("n").Dot(field.Name()))))
	}

	stmts := []jen.Code{
		// if n == nil { return nil }
		ifNilReturnNil("n"),
		// 	out := *n
		jen.Id("out").Op(":=").Op("*").Id("n"),
	}

	// handle all fields with CloneAble types
	stmts = append(stmts, fields...)

	stmts = append(stmts,
		// return &out
		jen.Return(jen.Op("&").Id("out")),
	)

	c.addFunc(funcName,
		funcDeclaration.Block(stmts...),
	)
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
		return t.Name()
	case *types.Interface:
		return t.String()
	default:
		panic(fmt.Sprintf("unknown type %T %v", t, t))
	}
}
