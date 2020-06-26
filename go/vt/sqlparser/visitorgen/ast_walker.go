/*
Copyright 2019 The Vitess Authors.

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

package visitorgen

import (
	"go/ast"
	"reflect"
)

var _ ast.Visitor = (*walker)(nil)

type walker struct {
	result SourceFile
}

// Walk walks the given AST and translates it to the simplified AST used by the next steps
func Walk(node ast.Node) *SourceFile {
	var w walker
	ast.Walk(&w, node)
	return &w.result
}

// Visit implements the ast.Visitor interface
func (w *walker) Visit(node ast.Node) ast.Visitor {
	switch n := node.(type) {
	case *ast.TypeSpec:
		switch t2 := n.Type.(type) {
		case *ast.InterfaceType:
			w.append(&InterfaceDeclaration{
				name:  n.Name.Name,
				block: "",
			})
		case *ast.StructType:
			var fields []*Field
			for _, f := range t2.Fields.List {
				for _, name := range f.Names {
					fields = append(fields, &Field{
						name: name.Name,
						typ:  sastType(f.Type),
					})
				}

			}
			w.append(&StructDeclaration{
				name:   n.Name.Name,
				fields: fields,
			})
		case *ast.ArrayType:
			w.append(&TypeAlias{
				name: n.Name.Name,
				typ:  &Array{inner: sastType(t2.Elt)},
			})
		case *ast.Ident:
			w.append(&TypeAlias{
				name: n.Name.Name,
				typ:  &TypeString{t2.Name},
			})

		default:
			panic(reflect.TypeOf(t2))
		}
	case *ast.FuncDecl:
		if len(n.Recv.List) > 1 || len(n.Recv.List[0].Names) > 1 {
			panic("don't know what to do!")
		}
		var f *Field
		if len(n.Recv.List) == 1 {
			r := n.Recv.List[0]
			t := sastType(r.Type)
			if len(r.Names) > 1 {
				panic("don't know what to do!")
			}
			if len(r.Names) == 1 {
				f = &Field{
					name: r.Names[0].Name,
					typ:  t,
				}
			} else {
				f = &Field{
					name: "",
					typ:  t,
				}
			}
		}

		w.append(&FuncDeclaration{
			receiver:  f,
			name:      n.Name.Name,
			block:     "",
			arguments: nil,
		})
	}

	return w
}

func (w *walker) append(line Sast) {
	w.result.lines = append(w.result.lines, line)
}

func sastType(e ast.Expr) Type {
	switch n := e.(type) {
	case *ast.StarExpr:
		return &Ref{sastType(n.X)}
	case *ast.Ident:
		return &TypeString{n.Name}
	case *ast.ArrayType:
		return &Array{inner: sastType(n.Elt)}
	case *ast.InterfaceType:
		return &TypeString{"interface{}"}
	case *ast.StructType:
		return &TypeString{"struct{}"}
	}

	panic(reflect.TypeOf(e))
}
