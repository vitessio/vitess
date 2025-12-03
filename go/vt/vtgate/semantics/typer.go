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

package semantics

import (
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// typer is responsible for setting the type for expressions
// it does it's work after visiting the children (up), since the children types is often needed to type a node.
type typer struct {
	m            map[sqlparser.Expr]evalengine.Type
	collationEnv *collations.Environment
}

func newTyper(collationEnv *collations.Environment) *typer {
	return &typer{
		m:            map[sqlparser.Expr]evalengine.Type{},
		collationEnv: collationEnv,
	}
}

func (t *typer) exprType(expr sqlparser.Expr) evalengine.Type {
	return t.m[expr]
}

func (t *typer) up(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case *sqlparser.Literal:
		t.m[node] = evalengine.NewType(node.SQLType(), collations.CollationForType(node.SQLType(), t.collationEnv.DefaultConnectionCharset()))
	case *sqlparser.Argument:
		if node.Type >= 0 {
			t.m[node] = evalengine.NewTypeEx(node.Type, collations.CollationForType(node.Type, t.collationEnv.DefaultConnectionCharset()), true, node.Size, node.Scale, nil)
		}
	case sqlparser.WindowFunc:
		name := node.WindowFuncName()
		var inputType evalengine.Type
		if arg := node.GetArg(); arg != nil {
			if tt, ok := t.m[arg]; ok {
				inputType = tt
			}
		}

		if code, ok := opcode.SupportedWindowFunctions[name]; ok {
			t.m[node] = code.ResolveType(inputType, t.collationEnv)
		} else if code, ok := opcode.SupportedAggregates[name]; ok {
			// Functions like SUM() implement both WindowFunc and AggrFunc.
			// If an OVER clause is present, it's acting as a window function.
			t.m[node] = code.ResolveType(inputType, t.collationEnv)
		}
	case sqlparser.AggrFunc:
		if code, ok := opcode.SupportedAggregates[node.AggrName()]; ok {
			var inputType evalengine.Type
			if arg := node.GetArg(); arg != nil {
				if tt, ok := t.m[arg]; ok {
					inputType = tt
				}
			}
			t.m[node] = code.ResolveType(inputType, t.collationEnv)
		}
	}
	return nil
}

func (t *typer) setTypeFor(node *sqlparser.ColName, typ evalengine.Type) {
	t.m[node] = typ
}
