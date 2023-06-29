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
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
)

// typer is responsible for setting the type for expressions
// it does it's work after visiting the children (up), since the children types is often needed to type a node.
type typer struct {
	exprTypes map[sqlparser.Expr]Type
}

// Type is the normal querypb.Type with collation
type Type struct {
	Type      querypb.Type
	Collation collations.ID
}

func newTyper() *typer {
	return &typer{
		exprTypes: map[sqlparser.Expr]Type{},
	}
}

func (t *typer) up(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case *sqlparser.Literal:
		t.exprTypes[node] = Type{Type: node.SQLType()}
	case *sqlparser.Argument:
		if node.Type >= 0 {
			t.exprTypes[node] = Type{Type: node.Type}
		}
	case sqlparser.AggrFunc:
		code, ok := opcode.SupportedAggregates[node.AggrName()]
		if ok {
			typ, ok := opcode.OpcodeType[code]
			if ok {
				t.exprTypes[node] = Type{Type: typ}
			}
		}
	}
	return nil
}

func (t *typer) setTypeFor(node *sqlparser.ColName, typ Type) {
	t.exprTypes[node] = typ
}
