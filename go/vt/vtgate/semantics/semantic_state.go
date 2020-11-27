/*
Copyright 2020 The Vitess Authors.

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
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
)

type (
	SemTable struct {
		outerScope *scope
	}
	Column struct {
	}

	Dependency interface {
		iDep()
	}
	TableExpression struct {
		te sqlparser.TableExpr
	}
	ColumnExpression struct {
		ae *sqlparser.AliasedExpr
	}
	column struct {
	}

	scope struct {
		inner   []*scope
		tables  []*TableExpression
		columns []*column
	}
)

func (*TableExpression) iDep()  {}
func (*ColumnExpression) iDep() {}

var _ Dependency = (*TableExpression)(nil)

func (t *SemTable) Columns() []Column {
	return nil
}

func (t *SemTable) DependenciesFor(expr sqlparser.Expr) ([]Dependency, error) {
	return nil, nil
}

func Analyse(statement sqlparser.Statement) (*SemTable, error) {
	s := &scope{}
	table := &SemTable{outerScope: s}
	down := func(cursor *sqlparser.Cursor) bool {
		switch cursor.Node().(type) {
		case *sqlparser.Subquery:
			fmt.Printf("1 %T - %s\n", cursor.Parent(), sqlparser.String(cursor.Parent()))
		case *sqlparser.Select:
			fmt.Printf("2 %T - %s\n", cursor.Parent(), sqlparser.String(cursor.Parent()))
		}
		return true
	}
	up := func(cursor *sqlparser.Cursor) bool {
		return true
	}
	sqlparser.Rewrite(statement, down, up)
	return table, nil
}
