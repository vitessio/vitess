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

package abstract

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	// Vindex stores the information about the vindex query
	Vindex struct {
		Table         VindexTable
		Vindex        vindexes.Vindex
		JoinPredicate []sqlparser.Expr
		NoDeps        []sqlparser.Expr
	}

	// VindexTable contains information about the vindex table we want to query
	VindexTable struct {
		TableID    semantics.TableSet
		Alias      *sqlparser.AliasedTableExpr
		Table      sqlparser.TableName
		Predicates []sqlparser.Expr
		VTable     *vindexes.Table
	}
)

var _ Operator = (*Vindex)(nil)

// TableID implements the Operator interface
func (v Vindex) TableID() semantics.TableSet {
	return v.Table.TableID
}

// PushPredicate implements the Operator interface
func (v Vindex) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	for _, e := range sqlparser.SplitAndExpression(nil, expr) {
		deps := semTable.BaseTableDependencies(e)
		switch deps.NumberOfTables() {
		case 0:
			v.NoDeps = append(v.NoDeps, e)
		case 1:
			if v.TableID().IsSolvedBy(deps) {
				v.Table.Predicates = append(v.Table.Predicates, e)
			} else {
				v.JoinPredicate = append(v.JoinPredicate, e)
			}
		default:
			v.JoinPredicate = append(v.JoinPredicate, e)
		}
	}
	return nil
}

// UnsolvedPredicates implements the Operator interface
func (v Vindex) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	panic("implement me")
}
