/*
Copyright 2023 The Vitess Authors.

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

package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Limit struct {
	Source ops.Operator
	AST    *sqlparser.Limit

	// Pushed marks whether the limit has been pushed down to the inputs but still need to keep the operator around.
	// For example, `select * from user order by id limit 10`. Even after we push the limit to the route, we need a limit on top
	// since it is a scatter.
	Pushed bool
}

func (l *Limit) Clone(inputs []ops.Operator) ops.Operator {
	return &Limit{
		Source: inputs[0],
		AST:    sqlparser.CloneRefOfLimit(l.AST),
	}
}

func (l *Limit) Inputs() []ops.Operator {
	return []ops.Operator{l.Source}
}

func (l *Limit) SetInputs(operators []ops.Operator) {
	l.Source = operators[0]
}

func (l *Limit) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newSrc, err := l.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	l.Source = newSrc
	return l, nil
}

func (l *Limit) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr) (ops.Operator, int, error) {
	newSrc, offset, err := l.Source.AddColumn(ctx, expr)
	if err != nil {
		return nil, 0, err
	}
	l.Source = newSrc
	return l, offset, nil
}

func (l *Limit) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	return l.Source.GetColumns()
}

func (l *Limit) GetOrdering() ([]ops.OrderBy, error) {
	return l.Source.GetOrdering()
}

func (l *Limit) Description() ops.OpDescription {
	other := map[string]any{}
	if l.AST.Offset != nil {
		other["Offset"] = sqlparser.String(l.AST.Offset)
	}
	if l.AST.Rowcount != nil {
		other["RowCount"] = sqlparser.String(l.AST.Rowcount)
	}
	return ops.OpDescription{
		OperatorType: "Limit",
		Other:        other,
	}
}

func (l *Limit) ShortDescription() string {
	return sqlparser.String(l.AST)
}
