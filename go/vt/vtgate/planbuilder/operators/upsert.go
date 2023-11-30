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
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Operator = (*Upsert)(nil)

// Upsert represents an insert on duplicate key operation on a table.
type Upsert struct {
	InsertOp Operator
	UpdateOp Operator

	noColumns
	noPredicates
}

func (u *Upsert) Clone(inputs []Operator) Operator {
	return &Upsert{
		InsertOp: inputs[0],
		UpdateOp: inputs[1],
	}
}

func (u *Upsert) Inputs() []Operator {
	return []Operator{u.InsertOp, u.UpdateOp}
}

func (u *Upsert) SetInputs(operators []Operator) {
	u.InsertOp = operators[0]
	u.UpdateOp = operators[1]
}

func (u *Upsert) ShortDescription() string {
	return ""
}

func (u *Upsert) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return nil
}

func createUpsertOperator(ctx *plancontext.PlanningContext, ins *sqlparser.Insert, insOp Operator, row sqlparser.ValTuple, vTbl *vindexes.Table) Operator {
	if len(vTbl.UniqueKeys) != 0 {
		panic(vterrors.VT12001("ON DUPLICATE KEY UPDATE with foreign keys with unique keys"))
	}

	pIndexes, _ := findPKIndexes(vTbl, ins)
	if len(pIndexes) == 0 {
		// nothing to compare for update.
		// Hence, only perform insert.
		return insOp
	}

	var whereExpr sqlparser.Expr
	for _, pIdx := range pIndexes {
		var expr sqlparser.Expr
		if pIdx.idx == -1 {
			expr = pIdx.def
		} else {
			expr = row[pIdx.idx]
		}
		equalExpr := sqlparser.NewComparisonExpr(sqlparser.EqualOp, sqlparser.NewColName(pIdx.col.String()), expr, nil)
		if whereExpr == nil {
			whereExpr = equalExpr
		} else {
			whereExpr = sqlparser.AndExpressions(whereExpr, equalExpr)
		}
	}

	var updExprs sqlparser.UpdateExprs
	for _, ue := range ins.OnDup {
		expr := sqlparser.CopyOnRewrite(ue.Expr, nil, func(cursor *sqlparser.CopyOnWriteCursor) {
			vfExpr, ok := cursor.Node().(*sqlparser.ValuesFuncExpr)
			if !ok {
				return
			}
			idx := ins.Columns.FindColumn(vfExpr.Name.Name)
			if idx == -1 {
				panic(vterrors.VT03014(sqlparser.String(vfExpr.Name), "field list"))
			}
			cursor.Replace(row[idx])
		}, nil).(sqlparser.Expr)
		updExprs = append(updExprs, &sqlparser.UpdateExpr{
			Name: ue.Name,
			Expr: expr,
		})
	}

	upd := &sqlparser.Update{
		Comments:   ins.Comments,
		TableExprs: sqlparser.TableExprs{ins.Table},
		Exprs:      updExprs,
		Where:      sqlparser.NewWhere(sqlparser.WhereClause, whereExpr),
	}
	updOp := createOpFromStmt(ctx, upd, false, "")

	// replan insert statement without on duplicate key update.
	ins = sqlparser.CloneRefOfInsert(ins)
	ins.OnDup = nil
	insOp = createOpFromStmt(ctx, ins, false, "")

	return &Upsert{
		InsertOp: insOp,
		UpdateOp: updOp,
	}
}
