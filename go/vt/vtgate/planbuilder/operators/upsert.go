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
	Sources []UpsertSource

	noColumns
	noPredicates
}

type UpsertSource struct {
	Insert Operator
	Update Operator
}

func (u *Upsert) Clone(inputs []Operator) Operator {
	up := &Upsert{}
	up.setInputs(inputs)
	return up
}

func (u *Upsert) setInputs(inputs []Operator) {
	for i := 0; i < len(inputs); i += 2 {
		u.Sources = append(u.Sources, UpsertSource{
			Insert: inputs[i],
			Update: inputs[i+1],
		})
	}
}

func (u *Upsert) Inputs() []Operator {
	var inputs []Operator
	for _, source := range u.Sources {
		inputs = append(inputs, source.Insert, source.Update)
	}
	return inputs
}

func (u *Upsert) SetInputs(inputs []Operator) {
	u.Sources = nil
	u.setInputs(inputs)
}

func (u *Upsert) ShortDescription() string {
	return ""
}

func (u *Upsert) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return nil
}

func createUpsertOperator(ctx *plancontext.PlanningContext, ins *sqlparser.Insert, insOp Operator, rows sqlparser.Values, vTbl *vindexes.Table) Operator {
	if len(vTbl.UniqueKeys) != 0 {
		panic(vterrors.VT12001("ON DUPLICATE KEY UPDATE with foreign keys with unique keys"))
	}

	pIndexes, _ := findPKIndexes(vTbl, ins)
	if len(pIndexes) == 0 {
		// nothing to compare for update.
		// Hence, only perform insert.
		return insOp
	}

	upsert := &Upsert{}
	for _, row := range rows {
		var comparisons []sqlparser.Expr
		for _, pIdx := range pIndexes {
			var expr sqlparser.Expr
			if pIdx.idx == -1 {
				expr = pIdx.def
			} else {
				expr = row[pIdx.idx]
			}
			comparisons = append(comparisons,
				sqlparser.NewComparisonExpr(sqlparser.EqualOp, sqlparser.NewColName(pIdx.col.String()), expr, nil))
		}
		whereExpr := sqlparser.AndExpressions(comparisons...)

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
		newInsert := sqlparser.CloneRefOfInsert(ins)
		newInsert.OnDup = nil
		newInsert.Rows = sqlparser.Values{row}
		insOp = createOpFromStmt(ctx, newInsert, false, "")
		upsert.Sources = append(upsert.Sources, UpsertSource{
			Insert: insOp,
			Update: updOp,
		})
	}

	return upsert
}
