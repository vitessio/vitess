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

package operators

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func predicatePushDown(ctx *plancontext.PlanningContext, op Operator) (Operator, bool, error) {
	filter, ok := op.(*Filter)
	if !ok {
		return op, false, nil
	}

	result := filter.Source
	var notPushed []sqlparser.Expr

	for _, expr := range filter.Predicates {
		switch op := result.(type) {
		case *Filter:
			op.Predicates = append(op.Predicates, expr)
		case *QueryGraph:
			updated, err := pushPredicateOnQG(ctx, expr, op)
			if err != nil {
				return nil, false, err
			}
			result = updated
		case *Join:
			deps := ctx.SemTable.RecursiveDeps(expr)

			switch {
			// can push to the left
			case deps.IsSolvedBy(TableID(op.LHS)):
				op.LHS = addFilter(op.LHS, expr)

			// can push to the right only for inner joins
			case deps.IsSolvedBy(TableID(op.RHS)):
				op.tryConvertToInnerJoin(ctx, expr)

				if !op.LeftJoin {
					op.RHS = addFilter(op.RHS, expr)
					continue
				}

				notPushed = append(notPushed, expr)
			case deps.IsSolvedBy(TableID(op)):
				op.tryConvertToInnerJoin(ctx, expr)

				if !op.LeftJoin {
					op.Predicate = sqlparser.AndExpressions(op.Predicate, expr)
					continue
				}

				notPushed = append(notPushed, expr)
			}
		case *Derived:
			if _, isUNion := op.Source.(*Union); isUNion {
				op.Source = addFilter(op.Source, expr)
				result = op
				continue
			}
			tableInfo, err := ctx.SemTable.TableInfoForExpr(expr)
			if err != nil {
				if err == semantics.ErrMultipleTables {
					return nil, false, semantics.ProjError{Inner: vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: unable to split predicates to derived table: %s", sqlparser.String(expr))}
				}
				return nil, false, err
			}

			newExpr, err := semantics.RewriteDerivedTableExpression(expr, tableInfo)
			if err != nil {
				return nil, false, err
			}
			op.Source = addFilter(op.Source, newExpr)
		case *Vindex:
			err := op.addPredicate(ctx, expr)
			if err != nil {
				return nil, false, err
			}

		case *Union:
			offsets := make(map[string]int)
			for i, selectExpr := range op.SelectStmts[0].SelectExprs {
				ae, ok := selectExpr.(*sqlparser.AliasedExpr)
				if !ok {
					return nil, false, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't push predicates on UNION where the first SELECT contains star or next")
				}
				if !ae.As.IsEmpty() {
					offsets[ae.As.String()] = i
					continue
				}
				col, ok := ae.Expr.(*sqlparser.ColName)
				if ok {
					offsets[col.Name.Lowered()] = i
				}
			}

			for i := range op.Sources {
				predicate := sqlparser.CloneExpr(expr)
				var err error
				predicate = sqlparser.Rewrite(predicate, func(cursor *sqlparser.Cursor) bool {
					col, ok := cursor.Node().(*sqlparser.ColName)
					if !ok {
						return err == nil
					}

					idx, ok := offsets[col.Name.Lowered()]
					if !ok {
						err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't push predicates on concatenate")
						return false
					}

					ae, ok := op.SelectStmts[i].SelectExprs[idx].(*sqlparser.AliasedExpr)
					if !ok {
						err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't push predicates on concatenate")
						return false
					}
					cursor.Replace(ae.Expr)
					return false
				}, nil).(sqlparser.Expr)
				op.Sources[i] = addFilter(op.Sources[i], predicate)
			}

		default:
			notPushed = append(notPushed, expr)
		}
	}

	if len(filter.Predicates) == len(notPushed) {
		// we were not able to push anything. let's return the original operator
		return filter, false, nil
	}
	if len(notPushed) > 0 {
		// we were able to push some but not all predicates
		filter.Predicates = notPushed
		filter.Source = result
		return filter, true, nil
	}

	// we successfully pushed everything
	return result, true, nil
}

func addFilter(op Operator, expr ...sqlparser.Expr) Operator {
	return &Filter{
		Source: op, Predicates: expr,
	}
}

func pushPredicateOnQG(ctx *plancontext.PlanningContext, expr sqlparser.Expr, qg *QueryGraph) (Operator, error) {
	for _, e := range sqlparser.SplitAndExpression(nil, expr) {
		err := qg.collectPredicate(ctx, e)
		if err != nil {
			return nil, err
		}
	}
	return qg, nil
}

func (v *Vindex) addPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) error {
	for _, e := range sqlparser.SplitAndExpression(nil, expr) {
		deps := ctx.SemTable.RecursiveDeps(e)
		if deps.NumberOfTables() > 1 {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, vindexUnsupported+" (multiple tables involved)")
		}
		// check if we already have a predicate
		if v.OpCode != engine.VindexNone {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (multiple filters)")
		}

		// check LHS
		comparison, ok := e.(*sqlparser.ComparisonExpr)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (not a comparison)")
		}
		if comparison.Operator != sqlparser.EqualOp && comparison.Operator != sqlparser.InOp {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (not equality)")
		}
		colname, ok := comparison.Left.(*sqlparser.ColName)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (lhs is not a column)")
		}
		if !colname.Name.EqualString("id") {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (lhs is not id)")
		}

		// check RHS
		var err error
		if sqlparser.IsValue(comparison.Right) || sqlparser.IsSimpleTuple(comparison.Right) {
			v.Value = comparison.Right
		} else {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (rhs is not a value)")
		}
		if err != nil {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+": %v", err)
		}
		v.OpCode = engine.VindexMap
		v.Table.Predicates = append(v.Table.Predicates, e)
	}
	return nil
}
