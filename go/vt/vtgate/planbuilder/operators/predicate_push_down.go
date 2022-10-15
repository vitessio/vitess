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
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func LogicalPushPredicate(op Operator, expr sqlparser.Expr, semTable *semantics.SemTable) (Operator, error) {
	switch op := op.(type) {
	case *Concatenate:
		return pushPredicateOnConcatenate(expr, semTable, op)
	case *Derived:
		return pushPredicateOnDerived(expr, semTable, op)
	case *Filter:
		return pushPredicateOnFilter(expr, semTable, op)
	case *Join:
		return pushPredicateOnJoin(expr, semTable, op)
	case *QueryGraph:
		return pushPredicateOnQG(expr, semTable, op)
	case *Vindex:
		return pushPredicateOnVindex(expr, semTable, op)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[%T] can't accept predicates", op)
	}
}

func pushPredicateOnConcatenate(expr sqlparser.Expr, semTable *semantics.SemTable, c *Concatenate) (Operator, error) {
	newSources := make([]Operator, 0, len(c.Sources))
	for index, source := range c.Sources {
		if len(c.SelectStmts[index].SelectExprs) != 1 {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't push predicates on concatenate")
		}
		if _, isStarExpr := c.SelectStmts[index].SelectExprs[0].(*sqlparser.StarExpr); !isStarExpr {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't push predicates on concatenate")
		}

		newSrc, err := LogicalPushPredicate(source, expr, semTable)
		if err != nil {
			return nil, err
		}
		newSources = append(newSources, newSrc)
	}
	c.Sources = newSources
	return c, nil
}

func pushPredicateOnDerived(expr sqlparser.Expr, semTable *semantics.SemTable, d *Derived) (Operator, error) {
	tableInfo, err := semTable.TableInfoForExpr(expr)
	if err != nil {
		if err == semantics.ErrMultipleTables {
			return nil, semantics.ProjError{Inner: vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: unable to split predicates to derived table: %s", sqlparser.String(expr))}
		}
		return nil, err
	}

	newExpr, err := semantics.RewriteDerivedTableExpression(expr, tableInfo)
	if err != nil {
		return nil, err
	}
	newSrc, err := LogicalPushPredicate(d.Inner, newExpr, semTable)
	d.Inner = newSrc
	return d, err
}

func pushPredicateOnFilter(expr sqlparser.Expr, semTable *semantics.SemTable, f *Filter) (Operator, error) {
	op, err := LogicalPushPredicate(f.Source, expr, semTable)
	if err != nil {
		return nil, err
	}

	if filter, isFilter := op.(*Filter); isFilter {
		filter.Predicates = append(f.Predicates, filter.Predicates...)
		return filter, err
	}

	return &Filter{
		Source:     op,
		Predicates: f.Predicates,
	}, nil
}

func pushPredicateOnJoin(expr sqlparser.Expr, semTable *semantics.SemTable, j *Join) (Operator, error) {
	deps := semTable.RecursiveDeps(expr)
	switch {
	case deps.IsSolvedBy(TableID(j.LHS)):
		lhs, err := LogicalPushPredicate(j.LHS, expr, semTable)
		if err != nil {
			return nil, err
		}
		j.LHS = lhs
		return j, nil

	case deps.IsSolvedBy(TableID(j.RHS)):
		j.tryConvertToInnerJoin(expr, semTable)

		if !j.LeftJoin {
			rhs, err := LogicalPushPredicate(j.RHS, expr, semTable)
			if err != nil {
				return nil, err
			}
			j.RHS = rhs
			return j, err
		}

		op := &Filter{
			Source:     j,
			Predicates: []sqlparser.Expr{expr},
		}
		return op, nil

	case deps.IsSolvedBy(TableID(j)):
		j.tryConvertToInnerJoin(expr, semTable)

		if !j.LeftJoin {
			j.Predicate = sqlparser.AndExpressions(j.Predicate, expr)
			return j, nil
		}

		op := &Filter{
			Source:     j,
			Predicates: []sqlparser.Expr{expr},
		}
		return op, nil
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Cannot push predicate: %s", sqlparser.String(expr))
}

func pushPredicateOnQG(expr sqlparser.Expr, semTable *semantics.SemTable, qg *QueryGraph) (Operator, error) {
	for _, e := range sqlparser.SplitAndExpression(nil, expr) {
		err := qg.collectPredicate(e, semTable)
		if err != nil {
			return nil, err
		}
	}
	return qg, nil
}

func pushPredicateOnVindex(expr sqlparser.Expr, semTable *semantics.SemTable, v *Vindex) (Operator, error) {
	for _, e := range sqlparser.SplitAndExpression(nil, expr) {
		deps := semTable.RecursiveDeps(e)
		if deps.NumberOfTables() > 1 {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, vindexUnsupported+" (multiple tables involved)")
		}
		// check if we already have a predicate
		if v.OpCode != engine.VindexNone {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (multiple filters)")
		}

		// check LHS
		comparison, ok := e.(*sqlparser.ComparisonExpr)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (not a comparison)")
		}
		if comparison.Operator != sqlparser.EqualOp && comparison.Operator != sqlparser.InOp {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (not equality)")
		}
		colname, ok := comparison.Left.(*sqlparser.ColName)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (lhs is not a column)")
		}
		if !colname.Name.EqualString("id") {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (lhs is not id)")
		}

		// check RHS
		var err error
		if sqlparser.IsValue(comparison.Right) || sqlparser.IsSimpleTuple(comparison.Right) {
			v.Value = comparison.Right
		} else {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+" (rhs is not a value)")
		}
		if err != nil {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, vindexUnsupported+": %v", err)
		}
		v.OpCode = engine.VindexMap
		v.Table.Predicates = append(v.Table.Predicates, e)
	}
	return v, nil
}
