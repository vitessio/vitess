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

// PushPredicate implements the Operator interface
func (c *Concatenate) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) (LogicalOperator, error) {
	return pushPredicateOnConcatenate(expr, semTable, c)
}

func pushPredicateOnConcatenate(expr sqlparser.Expr, semTable *semantics.SemTable, c *Concatenate) (LogicalOperator, error) {
	newSources := make([]LogicalOperator, 0, len(c.Sources))
	for index, source := range c.Sources {
		if len(c.SelectStmts[index].SelectExprs) != 1 {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't push predicates on concatenate")
		}
		if _, isStarExpr := c.SelectStmts[index].SelectExprs[0].(*sqlparser.StarExpr); !isStarExpr {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "can't push predicates on concatenate")
		}

		newSrc, err := source.PushPredicate(expr, semTable)
		if err != nil {
			return nil, err
		}
		newSources = append(newSources, newSrc)
	}
	c.Sources = newSources
	return c, nil
}

// PushPredicate implements the Operator interface
func (d *Derived) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) (LogicalOperator, error) {
	return pushPredicateOnDerived(expr, semTable, d)
}

func pushPredicateOnDerived(expr sqlparser.Expr, semTable *semantics.SemTable, d *Derived) (LogicalOperator, error) {
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
	newSrc, err := d.Inner.PushPredicate(newExpr, semTable)
	d.Inner = newSrc
	return d, err
}

// PushPredicate implements the LogicalOperator interface
func (f *Filter) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) (LogicalOperator, error) {
	return pushPredicateOnFilter(expr, semTable, f)
}

func pushPredicateOnFilter(expr sqlparser.Expr, semTable *semantics.SemTable, f *Filter) (LogicalOperator, error) {
	op, err := f.Source.PushPredicate(expr, semTable)
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

// PushPredicate implements the Operator interface
func (j *Join) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) (LogicalOperator, error) {
	return pushPredicateOnJoin(expr, semTable, j)
}

func pushPredicateOnJoin(expr sqlparser.Expr, semTable *semantics.SemTable, j *Join) (LogicalOperator, error) {
	deps := semTable.RecursiveDeps(expr)
	switch {
	case deps.IsSolvedBy(j.LHS.TableID()):
		lhs, err := j.LHS.PushPredicate(expr, semTable)
		if err != nil {
			return nil, err
		}
		j.LHS = lhs
		return j, nil

	case deps.IsSolvedBy(j.RHS.TableID()):
		j.tryConvertToInnerJoin(expr, semTable)

		if !j.LeftJoin {
			rhs, err := j.RHS.PushPredicate(expr, semTable)
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

	case deps.IsSolvedBy(j.LHS.TableID().Merge(j.RHS.TableID())):
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

// PushPredicate implements the Operator interface
func (qg *QueryGraph) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) (LogicalOperator, error) {
	return pushPredicateOnQG(expr, semTable, qg)
}

func pushPredicateOnQG(expr sqlparser.Expr, semTable *semantics.SemTable, qg *QueryGraph) (LogicalOperator, error) {
	for _, e := range sqlparser.SplitAndExpression(nil, expr) {
		err := qg.collectPredicate(e, semTable)
		if err != nil {
			return nil, err
		}
	}
	return qg, nil
}

// PushPredicate implements the Operator interface
func (v *Vindex) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) (LogicalOperator, error) {
	return pushPredicateOnVindex(expr, semTable, v)
}

func pushPredicateOnVindex(expr sqlparser.Expr, semTable *semantics.SemTable, v *Vindex) (LogicalOperator, error) {
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

// PushPredicate implements the Operator interface
func (s *SubQuery) PushPredicate(sqlparser.Expr, *semantics.SemTable) (LogicalOperator, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] should not try to push predicate on subquery")
}

// PushPredicate implements the LogicalOperator interface
func (u *Update) PushPredicate(sqlparser.Expr, *semantics.SemTable) (LogicalOperator, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "can't accept predicates")
}

// PushPredicate implements the LogicalOperator interface
func (d *Delete) PushPredicate(sqlparser.Expr, *semantics.SemTable) (LogicalOperator, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "can't accept predicates")
}
