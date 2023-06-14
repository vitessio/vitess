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
	"strings"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type Filter struct {
	Source     ops.Operator
	Predicates []sqlparser.Expr

	// FinalPredicate is the evalengine expression that will finally be used.
	// It contains the ANDed predicates in Predicates, with ColName:s replaced by Offset:s
	FinalPredicate evalengine.Expr
}

func newFilter(op ops.Operator, expr sqlparser.Expr) ops.Operator {
	return &Filter{
		Source: op, Predicates: []sqlparser.Expr{expr},
	}
}

// Clone implements the Operator interface
func (f *Filter) Clone(inputs []ops.Operator) ops.Operator {
	predicatesClone := make([]sqlparser.Expr, len(f.Predicates))
	copy(predicatesClone, f.Predicates)
	return &Filter{
		Source:     inputs[0],
		Predicates: predicatesClone,
	}
}

// Inputs implements the Operator interface
func (f *Filter) Inputs() []ops.Operator {
	return []ops.Operator{f.Source}
}

// SetInputs implements the Operator interface
func (f *Filter) SetInputs(ops []ops.Operator) {
	f.Source = ops[0]
}

// UnsolvedPredicates implements the unresolved interface
func (f *Filter) UnsolvedPredicates(st *semantics.SemTable) []sqlparser.Expr {
	var result []sqlparser.Expr
	id := TableID(f)
	for _, p := range f.Predicates {
		deps := st.RecursiveDeps(p)
		if !deps.IsSolvedBy(id) {
			result = append(result, p)
		}
	}
	return result
}

func (f *Filter) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newSrc, err := f.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	f.Source = newSrc
	return f, nil
}

func (f *Filter) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, reuseExisting, addToGroupBy bool) (ops.Operator, int, error) {
	newSrc, offset, err := f.Source.AddColumn(ctx, expr, reuseExisting, addToGroupBy)
	if err != nil {
		return nil, 0, err
	}
	f.Source = newSrc
	return f, offset, nil
}

func (f *Filter) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	return f.Source.GetColumns()
}

func (f *Filter) GetOrdering() ([]ops.OrderBy, error) {
	return f.Source.GetOrdering()
}

func (f *Filter) Compact(*plancontext.PlanningContext) (ops.Operator, *rewrite.ApplyResult, error) {
	if len(f.Predicates) == 0 {
		return f.Source, rewrite.NewTree("filter with no predicates removed", f), nil
	}

	other, isFilter := f.Source.(*Filter)
	if !isFilter {
		return f, rewrite.SameTree, nil
	}
	f.Source = other.Source
	f.Predicates = append(f.Predicates, other.Predicates...)
	return f, rewrite.NewTree("two filters merged into one", f), nil
}

func (f *Filter) planOffsets(ctx *plancontext.PlanningContext) error {
	cfg := &evalengine.Config{
		ResolveType: ctx.SemTable.TypeForExpr,
		Collation:   ctx.SemTable.Collation,
	}

	predicate := sqlparser.AndExpressions(f.Predicates...)
	rewritten, err := useOffsets(ctx, predicate, f)
	if err != nil {
		return err
	}
	eexpr, err := evalengine.Translate(rewritten, cfg)
	if err != nil {
		if strings.HasPrefix(err.Error(), evalengine.ErrTranslateExprNotSupported) {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%s: %s", evalengine.ErrTranslateExprNotSupported, sqlparser.String(predicate))
		}
		return err
	}

	f.FinalPredicate = eexpr
	return nil
}

func (f *Filter) ShortDescription() string {
	return sqlparser.String(sqlparser.AndExpressions(f.Predicates...))
}
