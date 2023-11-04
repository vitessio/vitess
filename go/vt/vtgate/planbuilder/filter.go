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

package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// filter is the logicalPlan for engine.Filter.
	filter struct {
		logicalPlanCommon
		efilter *engine.Filter
	}
)

var _ logicalPlan = (*filter)(nil)

func resolveFromPlan(ctx *plancontext.PlanningContext, plan logicalPlan, canPushProjection bool) evalengine.ColumnResolver {
	return func(expr *sqlparser.ColName) (int, error) {
		offset, added, err := pushProjection(ctx, &sqlparser.AliasedExpr{Expr: expr}, plan, true, true, false)
		if err != nil {
			return 0, err
		}
		if added && !canPushProjection {
			return 0, vterrors.VT13001("column should not be pushed to projection while doing a column lookup")
		}
		return offset, nil
	}
}

// newFilter builds a new filter.
func newFilter(ctx *plancontext.PlanningContext, plan logicalPlan, expr sqlparser.Expr) (*filter, error) {
	predicate, err := evalengine.Translate(expr, &evalengine.Config{
		ResolveColumn: resolveFromPlan(ctx, plan, false),
		ResolveType:   ctx.SemTable.TypeForExpr,
		Collation:     ctx.SemTable.Collation,
	})
	if err != nil {
		return nil, err
	}
	return &filter{
		logicalPlanCommon: newBuilderCommon(plan),
		efilter: &engine.Filter{
			Predicate:    predicate,
			ASTPredicate: expr,
		},
	}, nil
}

// Primitive implements the logicalPlan interface
func (l *filter) Primitive() engine.Primitive {
	l.efilter.Input = l.input.Primitive()
	return l.efilter
}
