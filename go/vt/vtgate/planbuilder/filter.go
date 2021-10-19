/*
Copyright 2019 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ logicalPlan = (*filter)(nil)

// filter is the logicalPlan for engine.Filter.
type filter struct {
	logicalPlanCommon
	efilter *engine.Filter
}

// newFilter builds a new filter.
func newFilter(semTable *semantics.SemTable, plan logicalPlan, expr sqlparser.Expr) (*filter, error) {
	predicate, err := sqlparser.Convert(expr, func(col *sqlparser.ColName) (int, error) {
		offset, _, err := pushProjection(&sqlparser.AliasedExpr{Expr: col}, plan, semTable, true, true)
		if err != nil {
			return 0, err
		}
		return offset, nil
	})
	if err != nil {
		return nil, err
	}
	return &filter{
		logicalPlanCommon: newBuilderCommon(plan),
		efilter: &engine.Filter{
			Predicate: predicate,
		},
	}, nil
}

// Primitive implements the logicalPlan interface
func (l *filter) Primitive() engine.Primitive {
	l.efilter.Input = l.input.Primitive()
	return l.efilter
}
