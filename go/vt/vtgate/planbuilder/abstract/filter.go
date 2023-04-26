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
)

type Filter struct {
	Source     LogicalOperator
	Predicates []sqlparser.Expr
}

var _ LogicalOperator = (*Filter)(nil)

// iLogical implements the LogicalOperator interface
func (f *Filter) iLogical() {}

// TableID implements the LogicalOperator interface
func (f *Filter) TableID() semantics.TableSet {
	return f.Source.TableID()
}

// UnsolvedPredicates implements the LogicalOperator interface
func (f *Filter) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return f.Source.UnsolvedPredicates(semTable)
}

// CheckValid implements the LogicalOperator interface
func (f *Filter) CheckValid() error {
	return f.Source.CheckValid()
}

// PushPredicate implements the LogicalOperator interface
func (f *Filter) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) (LogicalOperator, error) {
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

// Compact implements the LogicalOperator interface
func (f *Filter) Compact(semTable *semantics.SemTable) (LogicalOperator, error) {
	if len(f.Predicates) == 0 {
		return f.Source, nil
	}

	return f, nil
}
