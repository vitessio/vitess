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
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type correlatedSubqueryTree struct {
	outer     queryTree
	inner     queryTree
	extracted *sqlparser.ExtractedSubquery
	// arguments that need to be copied from the uter to inner
	vars map[string]int
}

var _ queryTree = (*correlatedSubqueryTree)(nil)

func (s *correlatedSubqueryTree) tableID() semantics.TableSet {
	return s.inner.tableID().Merge(s.outer.tableID())
}

func (s *correlatedSubqueryTree) cost() int {
	return s.inner.cost() + s.outer.cost()
}

func (s *correlatedSubqueryTree) clone() queryTree {
	result := &correlatedSubqueryTree{
		outer:     s.outer.clone(),
		inner:     s.inner.clone(),
		extracted: s.extracted,
	}
	return result
}

func (s *correlatedSubqueryTree) pushOutputColumns(colnames []*sqlparser.ColName, semTable *semantics.SemTable) ([]int, error) {
	return s.outer.pushOutputColumns(colnames, semTable)
}

func (s *correlatedSubqueryTree) pushPredicate(ctx *planningContext, expr sqlparser.Expr) error {
	return s.outer.pushPredicate(ctx, expr)
}

func (s *correlatedSubqueryTree) removePredicate(ctx *planningContext, expr sqlparser.Expr) error {
	return s.outer.removePredicate(ctx, expr)
}
