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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type subqueryTree struct {
	outer     queryTree
	inner     queryTree
	extracted *sqlparser.ExtractedSubquery
}

var _ queryTree = (*subqueryTree)(nil)

func (s *subqueryTree) tableID() semantics.TableSet {
	return s.inner.tableID().Merge(s.outer.tableID())
}

func (s *subqueryTree) cost() int {
	return s.inner.cost() + s.outer.cost()
}

func (s *subqueryTree) clone() queryTree {
	result := &subqueryTree{
		outer:     s.outer.clone(),
		inner:     s.inner.clone(),
		extracted: s.extracted,
	}
	return result
}

func (s *subqueryTree) pushOutputColumns([]*sqlparser.ColName, *semantics.SemTable) ([]int, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] should not try to push output columns on subquery")
}

func (s *subqueryTree) pushPredicate(ctx *planningContext, expr sqlparser.Expr) error {
	return s.outer.pushPredicate(ctx, expr)
}

func (s *subqueryTree) removePredicate(ctx *planningContext, expr sqlparser.Expr) error {
	return s.outer.removePredicate(ctx, expr)
}
