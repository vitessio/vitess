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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// SubQuery stores the information about subquery
type SubQuery struct {
	Inner []*SubQueryInner
	Outer LogicalOperator
}

var _ LogicalOperator = (*SubQuery)(nil)

func (*SubQuery) iLogical() {}

// SubQueryInner stores the subquery information for a select statement
type SubQueryInner struct {
	// Inner is the Operator inside the parenthesis of the subquery.
	// i.e: select (select 1 union select 1), the Inner here would be
	// of type Concatenate since we have a Union.
	Inner LogicalOperator

	// ExtractedSubquery contains all information we need about this subquery
	ExtractedSubquery *sqlparser.ExtractedSubquery
}

// TableID implements the Operator interface
func (s *SubQuery) TableID() semantics.TableSet {
	ts := s.Outer.TableID()
	for _, inner := range s.Inner {
		ts = ts.Merge(inner.Inner.TableID())
	}
	return ts
}

// PushPredicate implements the Operator interface
func (s *SubQuery) PushPredicate(sqlparser.Expr, *semantics.SemTable) (LogicalOperator, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] should not try to push predicate on subquery")
}

// UnsolvedPredicates implements the Operator interface
func (s *SubQuery) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	ts := s.TableID()
	var result []sqlparser.Expr

	for _, expr := range s.Outer.UnsolvedPredicates(semTable) {
		deps := semTable.DirectDeps(expr)
		if !deps.IsSolvedBy(ts) {
			result = append(result, expr)
		}
	}
	for _, inner := range s.Inner {
		for _, expr := range inner.Inner.UnsolvedPredicates(semTable) {
			deps := semTable.DirectDeps(expr)
			if !deps.IsSolvedBy(ts) {
				result = append(result, expr)
			}
		}
	}
	return result
}

// CheckValid implements the Operator interface
func (s *SubQuery) CheckValid() error {
	for _, inner := range s.Inner {
		err := inner.Inner.CheckValid()
		if err != nil {
			return err
		}
	}
	return s.Outer.CheckValid()
}

// Compact implements the Operator interface
func (s *SubQuery) Compact(*semantics.SemTable) (LogicalOperator, error) {
	return s, nil
}
