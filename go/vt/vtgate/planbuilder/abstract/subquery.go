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
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// SubQuery stores the information about subquery
type SubQuery struct {
	Inner []*SubQueryInner
	Outer Operator
}

var _ Operator = (*SubQuery)(nil)

// SubQueryInner stores the subquery information for a select statement
type SubQueryInner struct {
	// Inner is the Operator inside the parenthesis of the subquery.
	// i.e: select (select 1 union select 1), the Inner here would be
	// of type Concatenate since we have a Union.
	Inner Operator

	// Type represents the type of the subquery (value, in, not in, exists)
	Type engine.PulloutOpcode

	// SelectStatement is the inner's select
	SelectStatement *sqlparser.Select

	// ArgName is the substitution argument string for the subquery.
	// Subquery argument name looks like: `__sq1`, with `1` being an
	// unique identifier. This is used when we wish to replace the
	// subquery by an argument for PullOut subqueries.
	ArgName string

	// HasValues is a string of form `__sq_has_values1` with `1` being
	// a unique identifier that matches the one used in ArgName.
	// We use `__sq_has_values` for in and not in subqueries.
	HasValues string

	// ExprsNeedReplace is a slice of all the expressions that were
	// introduced by the rewrite of the subquery and that potentially
	// need to be re-replace if we can merge the subquery into a route.
	// An expression that contains at least all of ExprsNeedReplace will
	// be replaced by the expression in ReplaceBy.
	ExprsNeedReplace []sqlparser.Expr
	ReplaceBy        sqlparser.Expr
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
func (s *SubQuery) PushPredicate(sqlparser.Expr, *semantics.SemTable) error {
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] should not try to push predicate on subquery")
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
func (s *SubQuery) Compact(*semantics.SemTable) (Operator, error) {
	return s, nil
}
