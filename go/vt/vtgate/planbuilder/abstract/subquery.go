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
