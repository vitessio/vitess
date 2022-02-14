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
	f.Predicates = append(f.Predicates, expr)
	return f, nil
}

// Compact implements the LogicalOperator interface
func (f *Filter) Compact(semTable *semantics.SemTable) (LogicalOperator, error) {
	if len(f.Predicates) == 0 {
		return f.Source, nil
	}

	return f, nil
}
