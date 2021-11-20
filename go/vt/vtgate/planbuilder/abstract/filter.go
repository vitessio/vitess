package abstract

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type Filter struct {
	Source     Operator
	Predicates []sqlparser.Expr
}

var _ Operator = (*Filter)(nil)

func (f *Filter) TableID() semantics.TableSet {
	return f.Source.TableID()
}

func (f *Filter) PushPredicate(expr sqlparser.Expr, _ *semantics.SemTable) error {
	f.Predicates = append(f.Predicates, expr)
	return nil
}

func (f *Filter) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return f.Source.UnsolvedPredicates(semTable)
}

func (f *Filter) CheckValid() error {
	return f.Source.CheckValid()
}

func (f *Filter) Compact(semTable *semantics.SemTable) (Operator, error) {
	return f.Source.Compact(semTable)
}
