package abstract

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ Operator = (*QueryTable)(nil)

func (t *QueryTable) TableID() semantics.TableSet {
	return t.ID
}

func (t *QueryTable) PushPredicate(expr sqlparser.Expr, _ *semantics.SemTable) error {
	t.Predicates = append(t.Predicates, expr)
	return nil
}

func (t *QueryTable) UnsolvedPredicates(*semantics.SemTable) []sqlparser.Expr {
	return nil
}

func (t *QueryTable) CheckValid() error {
	return nil
}

func (t *QueryTable) Compact(*semantics.SemTable) (Operator, error) {
	return t, nil
}
