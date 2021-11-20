package abstract

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type Projection struct {
	Source      Operator
	Expressions []*sqlparser.AliasedExpr
}

var _ Operator = (*Projection)(nil)

func (p *Projection) TableID() semantics.TableSet {
	return p.Source.TableID()
}

func (p *Projection) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	return p.Source.PushPredicate(expr, semTable)
}

func (p *Projection) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return p.UnsolvedPredicates(semTable)
}

func (p *Projection) CheckValid() error {
	return p.Source.CheckValid()
}

func (p *Projection) Compact(semTable *semantics.SemTable) (Operator, error) {
	return p.Compact(semTable)
}
