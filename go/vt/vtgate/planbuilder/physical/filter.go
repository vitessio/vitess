package physical

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type Filter struct {
	Source     abstract.PhysicalOperator
	Predicates []sqlparser.Expr
}

var _ abstract.PhysicalOperator = (*Filter)(nil)

// IPhysical implements the PhysicalOperator interface
func (f *Filter) IPhysical() {}

// TableID implements the PhysicalOperator interface
func (f *Filter) TableID() semantics.TableSet {
	return f.Source.TableID()
}

// PushPredicate implements the PhysicalOperator interface
func (f *Filter) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	panic("unimplemented")
}

// UnsolvedPredicates implements the PhysicalOperator interface
func (f *Filter) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	panic("implement me")
}

// CheckValid implements the PhysicalOperator interface
func (f *Filter) CheckValid() error {
	return f.Source.CheckValid()
}

// Compact implements the PhysicalOperator interface
func (f *Filter) Compact(semTable *semantics.SemTable) (abstract.Operator, error) {
	return f, nil
}

// Cost implements the PhysicalOperator interface
func (f *Filter) Cost() int {
	return f.Source.Cost()
}

// Clone implements the PhysicalOperator interface
func (f *Filter) Clone() abstract.PhysicalOperator {
	predicatesClone := make([]sqlparser.Expr, len(f.Predicates))
	copy(predicatesClone, f.Predicates)
	return &Filter{
		Source:     f.Source.Clone(),
		Predicates: predicatesClone,
	}
}
