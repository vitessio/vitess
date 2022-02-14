package physical

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// ApplyJoin is a nested loop join - for each row on the LHS,
// we'll execute the plan on the RHS, feeding data from left to right
type ApplyJoin struct {
	LHS, RHS abstract.PhysicalOperator

	// Columns stores the column indexes of the columns coming from the left and right side
	// negative value comes from LHS and positive from RHS
	Columns []int

	// Vars are the arguments that need to be copied from the LHS to the RHS
	Vars map[string]int

	// LeftJoin will be true in the case of an outer join
	LeftJoin bool

	Predicate sqlparser.Expr
}

var _ abstract.PhysicalOperator = (*ApplyJoin)(nil)

// IPhysical implements the PhysicalOperator interface
func (a *ApplyJoin) IPhysical() {}

// TableID implements the PhysicalOperator interface
func (a *ApplyJoin) TableID() semantics.TableSet {
	return a.LHS.TableID().Merge(a.RHS.TableID())
}

// UnsolvedPredicates implements the PhysicalOperator interface
func (a *ApplyJoin) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	panic("implement me")
}

// CheckValid implements the PhysicalOperator interface
func (a *ApplyJoin) CheckValid() error {
	err := a.LHS.CheckValid()
	if err != nil {
		return err
	}
	return a.RHS.CheckValid()
}

// Compact implements the PhysicalOperator interface
func (a *ApplyJoin) Compact(semTable *semantics.SemTable) (abstract.Operator, error) {
	return a, nil
}

// Cost implements the PhysicalOperator interface
func (a *ApplyJoin) Cost() int {
	return a.LHS.Cost() + a.RHS.Cost()
}

// Clone implements the PhysicalOperator interface
func (a *ApplyJoin) Clone() abstract.PhysicalOperator {
	varsClone := map[string]int{}
	for key, value := range a.Vars {
		varsClone[key] = value
	}
	columnsClone := make([]int, len(a.Columns))
	copy(columnsClone, a.Columns)
	return &ApplyJoin{
		LHS:       a.LHS.Clone(),
		RHS:       a.RHS.Clone(),
		Columns:   columnsClone,
		Vars:      varsClone,
		LeftJoin:  a.LeftJoin,
		Predicate: sqlparser.CloneExpr(a.Predicate),
	}
}
