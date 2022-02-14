package physical

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type Table struct {
	QTable  *abstract.QueryTable
	VTable  *vindexes.Table
	Columns []*sqlparser.ColName
}

var _ abstract.PhysicalOperator = (*Table)(nil)

// IPhysical implements the PhysicalOperator interface
func (to *Table) IPhysical() {}

// Cost implements the PhysicalOperator interface
func (to *Table) Cost() int {
	return 0
}

// Clone implements the PhysicalOperator interface
func (to *Table) Clone() abstract.PhysicalOperator {
	var columns []*sqlparser.ColName
	for _, name := range to.Columns {
		columns = append(columns, sqlparser.CloneRefOfColName(name))
	}
	return &Table{
		QTable:  to.QTable,
		VTable:  to.VTable,
		Columns: columns,
	}
}

// TableID implements the PhysicalOperator interface
func (to *Table) TableID() semantics.TableSet {
	return to.QTable.ID
}

// PushPredicate implements the PhysicalOperator interface
func (to *Table) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "we should not push Predicates into a Table. It is meant to be immutable")
}

// UnsolvedPredicates implements the PhysicalOperator interface
func (to *Table) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	panic("implement me")
}

// CheckValid implements the PhysicalOperator interface
func (to *Table) CheckValid() error {
	return nil
}

// Compact implements the PhysicalOperator interface
func (to *Table) Compact(semTable *semantics.SemTable) (abstract.Operator, error) {
	return to, nil
}
