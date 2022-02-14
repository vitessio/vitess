package planbuilder

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ logicalPlan = (*sqlCalcFoundRows)(nil)

type sqlCalcFoundRows struct {
	LimitQuery, CountQuery logicalPlan

	// only used by WireUp for V3
	ljt, cjt *jointab
}

// Wireup implements the logicalPlan interface
func (s *sqlCalcFoundRows) Wireup(logicalPlan, *jointab) error {
	err := s.LimitQuery.Wireup(s.LimitQuery, s.ljt)
	if err != nil {
		return err
	}
	return s.CountQuery.Wireup(s.CountQuery, s.cjt)
}

// WireupGen4 implements the logicalPlan interface
func (s *sqlCalcFoundRows) WireupGen4(semTable *semantics.SemTable) error {
	err := s.LimitQuery.WireupGen4(semTable)
	if err != nil {
		return err
	}
	return s.CountQuery.WireupGen4(semTable)
}

// ContainsTables implements the logicalPlan interface
func (s *sqlCalcFoundRows) ContainsTables() semantics.TableSet {
	return s.LimitQuery.ContainsTables()
}

//Primitive implements the logicalPlan interface
func (s *sqlCalcFoundRows) Primitive() engine.Primitive {
	return engine.SQLCalcFoundRows{
		LimitPrimitive: s.LimitQuery.Primitive(),
		CountPrimitive: s.CountQuery.Primitive(),
	}
}

// All the methods below are not implemented. They should not be called on a sqlCalcFoundRows plan

// Order implements the logicalPlan interface
func (s *sqlCalcFoundRows) Order() int {
	return s.LimitQuery.Order()
}

// ResultColumns implements the logicalPlan interface
func (s *sqlCalcFoundRows) ResultColumns() []*resultColumn {
	return s.LimitQuery.ResultColumns()
}

// Reorder implements the logicalPlan interface
func (s *sqlCalcFoundRows) Reorder(order int) {
	s.LimitQuery.Reorder(order)
}

// SupplyVar implements the logicalPlan interface
func (s *sqlCalcFoundRows) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	s.LimitQuery.SupplyVar(from, to, col, varname)
}

// SupplyCol implements the logicalPlan interface
func (s *sqlCalcFoundRows) SupplyCol(col *sqlparser.ColName) (*resultColumn, int) {
	return s.LimitQuery.SupplyCol(col)
}

// SupplyWeightString implements the logicalPlan interface
func (s *sqlCalcFoundRows) SupplyWeightString(int, bool) (weightcolNumber int, err error) {
	return 0, UnsupportedSupplyWeightString{Type: "sqlCalcFoundRows"}
}

// Rewrite implements the logicalPlan interface
func (s *sqlCalcFoundRows) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 2 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] wrong number of inputs for SQL_CALC_FOUND_ROWS: %d", len(inputs))
	}
	s.LimitQuery = inputs[0]
	s.CountQuery = inputs[1]
	return nil
}

// Inputs implements the logicalPlan interface
func (s *sqlCalcFoundRows) Inputs() []logicalPlan {
	return []logicalPlan{s.LimitQuery, s.CountQuery}
}

// OutputColumns implements the logicalPlan interface
func (s *sqlCalcFoundRows) OutputColumns() []sqlparser.SelectExpr {
	return s.LimitQuery.OutputColumns()
}
