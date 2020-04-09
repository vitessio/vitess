package planbuilder

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ builder = (*vtgateExecution)(nil)

type vtgateExecution struct {
	Exprs []sqltypes.Expr
	Cols  []string
}

func (v *vtgateExecution) Order() int {
	panic("implement me")
}

func (v *vtgateExecution) ResultColumns() []*resultColumn {
	panic("implement me")
}

func (v *vtgateExecution) Reorder(int) {
	panic("implement me")
}

func (v *vtgateExecution) First() builder {
	panic("implement me")
}

func (v *vtgateExecution) PushFilter(pb *primitiveBuilder, filter sqlparser.Expr, whereType string, origin builder) error {
	panic("implement me")
}

func (v *vtgateExecution) PushSelect(pb *primitiveBuilder, expr *sqlparser.AliasedExpr, origin builder) (rc *resultColumn, colNumber int, err error) {
	panic("implement me")
}

func (v *vtgateExecution) MakeDistinct() error {
	panic("implement me")
}

func (v *vtgateExecution) PushGroupBy(sqlparser.GroupBy) error {
	panic("implement me")
}

func (v *vtgateExecution) PushOrderBy(sqlparser.OrderBy) (builder, error) {
	panic("implement me")
}

func (v *vtgateExecution) SetUpperLimit(count *sqlparser.SQLVal) {
	panic("implement me")
}

func (v *vtgateExecution) PushMisc(sel *sqlparser.Select) {
	panic("implement me")
}

func (v *vtgateExecution) Wireup(bldr builder, jt *jointab) error {
	return nil
}

func (v *vtgateExecution) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	panic("implement me")
}

func (v *vtgateExecution) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	panic("implement me")
}

func (v *vtgateExecution) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	panic("implement me")
}

func (v *vtgateExecution) Primitive() engine.Primitive {
	return &engine.Projection{
		Exprs: v.Exprs,
		Cols:  v.Cols,
		Input: &engine.SingleRow{},
	}
}
