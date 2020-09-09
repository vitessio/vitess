package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ builder = (*sqlCalcFoundRows)(nil)

type sqlCalcFoundRows struct {
	LimitQuery, CountQuery builder
}

func (s sqlCalcFoundRows) Order() int {
	panic("implement me")
}

func (s sqlCalcFoundRows) ResultColumns() []*resultColumn {
	panic("implement me")
}

func (s sqlCalcFoundRows) Reorder(i int) {
	panic("implement me")
}

func (s sqlCalcFoundRows) First() builder {
	panic("implement me")
}

func (s sqlCalcFoundRows) PushFilter(pb *primitiveBuilder, filter sqlparser.Expr, whereType string, origin builder) error {
	panic("implement me")
}

func (s sqlCalcFoundRows) PushSelect(pb *primitiveBuilder, expr *sqlparser.AliasedExpr, origin builder) (rc *resultColumn, colNumber int, err error) {
	panic("implement me")
}

func (s sqlCalcFoundRows) MakeDistinct() error {
	panic("implement me")
}

func (s sqlCalcFoundRows) PushGroupBy(by sqlparser.GroupBy) error {
	panic("implement me")
}

func (s sqlCalcFoundRows) PushOrderBy(by sqlparser.OrderBy) (builder, error) {
	panic("implement me")
}

func (s sqlCalcFoundRows) SetUpperLimit(count sqlparser.Expr) {
	panic("implement me")
}

func (s sqlCalcFoundRows) PushMisc(sel *sqlparser.Select) {
	panic("implement me")
}

func (s sqlCalcFoundRows) Wireup(bldr builder, jt *jointab) error {
	err := s.LimitQuery.Wireup(bldr, jt)
	if err != nil {
		return err
	}
	return s.CountQuery.Wireup(bldr, jt)
}

func (s sqlCalcFoundRows) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	panic("implement me")
}

func (s sqlCalcFoundRows) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	panic("implement me")
}

func (s sqlCalcFoundRows) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	panic("implement me")
}

func (s sqlCalcFoundRows) PushLock(lock string) error {
	panic("implement me")
}

func (s sqlCalcFoundRows) Primitive() engine.Primitive {
	return engine.SQLCalFoundRows{
		LimitPrimitive: s.LimitQuery.Primitive(),
		CountPrimitive: s.CountQuery.Primitive(),
	}
}
