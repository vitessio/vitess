/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package planbuilder

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ builder = (*sqlCalcFoundRows)(nil)

type sqlCalcFoundRows struct {
	LimitQuery, CountQuery builder
	ljt, cjt               *jointab
}

//Wireup implements the builder interface
func (s *sqlCalcFoundRows) Wireup(builder, *jointab) error {
	err := s.LimitQuery.Wireup(s.LimitQuery, s.ljt)
	if err != nil {
		return err
	}
	return s.CountQuery.Wireup(s.CountQuery, s.cjt)
}

//Primitive implements the builder interface
func (s *sqlCalcFoundRows) Primitive() engine.Primitive {
	return engine.SQLCalcFoundRows{
		LimitPrimitive: s.LimitQuery.Primitive(),
		CountPrimitive: s.CountQuery.Primitive(),
	}
}

// All the methods below are not implemented. They should not be called on a sqlCalcFoundRows builder

//Order implements the builder interface
func (s *sqlCalcFoundRows) Order() int {
	return s.LimitQuery.Order()
}

//ResultColumns implements the builder interface
func (s *sqlCalcFoundRows) ResultColumns() []*resultColumn {
	return s.LimitQuery.ResultColumns()
}

//Reorder implements the builder interface
func (s *sqlCalcFoundRows) Reorder(order int) {
	s.LimitQuery.Reorder(order)
}

//First implements the builder interface
func (s *sqlCalcFoundRows) First() builder {
	return s.LimitQuery.First()
}

//PushFilter implements the builder interface
func (s *sqlCalcFoundRows) PushFilter(*primitiveBuilder, sqlparser.Expr, string, builder) error {
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unreachable: sqlCalcFoundRows.PushFilter")
}

//PushSelect implements the builder interface
func (s *sqlCalcFoundRows) PushSelect(*primitiveBuilder, *sqlparser.AliasedExpr, builder) (rc *resultColumn, colNumber int, err error) {
	return nil, 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unreachable: sqlCalcFoundRows.PushSelect")
}

//MakeDistinct implements the builder interface
func (s *sqlCalcFoundRows) MakeDistinct() (builder, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unreachable: sqlCalcFoundRows.MakeDistinct")
}

//PushGroupBy implements the builder interface
func (s *sqlCalcFoundRows) PushGroupBy(sqlparser.GroupBy) error {
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unreachable: sqlCalcFoundRows.PushGroupBy")
}

//PushOrderBy implements the builder interface
func (s *sqlCalcFoundRows) PushOrderBy(sqlparser.OrderBy) (builder, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unreachable: sqlCalcFoundRows.PushOrderBy")
}

//SetUpperLimit implements the builder interface
func (s *sqlCalcFoundRows) SetUpperLimit(count sqlparser.Expr) {
	s.LimitQuery.SetUpperLimit(count)
}

//PushMisc implements the builder interface
func (s *sqlCalcFoundRows) PushMisc(sel *sqlparser.Select) error {
	return s.LimitQuery.PushMisc(sel)
}

//SupplyVar implements the builder interface
func (s *sqlCalcFoundRows) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	s.LimitQuery.SupplyVar(from, to, col, varname)
}

//SupplyCol implements the builder interface
func (s *sqlCalcFoundRows) SupplyCol(col *sqlparser.ColName) (*resultColumn, int) {
	return s.LimitQuery.SupplyCol(col)
}

//SupplyWeightString implements the builder interface
func (s *sqlCalcFoundRows) SupplyWeightString(int) (weightcolNumber int, err error) {
	return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unreachable: sqlCalcFoundRows.SupplyWeightString")
}

//PushLock implements the builder interface
func (s *sqlCalcFoundRows) PushLock(sqlparser.Lock) error {
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unreachable: sqlCalcFoundRows.PushLock")
}
