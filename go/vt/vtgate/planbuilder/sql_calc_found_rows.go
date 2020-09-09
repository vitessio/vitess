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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ builder = (*sqlCalcFoundRows)(nil)

type sqlCalcFoundRows struct {
	LimitQuery, CountQuery builder
	ljt, cjt               *jointab
}

func (s *sqlCalcFoundRows) Order() int {
	panic("implement me")
}

func (s *sqlCalcFoundRows) ResultColumns() []*resultColumn {
	panic("implement me")
}

func (s *sqlCalcFoundRows) Reorder(i int) {
	panic("implement me")
}

func (s *sqlCalcFoundRows) First() builder {
	panic("implement me")
}

func (s *sqlCalcFoundRows) PushFilter(pb *primitiveBuilder, filter sqlparser.Expr, whereType string, origin builder) error {
	panic("implement me")
}

func (s *sqlCalcFoundRows) PushSelect(pb *primitiveBuilder, expr *sqlparser.AliasedExpr, origin builder) (rc *resultColumn, colNumber int, err error) {
	panic("implement me")
}

func (s *sqlCalcFoundRows) MakeDistinct() error {
	panic("implement me")
}

func (s *sqlCalcFoundRows) PushGroupBy(by sqlparser.GroupBy) error {
	panic("implement me")
}

func (s *sqlCalcFoundRows) PushOrderBy(by sqlparser.OrderBy) (builder, error) {
	panic("implement me")
}

func (s *sqlCalcFoundRows) SetUpperLimit(count sqlparser.Expr) {
	panic("implement me")
}

func (s *sqlCalcFoundRows) PushMisc(sel *sqlparser.Select) {
	panic("implement me")
}

func (s *sqlCalcFoundRows) Wireup(builder, *jointab) error {
	err := s.LimitQuery.Wireup(s.LimitQuery, s.ljt)
	if err != nil {
		return err
	}
	return s.CountQuery.Wireup(s.CountQuery, s.cjt)
}

func (s *sqlCalcFoundRows) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	panic("implement me")
}

func (s *sqlCalcFoundRows) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	panic("implement me")
}

func (s *sqlCalcFoundRows) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	panic("implement me")
}

func (s *sqlCalcFoundRows) PushLock(lock string) error {
	panic("implement me")
}

func (s *sqlCalcFoundRows) Primitive() engine.Primitive {
	return engine.SQLCalcFoundRows{
		LimitPrimitive: s.LimitQuery.Primitive(),
		CountPrimitive: s.CountQuery.Primitive(),
	}
}
