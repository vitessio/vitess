/*
Copyright 2019 The Vitess Authors.

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
	"errors"
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ builder = (*limit)(nil)

// limit is the builder for engine.Limit.
// This gets built if a limit needs to be applied
// after rows are returned from an underlying
// operation. Since a limit is the final operation
// of a SELECT, most pushes are not applicable.
type limit struct {
	builderCommon
	elimit *engine.Limit
}

// newLimit builds a new limit.
func newLimit(bldr builder) *limit {
	return &limit{
		builderCommon: newBuilderCommon(bldr),
		elimit:        &engine.Limit{},
	}
}

// Primitive satisfies the builder interface.
func (l *limit) Primitive() engine.Primitive {
	l.elimit.Input = l.input.Primitive()
	return l.elimit
}

// PushFilter satisfies the builder interface.
func (l *limit) PushFilter(_ *primitiveBuilder, _ sqlparser.Expr, whereType string, _ builder) error {
	return errors.New("limit.PushFilter: unreachable")
}

// PushSelect satisfies the builder interface.
func (l *limit) PushSelect(_ *primitiveBuilder, expr *sqlparser.AliasedExpr, origin builder) (rc *resultColumn, colNumber int, err error) {
	return nil, 0, errors.New("limit.PushSelect: unreachable")
}

// MakeDistinct satisfies the builder interface.
func (l *limit) MakeDistinct() error {
	return errors.New("limit.MakeDistinct: unreachable")
}

// PushGroupBy satisfies the builder interface.
func (l *limit) PushGroupBy(_ sqlparser.GroupBy) error {
	return errors.New("limit.PushGroupBy: unreachable")
}

// PushGroupBy satisfies the builder interface.
func (l *limit) PushOrderBy(orderBy sqlparser.OrderBy) (builder, error) {
	return nil, errors.New("limit.PushOrderBy: unreachable")
}

// SetLimit sets the limit for the primitive. It calls the underlying
// primitive's SetUpperLimit, which is an optimization hint that informs
// the underlying primitive that it doesn't need to return more rows than
// specified.
func (l *limit) SetLimit(limit *sqlparser.Limit) error {
	count, ok := limit.Rowcount.(*sqlparser.SQLVal)
	if !ok {
		return fmt.Errorf("unexpected expression in LIMIT: %v", sqlparser.String(limit))
	}
	pv, err := sqlparser.NewPlanValue(count)
	if err != nil {
		return err
	}
	l.elimit.Count = pv

	switch offset := limit.Offset.(type) {
	case *sqlparser.SQLVal:
		pv, err = sqlparser.NewPlanValue(offset)
		if err != nil {
			return err
		}
		l.elimit.Offset = pv
	case nil:
		// NOOP
	default:
		return fmt.Errorf("unexpected expression in LIMIT: %v", sqlparser.String(limit))
	}

	l.input.SetUpperLimit(sqlparser.NewValArg([]byte(":__upper_limit")))
	return nil
}

// SetUpperLimit satisfies the builder interface.
// This is a no-op because we actually call SetLimit for this primitive.
// In the future, we may have to honor this call for subqueries.
func (l *limit) SetUpperLimit(count *sqlparser.SQLVal) {
}
