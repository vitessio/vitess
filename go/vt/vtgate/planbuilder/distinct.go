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

var _ builder = (*distinct)(nil)

// limit is the builder for engine.Limit.
// This gets built if a limit needs to be applied
// after rows are returned from an underlying
// operation. Since a limit is the final operation
// of a SELECT, most pushes are not applicable.
type distinct struct {
	builderCommon
}

func newDistinct(source builder) builder {
	return &distinct{
		builderCommon: newBuilderCommon(source),
	}
}

func (d *distinct) PushFilter(pb *primitiveBuilder, filter sqlparser.Expr, whereType string, origin builder) error {
	return d.input.PushFilter(pb, filter, whereType, origin)
}

func (d *distinct) PushSelect(pb *primitiveBuilder, expr *sqlparser.AliasedExpr, origin builder) (rc *resultColumn, colNumber int, err error) {
	return d.input.PushSelect(pb, expr, origin)
}

func (d *distinct) MakeDistinct() error {
	return nil
}

func (d *distinct) PushGroupBy(by sqlparser.GroupBy) error {
	return d.input.PushGroupBy(by)
}

func (d *distinct) PushOrderBy(by sqlparser.OrderBy) (builder, error) {
	orderBy, err := d.input.PushOrderBy(by)
	if err != nil {
		return nil, err
	}
	return &distinct{builderCommon: newBuilderCommon(orderBy)}, nil
}

func (d *distinct) PushLock(lock sqlparser.Lock) error {
	return d.input.PushLock(lock)
}

func (d *distinct) Primitive() engine.Primitive {
	return &engine.Distinct{
		Source: d.input.Primitive(),
	}
}
