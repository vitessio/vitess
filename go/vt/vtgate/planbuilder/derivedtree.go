/*
Copyright 2021 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type derivedTree struct {
	query sqlparser.SelectStatement
	inner queryTree
	alias string
}

var _ queryTree = (*derivedTree)(nil)

func (d *derivedTree) tableID() semantics.TableSet {
	return d.inner.tableID()
}

func (d *derivedTree) cost() int {
	panic("implement me")
}

func (d *derivedTree) clone() queryTree {
	other := *d
	other.inner = d.inner.clone()
	return &other
}

func (d *derivedTree) pushOutputColumns(names []*sqlparser.ColName, _ *semantics.SemTable) (offsets []int, err error) {
	for _, name := range names {
		offset, err := d.findOutputColumn(name)
		if err != nil {
			return nil, err
		}
		offsets = append(offsets, offset)
	}
	return
}

func (d *derivedTree) findOutputColumn(name *sqlparser.ColName) (int, error) {
	for j, exp := range sqlparser.GetFirstSelect(d.query).SelectExprs {
		ae, ok := exp.(*sqlparser.AliasedExpr)
		if !ok {
			return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "expected AliasedExpr")
		}
		if !ae.As.IsEmpty() && ae.As.Equal(name.Name) {
			return j, nil
		}
		if ae.As.IsEmpty() {
			col, ok := ae.Expr.(*sqlparser.ColName)
			if !ok {
				return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "complex expression needs column alias: %s", sqlparser.String(ae))
			}
			if name.Name.Equal(col.Name) {
				return j, nil
			}
		}
	}
	return 0, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.BadFieldError, "Unknown column '%s' in 'field list'", name.Name.String())
}
