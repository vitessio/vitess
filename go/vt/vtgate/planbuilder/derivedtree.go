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
	query         sqlparser.SelectStatement
	inner         queryTree
	alias         string
	columnAliases sqlparser.Columns

	// columns needed to feed other plans
	columns       []*sqlparser.ColName
	columnsOffset []int
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

func (d *derivedTree) pushOutputColumns(names []*sqlparser.ColName, semTable *semantics.SemTable) (offsets []int, err error) {
	var noQualifierNames []*sqlparser.ColName
	if len(names) == 0 {
		return
	}
	for _, name := range names {
		i, err := d.findOutputColumn(name)
		if err != nil {
			return nil, err
		}
		if i > -1 {
			d.columnsOffset = append(d.columnsOffset, i)
			continue
		}
		d.columnsOffset = append(d.columnsOffset, i)
		d.columns = append(d.columns, name)
		noQualifierNames = append(noQualifierNames, sqlparser.NewColName(name.Name.String()))
	}
	if len(noQualifierNames) > 0 {
		_, _ = d.inner.pushOutputColumns(noQualifierNames, semTable)
	}
	return d.columnsOffset, nil
}

// findOutputColumn returns the index on which the given name is found in the slice of
// *sqlparser.SelectExprs of the derivedTree. The *sqlparser.SelectExpr must be of type
// *sqlparser.AliasedExpr and match the given name.
// If name is not present but the query's select expressions contain a *sqlparser.StarExpr
// the function will return no error and an index equal to -1.
// If name is not present and the query does not have a *sqlparser.StarExpr, the function
// will return an unknown column error.
func (d *derivedTree) findOutputColumn(name *sqlparser.ColName) (int, error) {
	hasStar := false
	for j, exp := range sqlparser.GetFirstSelect(d.query).SelectExprs {
		switch exp := exp.(type) {
		case *sqlparser.AliasedExpr:
			if !exp.As.IsEmpty() && exp.As.Equal(name.Name) {
				return j, nil
			}
			if exp.As.IsEmpty() {
				col, ok := exp.Expr.(*sqlparser.ColName)
				if !ok {
					return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "complex expression needs column alias: %s", sqlparser.String(exp))
				}
				if name.Name.Equal(col.Name) {
					return j, nil
				}
			}
		case *sqlparser.StarExpr:
			hasStar = true
		}
	}

	// we have found a star but no matching *sqlparser.AliasedExpr, thus we return -1 with no error.
	if hasStar {
		return -1, nil
	}
	return 0, vterrors.NewErrorf(vtrpcpb.Code_NOT_FOUND, vterrors.BadFieldError, "Unknown column '%s' in 'field list'", name.Name.String())
}
