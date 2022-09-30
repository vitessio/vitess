/*
Copyright 2022 The Vitess Authors.

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

package physical

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type Derived struct {
	Source abstract.PhysicalOperator

	Query         sqlparser.SelectStatement
	Alias         string
	ColumnAliases sqlparser.Columns

	// Columns needed to feed other plans
	Columns       []*sqlparser.ColName
	ColumnsOffset []int
}

var _ abstract.PhysicalOperator = (*Derived)(nil)

// TableID implements the PhysicalOperator interface
func (d *Derived) TableID() semantics.TableSet {
	return d.Source.TableID()
}

// UnsolvedPredicates implements the PhysicalOperator interface
func (d *Derived) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return d.Source.UnsolvedPredicates(semTable)
}

// CheckValid implements the PhysicalOperator interface
func (d *Derived) CheckValid() error {
	return d.Source.CheckValid()
}

// IPhysical implements the PhysicalOperator interface
func (d *Derived) IPhysical() {}

// Cost implements the PhysicalOperator interface
func (d *Derived) Cost() int {
	return d.Source.Cost()
}

// Clone implements the PhysicalOperator interface
func (d *Derived) Clone() abstract.PhysicalOperator {
	clone := *d
	clone.Source = d.Source.Clone()
	clone.ColumnAliases = sqlparser.CloneColumns(d.ColumnAliases)
	clone.Columns = make([]*sqlparser.ColName, 0, len(d.Columns))
	for _, x := range d.Columns {
		clone.Columns = append(clone.Columns, sqlparser.CloneRefOfColName(x))
	}
	clone.ColumnsOffset = make([]int, 0, len(d.ColumnsOffset))
	copy(clone.ColumnsOffset, d.ColumnsOffset)
	return &clone
}

// findOutputColumn returns the index on which the given name is found in the slice of
// *sqlparser.SelectExprs of the derivedTree. The *sqlparser.SelectExpr must be of type
// *sqlparser.AliasedExpr and match the given name.
// If name is not present but the query's select expressions contain a *sqlparser.StarExpr
// the function will return no error and an index equal to -1.
// If name is not present and the query does not have a *sqlparser.StarExpr, the function
// will return an unknown column error.
func (d *Derived) findOutputColumn(name *sqlparser.ColName) (int, error) {
	hasStar := false
	for j, exp := range sqlparser.GetFirstSelect(d.Query).SelectExprs {
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

// IsMergeable is not a great name for this function. Suggestions for a better one are welcome!
// This function will return false if the derived table inside it has to run on the vtgate side, and so can't be merged with subqueries
// This logic can also be used to check if this is a derived table that can be had on the left hand side of a vtgate join.
// Since vtgate joins are always nested loop joins, we can't execute them on the RHS
// if they do some things, like LIMIT or GROUP BY on wrong columns
func (d *Derived) IsMergeable(ctx *plancontext.PlanningContext) bool {
	return isMergeable(ctx, d.Query, d)
}
