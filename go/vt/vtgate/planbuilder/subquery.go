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
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ logicalPlan = (*subquery)(nil)

// subquery is a logicalPlan that wraps a subquery.
// This primitive wraps any subquery that results
// in something that's not a route. It builds a
// 'table' for the subquery allowing higher level
// constructs to reference its columns. If a subquery
// results in a route primitive, we instead build
// a new route that keeps the subquery in the FROM
// clause, because a route is more versatile than
// a subquery.
type subquery struct {
	logicalPlanCommon
	resultColumns []*resultColumn
	esubquery     *engine.Subquery
}

// newSubquery builds a new subquery.
func newSubquery(alias sqlparser.TableIdent, plan logicalPlan) (*subquery, *symtab, error) {
	sq := &subquery{
		logicalPlanCommon: newBuilderCommon(plan),
		esubquery:         &engine.Subquery{},
	}

	// Create a 'table' that represents the subquery.
	t := &table{
		alias:  sqlparser.TableName{Name: alias},
		origin: sq,
	}

	// Create column symbols based on the result column names.
	for _, rc := range plan.ResultColumns() {
		if _, ok := t.columns[rc.alias.Lowered()]; ok {
			return nil, nil, fmt.Errorf("duplicate column names in subquery: %s", sqlparser.String(rc.alias))
		}
		t.addColumn(rc.alias, &column{origin: sq})
	}
	t.isAuthoritative = true
	st := newSymtab()
	// AddTable will not fail because symtab is empty.
	_ = st.AddTable(t)
	return sq, st, nil
}

// Primitive implements the logicalPlan interface
func (sq *subquery) Primitive() engine.Primitive {
	sq.esubquery.Subquery = sq.input.Primitive()
	return sq.esubquery
}

// ResultColumns implements the logicalPlan interface
func (sq *subquery) ResultColumns() []*resultColumn {
	return sq.resultColumns
}

// SupplyCol implements the logicalPlan interface
func (sq *subquery) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	c := col.Metadata.(*column)
	for i, rc := range sq.resultColumns {
		if rc.column == c {
			return rc, i
		}
	}

	// columns that reference subqueries will have their colNumber set.
	// Let's use it here.
	sq.esubquery.Cols = append(sq.esubquery.Cols, c.colNumber)
	sq.resultColumns = append(sq.resultColumns, &resultColumn{column: c})
	return rc, len(sq.resultColumns) - 1
}
