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

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ logicalPlan = (*memorySort)(nil)

// memorySort is the logicalPlan for engine.Limit.
// This gets built if a limit needs to be applied
// after rows are returned from an underlying
// operation. Since a limit is the final operation
// of a SELECT, most pushes are not applicable.
type memorySort struct {
	resultsBuilder
	eMemorySort *engine.MemorySort
}

func findColNumber(ms *memorySort, expr *sqlparser.ColName) int {
	c := expr.Metadata.(*column)
	for i, rc := range ms.ResultColumns() {
		if rc.column == c {
			return i
		}
	}
	return -1
}

// newMemorySort builds a new memorySort.
func newMemorySort(plan logicalPlan, orderBy v3OrderBy) (*memorySort, error) {
	eMemorySort := &engine.MemorySort{}
	ms := &memorySort{
		resultsBuilder: newResultsBuilder(plan, eMemorySort),
		eMemorySort:    eMemorySort,
	}
	for _, order := range orderBy {
		var colNumber int
		switch expr := order.Expr.(type) {
		case *sqlparser.Literal:
			var err error
			if colNumber, err = ResultFromNumber(ms.ResultColumns(), expr, "order clause"); err != nil {
				return nil, err
			}
		case *sqlparser.ColName:
			colNumber = findColNumber(ms, expr)
		case *sqlparser.CastExpr:
			colName, ok := expr.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, vterrors.VT12001(fmt.Sprintf("memory sort: complex ORDER BY expression: %s", sqlparser.String(expr)))
			}
			colNumber = findColNumber(ms, colName)
		case *sqlparser.ConvertExpr:
			colName, ok := expr.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, vterrors.VT12001(fmt.Sprintf("memory sort: complex ORDER BY expression: %s", sqlparser.String(expr)))
			}
			colNumber = findColNumber(ms, colName)
		default:
			return nil, vterrors.VT12001(fmt.Sprintf("memory sort: complex ORDER BY expression: %s", sqlparser.String(expr)))
		}
		// If column is not found, then the order by is referencing
		// a column that's not on the select list.
		if colNumber == -1 {
			return nil, vterrors.VT12001(fmt.Sprintf("memory sort: ORDER BY must reference a column in the SELECT list: %s", sqlparser.String(order)))
		}
		// TODO(king-11) need to pass in collation here
		ob := engine.OrderByParams{
			Col:               colNumber,
			WeightStringCol:   -1,
			Desc:              order.Direction == sqlparser.DescOrder,
			StarColFixedIndex: colNumber,
			FromGroupBy:       order.fromGroupBy,
			CollationID:       collations.Unknown,
		}
		ms.eMemorySort.OrderBy = append(ms.eMemorySort.OrderBy, ob)
	}
	return ms, nil
}

// Primitive implements the logicalPlan interface
func (ms *memorySort) Primitive() engine.Primitive {
	ms.eMemorySort.Input = ms.input.Primitive()
	return ms.eMemorySort
}

// SetLimit implements the logicalPlan interface
func (ms *memorySort) SetLimit(limit *sqlparser.Limit) error {
	return vterrors.VT13001("memorySort.Limit: unreachable")
}

// Wireup implements the logicalPlan interface
// If text columns are detected in the keys, then the function modifies
// the primitive to pull a corresponding weight_string from mysql and
// compare those instead. This is because we currently don't have the
// ability to mimic mysql's collation behavior.
func (ms *memorySort) Wireup(plan logicalPlan, jt *jointab) error {
	for i, orderby := range ms.eMemorySort.OrderBy {
		rc := ms.resultColumns[orderby.Col]
		// Add a weight_string column if we know that the column is a textual column or if its type is unknown
		if sqltypes.IsText(rc.column.typ) || rc.column.typ == sqltypes.Null {
			weightcolNumber, err := ms.input.SupplyWeightString(orderby.Col, orderby.FromGroupBy)
			if err != nil {
				_, isUnsupportedErr := err.(UnsupportedSupplyWeightString)
				if isUnsupportedErr {
					continue
				}
				return err
			}
			ms.weightStrings[rc] = weightcolNumber
			ms.eMemorySort.OrderBy[i].WeightStringCol = weightcolNumber
			ms.eMemorySort.TruncateColumnCount = len(ms.resultColumns)
		}
	}
	return ms.input.Wireup(plan, jt)
}

func (ms *memorySort) WireupGen4(ctx *plancontext.PlanningContext) error {
	return ms.input.WireupGen4(ctx)
}
