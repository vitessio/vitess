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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

var _ logicalPlan = (*mergeSort)(nil)

// mergeSort is a pseudo-primitive. It amends the
// the underlying Route to perform a merge sort.
// It's differentiated as a separate primitive
// because some operations cannot be pushed down,
// which would otherwise be possible with a simple route.
// Since ORDER BY happens near the end of the SQL processing,
// most functions of this primitive are unreachable.
type mergeSort struct {
	resultsBuilder
	truncateColumnCount int
}

// SetTruncateColumnCount satisfies the truncater interface.
// This function records the truncate column count and sets
// it later on the eroute during wire-up phase.
func (ms *mergeSort) SetTruncateColumnCount(count int) {
	ms.truncateColumnCount = count
}

// Primitive implements the logicalPlan interface
func (ms *mergeSort) Primitive() engine.Primitive {
	return ms.input.Primitive()
}

func (ms *mergeSort) Wireup(ctx *plancontext.PlanningContext) error {
	return ms.input.Wireup(ctx)
}

// OutputColumns implements the logicalPlan interface
func (ms *mergeSort) OutputColumns() []sqlparser.SelectExpr {
	outputCols := ms.input.OutputColumns()
	if ms.truncateColumnCount > 0 {
		return outputCols[:ms.truncateColumnCount]
	}
	return outputCols
}
