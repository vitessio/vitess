/*
Copyright 2023 The Vitess Authors.

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

package operators

import (
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// InsertSelection operator represents an INSERT into SELECT FROM query.
// It holds the operators for running the selection and insertion.
type InsertSelection struct {
	SelectionOp ops.Operator
	InsertionOp ops.Operator

	// ForceNonStreaming when true, select first then insert, this is to avoid locking rows by select for insert.
	ForceNonStreaming bool

	noColumns
	noPredicates
}

func (is *InsertSelection) Clone(inputs []ops.Operator) ops.Operator {
	return &InsertSelection{
		SelectionOp: inputs[0],
		InsertionOp: inputs[1],
	}
}

func (is *InsertSelection) Inputs() []ops.Operator {
	return []ops.Operator{is.SelectionOp, is.InsertionOp}
}

func (is *InsertSelection) SetInputs(inputs []ops.Operator) {
	is.SelectionOp = inputs[0]
	is.InsertionOp = inputs[1]
}

func (is *InsertSelection) ShortDescription() string {
	return ""
}

func (is *InsertSelection) GetOrdering(*plancontext.PlanningContext) []ops.OrderBy {
	return nil
}

var _ ops.Operator = (*InsertSelection)(nil)
