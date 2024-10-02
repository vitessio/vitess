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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// InsertSelection operator represents an INSERT into SELECT FROM query.
// It holds the operators for running the selection and insertion.
type InsertSelection struct {
	binaryOperator

	// ForceNonStreaming when true, select first then insert, this is to avoid locking rows by select for insert.
	ForceNonStreaming bool

	noColumns
	noPredicates
}

var _ Operator = (*InsertSelection)(nil)

func (is *InsertSelection) Clone(inputs []Operator) Operator {
	klone := *is
	klone.LHS = inputs[0]
	klone.RHS = inputs[1]
	return &klone
}

func (is *InsertSelection) ShortDescription() string {
	if is.ForceNonStreaming {
		return "NonStreaming"
	}
	return ""
}

func (is *InsertSelection) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	return nil
}

func (is *InsertSelection) Select() Operator {
	return is.LHS
}

func (is *InsertSelection) Insert() Operator {
	return is.RHS
}
