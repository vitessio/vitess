/*
Copyright 2025 The Vitess Authors.

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

type ValuesJoin struct {
	binaryOperator

	bindVarName string

	noColumns
}

func (v *ValuesJoin) Clone(inputs []Operator) Operator {
	clone := *v
	clone.LHS = inputs[0]
	clone.RHS = inputs[1]
	return &clone
}

func (v *ValuesJoin) ShortDescription() string {
	return ""
}

func (v *ValuesJoin) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return v.RHS.GetOrdering(ctx)
}

var _ Operator = (*ValuesJoin)(nil)
