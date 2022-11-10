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

package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// Horizon is an operator we use until we decide how to handle the source to the horizon.
// It contains information about the planning we have to do after deciding how we will send the query to the tablets.
type Horizon struct {
	Source ops.Operator
	Select sqlparser.SelectStatement

	noColumns
}

var _ ops.Operator = (*Horizon)(nil)
var _ ops.PhysicalOperator = (*Horizon)(nil)

func (h *Horizon) IPhysical() {}

func (h *Horizon) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newSrc, err := h.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	h.Source = newSrc
	return h, nil
}

func (h *Horizon) Clone(inputs []ops.Operator) ops.Operator {
	return &Horizon{
		Source: inputs[0],
		Select: h.Select,
	}
}

func (h *Horizon) Inputs() []ops.Operator {
	return []ops.Operator{h.Source}
}
