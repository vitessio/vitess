/*
Copyright 2020 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

var _ logicalPlan = (*distinct)(nil)

// distinct is the logicalPlan for engine.Distinct.
type distinct struct {
	logicalPlanCommon
	checkCols []engine.CheckCol
}

func newDistinct(source logicalPlan, checkCols []engine.CheckCol) logicalPlan {
	return &distinct{
		logicalPlanCommon: newBuilderCommon(source),
		checkCols:         checkCols,
	}
}

func (d *distinct) Primitive() engine.Primitive {
	truncate := false
	for i, col := range d.checkCols {
		if col.Idx != i {
			truncate = true
			break
		}
	}
	return &engine.Distinct{
		Source:    d.input.Primitive(),
		CheckCols: d.checkCols,
		Truncate:  truncate,
	}
}

// Rewrite implements the logicalPlan interface
func (d *distinct) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "distinct: wrong number of inputs")
	}
	d.input = inputs[0]
	return nil
}

// Inputs implements the logicalPlan interface
func (d *distinct) Inputs() []logicalPlan {
	return []logicalPlan{d.input}
}
