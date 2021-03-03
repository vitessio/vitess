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

// limit is the logicalPlan for engine.Limit.
// This gets built if a limit needs to be applied
// after rows are returned from an underlying
// operation. Since a limit is the final operation
// of a SELECT, most pushes are not applicable.
type distinct struct {
	logicalPlanCommon
}

func newDistinct(source logicalPlan) logicalPlan {
	return &distinct{
		logicalPlanCommon: newBuilderCommon(source),
	}
}

func (d *distinct) Primitive() engine.Primitive {
	return &engine.Distinct{
		Source: d.input.Primitive(),
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
