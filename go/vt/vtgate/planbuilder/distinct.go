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
	checkCols      []engine.CheckCol
	needToTruncate bool
}

func newDistinct(source logicalPlan, checkCols []engine.CheckCol, needToTruncate bool) logicalPlan {
	return &distinct{
		logicalPlanCommon: newBuilderCommon(source),
		checkCols:         checkCols,
		needToTruncate:    needToTruncate,
	}
}

func newDistinctV3(source logicalPlan) logicalPlan {
	return &distinct{logicalPlanCommon: newBuilderCommon(source)}
}

func (d *distinct) Primitive() engine.Primitive {
	if d.checkCols == nil {
		// If we are missing the checkCols information, we are on the V3 planner and should produce a V3 Distinct
		return &engine.DistinctV3{Source: d.input.Primitive()}
	}
	truncate := false
	if d.needToTruncate {
		for _, col := range d.checkCols {
			if col.WsCol != nil {
				truncate = true
				break
			}
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
