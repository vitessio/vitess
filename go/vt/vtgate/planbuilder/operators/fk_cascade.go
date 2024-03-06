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
	"slices"

	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// FkChild is used to represent a foreign key child table operation
type FkChild struct {
	BVName         string
	Cols           []int // indexes
	NonLiteralInfo []engine.NonLiteralUpdateInfo
	Op             Operator

	noColumns
	noPredicates
}

// FkCascade is used to represent a foreign key cascade operation
// as an operator. This operator is created for DML queries that require
// cascades (for example, ON DELETE CASCADE).
type FkCascade struct {
	Selection Operator
	Children  []*FkChild
	Parent    Operator

	noColumns
	noPredicates
}

var _ Operator = (*FkCascade)(nil)

// Inputs implements the Operator interface
func (fkc *FkCascade) Inputs() []Operator {
	var inputs []Operator
	inputs = append(inputs, fkc.Parent)
	inputs = append(inputs, fkc.Selection)
	for _, child := range fkc.Children {
		inputs = append(inputs, child.Op)
	}
	return inputs
}

// SetInputs implements the Operator interface
func (fkc *FkCascade) SetInputs(operators []Operator) {
	if len(operators) < 2 {
		panic("incorrect count of inputs for FkCascade")
	}
	fkc.Parent = operators[0]
	fkc.Selection = operators[1]
	for idx, operator := range operators {
		if idx < 2 {
			continue
		}
		fkc.Children[idx-2].Op = operator
	}
}

// Clone implements the Operator interface
func (fkc *FkCascade) Clone(inputs []Operator) Operator {
	if len(inputs) < 2 {
		panic("incorrect count of inputs for FkCascade")
	}
	newFkc := &FkCascade{
		Parent:    inputs[0],
		Selection: inputs[1],
	}
	for idx, operator := range inputs {
		if idx < 2 {
			continue
		}

		newFkc.Children = append(newFkc.Children, &FkChild{
			BVName:         fkc.Children[idx-2].BVName,
			Cols:           slices.Clone(fkc.Children[idx-2].Cols),
			NonLiteralInfo: slices.Clone(fkc.Children[idx-2].NonLiteralInfo),
			Op:             operator,
		})
	}
	return newFkc
}

// GetOrdering implements the Operator interface
func (fkc *FkCascade) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	return nil
}

// ShortDescription implements the Operator interface
func (fkc *FkCascade) ShortDescription() string {
	return ""
}
