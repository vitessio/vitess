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
)

type FkCascade struct {
	Selection ops.Operator
	Children  []ops.Operator
	Parent    ops.Operator

	noColumns
	noPredicates
}

var _ ops.Operator = (*FkCascade)(nil)

func (fkc *FkCascade) Inputs() []ops.Operator {
	var inputs []ops.Operator
	inputs = append(inputs, fkc.Parent)
	inputs = append(inputs, fkc.Selection)
	inputs = append(inputs, fkc.Children...)
	return inputs
}

func (fkc *FkCascade) SetInputs(operators []ops.Operator) {
	if len(operators) < 2 {
		panic("incorrect count of inputs for FkCascade")
	}
	fkc.Parent = operators[0]
	fkc.Selection = operators[1]
	fkc.Children = operators[2:]
}

// Clone implements the Operator interface
func (fkc *FkCascade) Clone(inputs []ops.Operator) ops.Operator {
	if len(inputs) < 2 {
		panic("incorrect count of inputs for FkCascade")
	}
	return &FkCascade{
		Parent:    inputs[0],
		Selection: inputs[1],
		Children:  inputs[2:],
	}
}

func (fkc *FkCascade) GetOrdering() ([]ops.OrderBy, error) {
	return nil, nil
}

func (fkc *FkCascade) ShortDescription() string {
	return "FkCascade"
}
