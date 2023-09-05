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

// FkVerify is used to represent a foreign key verification operation
// as an operator. This operator is created for DML queries that require
// verifications on the existence of the rows in the parent table (for example, INSERT and UPDATE).
type FkVerify struct {
	Verify []ops.Operator
	Input  ops.Operator

	noColumns
	noPredicates
}

var _ ops.Operator = (*FkVerify)(nil)

// Inputs implements the Operator interface
func (fkv *FkVerify) Inputs() []ops.Operator {
	inputs := []ops.Operator{fkv.Input}
	inputs = append(inputs, fkv.Verify...)
	return inputs
}

// SetInputs implements the Operator interface
func (fkv *FkVerify) SetInputs(operators []ops.Operator) {
	fkv.Input = operators[0]
	fkv.Verify = nil
	if len(operators) > 1 {
		fkv.Verify = operators[1:]
	}
}

// Clone implements the Operator interface
func (fkv *FkVerify) Clone(inputs []ops.Operator) ops.Operator {
	newFkv := &FkVerify{}
	newFkv.SetInputs(inputs)
	return newFkv
}

// GetOrdering implements the Operator interface
func (fkv *FkVerify) GetOrdering() ([]ops.OrderBy, error) {
	return nil, nil
}

// ShortDescription implements the Operator interface
func (fkv *FkVerify) ShortDescription() string {
	return "FkVerify"
}
