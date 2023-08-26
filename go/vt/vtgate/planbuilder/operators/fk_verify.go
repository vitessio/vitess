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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
)

type FkParent struct {
	Values []sqlparser.Exprs
	Cols   []engine.CheckCol
	BvName string

	Op ops.Operator
}

// FkVerify is used to represent a foreign key verification operation
// as an operator. This operator is created for DML queries that require
// verifications on the existence of the rows in the parent table (for example, INSERT and UPDATE).
type FkVerify struct {
	Verify []*FkParent
	Input  ops.Operator

	noColumns
	noPredicates
}

var _ ops.Operator = (*FkVerify)(nil)

// Inputs implements the Operator interface
func (fkv *FkVerify) Inputs() []ops.Operator {
	var inputs []ops.Operator
	inputs = append(inputs, fkv.Input)
	for _, child := range fkv.Verify {
		inputs = append(inputs, child.Op)
	}
	return inputs
}

// SetInputs implements the Operator interface
func (fkv *FkVerify) SetInputs(operators []ops.Operator) {
	if len(operators) < 1 {
		panic("incorrect count of inputs for FkVerify")
	}
	fkv.Input = operators[0]
	for idx, operator := range operators {
		if idx < 1 {
			continue
		}
		fkv.Verify[idx-1].Op = operator
	}
}

// Clone implements the Operator interface
func (fkv *FkVerify) Clone(inputs []ops.Operator) ops.Operator {
	if len(inputs) < 1 {
		panic("incorrect count of inputs for FkVerify")
	}
	newFkv := &FkVerify{
		Input: inputs[0],
	}
	for idx, operator := range inputs {
		if idx < 1 {
			continue
		}
		newFkv.Verify = append(newFkv.Verify, &FkParent{
			Values: fkv.Verify[idx-1].Values,
			BvName: fkv.Verify[idx-1].BvName,
			Cols:   fkv.Verify[idx-1].Cols,
			Op:     operator,
		})
	}
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
