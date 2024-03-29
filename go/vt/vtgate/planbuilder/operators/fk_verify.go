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

// VerifyOp keeps the information about the foreign key verification operation.
// It is a Parent verification or a Child verification.
type VerifyOp struct {
	Op  Operator
	Typ string
}

// FkVerify is used to represent a foreign key verification operation
// as an operator. This operator is created for DML queries that require
// verifications on the existence of the rows in the parent table (for example, INSERT and UPDATE).
type FkVerify struct {
	Verify []*VerifyOp
	Input  Operator

	noColumns
	noPredicates
}

var _ Operator = (*FkVerify)(nil)

// Inputs implements the Operator interface
func (fkv *FkVerify) Inputs() []Operator {
	inputs := []Operator{fkv.Input}
	for _, v := range fkv.Verify {
		inputs = append(inputs, v.Op)
	}
	return inputs
}

// SetInputs implements the Operator interface
func (fkv *FkVerify) SetInputs(operators []Operator) {
	fkv.Input = operators[0]
	if len(fkv.Verify) != len(operators)-1 {
		panic("mismatched number of verify inputs")
	}
	for i := 1; i < len(operators); i++ {
		fkv.Verify[i-1].Op = operators[i]
	}
}

// Clone implements the Operator interface
func (fkv *FkVerify) Clone(inputs []Operator) Operator {
	newFkv := &FkVerify{
		Verify: fkv.Verify,
	}
	newFkv.SetInputs(inputs)
	return newFkv
}

// GetOrdering implements the Operator interface
func (fkv *FkVerify) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	return nil
}

// ShortDescription implements the Operator interface
func (fkv *FkVerify) ShortDescription() string {
	return ""
}
