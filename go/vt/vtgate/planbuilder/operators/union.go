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
)

type Union struct {
	Sources     []Operator
	SelectStmts []*sqlparser.Select
	Distinct    bool

	// TODO this should be removed. For now it's used to fail queries
	Ordering sqlparser.OrderBy
}

var _ PhysicalOperator = (*Union)(nil)

// IPhysical implements the PhysicalOperator interface
func (u *Union) IPhysical() {}

// Clone implements the Operator interface
func (u *Union) Clone(inputs []Operator) Operator {
	newOp := *u
	checkSize(inputs, len(u.Sources))
	newOp.Sources = inputs
	return &newOp
}

// Inputs implements the Operator interface
func (u *Union) Inputs() []Operator {
	return u.Sources
}
