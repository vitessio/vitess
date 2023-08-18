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
	"fmt"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
)

type FkChild struct {
	BVName string
	Cols   []int // indexes
	Op     ops.Operator

	noColumns
	noPredicates
}

func (f *FkChild) Clone(inputs []ops.Operator) ops.Operator {
	return &FkChild{
		BVName: f.BVName,
		Cols:   f.Cols,
		Op:     inputs[0],
	}
}

func (f *FkChild) Inputs() []ops.Operator {
	return []ops.Operator{f.Op}
}

func (f *FkChild) SetInputs(operators []ops.Operator) {
	f.Op = operators[0]
}

func (f *FkChild) ShortDescription() string {
	return fmt.Sprintf("BvName: %s, Cols: %v", f.BVName, f.Cols)
}

func (f *FkChild) GetOrdering() ([]ops.OrderBy, error) {
	return nil, nil
}

var _ ops.Operator = (*FkChild)(nil)
