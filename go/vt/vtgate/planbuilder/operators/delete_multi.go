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

import "vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

type DeleteMulti struct {
	Source Operator
	Delete Operator

	noColumns
	noPredicates
}

func (d *DeleteMulti) Clone(inputs []Operator) Operator {
	newD := *d
	newD.SetInputs(inputs)
	return &newD
}

func (d *DeleteMulti) Inputs() []Operator {
	return []Operator{d.Source, d.Delete}
}

func (d *DeleteMulti) SetInputs(inputs []Operator) {
	if len(inputs) != 2 {
		panic("unexpected number of inputs for DeleteMulti operator")
	}
	d.Source = inputs[0]
	d.Delete = inputs[1]
}

func (d *DeleteMulti) ShortDescription() string {
	return ""
}

func (d *DeleteMulti) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return nil
}

var _ Operator = (*DeleteMulti)(nil)
