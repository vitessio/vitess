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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type Sequential struct {
	Sources []ops.Operator

	noPredicates
	noColumns
}

func newSequential(srcs []ops.Operator) *Sequential {
	return &Sequential{
		Sources: srcs,
	}
}

// Clone implements the Operator interface
func (s *Sequential) Clone(inputs []ops.Operator) ops.Operator {
	newOp := *s
	newOp.Sources = inputs
	return &newOp
}

func (s *Sequential) GetOrdering(*plancontext.PlanningContext) []ops.OrderBy {
	return nil
}

// Inputs implements the Operator interface
func (s *Sequential) Inputs() []ops.Operator {
	return s.Sources
}

// SetInputs implements the Operator interface
func (s *Sequential) SetInputs(ops []ops.Operator) {
	s.Sources = ops
}

func (s *Sequential) ShortDescription() string {
	return ""
}
