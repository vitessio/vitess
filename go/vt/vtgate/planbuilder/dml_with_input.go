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

package planbuilder

import (
	"vitess.io/vitess/go/vt/vtgate/engine"
)

type dmlWithInput struct {
	input logicalPlan
	dmls  []logicalPlan

	outputCols [][]int
}

var _ logicalPlan = (*dmlWithInput)(nil)

// Primitive implements the logicalPlan interface
func (d *dmlWithInput) Primitive() engine.Primitive {
	inp := d.input.Primitive()
	var dels []engine.Primitive
	for _, dml := range d.dmls {
		dels = append(dels, dml.Primitive())
	}
	return &engine.DMLWithInput{
		DMLs:       dels,
		Input:      inp,
		OutputCols: d.outputCols,
	}
}
