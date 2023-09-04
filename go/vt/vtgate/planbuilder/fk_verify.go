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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ logicalPlan = (*fkVerify)(nil)

// fkVerify is the logicalPlan for engine.FkVerify.
type fkVerify struct {
	input  logicalPlan
	verify []logicalPlan
}

// newFkVerify builds a new fkVerify.
func newFkVerify(input logicalPlan, verify []logicalPlan) *fkVerify {
	return &fkVerify{
		input:  input,
		verify: verify,
	}
}

// Primitive implements the logicalPlan interface
func (fkc *fkVerify) Primitive() engine.Primitive {
	var verify []engine.Primitive
	for _, v := range fkc.verify {
		verify = append(verify, v.Primitive())
	}
	return &engine.FkVerify{
		Exec:   fkc.input.Primitive(),
		Verify: verify,
	}
}

// Wireup implements the logicalPlan interface
func (fkc *fkVerify) Wireup(ctx *plancontext.PlanningContext) error {
	for _, v := range fkc.verify {
		err := v.Wireup(ctx)
		if err != nil {
			return err
		}
	}
	return fkc.input.Wireup(ctx)
}

// Rewrite implements the logicalPlan interface
func (fkc *fkVerify) Rewrite(inputs ...logicalPlan) error {
	fkc.input = inputs[0]
	fkc.verify = nil
	if len(inputs) > 1 {
		fkc.verify = inputs[1:]
	}
	return nil
}

// ContainsTables implements the logicalPlan interface
func (fkc *fkVerify) ContainsTables() semantics.TableSet {
	return fkc.input.ContainsTables()
}

// Inputs implements the logicalPlan interface
func (fkc *fkVerify) Inputs() []logicalPlan {
	inputs := []logicalPlan{fkc.input}
	inputs = append(inputs, fkc.verify...)
	return inputs
}

// OutputColumns implements the logicalPlan interface
func (fkc *fkVerify) OutputColumns() []sqlparser.SelectExpr {
	return nil
}
