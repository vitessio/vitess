/*
Copyright 2021 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type concatenate struct {
	sources []logicalPlan

	// These column offsets do not need to be typed checked - they usually contain weight_string()
	// columns that are not going to be returned to the user
	noNeedToTypeCheck []int
}

var _ logicalPlan = (*concatenate)(nil)

// WireupGen4 implements the logicalPlan interface
func (c *concatenate) Wireup(ctx *plancontext.PlanningContext) error {
	for _, source := range c.sources {
		err := source.Wireup(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// Primitive implements the logicalPlan interface
func (c *concatenate) Primitive() engine.Primitive {
	var sources []engine.Primitive
	for _, source := range c.sources {
		sources = append(sources, source.Primitive())
	}

	return engine.NewConcatenate(sources, c.noNeedToTypeCheck)
}

// Rewrite implements the logicalPlan interface
func (c *concatenate) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != len(c.sources) {
		return vterrors.VT13001("concatenate: wrong number of inputs")
	}
	c.sources = inputs
	return nil
}

// ContainsTables implements the logicalPlan interface
func (c *concatenate) ContainsTables() semantics.TableSet {
	var tableSet semantics.TableSet
	for _, source := range c.sources {
		tableSet = tableSet.Merge(source.ContainsTables())
	}
	return tableSet
}

// Inputs implements the logicalPlan interface
func (c *concatenate) Inputs() []logicalPlan {
	return c.sources
}

// OutputColumns implements the logicalPlan interface
func (c *concatenate) OutputColumns() []sqlparser.SelectExpr {
	return c.sources[0].OutputColumns()
}
