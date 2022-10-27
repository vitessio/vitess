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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type concatenateGen4 struct {
	sources []logicalPlan

	// These column offsets do not need to be typed checked - they usually contain weight_string()
	// columns that are not going to be returned to the user
	noNeedToTypeCheck []int
}

var _ logicalPlan = (*concatenateGen4)(nil)

// Order implements the logicalPlan interface
func (c *concatenateGen4) Order() int {
	panic("implement me")
}

// ResultColumns implements the logicalPlan interface
func (c *concatenateGen4) ResultColumns() []*resultColumn {
	panic("implement me")
}

// Reorder implements the logicalPlan interface
func (c *concatenateGen4) Reorder(order int) {
	panic("implement me")
}

// Wireup implements the logicalPlan interface
func (c *concatenateGen4) Wireup(plan logicalPlan, jt *jointab) error {
	panic("implement me")
}

// WireupGen4 implements the logicalPlan interface
func (c *concatenateGen4) WireupGen4(ctx *plancontext.PlanningContext) error {
	for _, source := range c.sources {
		err := source.WireupGen4(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// SupplyVar implements the logicalPlan interface
func (c *concatenateGen4) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	panic("implement me")
}

// SupplyCol implements the logicalPlan interface
func (c *concatenateGen4) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	panic("implement me")
}

// SupplyWeightString implements the logicalPlan interface
func (c *concatenateGen4) SupplyWeightString(colNumber int, alsoAddToGroupBy bool) (weightcolNumber int, err error) {
	panic("implement me")
}

// Primitive implements the logicalPlan interface
func (c *concatenateGen4) Primitive() engine.Primitive {
	var sources []engine.Primitive
	for _, source := range c.sources {
		sources = append(sources, source.Primitive())
	}

	return engine.NewConcatenate(sources, c.noNeedToTypeCheck)
}

// Rewrite implements the logicalPlan interface
func (c *concatenateGen4) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != len(c.sources) {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "concatenateGen4: wrong number of inputs")
	}
	c.sources = inputs
	return nil
}

// ContainsTables implements the logicalPlan interface
func (c *concatenateGen4) ContainsTables() semantics.TableSet {
	var tableSet semantics.TableSet
	for _, source := range c.sources {
		tableSet.MergeInPlace(source.ContainsTables())
	}
	return tableSet
}

// Inputs implements the logicalPlan interface
func (c *concatenateGen4) Inputs() []logicalPlan {
	return c.sources
}

// OutputColumns implements the logicalPlan interface
func (c *concatenateGen4) OutputColumns() []sqlparser.SelectExpr {
	return c.sources[0].OutputColumns()
}
