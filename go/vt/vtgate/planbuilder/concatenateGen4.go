package planbuilder

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type concatenateGen4 struct {
	sources []logicalPlan
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
func (c *concatenateGen4) WireupGen4(semTable *semantics.SemTable) error {
	for _, source := range c.sources {
		err := source.WireupGen4(semTable)
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
	return &engine.Concatenate{
		Sources: sources,
	}
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
