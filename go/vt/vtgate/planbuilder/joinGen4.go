package planbuilder

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ logicalPlan = (*joinGen4)(nil)

// joinGen4 is used to build a Join primitive.
// It's used to build an inner join and only used by the Gen4 planner
type joinGen4 struct {
	// Left and Right are the nodes for the join.
	Left, Right logicalPlan
	Opcode      engine.JoinOpcode
	Cols        []int
	Vars        map[string]int

	gen4Plan
}

// WireupGen4 implements the logicalPlan interface
func (j *joinGen4) WireupGen4(semTable *semantics.SemTable) error {
	err := j.Left.WireupGen4(semTable)
	if err != nil {
		return err
	}
	return j.Right.WireupGen4(semTable)
}

// Primitive implements the logicalPlan interface
func (j *joinGen4) Primitive() engine.Primitive {
	return &engine.Join{
		Left:   j.Left.Primitive(),
		Right:  j.Right.Primitive(),
		Cols:   j.Cols,
		Vars:   j.Vars,
		Opcode: j.Opcode,
	}
}

// Inputs implements the logicalPlan interface
func (j *joinGen4) Inputs() []logicalPlan {
	return []logicalPlan{j.Left, j.Right}
}

// Rewrite implements the logicalPlan interface
func (j *joinGen4) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 2 {
		return vterrors.New(vtrpcpb.Code_INTERNAL, "wrong number of children")
	}
	j.Left = inputs[0]
	j.Right = inputs[1]
	return nil
}

// ContainsTables implements the logicalPlan interface
func (j *joinGen4) ContainsTables() semantics.TableSet {
	return j.Left.ContainsTables().Merge(j.Right.ContainsTables())
}

// OutputColumns implements the logicalPlan interface
func (j *joinGen4) OutputColumns() []sqlparser.SelectExpr {
	return getOutputColumnsFromJoin(j.Cols, j.Left.OutputColumns(), j.Right.OutputColumns())
}
