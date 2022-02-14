package planbuilder

import (
	"vitess.io/vitess/go/mysql/collations"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ logicalPlan = (*hashJoin)(nil)

// hashJoin is used to build a HashJoin primitive.
type hashJoin struct {
	gen4Plan

	// Left and Right are the nodes for the join.
	Left, Right logicalPlan

	Opcode engine.JoinOpcode

	Cols []int

	// The keys correspond to the column offset in the inputs where
	// the join columns can be found
	LHSKey, RHSKey int

	ComparisonType querypb.Type

	Collation collations.ID
}

// WireupGen4 implements the logicalPlan interface
func (hj *hashJoin) WireupGen4(semTable *semantics.SemTable) error {
	err := hj.Left.WireupGen4(semTable)
	if err != nil {
		return err
	}
	return hj.Right.WireupGen4(semTable)
}

// Primitive implements the logicalPlan interface
func (hj *hashJoin) Primitive() engine.Primitive {
	return &engine.HashJoin{
		Left:           hj.Left.Primitive(),
		Right:          hj.Right.Primitive(),
		Cols:           hj.Cols,
		Opcode:         hj.Opcode,
		LHSKey:         hj.LHSKey,
		RHSKey:         hj.RHSKey,
		ComparisonType: hj.ComparisonType,
		Collation:      hj.Collation,
	}
}

// Inputs implements the logicalPlan interface
func (hj *hashJoin) Inputs() []logicalPlan {
	return []logicalPlan{hj.Left, hj.Right}
}

// Rewrite implements the logicalPlan interface
func (hj *hashJoin) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 2 {
		return vterrors.New(vtrpcpb.Code_INTERNAL, "wrong number of children")
	}
	hj.Left = inputs[0]
	hj.Right = inputs[1]
	return nil
}

// ContainsTables implements the logicalPlan interface
func (hj *hashJoin) ContainsTables() semantics.TableSet {
	return hj.Left.ContainsTables().Merge(hj.Right.ContainsTables())
}

// OutputColumns implements the logicalPlan interface
func (hj *hashJoin) OutputColumns() []sqlparser.SelectExpr {
	return getOutputColumnsFromJoin(hj.Cols, hj.Left.OutputColumns(), hj.Right.OutputColumns())
}

func getOutputColumnsFromJoin(ints []int, lhs []sqlparser.SelectExpr, rhs []sqlparser.SelectExpr) (cols []sqlparser.SelectExpr) {
	for _, col := range ints {
		if col < 0 {
			col *= -1
			cols = append(cols, lhs[col-1])
		} else {
			cols = append(cols, rhs[col-1])
		}
	}
	return
}
