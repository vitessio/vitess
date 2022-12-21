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
	"fmt"

	"vitess.io/vitess/go/mysql/collations"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
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
func (hj *hashJoin) WireupGen4(ctx *plancontext.PlanningContext) error {
	err := hj.Left.WireupGen4(ctx)
	if err != nil {
		return err
	}
	return hj.Right.WireupGen4(ctx)
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
		return vterrors.VT13001(fmt.Sprintf("wrong number of children in hashJoin rewrite: %d; should be exactly 2", len(inputs)))
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
