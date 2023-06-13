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
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// planOffsets will walk the tree top down, adding offset information to columns in the tree for use in further optimization,
func planOffsets(ctx *plancontext.PlanningContext, root ops.Operator) (ops.Operator, error) {
	type offsettable interface {
		planOffsets(ctx *plancontext.PlanningContext) error
	}

	visitor := func(in ops.Operator, _ semantics.TableSet, _ bool) (ops.Operator, *rewrite.ApplyResult, error) {
		var err error
		switch op := in.(type) {
		case *Derived, *Horizon:
			return nil, nil, vterrors.VT13001(fmt.Sprintf("should not see %T here", in))
		case offsettable:
			err = op.planOffsets(ctx)
		}
		if err != nil {
			return nil, nil, err
		}
		return in, rewrite.SameTree, nil
	}

	op, err := rewrite.TopDown(root, TableID, visitor, stopAtRoute)
	if err != nil {
		if vterr, ok := err.(*vterrors.VitessError); ok && vterr.ID == "VT13001" {
			// we encountered a bug. let's try to back out
			return nil, errHorizonNotPlanned()
		}
		return nil, err
	}

	return op, nil
}

func fetchByOffset(e sqlparser.SQLNode) bool {
	switch e.(type) {
	case *sqlparser.ColName, sqlparser.AggrFunc:
		return true
	default:
		return false
	}
}

func planOffsetsOnJoins(ctx *plancontext.PlanningContext, op ops.Operator) error {
	err := rewrite.Visit(op, func(current ops.Operator) error {
		join, ok := current.(*ApplyJoin)
		if !ok {
			return nil
		}
		return join.planOffsets(ctx)
	})
	return err
}
