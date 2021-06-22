/*
Copyright 2020 The Vitess Authors.

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
	"errors"
	"fmt"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// planFilter solves this particular expression, either by pushing it down to a child or changing this logicalPlan
func planFilter(pb *primitiveBuilder, input logicalPlan, filter sqlparser.Expr, whereType string, origin logicalPlan) (logicalPlan, error) {
	switch node := input.(type) {
	case *join:
		isLeft := true
		var in logicalPlan
		if node.isOnLeft(origin.Order()) {
			in = node.Left
		} else {
			if node.ejoin.Opcode == engine.LeftJoin {
				return nil, errors.New("unsupported: cross-shard left join and where clause")
			}
			isLeft = false
			in = node.Right
		}

		filtered, err := planFilter(pb, in, filter, whereType, origin)
		if err != nil {
			return nil, err
		}
		if isLeft {
			node.Left = filtered
		} else {
			node.Right = filtered
		}
		return node, nil

	case *route:
		sel := node.Select.(*sqlparser.Select)
		switch whereType {
		case sqlparser.WhereStr:
			sel.AddWhere(filter)
		case sqlparser.HavingStr:
			sel.AddHaving(filter)
		}
		node.UpdatePlan(pb, filter)
		return node, nil
	case *pulloutSubquery:
		plan, err := planFilter(pb, node.underlying, filter, whereType, origin)
		if err != nil {
			return nil, err
		}
		node.underlying = plan
		return node, nil
	case *vindexFunc:
		return filterVindexFunc(node, filter)
	case *subquery:
		return nil, errors.New("unsupported: filtering on results of cross-shard subquery")
	case *orderedAggregate:
		return nil, errors.New("unsupported: filtering on results of aggregates")
	}

	return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "[BUG] unreachable %T.filtering", input)
}

func filterVindexFunc(node *vindexFunc, filter sqlparser.Expr) (logicalPlan, error) {
	if node.eVindexFunc.Opcode != engine.VindexNone {
		return nil, errors.New("unsupported: where clause for vindex function must be of the form id = <val> (multiple filters)")
	}

	// Check LHS.
	comparison, ok := filter.(*sqlparser.ComparisonExpr)
	if !ok {
		return nil, errors.New("unsupported: where clause for vindex function must be of the form id = <val> (not a comparison)")
	}
	if comparison.Operator != sqlparser.EqualOp {
		return nil, errors.New("unsupported: where clause for vindex function must be of the form id = <val> (not equality)")
	}
	colname, ok := comparison.Left.(*sqlparser.ColName)
	if !ok {
		return nil, errors.New("unsupported: where clause for vindex function must be of the form id = <val> (lhs is not a column)")
	}
	if !colname.Name.EqualString("id") {
		return nil, errors.New("unsupported: where clause for vindex function must be of the form id = <val> (lhs is not id)")
	}

	// Check RHS.
	// We have to check before calling NewPlanValue because NewPlanValue allows lists also.
	if !sqlparser.IsValue(comparison.Right) {
		return nil, errors.New("unsupported: where clause for vindex function must be of the form id = <val> (rhs is not a value)")
	}
	var err error
	node.eVindexFunc.Value, err = sqlparser.NewPlanValue(comparison.Right)
	if err != nil {
		return nil, fmt.Errorf("unsupported: where clause for vindex function must be of the form id = <val>: %v", err)
	}
	node.eVindexFunc.Opcode = engine.VindexMap
	return node, nil
}
