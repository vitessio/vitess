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

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// planProjection pushes the select expression to the specified
// originator. If successful, the originator must create
// a resultColumn entry and return it. The top level caller
// must accumulate these result columns and set the symtab
// after analysis.
func planProjection(pb *primitiveBuilder, in logicalPlan, expr *sqlparser.AliasedExpr, origin logicalPlan) (logicalPlan, *resultColumn, int, error) {
	switch node := in.(type) {
	case *join:
		var rc *resultColumn
		if node.isOnLeft(origin.Order()) {
			newLeft, col, colNumber, err := planProjection(pb, node.Left, expr, origin)
			if err != nil {
				return nil, nil, 0, err
			}
			node.ejoin.Cols = append(node.ejoin.Cols, -colNumber-1)
			rc = col
			node.Left = newLeft
		} else {
			// Pushing of non-trivial expressions not allowed for RHS of left joins.
			if _, ok := expr.Expr.(*sqlparser.ColName); !ok && node.ejoin.Opcode == engine.LeftJoin {
				return nil, nil, 0, errors.New("unsupported: cross-shard left join and column expressions")
			}

			newRight, col, colNumber, err := planProjection(pb, node.Right, expr, origin)
			if err != nil {
				return nil, nil, 0, err
			}
			node.ejoin.Cols = append(node.ejoin.Cols, colNumber+1)
			rc = col
			node.Right = newRight
		}
		node.resultColumns = append(node.resultColumns, rc)
		return in, rc, len(node.resultColumns) - 1, nil

		// orderedAggregate can accept expressions that are normal (a+b), or aggregate (MAX(v)).
		// Normal expressions are pushed through to the underlying route. But aggregate
		// expressions require post-processing. In such cases, oa shares the work with
		// the underlying route: It asks the scatter route to perform the MAX operation
		// also, and only performs the final aggregation with what the route returns.
		// Since the results are expected to be ordered, this is something that can
		// be performed 'as they come'. In this respect, oa is the originator for
		// aggregate expressions like MAX, which will be added to symtab. The underlying
		// MAX sent to the route will not be added to symtab and will not be reachable by
		// others. This functionality depends on the PushOrderBy to request that
		// the rows be correctly ordered.
	case *orderedAggregate:
		if inner, ok := expr.Expr.(*sqlparser.FuncExpr); ok {
			if _, ok := engine.SupportedAggregates[inner.Name.Lowered()]; ok {
				rc, colNumber, err := node.pushAggr(pb, expr, origin)
				if err != nil {
					return nil, nil, 0, err
				}
				return node, rc, colNumber, nil
			}
		}

		// Ensure that there are no aggregates in the expression.
		if nodeHasAggregates(expr.Expr) {
			return nil, nil, 0, errors.New("unsupported: in scatter query: complex aggregate expression")
		}

		newInput, innerRC, _, err := planProjection(pb, node.input, expr, origin)
		if err != nil {
			return nil, nil, 0, err
		}
		node.input = newInput
		node.resultColumns = append(node.resultColumns, innerRC)
		return node, innerRC, len(node.resultColumns) - 1, nil
	case *route:
		sel := node.Select.(*sqlparser.Select)
		sel.SelectExprs = append(sel.SelectExprs, expr)

		rc := newResultColumn(expr, node)
		node.resultColumns = append(node.resultColumns, rc)

		return node, rc, len(node.resultColumns) - 1, nil
	case *mergeSort:
		projectedInput, rc, idx, err := planProjection(pb, node.input, expr, origin)
		if err != nil {
			return nil, nil, 0, err
		}
		err = node.Rewrite(projectedInput)
		if err != nil {
			return nil, nil, 0, err
		}
		return node, rc, idx, nil
	case *distinct:
		projectedInput, rc, idx, err := planProjection(pb, node.input, expr, origin)
		if err != nil {
			return nil, nil, 0, err
		}
		err = node.Rewrite(projectedInput)
		if err != nil {
			return nil, nil, 0, err
		}
		return node, rc, idx, nil
	case *pulloutSubquery:
		projectedInput, rc, idx, err := planProjection(pb, node.underlying, expr, origin)
		if err != nil {
			return nil, nil, 0, err
		}
		err = node.Rewrite(projectedInput, node.subquery)
		if err != nil {
			return nil, nil, 0, err
		}
		return node, rc, idx, nil
	case *subquery:
		col, ok := expr.Expr.(*sqlparser.ColName)
		if !ok {
			return nil, nil, 0, errors.New("unsupported: expression on results of a cross-shard subquery")
		}

		// colNumber should already be set for subquery columns.
		inner := col.Metadata.(*column).colNumber
		node.esubquery.Cols = append(node.esubquery.Cols, inner)

		// Build a new column reference to represent the result column.
		rc := newResultColumn(expr, node)
		node.resultColumns = append(node.resultColumns, rc)

		return node, rc, len(node.resultColumns) - 1, nil
	case *vindexFunc:
		// Catch the case where no where clause was specified. If so, the opcode
		// won't be set.
		if node.eVindexFunc.Opcode == engine.VindexNone {
			return nil, nil, 0, errors.New("unsupported: where clause for vindex function must be of the form id = <val> (where clause missing)")
		}
		col, ok := expr.Expr.(*sqlparser.ColName)
		if !ok {
			return nil, nil, 0, errors.New("unsupported: expression on results of a vindex function")
		}
		rc := newResultColumn(expr, node)
		node.resultColumns = append(node.resultColumns, rc)
		node.eVindexFunc.Fields = append(node.eVindexFunc.Fields, &querypb.Field{
			Name: rc.alias.String(),
			Type: querypb.Type_VARBINARY,
		})
		node.eVindexFunc.Cols = append(node.eVindexFunc.Cols, col.Metadata.(*column).colNumber)
		return node, rc, len(node.resultColumns) - 1, nil

	}
	return nil, nil, 0, vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "[BUG] unreachable %T.projection", in)
}
