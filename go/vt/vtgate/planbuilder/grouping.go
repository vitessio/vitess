package planbuilder

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func planGroupBy(pb *primitiveBuilder, input logicalPlan, groupBy sqlparser.GroupBy) (logicalPlan, error) {
	if len(groupBy) == 0 {
		// if we have no grouping declared, we only want to visit orderedAggregate
		_, isOrdered := input.(*orderedAggregate)
		if !isOrdered {
			return input, nil
		}
	}

	switch node := input.(type) {
	case *mergeSort, *pulloutSubquery, *distinct:
		inputs := node.Inputs()
		input := inputs[0]

		newInput, err := planGroupBy(pb, input, groupBy)
		if err != nil {
			return nil, err
		}
		inputs[0] = newInput
		err = node.Rewrite(inputs...)
		if err != nil {
			return nil, err
		}
		return node, nil
	case *route:
		node.Select.(*sqlparser.Select).GroupBy = groupBy
		return node, nil
	case *orderedAggregate:
		for _, expr := range groupBy {
			colNumber := -1
			switch e := expr.(type) {
			case *sqlparser.ColName:
				c := e.Metadata.(*column)
				if c.Origin() == node {
					return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.WrongGroupField, "Can't group on '%s'", sqlparser.String(e))
				}
				for i, rc := range node.resultColumns {
					if rc.column == c {
						colNumber = i
						break
					}
				}
				if colNumber == -1 {
					return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: in scatter query: group by column must reference column in SELECT list")
				}
			case *sqlparser.Literal:
				num, err := ResultFromNumber(node.resultColumns, e, "group statement")
				if err != nil {
					return nil, err
				}
				colNumber = num
			default:
				return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: in scatter query: only simple references allowed")
			}
			node.eaggr.GroupByKeys = append(node.eaggr.GroupByKeys, &engine.GroupByParams{KeyCol: colNumber, WeightStringCol: -1, FromGroupBy: true})
		}
		// Append the distinct aggregate if any.
		if node.extraDistinct != nil {
			groupBy = append(groupBy, node.extraDistinct)
		}

		newInput, err := planGroupBy(pb, node.input, groupBy)
		if err != nil {
			return nil, err
		}
		node.input = newInput

		return node, nil
	}
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unreachable %T.groupBy: ", input)
}

// planDistinct makes the output distinct
func planDistinct(input logicalPlan) (logicalPlan, error) {
	switch node := input.(type) {
	case *route:
		node.Select.MakeDistinct()
		return node, nil
	case *orderedAggregate:
		for i, rc := range node.resultColumns {
			// If the column origin is oa (and not the underlying route),
			// it means that it's an aggregate function supplied by oa.
			// So, the distinct 'operator' cannot be pushed down into the
			// route.
			if rc.column.Origin() == node {
				return newDistinct(node, nil), nil
			}
			node.eaggr.GroupByKeys = append(node.eaggr.GroupByKeys, &engine.GroupByParams{KeyCol: i, WeightStringCol: -1, FromGroupBy: false})
		}
		newInput, err := planDistinct(node.input)
		if err != nil {
			return nil, err
		}
		node.input = newInput
		return node, nil

	case *distinct:
		return input, nil
	}

	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unreachable %T.distinct", input)
}
