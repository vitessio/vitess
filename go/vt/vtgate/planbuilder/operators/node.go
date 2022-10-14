package operators

import "vitess.io/vitess/go/vt/vtgate/semantics"

func inputs(op LogicalOperator) []LogicalOperator {
	switch op := op.(type) {
	case *Concatenate:
		return op.Sources
	case *Derived:
		return []LogicalOperator{op.Inner}
	case *Filter:
		return []LogicalOperator{op.Source}
	case *Join:
		return []LogicalOperator{op.LHS, op.RHS}
	case *SubQuery:
		inputs := []LogicalOperator{op.Outer}
		for _, inner := range op.Inner {
			inputs = append(inputs, inner)
		}
		return inputs
	case *SubQueryInner:
		return []LogicalOperator{op.Inner}
	case *Delete, *QueryGraph, *Update, *Vindex:
		return nil
	}

	panic("switch should be exhaustive")
}

// tableIDIntroducer is used to signal that this operator introduces data from a new source
type tableIDIntroducer interface {
	Introduces() semantics.TableSet
}

func tableID(op LogicalOperator) (result semantics.TableSet) {
	queue := []LogicalOperator{op}
	for len(queue) > 0 {
		this := queue[0]
		queue = append(queue[1:], inputs(this)...)
		if tbl, ok := this.(tableIDIntroducer); ok {
			result.MergeInPlace(tbl.Introduces())
		}
	}
	return
}
