package operators

import (
	"fmt"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func inputsP(op PhysicalOperator) []PhysicalOperator {
	switch op := op.(type) {
	case *ApplyJoin:
		return []PhysicalOperator{op.LHS, op.RHS}
	case *CorrelatedSubQueryOp:
		return []PhysicalOperator{op.Outer, op.Inner}
	case *PhysDerived:
		return []PhysicalOperator{op.Source}
	case *PhysFilter:
		return []PhysicalOperator{op.Source}
	case *Route:
		return []PhysicalOperator{op.Source}
	case *SubQueryOp:
		return []PhysicalOperator{op.Outer, op.Inner}
	case *Union:
		return op.Sources
	case *Table, *PhysVindex, *PhysDelete, *PhysUpdate:
		return nil
	}

	panic("switch should be exhaustive")
}

func inputsL(op LogicalOperator) []LogicalOperator {
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

func visitTopDownL(root LogicalOperator, visitor func(LogicalOperator) error) error {
	queue := []LogicalOperator{root}
	for len(queue) > 0 {
		this := queue[0]
		queue = append(queue[1:], inputsL(this)...)
		err := visitor(this)
		if err != nil {
			return err
		}
	}
	return nil
}

func visitTopDownP(root PhysicalOperator, visitor func(PhysicalOperator) error) error {
	queue := []PhysicalOperator{root}
	for len(queue) > 0 {
		this := queue[0]
		queue = append(queue[1:], inputsP(this)...)
		err := visitor(this)
		if err != nil {
			return err
		}
	}
	return nil
}

func tableID(op LogicalOperator) (result semantics.TableSet) {
	_ = visitTopDownL(op, func(this LogicalOperator) error {
		if tbl, ok := this.(tableIDIntroducer); ok {
			result.MergeInPlace(tbl.Introduces())
		}
		return nil
	})
	return
}

func TableID(op PhysicalOperator) (result semantics.TableSet) {
	_ = visitTopDownP(op, func(this PhysicalOperator) error {
		if tbl, ok := this.(tableIDIntroducer); ok {
			result.MergeInPlace(tbl.Introduces())
		}
		return nil
	})
	return
}

func unresolvedPredicates(op LogicalOperator, st *semantics.SemTable) (result []sqlparser.Expr) {
	_ = visitTopDownL(op, func(this LogicalOperator) error {
		if tbl, ok := this.(unresolved); ok {
			result = append(result, tbl.UnsolvedPredicates(st)...)
		}

		return nil
	})
	return
}

func CheckValid(op LogicalOperator) error {
	type checked interface {
		CheckValid() error
	}
	return visitTopDownL(op, func(this LogicalOperator) error {
		if chk, ok := this.(checked); ok {
			return chk.CheckValid()
		}
		return nil
	})
}

func CostOf(op PhysicalOperator) (cost int) {
	_ = visitTopDownP(op, func(op PhysicalOperator) error {
		if costlyOp, ok := op.(costly); ok {
			cost += costlyOp.Cost()
		}
		return nil
	})
	return
}

func Clone(op PhysicalOperator) PhysicalOperator {
	cl, ok := op.(clonable)
	if !ok {
		panic(fmt.Sprintf("tried to clone an operator that is not cloneable: %T", op))
	}
	inputs := inputsP(op)
	clones := make([]PhysicalOperator, len(inputs))
	for i, input := range inputs {
		clones[i] = Clone(input)
	}
	return cl.Clone(clones)
}

func checkSize(inputs []PhysicalOperator, shouldBe int) {
	if len(inputs) != shouldBe {
		panic(fmt.Sprintf("BUG: got the wrong number of inputs: got %d, expected %d", len(inputs), shouldBe))
	}
}
