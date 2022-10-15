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

func inputsL(op Operator) []Operator {
	switch op := op.(type) {
	case *ApplyJoin:
		return []Operator{op.LHS, op.RHS}
	case *CorrelatedSubQueryOp:
		return []Operator{op.Outer, op.Inner}
	case *PhysDerived:
		return []Operator{op.Source}
	case *PhysFilter:
		return []Operator{op.Source}
	case *Route:
		return []Operator{op.Source}
	case *SubQueryOp:
		return []Operator{op.Outer, op.Inner}
	case *Union:
		var sources []Operator
		for _, source := range op.Sources {
			sources = append(sources, source)
		}
		return sources
	case *Table, *PhysVindex, *PhysDelete, *PhysUpdate:
		return nil
	case *Concatenate:
		return op.Sources
	case *Derived:
		return []Operator{op.Inner}
	case *Filter:
		return []Operator{op.Source}
	case *Join:
		return []Operator{op.LHS, op.RHS}
	case *SubQuery:
		inputs := []Operator{op.Outer}
		for _, inner := range op.Inner {
			inputs = append(inputs, inner)
		}
		return inputs
	case *SubQueryInner:
		return []Operator{op.Inner}
	case *Delete, *QueryGraph, *Update, *Vindex:
		return nil
	}

	panic("switch should be exhaustive")
}

func visitTopDownL(root Operator, visitor func(Operator) error) error {
	queue := []Operator{root}
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

func TableID(op Operator) (result semantics.TableSet) {
	_ = visitTopDownL(op, func(this Operator) error {
		if tbl, ok := this.(tableIDIntroducer); ok {
			result.MergeInPlace(tbl.Introduces())
		}
		return nil
	})
	return
}

func unresolvedPredicates(op Operator, st *semantics.SemTable) (result []sqlparser.Expr) {
	_ = visitTopDownL(op, func(this Operator) error {
		if tbl, ok := this.(unresolved); ok {
			result = append(result, tbl.UnsolvedPredicates(st)...)
		}

		return nil
	})
	return
}

func CheckValid(op Operator) error {
	type checked interface {
		CheckValid() error
	}
	return visitTopDownL(op, func(this Operator) error {
		if chk, ok := this.(checked); ok {
			return chk.CheckValid()
		}
		return nil
	})
}

func CostOf(op Operator) (cost int) {
	_ = visitTopDownL(op, func(op Operator) error {
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
