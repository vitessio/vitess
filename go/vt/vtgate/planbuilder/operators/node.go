package operators

import (
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

// tableIDIntroducer is used to signal that this operator introduces data from a new source
type tableIDIntroducer interface {
	Introduces() semantics.TableSet
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

type unresolved interface {
	// UnsolvedPredicates returns any predicates that have dependencies on the given Operator and
	// on the outside of it (a parent Select expression, any other table not used by Operator, etc).
	UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr
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
