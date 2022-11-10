package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// Operator forms the tree of operators, representing the declarative query provided.
	// While planning, the operator tree starts with logical operators, and later moves to physical operators.
	// The difference between the two is that when we get to a physical operator, we have made decisions on in
	// which order to do the joins, and how to split them up across shards and keyspaces.
	// In some situation we go straight to the physical operator - when there are no options to consider,
	// we can go straight to the end result.
	Operator interface {
		Clone(inputs []Operator) Operator
		Inputs() []Operator

		// AddPredicate is used to push predicates. It pushed it as far down as is possible in the tree.
		// If we encounter a join and the predicate depends on both sides of the join, the predicate will be split into two parts,
		// where data is fetched from the LHS of the join to be used in the evaluation on the RHS
		AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Operator, error)

		// AddColumn tells an operator to also output an additional column specified.
		// The offset to the column is returned.
		AddColumn(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (int, error)
	}

	// PhysicalOperator means that this operator is ready to be turned into a logical plan
	PhysicalOperator interface {
		Operator
		IPhysical()
	}
)
