/*
Copyright 2022 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// Compact will optimise the operator tree into a smaller but equivalent version
func Compact(ctx *plancontext.PlanningContext, op Operator) (Operator, error) {
	switch op := op.(type) {
	case *Union:
		compactConcatenate(op)
	case *Filter:
		if len(op.Predicates) == 0 {
			return op.Source, nil
		}
	case *Join:
		return compactJoin(ctx, op)
	}

	return op, nil
}

func compactConcatenate(op *Union) {
	var newSources []Operator
	var newSels []*sqlparser.Select
	for i, source := range op.Sources {
		other, isConcat := source.(*Union)
		if !isConcat {
			newSources = append(newSources, source)
			newSels = append(newSels, op.SelectStmts[i])
			continue
		}
		switch {
		case len(other.Ordering) == 0 && !other.Distinct:
			fallthrough
		case op.Distinct:
			// if the current UNION is a DISTINCT, we can safely ignore everything from children UNIONs, except LIMIT
			newSources = append(newSources, other.Sources...)
			newSels = append(newSels, other.SelectStmts...)

		default:
			newSources = append(newSources, other)
			newSels = append(newSels, nil)
		}
	}
	op.Sources = newSources
	op.SelectStmts = newSels
}

func compactJoin(ctx *plancontext.PlanningContext, op *Join) (Operator, error) {
	if op.LeftJoin {
		// we can't merge outer joins into a single QG
		return op, nil
	}

	lqg, lok := op.LHS.(*QueryGraph)
	rqg, rok := op.RHS.(*QueryGraph)
	if !lok || !rok {
		return op, nil
	}

	newOp := &QueryGraph{
		Tables:     append(lqg.Tables, rqg.Tables...),
		innerJoins: append(lqg.innerJoins, rqg.innerJoins...),
		NoDeps:     sqlparser.AndExpressions(lqg.NoDeps, rqg.NoDeps),
	}
	err := newOp.collectPredicate(ctx, op.Predicate)
	if err != nil {
		return nil, err
	}
	return newOp, nil
}
