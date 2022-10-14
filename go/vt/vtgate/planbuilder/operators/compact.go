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
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// Compact will optimise the operator tree into a smaller but equivalent version
func Compact(op LogicalOperator, semTable *semantics.SemTable) (LogicalOperator, error) {
	switch op := op.(type) {
	case *Concatenate:
		compactConcatenate(op)
	case *Filter:
		if len(op.Predicates) == 0 {
			return op.Source, nil
		}
	case *Join:
		return compactJoin(op, semTable)
	}

	return op, nil
}

func compactConcatenate(op *Concatenate) {
	var newSources []LogicalOperator
	var newSels []*sqlparser.Select
	for i, source := range op.Sources {
		other, isConcat := source.(*Concatenate)
		if !isConcat {
			newSources = append(newSources, source)
			newSels = append(newSels, op.SelectStmts[i])
			continue
		}
		switch {
		case other.Limit == nil && len(other.OrderBy) == 0 && !other.Distinct:
			fallthrough
		case op.Distinct && other.Limit == nil:
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

func compactJoin(op *Join, semTable *semantics.SemTable) (LogicalOperator, error) {
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
	err := newOp.collectPredicate(op.Predicate, semTable)
	if err != nil {
		return nil, err
	}
	return newOp, nil
}
