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

// compact will optimise the operator tree into a smaller but equivalent version
func compact(ctx *plancontext.PlanningContext, op Operator) (Operator, error) {
	newOp, _, err := rewriteBottomUp(ctx, op, func(ctx *plancontext.PlanningContext, op Operator) (Operator, bool, error) {
		newOp, ok := op.(compactable)
		if !ok {
			return op, false, nil
		}
		return newOp.compact(ctx)
	})
	return newOp, err
}

func (f *Filter) compact(*plancontext.PlanningContext) (Operator, bool, error) {
	if len(f.Predicates) == 0 {
		return f.Source, true, nil
	}

	other, isFilter := f.Source.(*Filter)
	if !isFilter {
		return f, false, nil
	}
	f.Source = other.Source
	f.Predicates = append(f.Predicates, other.Predicates...)
	return f, true, nil
}

func (u *Union) compact(*plancontext.PlanningContext) (Operator, bool, error) {
	var newSources []Operator
	var newSels []*sqlparser.Select
	anythingChanged := false
	for i, source := range u.Sources {
		other, isUnion := source.(*Union)
		if !isUnion {
			newSources = append(newSources, source)
			newSels = append(newSels, u.SelectStmts[i])
			continue
		}
		anythingChanged = true
		switch {
		case len(other.Ordering) == 0 && !other.Distinct:
			fallthrough
		case u.Distinct:
			// if the current UNION is a DISTINCT, we can safely ignore everything from children UNIONs, except LIMIT
			newSources = append(newSources, other.Sources...)
			newSels = append(newSels, other.SelectStmts...)

		default:
			newSources = append(newSources, other)
			newSels = append(newSels, nil)
		}
	}
	if anythingChanged {
		u.Sources = newSources
		u.SelectStmts = newSels
	}
	return u, anythingChanged, nil
}

func (j *Join) compact(ctx *plancontext.PlanningContext) (Operator, bool, error) {
	if j.LeftJoin {
		// we can't merge outer joins into a single QG
		return j, false, nil
	}

	lqg, lok := j.LHS.(*QueryGraph)
	rqg, rok := j.RHS.(*QueryGraph)
	if !lok || !rok {
		return j, false, nil
	}

	newOp := &QueryGraph{
		Tables:     append(lqg.Tables, rqg.Tables...),
		innerJoins: append(lqg.innerJoins, rqg.innerJoins...),
		NoDeps:     sqlparser.AndExpressions(lqg.NoDeps, rqg.NoDeps),
	}
	if j.Predicate != nil {
		err := newOp.collectPredicate(ctx, j.Predicate)
		if err != nil {
			return nil, false, err
		}
	}
	return newOp, true, nil
}
