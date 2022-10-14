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

// Compact implements the Operator interface
func (c *Concatenate) Compact(*semantics.SemTable) (LogicalOperator, error) {
	var newSources []LogicalOperator
	var newSels []*sqlparser.Select
	for i, source := range c.Sources {
		other, isConcat := source.(*Concatenate)
		if !isConcat {
			newSources = append(newSources, source)
			newSels = append(newSels, c.SelectStmts[i])
			continue
		}
		switch {
		case other.Limit == nil && len(other.OrderBy) == 0 && !other.Distinct:
			fallthrough
		case c.Distinct && other.Limit == nil:
			// if the current UNION is a DISTINCT, we can safely ignore everything from children UNIONs, except LIMIT
			newSources = append(newSources, other.Sources...)
			newSels = append(newSels, other.SelectStmts...)

		default:
			newSources = append(newSources, other)
			newSels = append(newSels, nil)
		}
	}
	c.Sources = newSources
	c.SelectStmts = newSels
	return c, nil
}

// Compact implements the LogicalOperator interface
func (d *Delete) Compact(semTable *semantics.SemTable) (LogicalOperator, error) {
	return d, nil
}

// Compact implements the Operator interface
func (d *Derived) Compact(*semantics.SemTable) (LogicalOperator, error) {
	return d, nil
}

// Compact implements the LogicalOperator interface
func (f *Filter) Compact(semTable *semantics.SemTable) (LogicalOperator, error) {
	if len(f.Predicates) == 0 {
		return f.Source, nil
	}

	return f, nil
}

// Compact implements the Operator interface
func (j *Join) Compact(semTable *semantics.SemTable) (LogicalOperator, error) {
	if j.LeftJoin {
		// we can't merge outer joins into a single QG
		return j, nil
	}

	lqg, lok := j.LHS.(*QueryGraph)
	rqg, rok := j.RHS.(*QueryGraph)
	if !lok || !rok {
		return j, nil
	}

	op := &QueryGraph{
		Tables:     append(lqg.Tables, rqg.Tables...),
		innerJoins: append(lqg.innerJoins, rqg.innerJoins...),
		NoDeps:     sqlparser.AndExpressions(lqg.NoDeps, rqg.NoDeps),
	}
	err := op.collectPredicate(j.Predicate, semTable)
	if err != nil {
		return nil, err
	}
	return op, nil
}

// Compact implements the Operator interface
func (qg *QueryGraph) Compact(*semantics.SemTable) (LogicalOperator, error) {
	return qg, nil
}

// Compact implements the Operator interface
func (s *SubQuery) Compact(*semantics.SemTable) (LogicalOperator, error) {
	return s, nil
}

// Compact implements the LogicalOperator interface
func (u *Update) Compact(semTable *semantics.SemTable) (LogicalOperator, error) {
	return u, nil
}

// Compact implements the Operator interface
func (v *Vindex) Compact(*semantics.SemTable) (LogicalOperator, error) {
	return v, nil
}
