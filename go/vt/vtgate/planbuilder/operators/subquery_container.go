/*
Copyright 2023 The Vitess Authors.

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

type (
	// SubQueryContainer stores the information about a query and it's subqueries.
	// The inner subqueries can be executed in any order, so we store them like this so we can see more opportunities
	// for merging
	SubQueryContainer struct {
		Outer Operator
		Inner []*SubQuery
	}
)

var _ Operator = (*SubQueryContainer)(nil)

// Clone implements the Operator interface
func (sqc *SubQueryContainer) Clone(inputs []Operator) Operator {
	result := &SubQueryContainer{
		Outer: inputs[0],
	}
	for idx := range sqc.Inner {
		inner, ok := inputs[idx+1].(*SubQuery)
		if !ok {
			panic("got bad input")
		}
		result.addInner(inner)
	}
	// SubQuery.Clone is a shallow struct copy, so each clone inherits a
	// pointer to the *original* subqueryGroup. That breaks the contract of
	// the group (Members reference the originals; merged/emitted/Original
	// are shared mutable state) once both trees are alive at the same time.
	// Rebuild groups from scratch on the cloned inners so each tree has its
	// own coordinated state and its own predicate AST.
	rewireSubqueryGroups(sqc.Inner, result.Inner)
	return result
}

// rewireSubqueryGroups gives the cloned SubQueries fresh subqueryGroup
// instances that reference the clones (rather than the originals) and own a
// freshly-cloned predicate AST. After this runs, the original tree and the
// cloned tree no longer share any group state, so leader election, merged
// tracking, and predicate mutation cannot leak between them.
func rewireSubqueryGroups(originals, clones []*SubQuery) {
	if len(originals) != len(clones) {
		panic("subquery clone size mismatch")
	}

	origToClone := make(map[*SubQuery]*SubQuery, len(originals))
	for i, o := range originals {
		origToClone[o] = clones[i]
	}

	newGroups := make(map[*subqueryGroup]*subqueryGroup)
	for i, o := range originals {
		og := o.group
		if og == nil {
			continue
		}
		ng, ok := newGroups[og]
		if !ok {
			ng = &subqueryGroup{
				// Clone the shared predicate so merge's path-scoped Select
				// inlining and the leader's bind-var rewrite operate on the
				// cloned tree only.
				Original: sqlparser.Clone(og.Original),
				Members:  make([]*SubQuery, len(og.Members)),
				merged:   make(map[*SubQuery]bool, len(og.merged)),
				emitted:  og.emitted,
			}
			for j, m := range og.Members {
				if cm, ok := origToClone[m]; ok {
					ng.Members[j] = cm
				} else {
					// Member is no longer in this container's Inner (already
					// merged out, for instance). Keep the original pointer as
					// a placeholder; nothing on this clone references it for
					// active settle / merge work.
					ng.Members[j] = m
				}
			}
			for m := range og.merged {
				if cm, ok := origToClone[m]; ok {
					ng.merged[cm] = true
				} else {
					ng.merged[m] = true
				}
			}
			newGroups[og] = ng
		}
		clones[i].group = ng
		clones[i].Original = ng.Original
		// originalSubquery (and the path-based GetNodeFromPath lookups in
		// settle / merge) must resolve against the cloned predicate, not the
		// original — otherwise mutations on the clone (e.g. Select inlining
		// during merge) would target nodes in the original tree.
		if subqNode, ok := sqlparser.GetNodeFromPath(ng.Original, clones[i].path).(*sqlparser.Subquery); ok {
			clones[i].originalSubquery = subqNode
		}
	}
}

func (sqc *SubQueryContainer) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return sqc.Outer.GetOrdering(ctx)
}

// Inputs implements the Operator interface
func (sqc *SubQueryContainer) Inputs() []Operator {
	operators := []Operator{sqc.Outer}
	for _, inner := range sqc.Inner {
		operators = append(operators, inner)
	}
	return operators
}

// SetInputs implements the Operator interface
func (sqc *SubQueryContainer) SetInputs(ops []Operator) {
	sqc.Outer = ops[0]
}

func (sqc *SubQueryContainer) ShortDescription() string {
	return ""
}

func (sqc *SubQueryContainer) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	sqc.Outer = sqc.Outer.AddPredicate(ctx, expr)
	return sqc
}

func (sqc *SubQueryContainer) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, exprs *sqlparser.AliasedExpr) int {
	return sqc.Outer.AddColumn(ctx, reuseExisting, addToGroupBy, exprs)
}

func (sqc *SubQueryContainer) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	return sqc.Outer.AddWSColumn(ctx, offset, underRoute)
}

func (sqc *SubQueryContainer) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return sqc.Outer.FindCol(ctx, expr, underRoute)
}

func (sqc *SubQueryContainer) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return sqc.Outer.GetColumns(ctx)
}

func (sqc *SubQueryContainer) GetSelectExprs(ctx *plancontext.PlanningContext) []sqlparser.SelectExpr {
	return sqc.Outer.GetSelectExprs(ctx)
}

func (sqc *SubQueryContainer) addInner(inner *SubQuery) {
	for _, sq := range sqc.Inner {
		if sq.ArgName == inner.ArgName {
			// we already have this subquery
			return
		}
	}
	sqc.Inner = append(sqc.Inner, inner)
}
