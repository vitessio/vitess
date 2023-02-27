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

package plancontext

import (
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type PlanningContext struct {
	ReservedVars *sqlparser.ReservedVars
	SemTable     *semantics.SemTable
	VSchema      VSchema

	// here we add all predicates that were created because of a join condition
	// e.g. [FROM tblA JOIN tblB ON a.colA = b.colB] will be rewritten to [FROM tblB WHERE :a_colA = b.colB],
	// if we assume that tblB is on the RHS of the join. This last predicate in the WHERE clause is added to the
	// map below
	JoinPredicates     map[sqlparser.Expr][]sqlparser.Expr
	SkipPredicates     map[sqlparser.Expr]any
	PlannerVersion     querypb.ExecuteOptions_PlannerVersion
	RewriteDerivedExpr bool
}

func NewPlanningContext(reservedVars *sqlparser.ReservedVars, semTable *semantics.SemTable, vschema VSchema, version querypb.ExecuteOptions_PlannerVersion) *PlanningContext {
	ctx := &PlanningContext{
		ReservedVars:   reservedVars,
		SemTable:       semTable,
		VSchema:        vschema,
		JoinPredicates: map[sqlparser.Expr][]sqlparser.Expr{},
		SkipPredicates: map[sqlparser.Expr]any{},
		PlannerVersion: version,
	}
	return ctx
}

func (c PlanningContext) IsSubQueryToReplace(e sqlparser.Expr) bool {
	ext, ok := e.(*sqlparser.Subquery)
	if !ok {
		return false
	}
	for _, extractedSubq := range c.SemTable.GetSubqueryNeedingRewrite() {
		if extractedSubq.Merged && c.SemTable.EqualsExpr(extractedSubq.Subquery, ext) {
			return true
		}
	}
	return false
}
