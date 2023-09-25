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

	// If we during planning have turned this expression into an argument name,
	// we can continue using the same argument name
	ReservedArguments map[sqlparser.Expr]string

	// DelegateAggregation tells us when we are allowed to split an aggregation across vtgate and mysql
	// We aggregate within a shard, and then at the vtgate level we aggregate the incoming shard aggregates
	DelegateAggregation bool

	// VerifyAllFKs tells whether we need verification for all the fk constraints on VTGate.
	// This is required for queries we are running with /*+ SET_VAR(foreign_key_checks=OFF) */
	VerifyAllFKs bool

	// ParentFKToIgnore stores a specific parent foreign key that we would need to ignore while planning
	// a certain query. This field is used in UPDATE CASCADE planning, wherein while planning the child update
	// query, we need to ignore the parent foreign key constraint that caused the cascade in question.
	ParentFKToIgnore string
}

func CreatePlanningContext(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema VSchema, version querypb.ExecuteOptions_PlannerVersion) (*PlanningContext, error) {
	ksName := ""
	if ks, _ := vschema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}

	semTable, err := semantics.Analyze(stmt, ksName, vschema)
	if err != nil {
		return nil, err
	}

	// record any warning as planner warning.
	vschema.PlannerWarning(semTable.Warning)

	return &PlanningContext{
		ReservedVars:      reservedVars,
		SemTable:          semTable,
		VSchema:           vschema,
		JoinPredicates:    map[sqlparser.Expr][]sqlparser.Expr{},
		SkipPredicates:    map[sqlparser.Expr]any{},
		PlannerVersion:    version,
		ReservedArguments: map[sqlparser.Expr]string{},
	}, nil
}

func (ctx *PlanningContext) IsSubQueryToReplace(e sqlparser.Expr) bool {
	ext, ok := e.(*sqlparser.Subquery)
	if !ok {
		return false
	}
	for _, extractedSubq := range ctx.SemTable.GetSubqueryNeedingRewrite() {
		if extractedSubq.Merged && ctx.SemTable.EqualsExpr(extractedSubq.Subquery, ext) {
			return true
		}
	}
	return false
}

func (ctx *PlanningContext) GetArgumentFor(expr sqlparser.Expr, f func() string) string {
	for key, name := range ctx.ReservedArguments {
		if ctx.SemTable.EqualsExpr(key, expr) {
			return name
		}
	}
	bvName := f()
	ctx.ReservedArguments[expr] = bvName
	return bvName
}
