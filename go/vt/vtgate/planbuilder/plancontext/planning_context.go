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
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type PlanningContext struct {
	ReservedVars *sqlparser.ReservedVars
	SemTable     *semantics.SemTable
	VSchema      VSchema

	// joinPredicates maps each original join predicate (key) to a slice of
	// variations of the RHS predicates (value). This map is used to handle
	// different scenarios in join planning, where the RHS predicates are
	// modified to accommodate dependencies from the LHS, represented as Arguments.
	joinPredicates map[sqlparser.Expr][]sqlparser.Expr

	// skipPredicates tracks predicates that should be skipped, typically when
	// a join predicate is reverted to its original form during planning.
	skipPredicates map[sqlparser.Expr]any

	PlannerVersion querypb.ExecuteOptions_PlannerVersion

	// If we during planning have turned this expression into an argument name,
	// we can continue using the same argument name
	ReservedArguments map[sqlparser.Expr]string

	// VerifyAllFKs tells whether we need verification for all the fk constraints on VTGate.
	// This is required for queries we are running with /*+ SET_VAR(foreign_key_checks=OFF) */
	VerifyAllFKs bool

	// Projected subqueries that have been merged
	MergedSubqueries []*sqlparser.Subquery

	// CurrentPhase keeps track of how far we've gone in the planning process
	// The type should be operators.Phase, but depending on that would lead to circular dependencies
	CurrentPhase int

	// Statement contains the originally parsed statement
	Statement sqlparser.Statement
}

// CreatePlanningContext initializes a new PlanningContext with the given parameters.
// It analyzes the SQL statement within the given virtual schema context,
// handling default keyspace settings and semantic analysis.
// Returns an error if semantic analysis fails.
func CreatePlanningContext(stmt sqlparser.Statement,
	reservedVars *sqlparser.ReservedVars,
	vschema VSchema,
	version querypb.ExecuteOptions_PlannerVersion,
) (*PlanningContext, error) {
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
		joinPredicates:    map[sqlparser.Expr][]sqlparser.Expr{},
		skipPredicates:    map[sqlparser.Expr]any{},
		PlannerVersion:    version,
		ReservedArguments: map[sqlparser.Expr]string{},
		Statement:         stmt,
	}, nil
}

// GetReservedArgumentFor retrieves a reserved argument name for a given expression.
// If the expression already has a reserved argument, it returns that name;
// otherwise, it reserves a new name based on the expression type.
func (ctx *PlanningContext) GetReservedArgumentFor(expr sqlparser.Expr) string {
	for key, name := range ctx.ReservedArguments {
		if ctx.SemTable.EqualsExpr(key, expr) {
			return name
		}
	}
	var bvName string
	switch expr := expr.(type) {
	case *sqlparser.ColName:
		bvName = ctx.ReservedVars.ReserveColName(expr)
	case *sqlparser.Subquery:
		bvName = ctx.ReservedVars.ReserveSubQuery()
	default:
		bvName = ctx.ReservedVars.ReserveVariable(sqlparser.CompliantString(expr))
	}
	ctx.ReservedArguments[expr] = bvName

	return bvName
}

// ShouldSkip determines if a given expression should be ignored in the SQL output building.
// It checks against expressions that have been marked to be excluded from further processing.
func (ctx *PlanningContext) ShouldSkip(expr sqlparser.Expr) bool {
	for k := range ctx.skipPredicates {
		if ctx.SemTable.EqualsExpr(expr, k) {
			return true
		}
	}
	return false
}

// AddJoinPredicates associates additional RHS predicates with an existing join predicate.
// This is used to dynamically adjust the RHS predicates based on evolving join conditions.
func (ctx *PlanningContext) AddJoinPredicates(joinPred sqlparser.Expr, predicates ...sqlparser.Expr) {
	fn := func(original sqlparser.Expr, rhsExprs []sqlparser.Expr) {
		ctx.joinPredicates[original] = append(rhsExprs, predicates...)
	}
	if ctx.execOnJoinPredicateEqual(joinPred, fn) {
		return
	}

	// we didn't find an existing entry
	ctx.joinPredicates[joinPred] = predicates
}

// SkipJoinPredicates marks the predicates related to a specific join predicate as irrelevant
// for the current planning stage. This is used when a join has been pushed under a route and
// the original predicate will be used.
func (ctx *PlanningContext) SkipJoinPredicates(joinPred sqlparser.Expr) error {
	fn := func(_ sqlparser.Expr, rhsExprs []sqlparser.Expr) {
		ctx.skipThesePredicates(rhsExprs...)
	}
	if ctx.execOnJoinPredicateEqual(joinPred, fn) {
		return nil
	}
	return vterrors.VT13001("predicate does not exist: " + sqlparser.String(joinPred))
}

// KeepPredicateInfo transfers join predicate information from another context.
// This is useful when nesting queries, ensuring consistent predicate handling across contexts.
func (ctx *PlanningContext) KeepPredicateInfo(other *PlanningContext) {
	for k, v := range other.joinPredicates {
		ctx.AddJoinPredicates(k, v...)
	}
	for expr := range other.skipPredicates {
		ctx.skipThesePredicates(expr)
	}
}

// skipThesePredicates is a utility function to exclude certain predicates from SQL building
func (ctx *PlanningContext) skipThesePredicates(preds ...sqlparser.Expr) {
outer:
	for _, expr := range preds {
		for k := range ctx.skipPredicates {
			if ctx.SemTable.EqualsExpr(expr, k) {
				// already skipped
				continue outer
			}
		}
		ctx.skipPredicates[expr] = nil
	}
}

func (ctx *PlanningContext) execOnJoinPredicateEqual(joinPred sqlparser.Expr, fn func(original sqlparser.Expr, rhsExprs []sqlparser.Expr)) bool {
	for key, values := range ctx.joinPredicates {
		if ctx.SemTable.EqualsExpr(joinPred, key) {
			fn(key, values)
			return true
		}
	}
	return false
}
