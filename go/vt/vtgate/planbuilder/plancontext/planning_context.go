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
	JoinPredicates map[sqlparser.Expr][]sqlparser.Expr
	SkipPredicates map[sqlparser.Expr]interface{}
	PlannerVersion querypb.ExecuteOptions_PlannerVersion
}

func NewPlanningContext(reservedVars *sqlparser.ReservedVars, semTable *semantics.SemTable, vschema VSchema, version querypb.ExecuteOptions_PlannerVersion) *PlanningContext {
	ctx := &PlanningContext{
		ReservedVars:   reservedVars,
		SemTable:       semTable,
		VSchema:        vschema,
		JoinPredicates: map[sqlparser.Expr][]sqlparser.Expr{},
		SkipPredicates: map[sqlparser.Expr]interface{}{},
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
		if extractedSubq.NeedsRewrite && sqlparser.EqualsRefOfSubquery(extractedSubq.Subquery, ext) {
			return true
		}
	}
	return false
}
