/*
Copyright 2021 The Vitess Authors.

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

package planbuilder

import (
	"fmt"
	"strconv"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

func gen4Planner(query string, plannerVersion querypb.ExecuteOptions_PlannerVersion) stmtPlanner {
	return func(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
		switch stmt := stmt.(type) {
		case sqlparser.SelectStatement:
			return gen4SelectStmtPlanner(query, plannerVersion, stmt, reservedVars, vschema)
		case *sqlparser.Update:
			return gen4UpdateStmtPlanner(plannerVersion, stmt, reservedVars, vschema)
		case *sqlparser.Delete:
			return gen4DeleteStmtPlanner(plannerVersion, stmt, reservedVars, vschema)
		case *sqlparser.Insert:
			return gen4InsertStmtPlanner(plannerVersion, stmt, reservedVars, vschema)
		default:
			return nil, vterrors.VT12001(fmt.Sprintf("%T", stmt))
		}
	}
}

func pushCommentDirectivesOnPlan(plan logicalPlan, stmt sqlparser.Statement) logicalPlan {
	var directives *sqlparser.CommentDirectives
	cmt, ok := stmt.(sqlparser.Commented)
	if ok {
		directives = cmt.GetParsedComments().Directives()
		scatterAsWarns := directives.IsSet(sqlparser.DirectiveScatterErrorsAsWarnings)
		timeout := queryTimeout(directives)
		multiShardAutoCommit := directives.IsSet(sqlparser.DirectiveMultiShardAutocommit)

		if scatterAsWarns || timeout > 0 || multiShardAutoCommit {
			_, _ = visit(plan, func(logicalPlan logicalPlan) (bool, logicalPlan, error) {
				switch plan := logicalPlan.(type) {
				case *route:
					plan.eroute.ScatterErrorsAsWarnings = scatterAsWarns
					plan.eroute.QueryTimeout = timeout
				case *primitiveWrapper:
					setDirective(plan.prim, multiShardAutoCommit, timeout)
				case *insert:
					setDirective(plan.eInsert, multiShardAutoCommit, timeout)
				}
				return true, logicalPlan, nil
			})
		}
	}

	return plan
}

func setDirective(prim engine.Primitive, msac bool, timeout int) {
	switch edml := prim.(type) {
	case *engine.Insert:
		edml.MultiShardAutocommit = msac
		edml.QueryTimeout = timeout
	case *engine.Update:
		edml.MultiShardAutocommit = msac
		edml.QueryTimeout = timeout
	case *engine.Delete:
		edml.MultiShardAutocommit = msac
		edml.QueryTimeout = timeout
	}
}

// queryTimeout returns DirectiveQueryTimeout value if set, otherwise returns 0.
func queryTimeout(d *sqlparser.CommentDirectives) int {
	val, _ := d.GetString(sqlparser.DirectiveQueryTimeout, "0")
	if intVal, err := strconv.Atoi(val); err == nil {
		return intVal
	}
	return 0
}
