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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func gen4CompareV3Planner(query string) func(sqlparser.Statement, *sqlparser.ReservedVars, ContextVSchema) (engine.Primitive, error) {
	return func(statement sqlparser.Statement, vars *sqlparser.ReservedVars, schema ContextVSchema) (engine.Primitive, error) {
		// we will be switching the planner version to Gen4 and V3 in order to
		// create instructions using them, thus we make sure to switch back to
		// the Gen4CompareV3 planner before exiting this method.
		defer schema.SetPlannerVersion(Gen4CompareV3)

		primitive := &engine.Gen4CompareV3{}
		onlyGen4 := false

		// preliminary checks on the given statement
		switch s := statement.(type) {
		case *sqlparser.Insert:
			// we want to insert data only once into the database,
			// for this reason, we will only use Gen4's instructions.
			onlyGen4 = true
		case *sqlparser.Select:
			primitive.HasOrderBy = len(s.OrderBy) > 0
			for _, expr := range s.SelectExprs {
				// we are not executing the plan a second time if the query is a select next val,
				// since the first execution might increment the `next` value, results will almost
				// always be different between v3 and Gen4.
				if _, nextVal := expr.(*sqlparser.Nextval); nextVal {
					onlyGen4 = true
					break
				}
			}
		}

		// plan statement using Gen4
		gen4Primitive, gen4Err := planWithPlannerVersion(statement, vars, schema, query, Gen4)

		// if onlyGen4 is set to true or Gen4's instruction contain a lock primitive,
		// we use only Gen4's primitive and exit early without using V3's.
		// since lock primitives can imply the creation or deletion of locks,
		// we want to execute them once using Gen4 to avoid the duplicated locks
		// or double lock-releases.
		if onlyGen4 || hasLockPrimitive(gen4Primitive) {
			return gen4Primitive, gen4Err
		}

		// get V3's plan
		v3Primitive, v3Err := planWithPlannerVersion(statement, vars, schema, query, V3)

		// check potential errors from Gen4 and V3
		err := engine.CompareV3AndGen4Errors(v3Err, gen4Err)
		if err != nil {
			return nil, err
		}

		primitive.Gen4 = gen4Primitive
		primitive.V3 = v3Primitive
		return primitive, nil
	}
}

func planWithPlannerVersion(statement sqlparser.Statement, vars *sqlparser.ReservedVars, schema ContextVSchema, query string, version PlannerVersion) (engine.Primitive, error) {
	schema.SetPlannerVersion(version)
	stmt := sqlparser.CloneStatement(statement)
	return createInstructionFor(query, stmt, vars, schema, false, false)
}

// hasLockPrimitive recursively walks through the given primitive and its children
// to see if there are any engine.Lock primitive.
func hasLockPrimitive(primitive engine.Primitive) bool {
	switch primitive.(type) {
	case *engine.Lock:
		return true
	default:
		for _, p := range primitive.Inputs() {
			if hasLockPrimitive(p) {
				return true
			}
		}
	}
	return false
}
