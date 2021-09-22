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
		defer schema.SetPlannerVersion(Gen4CompareV3)
		primitive := &engine.Gen4CompareV3{}

		gen4Primitive, gen4Err := planWithPlannerVersion(statement, vars, schema, query, Gen4)

		switch s := statement.(type) {
		case *sqlparser.Insert:
			// we insert data only once using the gen4 planner to avoid duplicated rows in tables.
			return gen4Primitive, gen4Err
		case *sqlparser.Select:
			primitive.HasOrderBy = len(s.OrderBy) > 0
			for _, expr := range s.SelectExprs {
				if _, nextVal := expr.(*sqlparser.Nextval); nextVal {
					primitive.IsNextVal = true
					break
				}
			}
		}

		// since lock primitives can imply creation and deletion of new locks,
		// we execute them only once using Gen4 to avoid the duplicated locks
		// and double releases.
		if hasLockPrimitive(gen4Primitive) {
			return gen4Primitive, gen4Err
		}

		// get V3's plan
		v3Primitive, v3Err := planWithPlannerVersion(statement, vars, schema, query, V3)

		// check errors
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
