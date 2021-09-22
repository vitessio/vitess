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
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func gen4CompareV3Planner(query string) func(sqlparser.Statement, *sqlparser.ReservedVars, ContextVSchema) (engine.Primitive, error) {
	return func(statement sqlparser.Statement, vars *sqlparser.ReservedVars, schema ContextVSchema) (engine.Primitive, error) {
		defer schema.SetPlannerVersion(Gen4CompareV3)
		primitive := &gen4CompareV3{}

		gen4Primitive, gen4Err := planWithPlannerVersion(statement, vars, schema, query, Gen4)

		// we insert data only once using the gen4 planner to avoid duplicated rows in tables.
		switch s := statement.(type) {
		case *sqlparser.Insert:
			return gen4Primitive, gen4Err
		case *sqlparser.Select:
			primitive.hasOrderBy = len(s.OrderBy) > 0
			for _, expr := range s.SelectExprs {
				if _, nextVal := expr.(*sqlparser.Nextval); nextVal {
					primitive.isNextVal = true
					break
				}
			}
		}

		// get V3's plan
		v3Primitive, v3Err := planWithPlannerVersion(statement, vars, schema, query, V3)

		// check errors
		err := treatV3AndGen4Errors(v3Err, gen4Err)
		if err != nil {
			return nil, err
		}

		primitive.gen4 = gen4Primitive
		primitive.v3 = v3Primitive
		return primitive, nil
	}
}

func planWithPlannerVersion(statement sqlparser.Statement, vars *sqlparser.ReservedVars, schema ContextVSchema, query string, version PlannerVersion) (engine.Primitive, error) {
	schema.SetPlannerVersion(version)
	stmt := sqlparser.CloneStatement(statement)
	return createInstructionFor(query, stmt, vars, schema, false, false)
}

func treatV3AndGen4Errors(v3Err error, gen4Err error) error {
	if v3Err != nil && gen4Err != nil {
		if v3Err.Error() == gen4Err.Error() {
			return gen4Err
		}
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "v3 and Gen4 failed with different errors: v3: %s | Gen4: %s", v3Err.Error(), gen4Err.Error())
	}
	if v3Err == nil && gen4Err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Gen4 failed while v3 did not: %s", gen4Err.Error())
	}
	if v3Err != nil && gen4Err == nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "v3 failed while Gen4 did not: %s", v3Err.Error())
	}
	return nil
}

type gen4CompareV3 struct {
	v3, gen4              engine.Primitive
	hasOrderBy, isNextVal bool
}

var _ engine.Primitive = (*gen4CompareV3)(nil)

func (c *gen4CompareV3) RouteType() string {
	return c.gen4.RouteType()
}

func (c *gen4CompareV3) GetKeyspaceName() string {
	return c.gen4.GetKeyspaceName()
}

func (c *gen4CompareV3) GetTableName() string {
	return c.gen4.GetTableName()
}

func (c *gen4CompareV3) GetFields(vcursor engine.VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return c.gen4.GetFields(vcursor, bindVars)
}

func (c *gen4CompareV3) NeedsTransaction() bool {
	return c.gen4.NeedsTransaction()
}

func (c *gen4CompareV3) TryExecute(vcursor engine.VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	gen4Result, gen4Err := c.gen4.TryExecute(vcursor, bindVars, wantfields)

	// we are not executing the plan a second time if the query is a select next val,
	// since the first execution incremented the `next` value, results will always
	// mismatch between v3 and Gen4.
	if c.isNextVal {
		return gen4Result, gen4Err
	}

	v3Result, v3Err := c.v3.TryExecute(vcursor, bindVars, wantfields)
	err := treatV3AndGen4Errors(v3Err, gen4Err)
	if err != nil {
		return nil, err
	}
	match := sqltypes.ResultsEqualUnordered([]sqltypes.Result{*v3Result}, []sqltypes.Result{*gen4Result})
	if !match {
		log.Infof("V3 got: %s", v3Result.Rows)
		log.Infof("Gen4 got: %s", gen4Result.Rows)
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "results did not match")
	}
	return gen4Result, nil
}

func (c *gen4CompareV3) TryStreamExecute(vcursor engine.VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	v3Result, gen4Result := &sqltypes.Result{}, &sqltypes.Result{}
	gen4Error := c.gen4.TryStreamExecute(vcursor, bindVars, wantfields, func(result *sqltypes.Result) error {
		gen4Result.AppendResult(result)
		return nil
	})

	// we are not executing the plan a second time if the query is a select next val,
	// since the first execution incremented the `next` value, results will always
	// mismatch between v3 and Gen4.
	if c.isNextVal {
		if gen4Error != nil {
			return gen4Error
		}
		return callback(gen4Result)
	}

	v3Err := c.v3.TryStreamExecute(vcursor, bindVars, wantfields, func(result *sqltypes.Result) error {
		v3Result.AppendResult(result)
		return nil
	})
	err := treatV3AndGen4Errors(v3Err, gen4Error)
	if err != nil {
		return err
	}
	match := sqltypes.ResultsEqualUnordered([]sqltypes.Result{*v3Result}, []sqltypes.Result{*gen4Result})
	if !match {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "results did not match")
	}
	return callback(gen4Result)
}

func (c *gen4CompareV3) Inputs() []engine.Primitive {
	return c.gen4.Inputs()
}

func (c *gen4CompareV3) Description() engine.PrimitiveDescription {
	return c.gen4.Description()
}
