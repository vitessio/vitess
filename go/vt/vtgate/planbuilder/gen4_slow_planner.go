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
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func gen4SlowPlanner(query string) func(sqlparser.Statement, *sqlparser.ReservedVars, ContextVSchema) (engine.Primitive, error) {
	return func(statement sqlparser.Statement, vars *sqlparser.ReservedVars, schema ContextVSchema) (engine.Primitive, error) {
		defer schema.SetPlannerVersion(Gen4Slow)

		gen4Stmt := sqlparser.CloneStatement(statement)
		schema.SetPlannerVersion(Gen4)
		gen4Primitive, gen4Err := createInstructionFor(query, gen4Stmt, vars, schema, false, false)

		// We insert data only once using the gen4 planner to avoid duplicated rows.
		if _, isInsert := statement.(*sqlparser.Insert); isInsert {
			return gen4Primitive, gen4Err
		}

		v3Stmt := sqlparser.CloneStatement(statement)
		schema.SetPlannerVersion(V3)
		v3Primitive, v3Err := createInstructionFor(query, v3Stmt, vars, schema, false, false)

		err := treatV3AndGen4Errors(v3Err, gen4Err)
		if err != nil {
			return nil, err
		}
		return &comparer{v3: v3Primitive, gen4: gen4Primitive}, nil
	}
}

func treatV3AndGen4Errors(v3Err error, gen4Err error) error {
	if v3Err != nil && gen4Err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "v3 and Gen4 failed: v3: %s | Gen4: %s", v3Err.Error(), gen4Err.Error())
	}
	if v3Err == nil && gen4Err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Gen4 failed while v3 did not: %s", gen4Err.Error())
	}
	if v3Err != nil && gen4Err == nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "v3 failed while Gen4 did not: %s", v3Err.Error())
	}
	return nil
}

type comparer struct {
	v3, gen4 engine.Primitive
}

func (c *comparer) RouteType() string {
	return c.gen4.RouteType()
}

func (c *comparer) GetKeyspaceName() string {
	return c.gen4.GetKeyspaceName()
}

func (c *comparer) GetTableName() string {
	return c.gen4.GetTableName()
}

func (c *comparer) GetFields(vcursor engine.VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return c.gen4.GetFields(vcursor, bindVars)
}

func (c *comparer) NeedsTransaction() bool {
	return c.gen4.NeedsTransaction()
}

func (c *comparer) TryExecute(vcursor engine.VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	v3Result, v3Err := c.v3.TryExecute(vcursor, bindVars, wantfields)
	gen4Result, gen4Err := c.gen4.TryExecute(vcursor, bindVars, wantfields)
	err := treatV3AndGen4Errors(v3Err, gen4Err)
	if err != nil {
		return nil, err
	}
	match := sqltypes.ResultsEqualUnordered([]sqltypes.Result{*v3Result}, []sqltypes.Result{*gen4Result})
	if !match {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "results did not match")
	}
	return gen4Result, nil
}

func (c *comparer) TryStreamExecute(vcursor engine.VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	return c.gen4.TryStreamExecute(vcursor, bindVars, wantfields, callback)
}

func (c *comparer) Inputs() []engine.Primitive {
	return c.gen4.Inputs()
}

func (c *comparer) Description() engine.PrimitiveDescription {
	return c.gen4.Description()
}

var _ engine.Primitive = (*comparer)(nil)
