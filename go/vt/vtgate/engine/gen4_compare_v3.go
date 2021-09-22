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

package engine

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type Gen4CompareV3 struct {
	V3, Gen4              Primitive
	HasOrderBy, IsNextVal bool
}

var _ Primitive = (*Gen4CompareV3)(nil)
var _ Gen4Comparer = (*Gen4CompareV3)(nil)

func (c *Gen4CompareV3) GetGen4Primitive() Primitive {
	return c.Gen4
}

func (c *Gen4CompareV3) RouteType() string {
	return c.Gen4.RouteType()
}

func (c *Gen4CompareV3) GetKeyspaceName() string {
	return c.Gen4.GetKeyspaceName()
}

func (c *Gen4CompareV3) GetTableName() string {
	return c.Gen4.GetTableName()
}

func (c *Gen4CompareV3) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return c.Gen4.GetFields(vcursor, bindVars)
}

func (c *Gen4CompareV3) NeedsTransaction() bool {
	return c.Gen4.NeedsTransaction()
}

func (c *Gen4CompareV3) TryExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	gen4Result, gen4Err := c.Gen4.TryExecute(vcursor, bindVars, wantfields)

	// we are not executing the plan a second time if the query is a select next val,
	// since the first execution incremented the `next` value, results will always
	// mismatch between v3 and Gen4.
	if c.IsNextVal {
		return gen4Result, gen4Err
	}

	v3Result, v3Err := c.V3.TryExecute(vcursor, bindVars, wantfields)
	err := CompareV3AndGen4Errors(v3Err, gen4Err)
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

func (c *Gen4CompareV3) TryStreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	v3Result, gen4Result := &sqltypes.Result{}, &sqltypes.Result{}
	gen4Error := c.Gen4.TryStreamExecute(vcursor, bindVars, wantfields, func(result *sqltypes.Result) error {
		gen4Result.AppendResult(result)
		return nil
	})

	// we are not executing the plan a second time if the query is a select next val,
	// since the first execution incremented the `next` value, results will always
	// mismatch between v3 and Gen4.
	if c.IsNextVal {
		if gen4Error != nil {
			return gen4Error
		}
		return callback(gen4Result)
	}

	v3Err := c.V3.TryStreamExecute(vcursor, bindVars, wantfields, func(result *sqltypes.Result) error {
		v3Result.AppendResult(result)
		return nil
	})
	err := CompareV3AndGen4Errors(v3Err, gen4Error)
	if err != nil {
		return err
	}
	match := sqltypes.ResultsEqualUnordered([]sqltypes.Result{*v3Result}, []sqltypes.Result{*gen4Result})
	if !match {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "results did not match")
	}
	return callback(gen4Result)
}

func (c *Gen4CompareV3) Inputs() []Primitive {
	return c.Gen4.Inputs()
}

func (c *Gen4CompareV3) Description() PrimitiveDescription {
	return c.Gen4.Description()
}

func CompareV3AndGen4Errors(v3Err error, gen4Err error) error {
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
