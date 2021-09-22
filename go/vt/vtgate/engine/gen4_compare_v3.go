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
	"encoding/json"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// Gen4CompareV3 is a Primitive used to compare V3 and Gen4's plans.
type Gen4CompareV3 struct {
	V3, Gen4   Primitive
	HasOrderBy bool
}

var _ Primitive = (*Gen4CompareV3)(nil)
var _ Gen4Comparer = (*Gen4CompareV3)(nil)

// GetGen4Primitive implements the Gen4Comparer interface
func (gc *Gen4CompareV3) GetGen4Primitive() Primitive {
	return gc.Gen4
}

// RouteType implements the Primitive interface
func (gc *Gen4CompareV3) RouteType() string {
	return gc.Gen4.RouteType()
}

// GetKeyspaceName implements the Primitive interface
func (gc *Gen4CompareV3) GetKeyspaceName() string {
	return gc.Gen4.GetKeyspaceName()
}

// GetTableName implements the Primitive interface
func (gc *Gen4CompareV3) GetTableName() string {
	return gc.Gen4.GetTableName()
}

// GetFields implements the Primitive interface
func (gc *Gen4CompareV3) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return gc.Gen4.GetFields(vcursor, bindVars)
}

// NeedsTransaction implements the Primitive interface
func (gc *Gen4CompareV3) NeedsTransaction() bool {
	return gc.Gen4.NeedsTransaction()
}

// TryExecute implements the Primitive interface
func (gc *Gen4CompareV3) TryExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	gen4Result, gen4Err := gc.Gen4.TryExecute(vcursor, bindVars, wantfields)
	v3Result, v3Err := gc.V3.TryExecute(vcursor, bindVars, wantfields)
	err := CompareV3AndGen4Errors(v3Err, gen4Err)
	if err != nil {
		return nil, err
	}

	var match bool
	if gc.HasOrderBy {
		match = sqltypes.ResultsEqual([]sqltypes.Result{*v3Result}, []sqltypes.Result{*gen4Result})
	} else {
		match = sqltypes.ResultsEqualUnordered([]sqltypes.Result{*v3Result}, []sqltypes.Result{*gen4Result})
	}
	if !match {
		gc.printMismatch(v3Result, gen4Result)
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "results did not match")
	}
	return gen4Result, nil
}

// TryStreamExecute implements the Primitive interface
func (gc *Gen4CompareV3) TryStreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	v3Result, gen4Result := &sqltypes.Result{}, &sqltypes.Result{}

	gen4Error := gc.Gen4.TryStreamExecute(vcursor, bindVars, wantfields, func(result *sqltypes.Result) error {
		gen4Result.AppendResult(result)
		return nil
	})
	v3Err := gc.V3.TryStreamExecute(vcursor, bindVars, wantfields, func(result *sqltypes.Result) error {
		v3Result.AppendResult(result)
		return nil
	})

	err := CompareV3AndGen4Errors(v3Err, gen4Error)
	if err != nil {
		return err
	}

	var match bool
	if gc.HasOrderBy {
		match = sqltypes.ResultsEqual([]sqltypes.Result{*v3Result}, []sqltypes.Result{*gen4Result})
	} else {
		match = sqltypes.ResultsEqualUnordered([]sqltypes.Result{*v3Result}, []sqltypes.Result{*gen4Result})
	}
	if !match {
		gc.printMismatch(v3Result, gen4Result)
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "results did not match")
	}
	return callback(gen4Result)
}

func (gc *Gen4CompareV3) printMismatch(v3Result *sqltypes.Result, gen4Result *sqltypes.Result) {
	log.Infof("%T mismatch", gc)
	gen4plan := &Plan{
		Instructions: gc.Gen4,
	}
	gen4JSON, _ := json.MarshalIndent(gen4plan, "", "  ")
	log.Info("Gen4 plan:\n", string(gen4JSON))

	v3plan := &Plan{
		Instructions: gc.V3,
	}
	v3JSON, _ := json.MarshalIndent(v3plan, "", "  ")
	log.Info("V3 plan:\n", string(v3JSON))

	log.Infof("Gen4 got: %s", gen4Result.Rows)
	log.Infof("V3 got: %s", v3Result.Rows)
}

// Inputs implements the Primitive interface
func (gc *Gen4CompareV3) Inputs() []Primitive {
	return gc.Gen4.Inputs()
}

// Description implements the Primitive interface
func (gc *Gen4CompareV3) Description() PrimitiveDescription {
	return gc.Gen4.Description()
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
