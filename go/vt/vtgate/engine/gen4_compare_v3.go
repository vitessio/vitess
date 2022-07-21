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
	"context"
	"encoding/json"
	"sync"

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
func (gc *Gen4CompareV3) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return gc.Gen4.GetFields(ctx, vcursor, bindVars)
}

// NeedsTransaction implements the Primitive interface
func (gc *Gen4CompareV3) NeedsTransaction() bool {
	return gc.Gen4.NeedsTransaction()
}

// TryExecute implements the Primitive interface
func (gc *Gen4CompareV3) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	var v3Err, gen4Err error
	v3Result, gen4Result := &sqltypes.Result{}, &sqltypes.Result{}
	if gc.Gen4 != nil {
		gen4Result, gen4Err = gc.Gen4.TryExecute(ctx, vcursor, bindVars, wantfields)
	}
	if gc.V3 != nil {
		v3Result, v3Err = gc.V3.TryExecute(ctx, vcursor, bindVars, wantfields)
	}

	if err := CompareV3AndGen4Errors(v3Err, gen4Err); err != nil {
		return nil, err
	}

	if err := gc.compareResults(v3Result, gen4Result); err != nil {
		return nil, err
	}
	return gen4Result, nil
}

// TryStreamExecute implements the Primitive interface
func (gc *Gen4CompareV3) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	var mu sync.Mutex
	var v3Err, gen4Err error
	v3Result, gen4Result := &sqltypes.Result{}, &sqltypes.Result{}

	if gc.Gen4 != nil {
		gen4Err = gc.Gen4.TryStreamExecute(ctx, vcursor, bindVars, wantfields, func(result *sqltypes.Result) error {
			mu.Lock()
			defer mu.Unlock()
			gen4Result.AppendResult(result)
			return nil
		})
	}
	if gc.V3 != nil {
		v3Err = gc.V3.TryStreamExecute(ctx, vcursor, bindVars, wantfields, func(result *sqltypes.Result) error {
			mu.Lock()
			defer mu.Unlock()
			v3Result.AppendResult(result)
			return nil
		})
	}

	if err := CompareV3AndGen4Errors(v3Err, gen4Err); err != nil {
		return err
	}

	if err := gc.compareResults(v3Result, gen4Result); err != nil {
		return err
	}
	return callback(gen4Result)
}

func (gc *Gen4CompareV3) compareResults(v3Result *sqltypes.Result, gen4Result *sqltypes.Result) error {
	var match bool
	if gc.HasOrderBy {
		match = sqltypes.ResultsEqual([]sqltypes.Result{*v3Result}, []sqltypes.Result{*gen4Result})
	} else {
		match = sqltypes.ResultsEqualUnordered([]sqltypes.Result{*v3Result}, []sqltypes.Result{*gen4Result})
	}
	if !match {
		gc.printMismatch(v3Result, gen4Result)
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "results did not match, see VTGate's logs for more information")
	}
	return nil
}

func (gc *Gen4CompareV3) printMismatch(v3Result *sqltypes.Result, gen4Result *sqltypes.Result) {
	log.Warning("Results of Gen4 and V3 are not equal. Displaying diff.")

	// get Gen4 plan and print it
	gen4plan := &Plan{
		Instructions: gc.Gen4,
	}
	gen4JSON, _ := json.MarshalIndent(gen4plan, "", "  ")
	log.Warning("Gen4's plan:\n", string(gen4JSON))

	// get V3's plan and print it
	v3plan := &Plan{
		Instructions: gc.V3,
	}
	v3JSON, _ := json.MarshalIndent(v3plan, "", "  ")
	log.Warning("V3's plan:\n", string(v3JSON))

	log.Warning("Gen4's results:\n")
	log.Warningf("\t[rows affected: %d]\n", gen4Result.RowsAffected)
	for _, row := range gen4Result.Rows {
		log.Warningf("\t%s", row)
	}
	log.Warning("V3's results:\n")
	log.Warningf("\t[rows affected: %d]\n", v3Result.RowsAffected)
	for _, row := range v3Result.Rows {
		log.Warningf("\t%s", row)
	}
	log.Warning("End of diff.")
}

// Inputs implements the Primitive interface
func (gc *Gen4CompareV3) Inputs() []Primitive {
	return []Primitive{gc.Gen4, gc.V3}
}

// description implements the Primitive interface
func (gc *Gen4CompareV3) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: "Gen4CompareV3"}
}

// CompareV3AndGen4Errors compares the two errors, and if they don't match, produces an error
func CompareV3AndGen4Errors(v3Err, gen4Err error) error {
	if v3Err != nil && gen4Err != nil {
		if v3Err.Error() == gen4Err.Error() {
			return gen4Err
		}
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "v3 and Gen4 failed with different errors: v3: [%s], Gen4: [%s]", v3Err.Error(), gen4Err.Error())
	}
	if v3Err == nil && gen4Err != nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Gen4 failed while v3 did not: %s", gen4Err.Error())
	}
	if v3Err != nil && gen4Err == nil {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "v3 failed while Gen4 did not: %s", v3Err.Error())
	}
	return nil
}
