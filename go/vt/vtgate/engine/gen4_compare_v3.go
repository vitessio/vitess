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
	"sync"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// Gen4CompareV3 is a Primitive used to compare V3 and Gen4's plans.
type Gen4CompareV3 struct {
	V3, Gen4, Gen4MP Primitive
	HasOrderBy       bool
}

var _ Primitive = (*Gen4CompareV3)(nil)

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
	v3Result, gen4Result, gen4MPResult := &sqltypes.Result{}, &sqltypes.Result{}, &sqltypes.Result{}
	if gc.Gen4 != nil {
		gen4Result, gen4Err = gc.Gen4.TryExecute(ctx, vcursor, bindVars, wantfields)
	}
	if gc.Gen4MP != nil {
		gen4MPResult, _ = gc.Gen4.TryExecute(ctx, vcursor, bindVars, wantfields)
	}
	if gc.V3 != nil {
		v3Result, v3Err = gc.V3.TryExecute(ctx, vcursor, bindVars, wantfields)
	}

	if err := CompareErrors(v3Err, gen4Err, "v3", "Gen4"); err != nil {
		return nil, err
	}

	if err := gc.compareResults(v3Result, gen4Result, "v3", "Gen4"); err != nil {
		return nil, err
	}
	if err := gc.compareResults(gen4Result, gen4MPResult, "Gen4", "Gen4 Minimal Planning"); err != nil {
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

	if err := CompareErrors(v3Err, gen4Err, "v3", "Gen4"); err != nil {
		return err
	}

	if err := gc.compareResults(v3Result, gen4Result, "v3", "Gen4"); err != nil {
		return err
	}
	return callback(gen4Result)
}

func (gc *Gen4CompareV3) compareResults(v3Result, gen4Result *sqltypes.Result, lhs, rhs string) error {
	var match bool
	if gc.HasOrderBy {
		match = sqltypes.ResultsEqual([]sqltypes.Result{*v3Result}, []sqltypes.Result{*gen4Result})
	} else {
		match = sqltypes.ResultsEqualUnordered([]sqltypes.Result{*v3Result}, []sqltypes.Result{*gen4Result})
	}
	if !match {
		printMismatch(v3Result, gen4Result, gc.V3, gc.Gen4, lhs, rhs)
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "results did not match, see VTGate's logs for more information")
	}
	return nil
}

// Inputs implements the Primitive interface
func (gc *Gen4CompareV3) Inputs() []Primitive {
	return []Primitive{gc.Gen4, gc.V3}
}

// description implements the Primitive interface
func (gc *Gen4CompareV3) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: "Gen4CompareV3"}
}
