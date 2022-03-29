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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Filter)(nil)

// Filter is a primitive that performs the FILTER operation.
type Filter struct {
	Predicate    evalengine.Expr
	ASTPredicate sqlparser.Expr
	Input        Primitive

	noTxNeeded
}

// RouteType returns a description of the query routing type used by the primitive
func (f *Filter) RouteType() string {
	return f.Input.RouteType()
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (f *Filter) GetKeyspaceName() string {
	return f.Input.GetKeyspaceName()
}

// GetTableName specifies the table that this primitive routes to.
func (f *Filter) GetTableName() string {
	return f.Input.GetTableName()
}

// TryExecute satisfies the Primitive interface.
func (f *Filter) TryExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	result, err := f.Input.TryExecute(vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	env := evalengine.EnvWithBindVars(bindVars, vcursor.ConnCollation())
	var rows [][]sqltypes.Value
	env.Fields = result.Fields
	for _, row := range result.Rows {
		env.Row = row
		evalResult, err := env.Evaluate(f.Predicate)
		if err != nil {
			return nil, err
		}
		intEvalResult, err := evalResult.Value().ToInt64()
		if err != nil {
			return nil, err
		}
		if intEvalResult == 1 {
			rows = append(rows, row)
		}
	}
	result.Rows = rows
	return result, nil
}

// TryStreamExecute satisfies the Primitive interface.
func (f *Filter) TryStreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	env := evalengine.EnvWithBindVars(bindVars, vcursor.ConnCollation())
	filter := func(results *sqltypes.Result) error {
		var rows [][]sqltypes.Value
		env.Fields = results.Fields
		for _, row := range results.Rows {
			env.Row = row
			evalResult, err := env.Evaluate(f.Predicate)
			if err != nil {
				return err
			}
			intEvalResult, err := evalResult.Value().ToInt64()
			if err != nil {
				return err
			}
			if intEvalResult == 1 {
				rows = append(rows, row)
			}
		}
		results.Rows = rows
		return callback(results)
	}
	return f.Input.TryStreamExecute(vcursor, bindVars, wantfields, filter)
}

// GetFields implements the Primitive interface.
func (f *Filter) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return f.Input.GetFields(vcursor, bindVars)
}

// Inputs returns the input to limit
func (f *Filter) Inputs() []Primitive {
	return []Primitive{f.Input}
}

func (f *Filter) description() PrimitiveDescription {
	other := map[string]any{
		"Predicate": sqlparser.String(f.ASTPredicate),
	}

	return PrimitiveDescription{
		OperatorType: "Filter",
		Other:        other,
	}
}
