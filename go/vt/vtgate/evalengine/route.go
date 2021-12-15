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

package evalengine

import (
	"encoding/json"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

type RouteValue struct {
	Expr Expr
}

// ResolveValue allows for retrieval of the value we expose for public consumption
func (rv *RouteValue) ResolveValue(bindVars map[string]*querypb.BindVariable) (sqltypes.Value, error) {
	env := EnvWithBindVars(bindVars)
	evalResul, err := env.Evaluate(rv.Expr)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return evalResul.Value(), nil
}

// ResolveList allows for retrieval of the value we expose for public consumption
func (rv *RouteValue) ResolveList(bindVars map[string]*querypb.BindVariable) ([]sqltypes.Value, error) {
	env := EnvWithBindVars(bindVars)
	evalResul, err := env.Evaluate(rv.Expr)
	if err != nil {
		return nil, err
	}
	return evalResul.TupleValues(), nil
}

func (rv *RouteValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(FormatExpr(rv.Expr))
}
