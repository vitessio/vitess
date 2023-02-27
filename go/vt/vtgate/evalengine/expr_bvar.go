/*
Copyright 2023 The Vitess Authors.

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
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	BindVariable struct {
		Key    string
		col    collations.TypedCollation
		coerce sqltypes.Type
		tuple  bool
	}
)

var _ Expr = (*BindVariable)(nil)

func (bv *BindVariable) bvar(env *ExpressionEnv) (*querypb.BindVariable, error) {
	val, ok := env.BindVars[bv.Key]
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query arguments missing for %s", bv.Key)
	}
	return val, nil
}

// eval implements the Expr interface
func (bv *BindVariable) eval(env *ExpressionEnv) (eval, error) {
	bvar, err := bv.bvar(env)
	if err != nil {
		return nil, err
	}

	switch bvar.Type {
	case sqltypes.Tuple:
		if !bv.tuple {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query argument '%s' cannot be a tuple", bv.Key)
		}

		tuple := make([]eval, 0, len(bvar.Values))
		for _, value := range bvar.Values {
			e, err := valueToEval(sqltypes.MakeTrusted(value.Type, value.Value), collations.TypedCollation{})
			if err != nil {
				return nil, err
			}
			tuple = append(tuple, e)
		}
		return &evalTuple{t: tuple}, nil

	default:
		typ := bvar.Type
		if bv.coerce >= 0 {
			typ = bv.coerce
		}
		if bv.tuple {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query argument '%s' must be a tuple (is %s)", bv.Key, typ)
		}
		return valueToEval(sqltypes.MakeTrusted(typ, bvar.Value), bv.col)
	}
}

// typeof implements the Expr interface
func (bv *BindVariable) typeof(env *ExpressionEnv) (sqltypes.Type, typeFlag) {
	bvar, err := bv.bvar(env)
	if err != nil {
		return sqltypes.Null, flagNull | flagNullable
	}
	switch bvar.Type {
	case sqltypes.Null:
		return sqltypes.Null, flagNull | flagNullable
	case sqltypes.HexNum, sqltypes.HexVal:
		return sqltypes.VarBinary, flagHex
	default:
		if bv.coerce >= 0 {
			return bv.coerce, 0
		}
		return bvar.Type, 0
	}
}
