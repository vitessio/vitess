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
		Key       string
		Type      sqltypes.Type
		Collation collations.TypedCollation
		typed     bool
	}
)

var _ Expr = (*BindVariable)(nil)

func (env *ExpressionEnv) lookupBindVar(key string) (*querypb.BindVariable, error) {
	val, ok := env.BindVars[key]
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query arguments missing for %s", key)
	}
	return val, nil
}

// eval implements the Expr interface
func (bv *BindVariable) eval(env *ExpressionEnv) (eval, error) {
	bvar, err := env.lookupBindVar(bv.Key)
	if err != nil {
		return nil, err
	}

	switch bvar.Type {
	case sqltypes.Tuple:
		if bv.Type != sqltypes.Tuple {
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
		if bv.Type == sqltypes.Tuple {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query argument '%s' must be a tuple (is %s)", bv.Key, bvar.Type)
		}
		typ := bvar.Type
		if bv.typed {
			typ = bv.Type
		}
		return valueToEval(sqltypes.MakeTrusted(typ, bvar.Value), bv.Collation)
	}
}

// typeof implements the Expr interface
func (bv *BindVariable) typeof(env *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	var tt sqltypes.Type
	if bv.typed {
		tt = bv.Type
	} else {
		if bvar, err := env.lookupBindVar(bv.Key); err == nil {
			tt = bvar.Type
		}
	}
	switch tt {
	case sqltypes.Null:
		return sqltypes.Null, flagNull | flagNullable
	case sqltypes.HexNum, sqltypes.HexVal:
		return sqltypes.VarBinary, flagHex
	default:
		return tt, 0
	}
}
