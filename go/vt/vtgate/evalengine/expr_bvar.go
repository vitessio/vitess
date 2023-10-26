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
			e, err := valueToEval(sqltypes.MakeTrusted(value.Type, value.Value), defaultCoercionCollation(collations.CollationForType(value.Type, bv.Collation.Collation)))
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
		if bv.typed() {
			typ = bv.Type
		}
		return valueToEval(sqltypes.MakeTrusted(typ, bvar.Value), defaultCoercionCollation(collations.CollationForType(typ, bv.Collation.Collation)))
	}
}

// typeof implements the Expr interface
func (bv *BindVariable) typeof(env *ExpressionEnv, _ []*querypb.Field) (sqltypes.Type, typeFlag) {
	var tt sqltypes.Type
	if bv.typed() {
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
	case sqltypes.BitNum:
		return sqltypes.VarBinary, flagBit
	default:
		return tt, 0
	}
}

func (bvar *BindVariable) compile(c *compiler) (ctype, error) {
	if !bvar.typed() {
		return ctype{}, c.unsupported(bvar)
	}

	typ := ctype{
		Type: bvar.Type,
		Flag: 0,
		Col:  bvar.Collation,
	}

	switch tt := typ.Type; {
	case sqltypes.IsSigned(tt):
		c.asm.PushBVar_i(bvar.Key)
	case sqltypes.IsUnsigned(tt):
		c.asm.PushBVar_u(bvar.Key)
	case sqltypes.IsFloat(tt):
		c.asm.PushBVar_f(bvar.Key)
	case sqltypes.IsDecimal(tt):
		c.asm.PushBVar_d(bvar.Key)
	case sqltypes.IsText(tt):
		if tt == sqltypes.HexNum {
			c.asm.PushBVar_hexnum(bvar.Key)
			typ.Type = sqltypes.VarBinary
			typ.Flag |= flagHex
		} else if tt == sqltypes.HexVal {
			c.asm.PushBVar_hexval(bvar.Key)
			typ.Type = sqltypes.VarBinary
			typ.Flag |= flagHex
		} else if tt == sqltypes.BitNum {
			c.asm.PushBVar_bitnum(bvar.Key)
			typ.Type = sqltypes.VarBinary
			typ.Flag |= flagBit
		} else {
			c.asm.PushBVar_text(bvar.Key, typ.Col)
		}
	case sqltypes.IsBinary(tt):
		c.asm.PushBVar_bin(bvar.Key)
	case sqltypes.IsNull(tt):
		c.asm.PushNull()
	case tt == sqltypes.TypeJSON:
		c.asm.PushBVar_json(bvar.Key)
	default:
		return ctype{}, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Type is not supported: %s", tt)
	}
	return typ, nil
}

func (bvar *BindVariable) typed() bool {
	return bvar.Type >= 0
}
