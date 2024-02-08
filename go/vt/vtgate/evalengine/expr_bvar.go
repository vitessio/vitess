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
		Collation collations.ID

		// dynamicTypeOffset is set when the type of this bind variable cannot be calculated
		// at translation time. Since expressions with dynamic types cannot be compiled ahead of time,
		// compilation will be delayed until the expression is first executed with the bind variables
		// sent by the user. See: UntypedExpr
		dynamicTypeOffset int
	}
)

var _ IR = (*BindVariable)(nil)
var _ Expr = (*BindVariable)(nil)

func (env *ExpressionEnv) lookupBindVar(key string) (*querypb.BindVariable, error) {
	val, ok := env.BindVars[key]
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query arguments missing for %s", key)
	}
	return val, nil
}

func (bv *BindVariable) IR() IR {
	return bv
}

func (bv *BindVariable) IsExpr() {}

// eval implements the expression interface
func (bv *BindVariable) eval(env *ExpressionEnv) (eval, error) {
	bvar, err := env.lookupBindVar(bv.Key)
	if err != nil {
		return nil, err
	}

	switch bvar.Type {
	case sqltypes.Tuple:
		if bv.Type != sqltypes.Tuple {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query argument '%s' must be a tuple (is %s)", bv.Key, bvar.Type)
		}

		tuple := make([]eval, 0, len(bvar.Values))
		for _, value := range bvar.Values {
			e, err := valueToEval(sqltypes.MakeTrusted(value.Type, value.Value), typedCoercionCollation(value.Type, collations.CollationForType(value.Type, bv.Collation)))
			if err != nil {
				return nil, err
			}
			tuple = append(tuple, e)
		}
		return &evalTuple{t: tuple}, nil

	default:
		if bv.Type == sqltypes.Tuple {
			return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "query argument '%s' cannot be a tuple", bv.Key)
		}
		typ := bvar.Type
		if bv.typed() {
			typ = bv.Type
		}
		return valueToEval(sqltypes.MakeTrusted(typ, bvar.Value), typedCoercionCollation(typ, collations.CollationForType(typ, bv.Collation)))
	}
}

// typeof implements the expression interface
func (bv *BindVariable) typeof(env *ExpressionEnv) (ctype, error) {
	var tt sqltypes.Type
	if bv.typed() {
		tt = bv.Type
	} else {
		bvar, err := env.lookupBindVar(bv.Key)
		if err != nil {
			return ctype{}, err
		}
		tt = bvar.Type
	}
	switch tt {
	case sqltypes.Null:
		return ctype{Type: sqltypes.Null, Flag: flagNull | flagNullable, Col: collationNull}, nil
	case sqltypes.HexNum, sqltypes.HexVal:
		return ctype{Type: sqltypes.VarBinary, Flag: flagHex | flagNullable, Col: collationNumeric}, nil
	case sqltypes.BitNum:
		return ctype{Type: sqltypes.VarBinary, Flag: flagBit | flagNullable, Col: collationNumeric}, nil
	default:
		return ctype{Type: tt, Flag: flagNullable, Col: typedCoercionCollation(tt, collations.CollationForType(tt, bv.Collation))}, nil
	}
}

func (bvar *BindVariable) compile(c *compiler) (ctype, error) {
	var typ ctype

	if bvar.typed() {
		typ.Type = bvar.Type
		typ.Col = typedCoercionCollation(bvar.Type, collations.CollationForType(bvar.Type, bvar.Collation))
	} else if c.dynamicTypes != nil {
		typ = c.dynamicTypes[bvar.dynamicTypeOffset]
	} else {
		return ctype{}, c.unsupported(bvar)
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
	case tt == sqltypes.Datetime || tt == sqltypes.Timestamp:
		c.asm.PushBVar_datetime(bvar.Key)
	case tt == sqltypes.Date:
		c.asm.PushBVar_date(bvar.Key)
	case tt == sqltypes.Time:
		c.asm.PushBVar_time(bvar.Key)
	default:
		return ctype{}, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "Type is not supported: %s", tt)
	}
	return typ, nil
}

func (bvar *BindVariable) typed() bool {
	return bvar.Type >= 0
}
