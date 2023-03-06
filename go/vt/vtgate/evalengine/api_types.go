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
	"fmt"
	"strconv"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// CoerceTo takes two input types, and decides how they should be coerced before compared
func CoerceTo(v1, v2 sqltypes.Type) (sqltypes.Type, error) {
	if v1 == v2 {
		return v1, nil
	}
	if sqltypes.IsNull(v1) || sqltypes.IsNull(v2) {
		return sqltypes.Null, nil
	}
	if (sqltypes.IsText(v1) || sqltypes.IsBinary(v1)) && (sqltypes.IsText(v2) || sqltypes.IsBinary(v2)) {
		return sqltypes.VarChar, nil
	}
	if sqltypes.IsNumber(v1) || sqltypes.IsNumber(v2) {
		switch {
		case sqltypes.IsText(v1) || sqltypes.IsBinary(v1) || sqltypes.IsText(v2) || sqltypes.IsBinary(v2):
			return sqltypes.Float64, nil
		case sqltypes.IsFloat(v2) || v2 == sqltypes.Decimal || sqltypes.IsFloat(v1) || v1 == sqltypes.Decimal:
			return sqltypes.Float64, nil
		case sqltypes.IsSigned(v1):
			switch {
			case sqltypes.IsUnsigned(v2):
				return sqltypes.Uint64, nil
			case sqltypes.IsSigned(v2):
				return sqltypes.Int64, nil
			default:
				return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "types does not support hashcode yet: %v vs %v", v1, v2)
			}
		case sqltypes.IsUnsigned(v1):
			switch {
			case sqltypes.IsSigned(v2) || sqltypes.IsUnsigned(v2):
				return sqltypes.Uint64, nil
			default:
				return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "types does not support hashcode yet: %v vs %v", v1, v2)
			}
		}
	}
	return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "types does not support hashcode yet: %v vs %v", v1, v2)
}

// Cast converts a Value to the target type.
func Cast(v sqltypes.Value, typ sqltypes.Type) (sqltypes.Value, error) {
	if v.Type() == typ || v.IsNull() {
		return v, nil
	}
	vBytes, err := v.ToBytes()
	if err != nil {
		return v, err
	}
	if sqltypes.IsSigned(typ) && v.IsSigned() {
		return sqltypes.MakeTrusted(typ, vBytes), nil
	}
	if sqltypes.IsUnsigned(typ) && v.IsUnsigned() {
		return sqltypes.MakeTrusted(typ, vBytes), nil
	}
	if (sqltypes.IsFloat(typ) || typ == sqltypes.Decimal) && (v.IsIntegral() || v.IsFloat() || v.Type() == sqltypes.Decimal) {
		return sqltypes.MakeTrusted(typ, vBytes), nil
	}
	if sqltypes.IsQuoted(typ) && (v.IsIntegral() || v.IsFloat() || v.Type() == sqltypes.Decimal || v.IsQuoted()) {
		return sqltypes.MakeTrusted(typ, vBytes), nil
	}

	// Explicitly disallow Expression.
	if v.Type() == sqltypes.Expression {
		return sqltypes.NULL, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v cannot be cast to %v", v, typ)
	}

	// If the above fast-paths were not possible,
	// go through full validation.
	return sqltypes.NewValue(typ, vBytes)
}

// ToUint64 converts Value to uint64.
func ToUint64(v sqltypes.Value) (uint64, error) {
	num, err := valueToEvalNumeric(v)
	if err != nil {
		return 0, err
	}
	switch num := num.(type) {
	case *evalInt64:
		if num.i < 0 {
			return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "negative number cannot be converted to unsigned: %d", num.i)
		}
		return uint64(num.i), nil
	case *evalUint64:
		return num.u, nil
	default:
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected return from numeric evaluation (%T)", num)
	}
}

// ToInt64 converts Value to int64.
func ToInt64(v sqltypes.Value) (int64, error) {
	num, err := valueToEvalNumeric(v)
	if err != nil {
		return 0, err
	}
	switch num := num.(type) {
	case *evalInt64:
		return num.i, nil
	case *evalUint64:
		ival := int64(num.u)
		if ival < 0 {
			return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "unsigned number overflows int64 value: %d", num.u)
		}
		return ival, nil
	default:
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected return from numeric evaluation (%T)", num)
	}
}

// ToFloat64 converts Value to float64.
func ToFloat64(v sqltypes.Value) (float64, error) {
	num, err := valueToEval(v, collationNumeric)
	if err != nil {
		return 0, err
	}
	f, _ := evalToNumeric(num).toFloat()
	return f.f, nil
}

func LiteralToValue(literal *sqlparser.Literal) (sqltypes.Value, error) {
	lit, err := (&astCompiler{}).translateLiteral(literal)
	if err != nil {
		return sqltypes.Value{}, err
	}
	return evalToSQLValue(lit.inner), nil
}

// ToNative converts Value to a native go type.
// Decimal is returned as []byte.
func ToNative(v sqltypes.Value) (any, error) {
	var out any
	var err error
	switch {
	case v.Type() == sqltypes.Null:
		// no-op
	case v.IsSigned():
		return ToInt64(v)
	case v.IsUnsigned():
		return ToUint64(v)
	case v.IsFloat():
		return ToFloat64(v)
	case v.IsQuoted() || v.Type() == sqltypes.Bit || v.Type() == sqltypes.Decimal:
		out, err = v.ToBytes()
	case v.Type() == sqltypes.Expression:
		err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "%v cannot be converted to a go type", v)
	}
	return out, err
}

func NormalizeValue(v sqltypes.Value, coll collations.ID) string {
	typ := v.Type()
	if typ == sqltypes.Null {
		return "NULL"
	}
	if typ == sqltypes.VarChar && coll == collations.CollationBinaryID {
		return fmt.Sprintf("VARBINARY(%q)", v.Raw())
	}
	if v.IsQuoted() || typ == sqltypes.Bit {
		return fmt.Sprintf("%v(%q)", typ, v.Raw())
	}
	if typ == sqltypes.Float32 || typ == sqltypes.Float64 {
		var bitsize = 64
		if typ == sqltypes.Float32 {
			bitsize = 32
		}
		f, err := strconv.ParseFloat(v.RawStr(), bitsize)
		if err != nil {
			panic(err)
		}
		return fmt.Sprintf("%v(%s)", typ, FormatFloat(typ, f))
	}
	return fmt.Sprintf("%v(%s)", typ, v.Raw())
}
