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
	"math"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine/internal/decimal"
	"vitess.io/vitess/go/vt/vthash"
)

// HashCode is a type alias to the code easier to read
type HashCode = uint64

// NullsafeHashcode returns an int64 hashcode that is guaranteed to be the same
// for two values that are considered equal by `NullsafeCompare`.
func NullsafeHashcode(v sqltypes.Value, collation collations.ID, coerceType sqltypes.Type) (HashCode, error) {
	e, err := valueToEvalCast(v, coerceType)
	if err != nil {
		return 0, err
	}
	if e == nil {
		return HashCode(math.MaxUint64), nil
	}
	if e, ok := e.(*evalBytes); ok {
		e.col.Collation = collation
	}
	return e.Hash()
}

// NullsafeHashcode128 returns a 128-bit hashcode that is guaranteed to be the same
// for two values that are considered equal by `NullsafeCompare`.
// This can be used to avoid having to do comparison checks after a hash,
// since we consider the 128 bits of entropy enough to guarantee uniqueness.
func NullsafeHashcode128(hash *vthash.Hasher, v sqltypes.Value, collation collations.ID, coerceTo sqltypes.Type) error {
	switch {
	case v.IsNull(), sqltypes.IsNull(coerceTo):
		hash.Write16(0)
	case sqltypes.IsFloat(coerceTo):
		var f float64
		var err error

		switch {
		case v.IsSigned():
			var ival int64
			ival, err = v.ToInt64()
			f = float64(ival)
		case v.IsUnsigned():
			var uval uint64
			uval, err = v.ToUint64()
			f = float64(uval)
		case v.IsFloat() || v.IsDecimal():
			f, err = v.ToFloat64()
		case v.IsQuoted():
			f = parseStringToFloat(v.RawStr())
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected type %v", v.Type())
		}
		if err != nil {
			return err
		}
		hash.Write16(0xAAAA)
		hash.Write64(math.Float64bits(f))

	case sqltypes.IsSigned(coerceTo):
		var i int64
		var err error

		switch {
		case v.IsSigned():
			i, err = v.ToInt64()
		case v.IsUnsigned():
			var uval uint64
			uval, err = v.ToUint64()
			i = int64(uval)
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected type %v", v.Type())
		}
		if err != nil {
			return err
		}
		if i < 0 {
			hash.Write16(0xBBB1)
		} else {
			hash.Write16(0xBBB0)
		}
		hash.Write64(uint64(i))

	case sqltypes.IsUnsigned(coerceTo):
		var u uint64
		var err error

		switch {
		case v.IsSigned():
			var ival int64
			ival, err = v.ToInt64()
			u = uint64(ival)
		case v.IsUnsigned():
			u, err = v.ToUint64()
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected type %v", v.Type())
		}
		if err != nil {
			return err
		}
		hash.Write16(0xBBB0)
		hash.Write64(u)

	case sqltypes.IsBinary(coerceTo):
		coll := collations.Local().LookupByID(collations.CollationBinaryID)
		hash.Write16(0xCCCC)
		coll.Hash(hash, v.Raw(), 0)

	case sqltypes.IsText(coerceTo):
		coll := collations.Local().LookupByID(collation)
		if coll == nil {
			panic("cannot hash unsupported collation")
		}
		hash.Write16(0xCCCC)
		coll.Hash(hash, v.Raw(), 0)

	case sqltypes.IsDecimal(coerceTo):
		var dec decimal.Decimal
		switch {
		case v.IsIntegral() || v.IsDecimal():
			var err error
			dec, err = decimal.NewFromMySQL(v.Raw())
			if err != nil {
				return err
			}
		case v.IsFloat():
			fval, err := v.ToFloat64()
			if err != nil {
				return err
			}
			dec = decimal.NewFromFloat(fval)
		case v.IsText() || v.IsBinary():
			fval := parseStringToFloat(v.RawStr())
			dec = decimal.NewFromFloat(fval)
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value to a decimal: %v", v)
		}
		hash.Write16(0xDDDD)
		dec.Hash(hash)

	default:
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected type %v", v.Type())
	}
	return nil
}
