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
	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/mysql/fastparse"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
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

	h := vthash.New()
	switch e := e.(type) {
	case *evalBytes:
		if !collation.Valid() {
			return 0, UnsupportedCollationHashError
		}
		e.col.Collation = collation
		e.Hash(&h)
	case hashable:
		e.Hash(&h)
	default:
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cannot hash %s object", coerceType)
	}
	return h.Sum64(), nil
}

const (
	hashPrefixNil              = 0x0000
	hashPrefixFloat            = 0xAAAA
	hashPrefixIntegralNegative = 0xBBB1
	hashPrefixIntegralPositive = 0xBBB0
	hashPrefixBytes            = 0xCCCC
	hashPrefixDate             = 0xCCC0
	hashPrefixDecimal          = 0xDDDD
)

var ErrHashCoercionIsNotExact = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "cannot coerce into target type without losing precision")

// NullsafeHashcode128 returns a 128-bit hashcode that is guaranteed to be the same
// for two values that are considered equal by `NullsafeCompare`.
// This can be used to avoid having to do comparison checks after a hash,
// since we consider the 128 bits of entropy enough to guarantee uniqueness.
func NullsafeHashcode128(hash *vthash.Hasher, v sqltypes.Value, collation collations.ID, coerceTo sqltypes.Type) error {
	switch {
	case v.IsNull(), sqltypes.IsNull(coerceTo):
		hash.Write16(hashPrefixNil)
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
			f, _ = fastparse.ParseFloat64(v.RawStr())
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected type %v", v.Type())
		}
		if err != nil {
			return err
		}
		hash.Write16(hashPrefixFloat)
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
		case v.IsFloat():
			var fval float64
			fval, err = v.ToFloat64()
			if fval != math.Trunc(fval) {
				return ErrHashCoercionIsNotExact
			}
			i = int64(fval)
		case v.IsQuoted():
			i, err = fastparse.ParseInt64(v.RawStr(), 10)
			if err != nil {
				fval, _ := fastparse.ParseFloat64(v.RawStr())
				if fval != math.Trunc(fval) {
					return ErrHashCoercionIsNotExact
				}
				i, err = int64(fval), nil
			}
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected type %v", v.Type())
		}
		if err != nil {
			return err
		}
		if i < 0 {
			hash.Write16(hashPrefixIntegralNegative)
		} else {
			hash.Write16(hashPrefixIntegralPositive)
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
		case v.IsFloat():
			var fval float64
			fval, err = v.ToFloat64()
			if fval != math.Trunc(fval) || fval < 0 {
				return ErrHashCoercionIsNotExact
			}
			u = uint64(fval)
		case v.IsQuoted():
			u, err = fastparse.ParseUint64(v.RawStr(), 10)
			if err != nil {
				fval, _ := fastparse.ParseFloat64(v.RawStr())
				if fval != math.Trunc(fval) || fval < 0 {
					return ErrHashCoercionIsNotExact
				}
				u, err = uint64(fval), nil
			}
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected type %v", v.Type())
		}
		if err != nil {
			return err
		}
		hash.Write16(hashPrefixIntegralPositive)
		hash.Write64(u)

	case sqltypes.IsBinary(coerceTo):
		hash.Write16(hashPrefixBytes)
		collations.Binary.Hash(hash, v.Raw(), 0)

	case sqltypes.IsText(coerceTo):
		coll := collation.Get()
		if coll == nil {
			panic("cannot hash unsupported collation")
		}
		hash.Write16(hashPrefixBytes)
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
			fval, _ := fastparse.ParseFloat64(v.RawStr())
			dec = decimal.NewFromFloat(fval)
		default:
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value to a decimal: %v", v)
		}
		hash.Write16(hashPrefixDecimal)
		dec.Hash(hash)

	default:
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected type %v", v.Type())
	}
	return nil
}
