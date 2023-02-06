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
)

// HashCode is a type alias to the code easier to read
type HashCode = uintptr

// NullsafeHashCodeInPlace behaves like NullsafeHashCode but the type coercion is performed
// in-place for performance reasons. Eventually this method will replace the old implementation.
func NullsafeHashCodeInPlace(v sqltypes.Value, collation collations.ID, typ sqltypes.Type) (HashCode, error) {
	switch {
	case typ == sqltypes.Null:
		return HashCode(math.MaxUint64), nil

	case sqltypes.IsFloat(typ):
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
		case v.IsFloat() || v.Type() == sqltypes.Decimal:
			f, err = v.ToFloat64()
		case v.IsText() || v.IsBinary():
			f = parseStringToFloat(v.RawStr())
		default:
			return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value to a float: %v", v)
		}
		return HashCode(math.Float64bits(f)), err

	case sqltypes.IsSigned(typ):
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
			return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value to a signed int: %v", v)
		}
		return HashCode(uint64(i)), err

	case sqltypes.IsUnsigned(typ):
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
			return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "coercion should not try to coerce this value to a unsigned int: %v", v)
		}

		return HashCode(u), err

	case sqltypes.IsBinary(typ):
		coll := collations.Local().LookupByID(collations.CollationBinaryID)
		return coll.Hash(v.Raw(), 0), nil

	case sqltypes.IsText(typ):
		coll := collations.Local().LookupByID(collation)
		if coll == nil {
			return 0, UnsupportedCollationHashError
		}
		return coll.Hash(v.Raw(), 0), nil

	default:
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unsupported hash type: %v", v)
	}
}

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
	return e.hash()
}
