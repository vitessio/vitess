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
	"encoding/binary"
	"math"

	"vitess.io/vitess/go/hack"
	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/mysql/json"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// WeightString returns the weight string for a value.
// It appends to dst if an existing slice is given, otherwise it
// returns a new one.
// The returned boolean indicates whether the weight string is a
// fixed-width weight string, such as for fixed size integer values.
// Our WeightString implementation supports more types that MySQL
// externally communicates with the `WEIGHT_STRING` function, so that we
// can also use this to order / sort other types like Float and Decimal
// as well.
func WeightString(dst []byte, v sqltypes.Value, coerceTo sqltypes.Type, col collations.ID, length, precision int, sqlmode SQLMode) ([]byte, bool, error) {
	// We optimize here for the case where we already have the desired type.
	// Otherwise, we fall back to the general evalengine conversion logic.
	if v.Type() != coerceTo {
		return fallbackWeightString(dst, v, coerceTo, col, length, precision, sqlmode)
	}

	switch {
	case sqltypes.IsNull(coerceTo):
		return nil, true, nil

	case sqltypes.IsSigned(coerceTo):
		i, err := v.ToInt64()
		if err != nil {
			return dst, false, err
		}
		raw := uint64(i)
		raw = raw ^ (1 << 63)
		return binary.BigEndian.AppendUint64(dst, raw), true, nil

	case sqltypes.IsUnsigned(coerceTo):
		u, err := v.ToUint64()
		if err != nil {
			return dst, false, err
		}
		return binary.BigEndian.AppendUint64(dst, u), true, nil

	case sqltypes.IsFloat(coerceTo):
		f, err := v.ToFloat64()
		if err != nil {
			return dst, false, err
		}

		raw := math.Float64bits(f)
		if math.Signbit(f) {
			raw = ^raw
		} else {
			raw = raw ^ (1 << 63)
		}
		return binary.BigEndian.AppendUint64(dst, raw), true, nil

	case sqltypes.IsBinary(coerceTo):
		b := v.Raw()
		if length != 0 {
			if length > cap(b) {
				b = append(b, make([]byte, length-len(b))...)
			} else {
				b = b[:length]
			}
		}
		return append(dst, b...), false, nil

	case sqltypes.IsText(coerceTo):
		coll := colldata.Lookup(col)
		if coll == nil {
			return dst, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "cannot hash unsupported collation")
		}
		b := v.Raw()
		if length != 0 {
			b = charset.Slice(coll.Charset(), b, 0, length)
		}
		return coll.WeightString(dst, b, length), false, nil

	case sqltypes.IsDecimal(coerceTo):
		dec, err := decimal.NewFromMySQL(v.Raw())
		if err != nil {
			return dst, false, err
		}
		return dec.WeightString(dst, int32(length), int32(precision)), true, nil
	case coerceTo == sqltypes.TypeJSON:
		j, err := json.NewFromSQL(v)
		if err != nil {
			return dst, false, err
		}
		return j.WeightString(dst), false, nil
	default:
		return fallbackWeightString(dst, v, coerceTo, col, length, precision, sqlmode)
	}
}

func fallbackWeightString(dst []byte, v sqltypes.Value, coerceTo sqltypes.Type, col collations.ID, length, precision int, sqlmode SQLMode) ([]byte, bool, error) {
	e, err := valueToEvalCast(v, coerceTo, col, sqlmode)
	if err != nil {
		return dst, false, err
	}
	return evalWeightString(dst, e, length, precision)
}

func evalWeightString(dst []byte, e eval, length, precision int) ([]byte, bool, error) {
	switch e := e.(type) {
	case nil:
		return nil, true, nil
	case *evalInt64:
		raw := uint64(e.i)
		raw = raw ^ (1 << 63)
		return binary.BigEndian.AppendUint64(dst, raw), true, nil
	case *evalUint64:
		return binary.BigEndian.AppendUint64(dst, e.u), true, nil
	case *evalFloat:
		raw := math.Float64bits(e.f)
		if math.Signbit(e.f) {
			raw = ^raw
		} else {
			raw = raw ^ (1 << 63)
		}
		return binary.BigEndian.AppendUint64(dst, raw), true, nil
	case *evalDecimal:
		return e.dec.WeightString(dst, int32(length), int32(precision)), true, nil
	case *evalBytes:
		if e.isBinary() {
			b := e.bytes
			if length != 0 {
				if length > cap(b) {
					b = append(b, make([]byte, length-len(b))...)
				} else {
					b = b[:length]
				}
			}
			return append(dst, b...), false, nil
		}
		coll := colldata.Lookup(e.col.Collation)
		if coll == nil {
			return dst, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "cannot hash unsupported collation")
		}
		b := e.bytes
		if length != 0 {
			b = charset.Slice(coll.Charset(), b, 0, length)
		}
		return coll.WeightString(dst, b, length), false, nil
	case *evalTemporal:
		return e.dt.WeightString(dst), true, nil
	case *evalJSON:
		return e.WeightString(dst), false, nil
	}

	return dst, false, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected type %v", e.SQLType())
}

// TinyWeighter returns a callback to apply a Tiny Weight string to a sqltypes.Value.
// A tiny weight string is a compressed 4-byte representation of the value's full weight string that
// sorts identically to its full weight. Obviously, the tiny weight string can collide because
// it's represented in fewer bytes than the full one.
// Hence, for any 2 instances of sqltypes.Value: if both instances have a Tiny Weight string,
// and the weight strings are **different**, the two values will sort accordingly to the 32-bit
// numerical sort of their tiny weight strings. Otherwise, the relative sorting of the two values
// will not be known, and they will require a full sort using e.g. NullsafeCompare.
func TinyWeighter(f *querypb.Field, collation collations.ID) func(v *sqltypes.Value) {
	switch {
	case sqltypes.IsNull(f.Type):
		return nil

	case sqltypes.IsSigned(f.Type):
		return func(v *sqltypes.Value) {
			i, err := v.ToInt64()
			if err != nil {
				return
			}
			// The full weight string for an integer is just its MSB bit-inverted 64 bit representation.
			// However, we only have 4 bytes to work with here, so in order to minimize the amount
			// of collisions for the tiny weight string, instead of grabbing the top 32 bits of the
			// 64 bit representation, we're going to cast to float32. Floats are sortable once bit-inverted,
			// and although they cannot represent the full 64-bit range (duh!), that's perfectly fine
			// because close-by numbers will collide into the same tiny weight, allowing us to fall back
			// to a full comparison.
			raw := math.Float32bits(float32(i))
			if i < 0 {
				raw = ^raw
			} else {
				raw = raw ^ (1 << 31)
			}
			v.SetTinyWeight(raw)
		}

	case sqltypes.IsUnsigned(f.Type):
		return func(v *sqltypes.Value) {
			u, err := v.ToUint64()
			if err != nil {
				return
			}
			// See comment for the IsSigned block. No bit-inversion is required here as all floats will be positive.
			v.SetTinyWeight(math.Float32bits(float32(u)))
		}

	case sqltypes.IsFloat(f.Type):
		return func(v *sqltypes.Value) {
			fl, err := v.ToFloat64()
			if err != nil {
				return
			}
			// Similarly as the IsSigned block, we could take the top 32 bits of the float64 bit representation,
			// but by down-sampling to a float32 we reduce the amount of collisions.
			raw := math.Float32bits(float32(fl))
			if math.Signbit(fl) {
				raw = ^raw
			} else {
				raw = raw ^ (1 << 31)
			}
			v.SetTinyWeight(raw)
		}

	case sqltypes.IsBinary(f.Type):
		return func(v *sqltypes.Value) {
			if v.IsNull() {
				return
			}

			var w32 [4]byte
			copy(w32[:4], v.Raw())
			v.SetTinyWeight(binary.BigEndian.Uint32(w32[:4]))
		}

	case sqltypes.IsText(f.Type):
		if coll := colldata.Lookup(collation); coll != nil {
			if twcoll, ok := coll.(colldata.TinyWeightCollation); ok {
				return func(v *sqltypes.Value) {
					if v.IsNull() {
						return
					}
					v.SetTinyWeight(twcoll.TinyWeightString(v.Raw()))
				}
			}
		}
		return nil

	case sqltypes.IsDecimal(f.Type):
		return func(v *sqltypes.Value) {
			if v.IsNull() {
				return
			}
			// To generate a 32-bit weight string of the decimal, we'll just attempt a fast 32bit atof parse
			// of its contents. This can definitely fail for many corner cases, but that's OK: we'll just fall
			// back to a full decimal comparison in those cases.
			fl, _, err := hack.Atof32(v.RawStr())
			if err != nil {
				return
			}
			raw := math.Float32bits(fl)
			if raw&(1<<31) != 0 {
				raw = ^raw
			} else {
				raw = raw ^ (1 << 31)
			}
			v.SetTinyWeight(raw)
		}

	case f.Type == sqltypes.TypeJSON:
		return func(v *sqltypes.Value) {
			if v.IsNull() {
				return
			}
			j, err := json.NewFromSQL(*v)
			if err != nil {
				return
			}
			var w32 [4]byte
			// TODO: this can be done more efficiently without having to calculate the full weight string and
			// extracting its prefix.
			copy(w32[:4], j.WeightString(nil))
			v.SetTinyWeight(binary.BigEndian.Uint32(w32[:4]))
		}

	default:
		return nil
	}
}
