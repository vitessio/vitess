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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/charset"
	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/mysql/json"
	"vitess.io/vitess/go/sqltypes"
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
func WeightString(dst []byte, v sqltypes.Value, coerceTo sqltypes.Type, col collations.ID, length, precision int) ([]byte, bool, error) {
	// We optimize here for the case where we already have the desired type.
	// Otherwise, we fall back to the general evalengine conversion logic.
	if v.Type() != coerceTo {
		return fallbackWeightString(dst, v, coerceTo, col, length, precision)
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
		coll := col.Get()
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
		return fallbackWeightString(dst, v, coerceTo, col, length, precision)
	}
}

func fallbackWeightString(dst []byte, v sqltypes.Value, coerceTo sqltypes.Type, col collations.ID, length, precision int) ([]byte, bool, error) {
	e, err := valueToEvalCast(v, coerceTo, col)
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
		coll := e.col.Collation.Get()
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
