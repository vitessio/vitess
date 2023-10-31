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
	"bytes"
	"fmt"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/collations/colldata"
	"vitess.io/vitess/go/sqltypes"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// UnsupportedComparisonError represents the error where the comparison between the two types is unsupported on vitess
type UnsupportedComparisonError struct {
	Type1 sqltypes.Type
	Type2 sqltypes.Type
}

// Error function implements the error interface
func (err UnsupportedComparisonError) Error() string {
	return fmt.Sprintf("types are not comparable: %v vs %v", err.Type1, err.Type2)
}

// UnsupportedCollationError represents the error where the comparison using provided collation is unsupported on vitess
type UnsupportedCollationError struct {
	ID collations.ID
}

// Error function implements the error interface
func (err UnsupportedCollationError) Error() string {
	return fmt.Sprintf("cannot compare strings, collation is unknown or unsupported (collation ID: %d)", err.ID)
}

// UnsupportedCollationHashError is returned when we try to get the hash value and are missing the collation to use
var UnsupportedCollationHashError = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "text type with an unknown/unsupported collation cannot be hashed")

func compare(v1, v2 sqltypes.Value, collationID collations.ID) (int, error) {
	v1t := v1.Type()

	// We have a fast path here for the case where both values are
	// the same type, and it's one of the basic types we can compare
	// directly. This is a common case for equality checks.
	if v1t == v2.Type() {
		switch {
		case sqltypes.IsText(v1t):
			if collationID == collations.CollationBinaryID {
				return bytes.Compare(v1.Raw(), v2.Raw()), nil
			}
			coll := colldata.Lookup(collationID)
			if coll == nil {
				return 0, UnsupportedCollationError{ID: collationID}
			}
			result := coll.Collate(v1.Raw(), v2.Raw(), false)
			switch {
			case result < 0:
				return -1, nil
			case result > 0:
				return 1, nil
			default:
				return 0, nil
			}
		case sqltypes.IsBinary(v1t), v1t == sqltypes.Date, v1t == sqltypes.Datetime, v1t == sqltypes.Timestamp:
			// We can't optimize for Time here, since Time is not sortable
			// based on the raw bytes. This is because of cases like
			// '24:00:00' and '101:00:00' which are both valid times and
			// order wrong based on the raw bytes.
			return bytes.Compare(v1.Raw(), v2.Raw()), nil
		case sqltypes.IsSigned(v1t):
			i1, err := v1.ToInt64()
			if err != nil {
				return 0, err
			}
			i2, err := v2.ToInt64()
			if err != nil {
				return 0, err
			}
			switch {
			case i1 < i2:
				return -1, nil
			case i1 > i2:
				return 1, nil
			default:
				return 0, nil
			}
		case sqltypes.IsUnsigned(v1t):
			u1, err := v1.ToUint64()
			if err != nil {
				return 0, err
			}
			u2, err := v2.ToUint64()
			if err != nil {
				return 0, err
			}
			switch {
			case u1 < u2:
				return -1, nil
			case u1 > u2:
				return 1, nil
			default:
				return 0, nil
			}
		}
	}

	v1eval, err := valueToEval(v1, collations.TypedCollation{
		Collation:    collationID,
		Coercibility: collations.CoerceImplicit,
		Repertoire:   collations.RepertoireUnicode,
	})
	if err != nil {
		return 0, err
	}

	v2eval, err := valueToEval(v2, collations.TypedCollation{
		Collation:    collationID,
		Coercibility: collations.CoerceImplicit,
		Repertoire:   collations.RepertoireUnicode,
	})
	if err != nil {
		return 0, err
	}

	out, err := evalCompare(v1eval, v2eval)
	if err != nil {
		return 0, err
	}
	if out == 0 {
		return 0, nil
	}
	if out > 0 {
		return 1, nil
	}
	return -1, nil
}

// NullsafeCompare returns 0 if v1==v2, -1 if v1<v2, and 1 if v1>v2.
// NULL is the lowest value. If any value is
// numeric, then a numeric comparison is performed after
// necessary conversions. If none are numeric, then it's
// a simple binary comparison. Uncomparable values return an error.
func NullsafeCompare(v1, v2 sqltypes.Value, collationID collations.ID) (int, error) {
	// Based on the categorization defined for the types,
	// we're going to allow comparison of the following:
	// Null, isNumber, IsBinary. This will exclude IsQuoted
	// types that are not Binary, and Expression.
	if v1.IsNull() {
		if v2.IsNull() {
			return 0, nil
		}
		return -1, nil
	}
	if v2.IsNull() {
		return 1, nil
	}
	return compare(v1, v2, collationID)
}
