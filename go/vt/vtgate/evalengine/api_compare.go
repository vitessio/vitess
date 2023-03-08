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

// Min returns the minimum of v1 and v2. If one of the
// values is NULL, it returns the other value. If both
// are NULL, it returns NULL.
func Min(v1, v2 sqltypes.Value, collation collations.ID) (sqltypes.Value, error) {
	return minmax(v1, v2, true, collation)
}

// Max returns the maximum of v1 and v2. If one of the
// values is NULL, it returns the other value. If both
// are NULL, it returns NULL.
func Max(v1, v2 sqltypes.Value, collation collations.ID) (sqltypes.Value, error) {
	return minmax(v1, v2, false, collation)
}

func minmax(v1, v2 sqltypes.Value, min bool, collation collations.ID) (sqltypes.Value, error) {
	if v1.IsNull() {
		return v2, nil
	}
	if v2.IsNull() {
		return v1, nil
	}

	n, err := NullsafeCompare(v1, v2, collation)
	if err != nil {
		return sqltypes.NULL, err
	}

	// XNOR construct. See tests.
	v1isSmaller := n < 0
	if min == v1isSmaller {
		return v1, nil
	}
	return v2, nil
}

// isByteComparable returns true if the type is binary or date/time.
func isByteComparable(typ sqltypes.Type, collationID collations.ID) bool {
	if sqltypes.IsBinary(typ) {
		return true
	}
	if sqltypes.IsText(typ) {
		return collationID == collations.CollationBinaryID
	}
	switch typ {
	case sqltypes.Timestamp, sqltypes.Date, sqltypes.Time, sqltypes.Datetime, sqltypes.Enum, sqltypes.Set, sqltypes.TypeJSON, sqltypes.Bit:
		return true
	default:
		return false
	}
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

	if isByteComparable(v1.Type(), collationID) && isByteComparable(v2.Type(), collationID) {
		return bytes.Compare(v1.Raw(), v2.Raw()), nil
	}

	typ, err := CoerceTo(v1.Type(), v2.Type()) // TODO systay we should add a method where this decision is done at plantime
	if err != nil {
		return 0, err
	}

	switch {
	case sqltypes.IsText(typ):
		collation := collationID.Get()
		if collation == nil {
			return 0, UnsupportedCollationError{ID: collationID}
		}

		v1Bytes, err := v1.ToBytes()
		if err != nil {
			return 0, err
		}
		v2Bytes, err := v2.ToBytes()
		if err != nil {
			return 0, err
		}

		switch result := collation.Collate(v1Bytes, v2Bytes, false); {
		case result < 0:
			return -1, nil
		case result > 0:
			return 1, nil
		default:
			return 0, nil
		}

	case sqltypes.IsNumber(typ):
		v1cast, err := valueToEvalCast(v1, typ)
		if err != nil {
			return 0, err
		}
		v2cast, err := valueToEvalCast(v2, typ)
		if err != nil {
			return 0, err
		}
		return compareNumeric(v1cast, v2cast)

	default:
		return 0, UnsupportedComparisonError{Type1: v1.Type(), Type2: v2.Type()}
	}
}
