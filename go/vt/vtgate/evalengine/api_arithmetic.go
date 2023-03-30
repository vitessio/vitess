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
	"vitess.io/vitess/go/sqltypes"
)

// evalengine represents a numeric value extracted from
// a Value, used for arithmetic operations.
var zeroBytes = []byte("0")

// Add adds two values together
// if v1 or v2 is null, then it returns null
func Add(v1, v2 sqltypes.Value) (sqltypes.Value, error) {
	if v1.IsNull() || v2.IsNull() {
		return sqltypes.NULL, nil
	}
	e1, err := valueToEval(v1, collationNumeric)
	if err != nil {
		return sqltypes.NULL, err
	}
	e2, err := valueToEval(v2, collationNumeric)
	if err != nil {
		return sqltypes.NULL, err
	}
	r, err := addNumericWithError(e1, e2)
	if err != nil {
		return sqltypes.NULL, err
	}
	return evalToSQLValue(r), nil
}

// Subtract takes two values and subtracts them
func Subtract(v1, v2 sqltypes.Value) (sqltypes.Value, error) {
	if v1.IsNull() || v2.IsNull() {
		return sqltypes.NULL, nil
	}
	e1, err := valueToEval(v1, collationNumeric)
	if err != nil {
		return sqltypes.NULL, err
	}
	e2, err := valueToEval(v2, collationNumeric)
	if err != nil {
		return sqltypes.NULL, err
	}
	r, err := subtractNumericWithError(e1, e2)
	if err != nil {
		return sqltypes.NULL, err
	}
	return evalToSQLValue(r), nil
}

// Multiply takes two values and multiplies it together
func Multiply(v1, v2 sqltypes.Value) (sqltypes.Value, error) {
	if v1.IsNull() || v2.IsNull() {
		return sqltypes.NULL, nil
	}
	e1, err := valueToEval(v1, collationNumeric)
	if err != nil {
		return sqltypes.NULL, err
	}
	e2, err := valueToEval(v2, collationNumeric)
	if err != nil {
		return sqltypes.NULL, err
	}
	r, err := multiplyNumericWithError(e1, e2)
	if err != nil {
		return sqltypes.NULL, err
	}
	return evalToSQLValue(r), nil
}

// Divide (Float) for MySQL. Replicates behavior of "/" operator
func Divide(v1, v2 sqltypes.Value) (sqltypes.Value, error) {
	if v1.IsNull() || v2.IsNull() {
		return sqltypes.NULL, nil
	}
	e1, err := valueToEval(v1, collationNumeric)
	if err != nil {
		return sqltypes.NULL, err
	}
	e2, err := valueToEval(v2, collationNumeric)
	if err != nil {
		return sqltypes.NULL, err
	}
	r, err := divideNumericWithError(e1, e2, true)
	if err != nil {
		return sqltypes.NULL, err
	}
	return evalToSQLValue(r), nil
}

// NullSafeAdd adds two Values in a null-safe manner. A null value
// is treated as 0. If both values are null, then a null is returned.
// If both values are not null, a numeric value is built
// from each input: Signed->int64, Unsigned->uint64, Float->float64.
// Otherwise the 'best type fit' is chosen for the number: int64 or float64.
// opArithAdd is performed by upgrading types as needed, or in case
// of overflow: int64->uint64, int64->float64, uint64->float64.
// Unsigned ints can only be added to positive ints. After the
// addition, if one of the input types was Decimal, then
// a Decimal is built. Otherwise, the final type of the
// result is preserved.
func NullSafeAdd(v1, v2 sqltypes.Value, resultType sqltypes.Type) (sqltypes.Value, error) {
	if v1.IsNull() {
		v1 = sqltypes.MakeTrusted(resultType, zeroBytes)
	}
	if v2.IsNull() {
		v2 = sqltypes.MakeTrusted(resultType, zeroBytes)
	}

	e1, err := valueToEval(v1, collationNumeric)
	if err != nil {
		return sqltypes.NULL, err
	}
	e2, err := valueToEval(v2, collationNumeric)
	if err != nil {
		return sqltypes.NULL, err
	}
	r, err := addNumericWithError(e1, e2)
	if err != nil {
		return sqltypes.NULL, err
	}
	return evalToSQLValueWithType(r, resultType), nil
}
