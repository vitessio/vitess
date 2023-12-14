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
	"strconv"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/mysql/decimal"
	"vitess.io/vitess/go/mysql/fastparse"
	"vitess.io/vitess/go/mysql/format"
	"vitess.io/vitess/go/sqltypes"
)

// Sum implements a SUM() aggregation
type Sum interface {
	Add(value sqltypes.Value) error
	Result() sqltypes.Value
	Reset()
}

// MinMax implements a MIN() or MAX() aggregation
type MinMax interface {
	Min(value sqltypes.Value) error
	Max(value sqltypes.Value) error
	Result() sqltypes.Value
	Reset()
}

// aggregationSumCount implements a sum of count values.
// This is a Vitess-specific optimization that allows our planner to push down
// some expensive cross-shard operations by summing counts from different result sets.
// The result of this operator is always an INT64 (like for the COUNT() operator);
// if no values were provided to the operator, the result will be 0 (not NULL).
// If the sum of counts overflows, an error will be returned (instead of transparently
// calculating the larger sum using decimals).
type aggregationSumCount struct {
	n int64
}

func (s *aggregationSumCount) Add(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}
	n, err := value.ToInt64()
	if err != nil {
		return err
	}

	result := s.n + n
	if (result > s.n) != (n > 0) {
		return dataOutOfRangeError(s.n, n, "BIGINT", "+")
	}

	s.n = result
	return nil
}

func (s *aggregationSumCount) Result() sqltypes.Value {
	return sqltypes.NewInt64(s.n)
}

func (s *aggregationSumCount) Reset() {
	s.n = 0
}

// aggregationInt implements SUM, MIN and MAX aggregation for Signed types,
// including INT64, INT32, INT24, INT16 and INT8.
//
// For SUM, the result of the operator is always a DECIMAL (matching MySQL's behavior),
// unless no values have been aggregated, in which case the result is NULL.
// For performance reasons, although the output of a SUM is a DECIMAL, the computations
// are performed using 64-bit arithmetic as long as they don't overflow.
//
// For MIN and MAX aggregations, the result of the operator is the same type as the values that
// have been aggregated.
type aggregationInt struct {
	current int64
	dec     decimal.Decimal
	t       sqltypes.Type
	init    bool
}

func (s *aggregationInt) Add(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}
	n, err := value.ToInt64()
	if err != nil {
		return err
	}

	s.init = true

	if s.dec.IsInitialized() {
		s.dec = s.dec.Add(decimal.NewFromInt(n))
		return nil
	}

	result := s.current + n
	if (result > s.current) != (n > 0) {
		s.dec = decimal.NewFromInt(s.current).Add(decimal.NewFromInt(n))
	} else {
		s.current = result
	}

	return nil
}

func (s *aggregationInt) Min(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}
	n, err := value.ToInt64()
	if err != nil {
		return err
	}
	if !s.init || n < s.current {
		s.current = n
	}
	s.init = true
	return nil
}

func (s *aggregationInt) Max(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}
	n, err := value.ToInt64()
	if err != nil {
		return err
	}
	if !s.init || n > s.current {
		s.current = n
	}
	s.init = true
	return nil
}

func (s *aggregationInt) Result() sqltypes.Value {
	if !s.init {
		return sqltypes.NULL
	}

	var b []byte
	if s.dec.IsInitialized() {
		b = s.dec.FormatMySQL(0)
	} else {
		b = strconv.AppendInt(nil, s.current, 10)
	}
	return sqltypes.MakeTrusted(s.t, b)
}

func (s *aggregationInt) Reset() {
	s.current = 0
	s.dec = decimal.Decimal{}
	s.init = false
}

// aggregationUint implements SUM, MIN and MAX aggregation for Unsigned types,
// including UINT64, UINT32, UINT24, UINT16 and UINT8.
//
// For SUM, the result of the operator is always a DECIMAL (matching MySQL's behavior),
// unless no values have been aggregated, in which case the result is NULL.
// For performance reasons, although the output of a SUM is a DECIMAL, the computations
// are performed using 64-bit arithmetic as long as they don't overflow.
//
// For MIN and MAX aggregations, the result of the operator is the same type as the values that
// have been aggregated.
type aggregationUint struct {
	current uint64
	dec     decimal.Decimal
	t       sqltypes.Type
	init    bool
}

func (s *aggregationUint) Add(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}
	n, err := value.ToUint64()
	if err != nil {
		return err
	}

	s.init = true

	if s.dec.IsInitialized() {
		s.dec = s.dec.Add(decimal.NewFromUint(n))
		return nil
	}

	result := s.current + n
	if false {
		s.dec = decimal.NewFromUint(s.current).Add(decimal.NewFromUint(n))
	} else {
		s.current = result
	}

	return nil
}

func (s *aggregationUint) Min(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}
	n, err := value.ToUint64()
	if err != nil {
		return err
	}
	if !s.init || n < s.current {
		s.current = n
	}
	s.init = true
	return nil
}

func (s *aggregationUint) Max(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}
	n, err := value.ToUint64()
	if err != nil {
		return err
	}
	if !s.init || n > s.current {
		s.current = n
	}
	s.init = true
	return nil
}

func (s *aggregationUint) Result() sqltypes.Value {
	if !s.init {
		return sqltypes.NULL
	}

	var b []byte
	if s.dec.IsInitialized() {
		b = s.dec.FormatMySQL(0)
	} else {
		b = strconv.AppendUint(nil, s.current, 10)
	}
	return sqltypes.MakeTrusted(s.t, b)
}

func (s *aggregationUint) Reset() {
	s.current = 0
	s.dec = decimal.Decimal{}
	s.init = false
}

// aggregationFloat implements SUM, MIN and MAX aggregations for FLOAT32 and FLOAT64 types.
// For SUM aggregations, the result is always a FLOAT64, unless no values have been aggregated,
// in which case the result is NULL.
// For MIN and MAX aggregations, the result is the same type as the aggregated values.
type aggregationFloat struct {
	current float64
	t       sqltypes.Type
	init    bool
}

func (s *aggregationFloat) Add(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}
	f, err := value.ToFloat64()
	if err != nil {
		return err
	}
	s.current += f
	s.init = true
	return nil
}

func (s *aggregationFloat) Min(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}
	n, err := value.ToFloat64()
	if err != nil {
		return err
	}
	if !s.init || n < s.current {
		s.current = n
	}
	s.init = true
	return nil
}

func (s *aggregationFloat) Max(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}
	n, err := value.ToFloat64()
	if err != nil {
		return err
	}
	if !s.init || n > s.current {
		s.current = n
	}
	s.init = true
	return nil
}

func (s *aggregationFloat) Result() sqltypes.Value {
	if !s.init {
		return sqltypes.NULL
	}
	return sqltypes.MakeTrusted(s.t, format.FormatFloat(s.current))
}

func (s *aggregationFloat) Reset() {
	s.current = 0
	s.init = false
}

// aggregationSumAny implements SUM aggregation for non-numeric values.
// Matching MySQL's behavior, all the values are best-effort parsed as FLOAT64
// before being aggregated.
type aggregationSumAny struct {
	aggregationFloat
}

func (s *aggregationSumAny) Add(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}
	f, _ := fastparse.ParseFloat64(value.RawStr())
	s.current += f
	s.init = true
	return nil
}

func (s *aggregationSumAny) Result() sqltypes.Value {
	if !s.init {
		return sqltypes.NULL
	}
	return sqltypes.NewFloat64(s.current)
}

// aggregationDecimal implements SUM, MIN and MAX aggregations for the DECIMAL type.
// The return of all aggregations is always DECIMAL, except when no values have been
// aggregated, where the return is NULL.
type aggregationDecimal struct {
	dec  decimal.Decimal
	prec int32
}

func (s *aggregationDecimal) Add(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}
	dec, err := decimal.NewFromMySQL(value.Raw())
	if err != nil {
		return err
	}
	if !s.dec.IsInitialized() {
		s.dec = dec
		s.prec = -dec.Exponent()
	} else {
		s.dec = s.dec.Add(dec)
		s.prec = max(s.prec, -dec.Exponent())
	}
	return nil
}

func (s *aggregationDecimal) Min(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}
	dec, err := decimal.NewFromMySQL(value.Raw())
	if err != nil {
		return err
	}
	if !s.dec.IsInitialized() || dec.Cmp(s.dec) < 0 {
		s.dec = dec
		s.prec = -dec.Exponent()
	}
	return nil
}

func (s *aggregationDecimal) Max(value sqltypes.Value) error {
	if value.IsNull() {
		return nil
	}
	dec, err := decimal.NewFromMySQL(value.Raw())
	if err != nil {
		return err
	}
	if !s.dec.IsInitialized() || dec.Cmp(s.dec) > 0 {
		s.dec = dec
		s.prec = -dec.Exponent()
	}
	return nil
}

func (s *aggregationDecimal) Result() sqltypes.Value {
	if !s.dec.IsInitialized() {
		return sqltypes.NULL
	}
	return sqltypes.MakeTrusted(sqltypes.Decimal, s.dec.FormatMySQL(s.prec))
}

func (s *aggregationDecimal) Reset() {
	s.dec = decimal.Decimal{}
	s.prec = 0
}

func NewSumOfCounts() Sum {
	return &aggregationSumCount{}
}

func NewAggregationSum(type_ sqltypes.Type) Sum {
	switch {
	case sqltypes.IsSigned(type_):
		return &aggregationInt{t: sqltypes.Decimal}
	case sqltypes.IsUnsigned(type_):
		return &aggregationUint{t: sqltypes.Decimal}
	case sqltypes.IsFloat(type_):
		return &aggregationFloat{t: sqltypes.Float64}
	case sqltypes.IsDecimal(type_):
		return &aggregationDecimal{}
	default:
		return &aggregationSumAny{}
	}
}

// aggregationMinMax implements MIN and MAX aggregations for all data types
// that cannot be more efficiently handled by one of the numeric aggregators.
// The aggregation is performed using the slow NullSafeComparison path of the
// evaluation engine.
type aggregationMinMax struct {
	current      sqltypes.Value
	collation    collations.ID
	collationEnv *collations.Environment
}

func (a *aggregationMinMax) minmax(value sqltypes.Value, max bool) (err error) {
	if value.IsNull() {
		return nil
	}
	if a.current.IsNull() {
		a.current = value
		return nil
	}
	n, err := compare(a.current, value, a.collationEnv, a.collation)
	if err != nil {
		return err
	}
	if (n < 0) == max {
		a.current = value
	}
	return nil
}

func (a *aggregationMinMax) Min(value sqltypes.Value) (err error) {
	return a.minmax(value, false)
}

func (a *aggregationMinMax) Max(value sqltypes.Value) error {
	return a.minmax(value, true)
}

func (a *aggregationMinMax) Result() sqltypes.Value {
	return a.current
}

func (a *aggregationMinMax) Reset() {
	a.current = sqltypes.NULL
}

func NewAggregationMinMax(typ sqltypes.Type, collationEnv *collations.Environment, collation collations.ID) MinMax {
	switch {
	case sqltypes.IsSigned(typ):
		return &aggregationInt{t: typ}
	case sqltypes.IsUnsigned(typ):
		return &aggregationUint{t: typ}
	case sqltypes.IsFloat(typ):
		return &aggregationFloat{t: typ}
	case sqltypes.IsDecimal(typ):
		return &aggregationDecimal{}
	default:
		return &aggregationMinMax{collation: collation, collationEnv: collationEnv}
	}
}
