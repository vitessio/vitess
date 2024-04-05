/*
Copyright 2019 The Vitess Authors.

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

package sqltypes

import (
	crand "crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

// Functions in this file should only be used for testing.
// This is an experiment to see if test code bloat can be
// reduced and readability improved.

// MakeTestFields builds a []*querypb.Field for testing.
//
//	fields := sqltypes.MakeTestFields(
//	  "a|b",
//	  "int64|varchar",
//	)
//
// The field types are as defined in querypb and are case
// insensitive. Column delimiters must be used only to sepearate
// strings and not at the beginning or the end.
func MakeTestFields(names, types string) []*querypb.Field {
	n := split(names)
	t := split(types)
	var fields []*querypb.Field
	for i := range n {
		fields = append(fields, &querypb.Field{
			Name: n[i],
			Type: querypb.Type(querypb.Type_value[strings.ToUpper(t[i])]),
		})
	}
	return fields
}

// MakeTestResult builds a *sqltypes.Result object for testing.
//
//	result := sqltypes.MakeTestResult(
//	  fields,
//	  " 1|a",
//	  "10|abcd",
//	)
//
// The field type values are set as the types for the rows built.
// Spaces are trimmed from row values. "null" is treated as NULL.
func MakeTestResult(fields []*querypb.Field, rows ...string) *Result {
	result := &Result{
		Fields: fields,
	}
	if len(rows) > 0 {
		result.Rows = make([][]Value, len(rows))
	}
	for i, row := range rows {
		result.Rows[i] = make([]Value, len(fields))
		for j, col := range split(row) {
			if strings.ToLower(col) == "null" {
				result.Rows[i][j] = NULL
				continue
			}
			result.Rows[i][j] = MakeTrusted(fields[j].Type, []byte(col))
		}
	}
	return result
}

// MakeTestStreamingResults builds a list of results for streaming.
//
//	  results := sqltypes.MakeStreamingResults(
//	    fields,
//			 "1|a",
//	    "2|b",
//	    "---",
//	    "c|c",
//	  )
//
// The first result contains only the fields. Subsequent results
// are built using the field types. Every input that starts with a "-"
// is treated as streaming delimiter for one result. A final
// delimiter must not be supplied.
func MakeTestStreamingResults(fields []*querypb.Field, rows ...string) []*Result {
	var results []*Result
	results = append(results, &Result{Fields: fields})
	start := 0
	cur := 0
	// Add a final streaming delimiter to simplify the loop below.
	rows = append(rows, "-")
	for cur < len(rows) {
		if rows[cur][0] != '-' {
			cur++
			continue
		}
		result := MakeTestResult(fields, rows[start:cur]...)
		result.Fields = nil
		result.RowsAffected = 0
		results = append(results, result)
		start = cur + 1
		cur = start
	}
	return results
}

// TestBindVariable makes a *querypb.BindVariable from any.
// It panics on invalid input.
// This function should only be used for testing.
func TestBindVariable(v any) *querypb.BindVariable {
	if v == nil {
		return NullBindVariable
	}
	bv, err := BuildBindVariable(v)
	if err != nil {
		panic(err)
	}
	return bv
}

// TestValue builds a Value from typ and val.
// This function should only be used for testing.
func TestValue(typ querypb.Type, val string) Value {
	return MakeTrusted(typ, []byte(val))
}

// TestTuple builds a tuple Value from a list of Values.
// This function should only be used for testing.
func TestTuple(vals ...Value) Value {
	return Value{
		typ: uint16(Tuple),
		val: encodeTuple(vals),
	}
}

// PrintResults prints []*Results into a string.
// This function should only be used for testing.
func PrintResults(results []*Result) string {
	var b strings.Builder
	for i, r := range results {
		if i == 0 {
			fmt.Fprintf(&b, "%v", r)
			continue
		}
		fmt.Fprintf(&b, ", %v", r)
	}
	return b.String()
}

func split(str string) []string {
	return strings.Split(str, "|")
}

func TestRandomValues() (Value, Value) {
	if rand.Int()%2 == 0 {
		// create a single value, and turn it into two different types
		v := rand.Int()
		return randomNumericType(v), randomNumericType(v)
	}

	// just produce two arbitrary random values and compare
	return randomNumericType(rand.Int()), randomNumericType(rand.Int())
}

func randomNumericType(i int) Value {
	r := rand.IntN(len(numericTypes))
	return numericTypes[r](i)
}

var numericTypes = []func(int) Value{
	func(i int) Value { return NULL },
	func(i int) Value { return NewInt8(int8(i)) },
	func(i int) Value { return NewInt32(int32(i)) },
	func(i int) Value { return NewInt64(int64(i)) },
	func(i int) Value { return NewUint64(uint64(i)) },
	func(i int) Value { return NewUint32(uint32(i)) },
	func(i int) Value { return NewFloat64(float64(i)) },
	func(i int) Value { return NewDecimal(fmt.Sprintf("%d", i)) },
	func(i int) Value { return NewVarChar(fmt.Sprintf("%d", i)) },
	func(i int) Value { return NewVarChar(fmt.Sprintf("  %f aa", float64(i))) },
}

type RandomGenerator func() Value

func randomBytes() []byte {
	b := make([]byte, rand.IntN(128))
	_, _ = crand.Read(b)
	return b
}

var RandomGenerators = map[Type]RandomGenerator{
	Null: func() Value {
		return NULL
	},
	Int8: func() Value {
		return NewInt8(int8(rand.IntN(255)))
	},
	Int32: func() Value {
		return NewInt32(rand.Int32())
	},
	Int64: func() Value {
		return NewInt64(rand.Int64())
	},
	Uint32: func() Value {
		return NewUint32(rand.Uint32())
	},
	Uint64: func() Value {
		return NewUint64(rand.Uint64())
	},
	Float64: func() Value {
		return NewFloat64(rand.ExpFloat64())
	},
	Decimal: func() Value {
		dec := fmt.Sprintf("%d.%d", rand.IntN(999999999), rand.IntN(999999999))
		if rand.Int()&0x1 == 1 {
			dec = "-" + dec
		}
		return NewDecimal(dec)
	},
	VarChar: func() Value {
		return NewVarChar(base64.StdEncoding.EncodeToString(randomBytes()))
	},
	VarBinary: func() Value {
		return NewVarBinary(string(randomBytes()))
	},
	Date: func() Value {
		return NewDate(randTime().Format(time.DateOnly))
	},
	Datetime: func() Value {
		return NewDatetime(randTime().Format(time.DateTime))
	},
	Timestamp: func() Value {
		return NewTimestamp(randTime().Format(time.DateTime))
	},
	Time: func() Value {
		return NewTime(randTime().Format(time.TimeOnly))
	},
	TypeJSON: func() Value {
		var j string
		switch rand.IntN(6) {
		case 0:
			j = "null"
		case 1:
			i := rand.Int64()
			if rand.Int()&0x1 == 1 {
				i = -i
			}
			j = strconv.FormatInt(i, 10)
		case 2:
			j = strconv.FormatFloat(rand.NormFloat64(), 'g', -1, 64)
		case 3:
			j = strconv.Quote(hex.EncodeToString(randomBytes()))
		case 4:
			j = "true"
		case 5:
			j = "false"
		}
		v, err := NewJSON(j)
		if err != nil {
			panic(err)
		}
		return v
	},
}

func randTime() time.Time {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2070, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int64N(delta) + min
	return time.Unix(sec, 0)
}
