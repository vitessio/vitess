/*
Copyright 2017 Google Inc.

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

package querytypes

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestBindVariablesToProto3(t *testing.T) {
	testcases := []struct {
		name string
		in   interface{}
		out  *querypb.BindVariable
	}{{
		name: "string",
		in:   "aa",
		out: &querypb.BindVariable{
			Type:  sqltypes.VarChar,
			Value: []byte("aa"),
		},
	}, {
		name: "[]byte",
		in:   []byte("aa"),
		out: &querypb.BindVariable{
			Type:  sqltypes.VarBinary,
			Value: []byte("aa"),
		},
	}, {
		name: "int",
		in:   int(1),
		out: &querypb.BindVariable{
			Type:  sqltypes.Int64,
			Value: []byte("1"),
		},
	}, {
		name: "int8",
		in:   int8(-1),
		out: &querypb.BindVariable{
			Type:  sqltypes.Int64,
			Value: []byte("-1"),
		},
	}, {
		name: "int16",
		in:   int16(-1),
		out: &querypb.BindVariable{
			Type:  sqltypes.Int64,
			Value: []byte("-1"),
		},
	}, {
		name: "int32",
		in:   int32(-1),
		out: &querypb.BindVariable{
			Type:  sqltypes.Int64,
			Value: []byte("-1"),
		},
	}, {
		name: "int64",
		in:   int64(-1),
		out: &querypb.BindVariable{
			Type:  sqltypes.Int64,
			Value: []byte("-1"),
		},
	}, {
		name: "uint",
		in:   uint(1),
		out: &querypb.BindVariable{
			Type:  sqltypes.Uint64,
			Value: []byte("1"),
		},
	}, {
		name: "uint8",
		in:   uint8(1),
		out: &querypb.BindVariable{
			Type:  sqltypes.Uint64,
			Value: []byte("1"),
		},
	}, {
		name: "uint16",
		in:   uint16(1),
		out: &querypb.BindVariable{
			Type:  sqltypes.Uint64,
			Value: []byte("1"),
		},
	}, {
		name: "uint32",
		in:   uint32(1),
		out: &querypb.BindVariable{
			Type:  sqltypes.Uint64,
			Value: []byte("1"),
		},
	}, {
		name: "uint64",
		in:   uint64(1),
		out: &querypb.BindVariable{
			Type:  sqltypes.Uint64,
			Value: []byte("1"),
		},
	}, {
		name: "float32",
		in:   float32(1.5),
		out: &querypb.BindVariable{
			Type:  sqltypes.Float64,
			Value: []byte("1.5"),
		},
	}, {
		name: "float64",
		in:   float64(1.5),
		out: &querypb.BindVariable{
			Type:  sqltypes.Float64,
			Value: []byte("1.5"),
		},
	}, {
		name: "sqltypes.NULL",
		in:   sqltypes.NULL,
		out:  &querypb.BindVariable{},
	}, {
		name: "nil",
		in:   nil,
		out:  &querypb.BindVariable{},
	}, {
		name: "sqltypes.Integral",
		in:   sqltypes.MakeTrusted(sqltypes.Int64, []byte("1")),
		out: &querypb.BindVariable{
			Type:  sqltypes.Int64,
			Value: []byte("1"),
		},
	}, {
		name: "sqltypes.Fractional",
		in:   sqltypes.MakeTrusted(sqltypes.Float64, []byte("1.5")),
		out: &querypb.BindVariable{
			Type:  sqltypes.Float64,
			Value: []byte("1.5"),
		},
	}, {
		name: "sqltypes.String",
		in:   sqltypes.MakeString([]byte("aa")),
		out: &querypb.BindVariable{
			Type:  sqltypes.VarBinary,
			Value: []byte("aa"),
		},
	}, {
		name: "[]interface{}",
		in:   []interface{}{1, "aa", sqltypes.MakeTrusted(sqltypes.Float64, []byte("1.5"))},
		out: &querypb.BindVariable{
			Type: sqltypes.Tuple,
			Values: []*querypb.Value{
				{
					Type:  sqltypes.Int64,
					Value: []byte("1"),
				},
				{
					Type:  sqltypes.VarChar,
					Value: []byte("aa"),
				},
				{
					Type:  sqltypes.Float64,
					Value: []byte("1.5"),
				},
			},
		},
	}, {
		name: "[]string",
		in:   []string{"aa", "bb"},
		out: &querypb.BindVariable{
			Type: sqltypes.Tuple,
			Values: []*querypb.Value{
				{
					Type:  sqltypes.VarChar,
					Value: []byte("aa"),
				},
				{
					Type:  sqltypes.VarChar,
					Value: []byte("bb"),
				},
			},
		},
	}, {
		name: "[][]byte",
		in:   [][]byte{[]byte("aa"), []byte("bb")},
		out: &querypb.BindVariable{
			Type: sqltypes.Tuple,
			Values: []*querypb.Value{
				{
					Type:  sqltypes.VarBinary,
					Value: []byte("aa"),
				},
				{
					Type:  sqltypes.VarBinary,
					Value: []byte("bb"),
				},
			},
		},
	}, {
		name: "[]int",
		in:   []int{1, 2},
		out: &querypb.BindVariable{
			Type: sqltypes.Tuple,
			Values: []*querypb.Value{
				{
					Type:  sqltypes.Int64,
					Value: []byte("1"),
				},
				{
					Type:  sqltypes.Int64,
					Value: []byte("2"),
				},
			},
		},
	}, {
		name: "[]int64",
		in:   []int64{1, 2},
		out: &querypb.BindVariable{
			Type: sqltypes.Tuple,
			Values: []*querypb.Value{
				{
					Type:  sqltypes.Int64,
					Value: []byte("1"),
				},
				{
					Type:  sqltypes.Int64,
					Value: []byte("2"),
				},
			},
		},
	}, {
		name: "[]uint64",
		in:   []uint64{1, 2},
		out: &querypb.BindVariable{
			Type: sqltypes.Tuple,
			Values: []*querypb.Value{
				{
					Type:  sqltypes.Uint64,
					Value: []byte("1"),
				},
				{
					Type:  sqltypes.Uint64,
					Value: []byte("2"),
				},
			},
		},
	}}
	for _, tcase := range testcases {
		bv := map[string]interface{}{
			"bv": tcase.in,
		}
		p3, err := BindVariablesToProto3(bv)
		if err != nil {
			t.Errorf("Error on %v: %v", tcase.name, err)
		}
		if !proto.Equal(p3["bv"], tcase.out) {
			t.Errorf("Mismatch on %v: %+v, want %+v", tcase.name, p3["bv"], tcase.out)
		}
	}
}

func TestBindVariablesToProto3Errors(t *testing.T) {
	testcases := []struct {
		name string
		in   interface{}
		out  string
	}{{
		name: "chan",
		in:   make(chan bool),
		out:  "key: bv: bindVariableToValue: unexpected type chan bool",
	}, {
		name: "empty []interface{}",
		in:   []interface{}{},
		out:  "empty list not allowed: bv",
	}, {
		name: "empty []string",
		in:   []string{},
		out:  "empty list not allowed: bv",
	}, {
		name: "empty [][]byte",
		in:   [][]byte{},
		out:  "empty list not allowed: bv",
	}, {
		name: "empty []int",
		in:   []int{},
		out:  "empty list not allowed: bv",
	}, {
		name: "empty []int64",
		in:   []int64{},
		out:  "empty list not allowed: bv",
	}, {
		name: "empty []uint64",
		in:   []uint64{},
		out:  "empty list not allowed: bv",
	}, {
		name: "chan in []interface{}",
		in:   []interface{}{make(chan bool)},
		out:  "key: bv: bindVariableToValue: unexpected type chan bool",
	}}
	for _, tcase := range testcases {
		bv := map[string]interface{}{
			"bv": tcase.in,
		}
		_, err := BindVariablesToProto3(bv)
		if err == nil || err.Error() != tcase.out {
			t.Errorf("Error: %v, want %v", err, tcase.out)
		}
	}
}

func TestProto3ToBindVariables(t *testing.T) {
	testcases := []struct {
		name   string
		in     *querypb.BindVariable
		out    *querypb.BindVariable
		unsafe bool
	}{{
		name: "value set",
		in: &querypb.BindVariable{
			Type:  sqltypes.Int16,
			Value: []byte("-1"),
		},
		out: &querypb.BindVariable{
			Type:  sqltypes.Int16,
			Value: []byte("-1"),
		},
		unsafe: false,
	}, {
		name: "Null",
		in: &querypb.BindVariable{
			Type: sqltypes.Null,
		},
		out: &querypb.BindVariable{
			Type: sqltypes.Null,
		},
		unsafe: false,
	}, {
		name:   "nil",
		in:     nil,
		out:    nil,
		unsafe: false,
	}, {
		name: "unsafe expression",
		in: &querypb.BindVariable{
			Type:  sqltypes.Expression,
			Value: []byte("rand()"),
		},
		out: &querypb.BindVariable{
			Type:  sqltypes.Expression,
			Value: []byte("rand()"),
		},
		unsafe: true,
	}, {
		name: "Tuple",
		in: &querypb.BindVariable{
			Type: sqltypes.Tuple,
			Values: []*querypb.Value{
				{
					Type:  sqltypes.Int64,
					Value: []byte("1"),
				},
				{
					Type:  sqltypes.VarChar,
					Value: []byte("aa"),
				},
				{
					Type:  sqltypes.Float64,
					Value: []byte("1.5"),
				},
				{
					Type:  sqltypes.Expression,
					Value: []byte("rand()"),
				},
			},
		},
		out: &querypb.BindVariable{
			Type: sqltypes.Tuple,
			Values: []*querypb.Value{
				{
					Type:  sqltypes.Int64,
					Value: []byte("1"),
				},
				{
					Type:  sqltypes.VarChar,
					Value: []byte("aa"),
				},
				{
					Type:  sqltypes.Float64,
					Value: []byte("1.5"),
				},
				{
					Type:  sqltypes.Expression,
					Value: []byte("rand()"),
				},
			},
		},
		unsafe: true,
	}}
	for _, tcase := range testcases {
		p3 := map[string]*querypb.BindVariable{
			"bv": tcase.in,
		}
		bv, err := Proto3ToBindVariables(p3, false /* enforceSafety */)
		if err != nil {
			t.Errorf("Error on %v: %v", tcase.name, err)
		}
		if tcase.out == nil {
			if bv["bv"] != nil {
				t.Errorf("Mismatch on %v: %+v, want %+v", tcase.name, bv["bv"], tcase.out)
			}
		} else {
			if !proto.Equal(bv["bv"].(*querypb.BindVariable), tcase.out) {
				t.Errorf("Mismatch on %v: %+v, want %+v", tcase.name, bv["bv"], tcase.out)
			}
		}

		_, err = Proto3ToBindVariables(p3, true /* enforceSafety */)
		// XOR: If the operation is unsafe, it should return an error.
		if tcase.unsafe != (err != nil) {
			t.Errorf("Proto3ToBindVariables(enforceSafety): %v, unsafe: %v", err, tcase.unsafe)
		}
	}
}
