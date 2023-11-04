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

package sqltypes

import (
	"reflect"
	"testing"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func TestCast(t *testing.T) {
	tcases := []struct {
		typ Type
		v   Value
		out Value
		err error
	}{{
		typ: VarChar,
		v:   NULL,
		out: NULL,
	}, {
		typ: VarChar,
		v:   TestValue(VarChar, "exact types"),
		out: TestValue(VarChar, "exact types"),
	}, {
		typ: Int64,
		v:   TestValue(Int32, "32"),
		out: TestValue(Int64, "32"),
	}, {
		typ: Int24,
		v:   TestValue(Uint64, "64"),
		out: TestValue(Int24, "64"),
	}, {
		typ: Int24,
		v:   TestValue(VarChar, "bad int"),
		err: vterrors.New(vtrpcpb.Code_UNKNOWN, `cannot parse int64 from "bad int"`),
	}, {
		typ: Uint64,
		v:   TestValue(Uint32, "32"),
		out: TestValue(Uint64, "32"),
	}, {
		typ: Uint24,
		v:   TestValue(Int64, "64"),
		out: TestValue(Uint24, "64"),
	}, {
		typ: Uint24,
		v:   TestValue(Int64, "-1"),
		err: vterrors.New(vtrpcpb.Code_UNKNOWN, `cannot parse uint64 from "-1"`),
	}, {
		typ: Float64,
		v:   TestValue(Int64, "64"),
		out: TestValue(Float64, "64"),
	}, {
		typ: Float32,
		v:   TestValue(Float64, "64"),
		out: TestValue(Float32, "64"),
	}, {
		typ: Float32,
		v:   TestValue(Decimal, "1.24"),
		out: TestValue(Float32, "1.24"),
	}, {
		typ: Float64,
		v:   TestValue(VarChar, "1.25"),
		out: TestValue(Float64, "1.25"),
	}, {
		typ: Float64,
		v:   TestValue(VarChar, "bad float"),
		err: vterrors.New(vtrpcpb.Code_UNKNOWN, `unparsed tail left after parsing float64 from "bad float": "bad float"`),
	}, {
		typ: VarChar,
		v:   TestValue(Int64, "64"),
		out: TestValue(VarChar, "64"),
	}, {
		typ: VarBinary,
		v:   TestValue(Float64, "64"),
		out: TestValue(VarBinary, "64"),
	}, {
		typ: VarBinary,
		v:   TestValue(Decimal, "1.24"),
		out: TestValue(VarBinary, "1.24"),
	}, {
		typ: VarBinary,
		v:   TestValue(VarChar, "1.25"),
		out: TestValue(VarBinary, "1.25"),
	}, {
		typ: VarChar,
		v:   TestValue(VarBinary, "valid string"),
		out: TestValue(VarChar, "valid string"),
	}, {
		typ: VarChar,
		v:   TestValue(Expression, "bad string"),
		err: vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "expression cannot be converted to bytes"),
	}}
	for _, tcase := range tcases {
		got, err := Cast(tcase.v, tcase.typ)
		if !vterrors.Equals(err, tcase.err) {
			t.Errorf("Cast(%v) error: %v, want %v", tcase.v, vterrors.Print(err), vterrors.Print(tcase.err))
		}
		if tcase.err != nil {
			continue
		}

		if !reflect.DeepEqual(got, tcase.out) {
			t.Errorf("Cast(%v): %v, want %v", tcase.v, got, tcase.out)
		}
	}
}
