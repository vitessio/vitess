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

package sqlparser

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

func TestNormalize(t *testing.T) {
	prefix := "bv"
	testcases := []struct {
		in      string
		outstmt string
		outbv   map[string]interface{}
	}{{
		// str val
		in:      "select * from t where v1 = 'aa'",
		outstmt: "select * from t where v1 = :bv1",
		outbv: map[string]interface{}{
			"bv1": &querypb.BindVariable{
				Type:  sqltypes.VarBinary,
				Value: []byte("aa"),
			},
		},
	}, {
		// int val
		in:      "select * from t where v1 = 1",
		outstmt: "select * from t where v1 = :bv1",
		outbv: map[string]interface{}{
			"bv1": &querypb.BindVariable{
				Type:  sqltypes.Int64,
				Value: []byte("1"),
			},
		},
	}, {
		// float val
		in:      "select * from t where v1 = 1.2",
		outstmt: "select * from t where v1 = :bv1",
		outbv: map[string]interface{}{
			"bv1": &querypb.BindVariable{
				Type:  sqltypes.Float64,
				Value: []byte("1.2"),
			},
		},
	}, {
		// multiple vals
		in:      "select * from t where v1 = 1.2 and v2 = 2",
		outstmt: "select * from t where v1 = :bv1 and v2 = :bv2",
		outbv: map[string]interface{}{
			"bv1": &querypb.BindVariable{
				Type:  sqltypes.Float64,
				Value: []byte("1.2"),
			},
			"bv2": &querypb.BindVariable{
				Type:  sqltypes.Int64,
				Value: []byte("2"),
			},
		},
	}, {
		// bv collision
		in:      "select * from t where v1 = :bv1 and v2 = 1",
		outstmt: "select * from t where v1 = :bv1 and v2 = :bv2",
		outbv: map[string]interface{}{
			"bv2": &querypb.BindVariable{
				Type:  sqltypes.Int64,
				Value: []byte("1"),
			},
		},
	}, {
		// val reuse
		in:      "select * from t where v1 = 1 and v2 = 1",
		outstmt: "select * from t where v1 = :bv1 and v2 = :bv1",
		outbv: map[string]interface{}{
			"bv1": &querypb.BindVariable{
				Type:  sqltypes.Int64,
				Value: []byte("1"),
			},
		},
	}, {
		// ints and strings are different
		in:      "select * from t where v1 = 1 and v2 = '1'",
		outstmt: "select * from t where v1 = :bv1 and v2 = :bv2",
		outbv: map[string]interface{}{
			"bv1": &querypb.BindVariable{
				Type:  sqltypes.Int64,
				Value: []byte("1"),
			},
			"bv2": &querypb.BindVariable{
				Type:  sqltypes.VarBinary,
				Value: []byte("1"),
			},
		},
	}, {
		// comparison with no vals
		in:      "select * from t where v1 = v2",
		outstmt: "select * from t where v1 = v2",
		outbv:   map[string]interface{}{},
	}, {
		// IN clause with existing bv
		in:      "select * from t where v1 in ::list",
		outstmt: "select * from t where v1 in ::list",
		outbv:   map[string]interface{}{},
	}, {
		// IN clause with non-val values
		in:      "select * from t where v1 in (1, a)",
		outstmt: "select * from t where v1 in (:bv1, a)",
		outbv: map[string]interface{}{
			"bv1": &querypb.BindVariable{
				Type:  sqltypes.Int64,
				Value: []byte("1"),
			},
		},
	}, {
		// IN clause with vals
		in:      "select * from t where v1 in (1, '2')",
		outstmt: "select * from t where v1 in ::bv1",
		outbv: map[string]interface{}{
			"bv1": &querypb.BindVariable{
				Type: sqltypes.Tuple,
				Values: []*querypb.Value{{
					Type:  sqltypes.Int64,
					Value: []byte("1"),
				}, {
					Type:  sqltypes.VarBinary,
					Value: []byte("2"),
				}},
			},
		},
	}, {
		// NOT IN clause
		in:      "select * from t where v1 not in (1, '2')",
		outstmt: "select * from t where v1 not in ::bv1",
		outbv: map[string]interface{}{
			"bv1": &querypb.BindVariable{
				Type: sqltypes.Tuple,
				Values: []*querypb.Value{{
					Type:  sqltypes.Int64,
					Value: []byte("1"),
				}, {
					Type:  sqltypes.VarBinary,
					Value: []byte("2"),
				}},
			},
		},
	}}
	for _, tc := range testcases {
		stmt, err := Parse(tc.in)
		if err != nil {
			t.Error(err)
			continue
		}
		bv := make(map[string]interface{})
		Normalize(stmt, bv, prefix)
		outstmt := String(stmt)
		if outstmt != tc.outstmt {
			t.Errorf("Query:\n%s:\n%s, want\n%s", tc.in, outstmt, tc.outstmt)
		}
		if !reflect.DeepEqual(tc.outbv, bv) {
			t.Errorf("Query:\n%s:\n%v, want\n%v", tc.in, bv, tc.outbv)
		}
	}
}

func TestGetBindVars(t *testing.T) {
	stmt, err := Parse("select * from t where :v1 = :v2 and :v2 = :v3 and :v4 in ::v5")
	if err != nil {
		t.Fatal(err)
	}
	got := GetBindvars(stmt)
	want := map[string]struct{}{
		"v1": {},
		"v2": {},
		"v3": {},
		"v4": {},
		"v5": {},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetBindVars: %v, wnat %v", got, want)
	}
}
