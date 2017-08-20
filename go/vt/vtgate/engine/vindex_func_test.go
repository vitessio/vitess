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

package engine

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"
)

// uvindex is Unique.
type uvindex struct{ match bool }

func (*uvindex) String() string { return "uvindex" }
func (*uvindex) Cost() int      { return 1 }
func (*uvindex) Verify(vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (v *uvindex) Map(vindexes.VCursor, []sqltypes.Value) ([][]byte, error) {
	if v.match {
		return [][]byte{
			[]byte("foo"),
		}, nil
	}
	return [][]byte{nil}, nil
}

// nvindex is NonUnique.
type nvindex struct{ match bool }

func (*nvindex) String() string { return "nvindex" }
func (*nvindex) Cost() int      { return 1 }
func (*nvindex) Verify(vindexes.VCursor, []sqltypes.Value, [][]byte) ([]bool, error) {
	return []bool{}, nil
}
func (v *nvindex) Map(vindexes.VCursor, []sqltypes.Value) ([][][]byte, error) {
	if v.match {
		return [][][]byte{{
			[]byte("foo"), []byte("bar"),
		}}, nil
	}
	return [][][]byte{nil}, nil
}

func TestVindexFuncMap(t *testing.T) {
	// Unique Vindex returning 0 rows.
	vf := &VindexFunc{
		Opcode: VindexMap,
		Vindex: &uvindex{},
		Value:  int64PlanValue(1),
	}
	got, err := vf.Execute(nil, nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	want := &sqltypes.Result{
		Fields: sqltypes.MakeTestFields("id|keyspace_id", "int64|varbinary"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute(Map, uvindex(none)):\n%v, want\n%v", got, want)
	}

	// Unique Vindex returning 1 row.
	vf = &VindexFunc{
		Opcode: VindexMap,
		Vindex: &uvindex{match: true},
		Value:  int64PlanValue(1),
	}
	got, err = vf.Execute(nil, nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	want = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|keyspace_id", "int64|varbinary"),
		"1|foo",
	)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute(Map, uvindex(none)):\n%v, want\n%v", got, want)
	}

	// NonUnique Vindex returning 0 rows.
	vf = &VindexFunc{
		Opcode: VindexMap,
		Vindex: &nvindex{},
		Value:  int64PlanValue(1),
	}
	got, err = vf.Execute(nil, nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	want = &sqltypes.Result{
		Fields: sqltypes.MakeTestFields("id|keyspace_id", "int64|varbinary"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute(Map, uvindex(none)):\n%v, want\n%v", got, want)
	}

	// NonUnique Vindex returning 2 rows.
	vf = &VindexFunc{
		Opcode: VindexMap,
		Vindex: &nvindex{match: true},
		Value:  int64PlanValue(1),
	}
	got, err = vf.Execute(nil, nil, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	want = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("id|keyspace_id", "int64|varbinary"),
		"1|foo",
		"1|bar",
	)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute(Map, uvindex(none)):\n%v, want\n%v", got, want)
	}
}

func TestVindexFuncStreamExecute(t *testing.T) {
	vf := &VindexFunc{
		Opcode: VindexMap,
		Vindex: &nvindex{match: true},
		Value:  int64PlanValue(1),
	}
	want := []*sqltypes.Result{{
		Fields: sqltypes.MakeTestFields("id|keyspace_id", "int64|varbinary"),
	}, {
		Rows: [][]sqltypes.Value{{
			sqltypes.NewInt64(1), sqltypes.NewVarBinary("foo"),
		}, {
			sqltypes.NewInt64(1), sqltypes.NewVarBinary("bar"),
		}},
	}}
	i := 0
	err := vf.StreamExecute(nil, nil, nil, false, func(qr *sqltypes.Result) error {
		if !reflect.DeepEqual(qr, want[i]) {
			t.Errorf("callback(%d):\n%v, want\n%v", i, qr, want[i])
		}
		i++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestVindexFuncGetFields(t *testing.T) {
	vf := &VindexFunc{
		Opcode: VindexMap,
		Vindex: &uvindex{match: true},
		Value:  int64PlanValue(1),
	}
	got, err := vf.GetFields(nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := &sqltypes.Result{
		Fields: sqltypes.MakeTestFields("id|keyspace_id", "int64|varbinary"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Execute(Map, uvindex(none)):\n%v, want\n%v", got, want)
	}
}
