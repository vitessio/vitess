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

package vindexes

import (
	"reflect"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
)

var lhu Vindex

func init() {
	h, err := CreateVindex("lookup_hash_unique", "nn", map[string]string{"table": "t", "from": "fromc", "to": "toc"})
	if err != nil {
		panic(err)
	}
	lhu = h
}

func TestLookupHashUniqueCost(t *testing.T) {
	if lhu.Cost() != 10 {
		t.Errorf("Cost(): %d, want 10", lhu.Cost())
	}
}

func TestLookupHashUniqueMap(t *testing.T) {
	vc := &vcursor{numRows: 1}
	got, err := lhu.(Unique).Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	want := [][]byte{
		[]byte("\x16k@\xb4J\xbaK\xd6"),
		[]byte("\x16k@\xb4J\xbaK\xd6"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}

	vc.numRows = 0
	got, err = lhu.(Unique).Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	want = [][]byte{nil, nil}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}

	vc.numRows = 2
	_, err = lhu.(Unique).Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	wantErr := "LookupHash.Map: unexpected multiple results from vindex t: INT64(1)"
	if err == nil || err.Error() != wantErr {
		t.Errorf("lhu(query fail) err: %v, want %s", err, wantErr)
	}

	// Test conversion fail.
	vc.result = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("a", "varbinary"),
		"notint",
	)
	got, err = lhu.(Unique).Map(vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	if err != nil {
		t.Error(err)
	}
	want = [][]byte{nil}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}

	// Test query fail.
	vc.mustFail = true
	_, err = lhu.(Unique).Map(vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	wantErr = "lookup.Map: execute failed"
	if err == nil || err.Error() != wantErr {
		t.Errorf("lhu(query fail) err: %v, want %s", err, wantErr)
	}
	vc.mustFail = false
}

func TestLookupHashUniqueVerify(t *testing.T) {
	vc := &vcursor{numRows: 1}
	// The check doesn't actually happen. But we give correct values
	// to avoid confusion.
	got, err := lhu.Verify(vc,
		[]sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)},
		[][]byte{[]byte("\x16k@\xb4J\xbaK\xd6"), []byte("\x06\xe7\xea\"Βp\x8f")})
	if err != nil {
		t.Error(err)
	}
	want := []bool{true, true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("lhu.Verify(match): %v, want %v", got, want)
	}

	vc.numRows = 0
	got, err = lhu.Verify(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	if err != nil {
		t.Error(err)
	}
	want = []bool{false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("lhu.Verify(mismatch): %v, want %v", got, want)
	}

	_, err = lhu.Verify(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("bogus")})
	wantErr := "lookup.Verify.vunhash: invalid keyspace id: 626f677573"
	if err == nil || err.Error() != wantErr {
		t.Errorf("lhu.Verify(bogus) err: %v, want %s", err, wantErr)
	}
}

func TestLookupHashUniqueCreate(t *testing.T) {
	vc := &vcursor{}
	err := lhu.(Lookup).Create(vc, [][]sqltypes.Value{[]sqltypes.Value{sqltypes.NewInt64(1)}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")}, false /* ignoreMode */)
	if err != nil {
		t.Error(err)
	}
	if got, want := len(vc.queries), 1; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}

	err = lhu.(Lookup).Create(vc, [][]sqltypes.Value{[]sqltypes.Value{sqltypes.NewInt64(1)}}, [][]byte{[]byte("bogus")}, false /* ignoreMode */)
	want := "lookup.Create.vunhash: invalid keyspace id: 626f677573"
	if err == nil || err.Error() != want {
		t.Errorf("lhu.Create(bogus) err: %v, want %s", err, want)
	}
}

func TestLookupHashUniqueDelete(t *testing.T) {
	vc := &vcursor{}
	err := lhu.(Lookup).Delete(vc, [][]sqltypes.Value{[]sqltypes.Value{sqltypes.NewInt64(1)}}, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	if got, want := len(vc.queries), 1; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}

	err = lhu.(Lookup).Delete(vc, [][]sqltypes.Value{[]sqltypes.Value{sqltypes.NewInt64(1)}}, []byte("bogus"))
	want := "lookup.Delete.vunhash: invalid keyspace id: 626f677573"
	if err == nil || err.Error() != want {
		t.Errorf("lhu.Delete(bogus) err: %v, want %s", err, want)
	}
}
