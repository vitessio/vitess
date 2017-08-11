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
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
)

var lookuphash Vindex
var lookuphashunique Vindex

func init() {
	lh, err := CreateVindex("lookup_hash", "lookup_hash", map[string]string{"table": "t", "from": "fromc", "to": "toc"})
	if err != nil {
		panic(err)
	}
	lu, err := CreateVindex("lookup_hash_unique", "lookup_hash_unique", map[string]string{"table": "t", "from": "fromc", "to": "toc"})
	if err != nil {
		panic(err)
	}
	lookuphash = lh
	lookuphashunique = lu
}

func TestLookupHashCost(t *testing.T) {
	if lookuphash.Cost() != 20 {
		t.Errorf("Cost(): %d, want 20", lookuphash.Cost())
	}
	if lookuphashunique.Cost() != 10 {
		t.Errorf("Cost(): %d, want 10", lookuphashunique.Cost())
	}
}

func TestLookupHashString(t *testing.T) {
	if strings.Compare("lookup_hash", lookuphash.String()) != 0 {
		t.Errorf("String(): %s, want lookup_hash", lookuphash.String())
	}
	if strings.Compare("lookup_hash_unique", lookuphashunique.String()) != 0 {
		t.Errorf("String(): %s, want lookup_hash_unique", lookuphashunique.String())
	}
}

func TestLookupHashMap(t *testing.T) {
	vc := &vcursor{numRows: 2}
	got, err := lookuphash.(NonUnique).Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	want := [][][]byte{{
		[]byte("\x16k@\xb4J\xbaK\xd6"),
		[]byte("\x06\xe7\xea\"Βp\x8f"),
	}, {
		[]byte("\x16k@\xb4J\xbaK\xd6"),
		[]byte("\x06\xe7\xea\"Βp\x8f"),
	}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}

	// Test conversion fail.
	vc.result = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("a", "varbinary"),
		"notint",
	)
	_, err = lookuphash.(NonUnique).Map(vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	wantErr := "lookupHash.Map.ToUint64: could not parse value: notint"
	if err == nil || err.Error() != wantErr {
		t.Errorf("lookuphash(query fail) err: %v, want %s", err, wantErr)
	}

	// Test query fail.
	vc.mustFail = true
	_, err = lookuphash.(NonUnique).Map(vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	wantErr = "lookup.Map: execute failed"
	if err == nil || err.Error() != wantErr {
		t.Errorf("lookuphash(query fail) err: %v, want %s", err, wantErr)
	}
	vc.mustFail = false
}

func TestLookupHashVerify(t *testing.T) {
	vc := &vcursor{numRows: 1}
	// The check doesn't actually happen. But we give correct values
	// to avoid confusion.
	got, err := lookuphash.Verify(vc,
		[]sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)},
		[][]byte{[]byte("\x16k@\xb4J\xbaK\xd6"), []byte("\x06\xe7\xea\"Βp\x8f")})
	if err != nil {
		t.Error(err)
	}
	want := []bool{true, true}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("lookuphash.Verify(match): %v, want %v", got, want)
	}

	vc.numRows = 0
	got, err = lookuphash.Verify(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")})
	if err != nil {
		t.Error(err)
	}
	want = []bool{false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("lookuphash.Verify(mismatch): %v, want %v", got, want)
	}

	_, err = lookuphash.Verify(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("bogus")})
	wantErr := "lookup.Verify.vunhash: invalid keyspace id: 626f677573"
	if err == nil || err.Error() != wantErr {
		t.Errorf("lookuphash.Verify(bogus) err: %v, want %s", err, wantErr)
	}
}

func TestLookupHashCreate(t *testing.T) {
	vc := &vcursor{}
	err := lookuphash.(Lookup).Create(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")}, false /* ignoreMode */)
	if err != nil {
		t.Error(err)
	}
	if got, want := len(vc.queries), 1; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}

	err = lookuphash.(Lookup).Create(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("bogus")}, false /* ignoreMode */)
	want := "lookup.Create.vunhash: invalid keyspace id: 626f677573"
	if err == nil || err.Error() != want {
		t.Errorf("lookuphash.Create(bogus) err: %v, want %s", err, want)
	}
}

func TestLookupHashDelete(t *testing.T) {
	vc := &vcursor{}
	err := lookuphash.(Lookup).Delete(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	if got, want := len(vc.queries), 1; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}

	err = lookuphash.(Lookup).Delete(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, []byte("bogus"))
	want := "lookup.Delete.vunhash: invalid keyspace id: 626f677573"
	if err == nil || err.Error() != want {
		t.Errorf("lookuphash.Delete(bogus) err: %v, want %s", err, want)
	}
}
