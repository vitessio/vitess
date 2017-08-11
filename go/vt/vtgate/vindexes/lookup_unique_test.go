/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
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

func TestLookupUniqueCost(t *testing.T) {
	if lookupUnique.Cost() != 10 {
		t.Errorf("Cost(): %d, want 10", lookupUnique.Cost())
	}
}

func TestLookupUniqueString(t *testing.T) {
	if strings.Compare("lookupUnique", lookupUnique.String()) != 0 {
		t.Errorf("String(): %s, want lookupUnique", lookupUnique.String())
	}
}

func TestLookupUniqueMap(t *testing.T) {
	vc := &vcursor{numRows: 1}
	got, err := lookupUnique.(Unique).Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	want := [][]byte{
		[]byte("1"),
		[]byte("1"),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %+v, want %+v", got, want)
	}

	vc.numRows = 0
	got, err = lookupUnique.(Unique).Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	want = [][]byte{nil, nil}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}

	vc.numRows = 2
	_, err = lookupUnique.(Unique).Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	wantErr := "LookupHash.Map: unexpected multiple results from vindex t: INT64(1)"
	if err == nil || err.Error() != wantErr {
		t.Errorf("lookupUnique(query fail) err: %v, want %s", err, wantErr)
	}

	// Test query fail.
	vc.mustFail = true
	_, err = lookupUnique.(Unique).Map(vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	wantErr = "lookup.Map: execute failed"
	if err == nil || err.Error() != wantErr {
		t.Errorf("lookupUnique(query fail) err: %v, want %s", err, wantErr)
	}
	vc.mustFail = false
}

func TestLookupUniqueVerify(t *testing.T) {
	vc := &vcursor{numRows: 1}
	_, err := lookupUnique.Verify(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("test")})
	if err != nil {
		t.Error(err)
	}
	if got, want := len(vc.queries), 1; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}

	_, err = lookuphashunique.Verify(nil, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("test1test23")})
	want := "lookup.Verify.vunhash: invalid keyspace id: 7465737431746573743233"
	if err.Error() != want {
		t.Error(err)
	}
}

func TestLookupUniqueCreate(t *testing.T) {
	vc := &vcursor{}
	err := lookupUnique.(Lookup).Create(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("test")}, false /* ignoreMode */)
	if err != nil {
		t.Error(err)
	}
	if got, want := len(vc.queries), 1; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}

	err = lookuphashunique.(Lookup).Create(nil, []sqltypes.Value{sqltypes.NewInt64(1)}, [][]byte{[]byte("test1test23")}, false /* ignoreMode */)
	want := "lookup.Create.vunhash: invalid keyspace id: 7465737431746573743233"
	if err.Error() != want {
		t.Error(err)
	}
}

func TestLookupUniqueDelete(t *testing.T) {
	vc := &vcursor{}
	err := lookupUnique.(Lookup).Delete(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, []byte("test"))
	if err != nil {
		t.Error(err)
	}
	if got, want := len(vc.queries), 1; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}

	//Negative Test
	err = lookuphashunique.(Lookup).Delete(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, []byte("test1test23"))
	want := "lookup.Delete.vunhash: invalid keyspace id: 7465737431746573743233"
	if err.Error() != want {
		t.Error(err)
	}
}
