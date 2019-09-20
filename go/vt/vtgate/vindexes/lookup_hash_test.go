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

package vindexes

import (
	"reflect"
	"strings"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

func TestLookupHashNew(t *testing.T) {
	l := createLookup(t, "lookup_hash", false)
	if want, got := l.(*LookupHash).writeOnly, false; got != want {
		t.Errorf("Create(lookup, false): %v, want %v", got, want)
	}

	l = createLookup(t, "lookup_hash", true)
	if want, got := l.(*LookupHash).writeOnly, true; got != want {
		t.Errorf("Create(lookup, false): %v, want %v", got, want)
	}

	_, err := CreateVindex("lookup_hash", "lookup_hash", map[string]string{
		"table":      "t",
		"from":       "fromc",
		"to":         "toc",
		"write_only": "invalid",
	})
	want := "write_only value must be 'true' or 'false': 'invalid'"
	if err == nil || err.Error() != want {
		t.Errorf("Create(bad_scatter): %v, want %s", err, want)
	}
}

func TestLookupHashCost(t *testing.T) {
	lookuphash := createLookup(t, "lookup_hash", false)
	lookuphashunique := createLookup(t, "lookup_hash_unique", false)

	if lookuphash.Cost() != 20 {
		t.Errorf("Cost(): %d, want 20", lookuphash.Cost())
	}
	if lookuphashunique.Cost() != 10 {
		t.Errorf("Cost(): %d, want 10", lookuphashunique.Cost())
	}
}

func TestLookupHashString(t *testing.T) {
	lookuphash := createLookup(t, "lookup_hash", false)
	lookuphashunique := createLookup(t, "lookup_hash_unique", false)

	if strings.Compare("lookup_hash", lookuphash.String()) != 0 {
		t.Errorf("String(): %s, want lookup_hash", lookuphash.String())
	}
	if strings.Compare("lookup_hash_unique", lookuphashunique.String()) != 0 {
		t.Errorf("String(): %s, want lookup_hash_unique", lookuphashunique.String())
	}
}

func TestLookupHashMap(t *testing.T) {
	lookuphash := createLookup(t, "lookup_hash", false)
	vc := &vcursor{numRows: 2}

	got, err := lookuphash.Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	want := []key.Destination{
		key.DestinationKeyspaceIDs([][]byte{
			[]byte("\x16k@\xb4J\xbaK\xd6"),
			[]byte("\x06\xe7\xea\"Βp\x8f"),
		}),
		key.DestinationKeyspaceIDs([][]byte{
			[]byte("\x16k@\xb4J\xbaK\xd6"),
			[]byte("\x06\xe7\xea\"Βp\x8f"),
		}),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}

	// Test conversion fail.
	vc.result = sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("a", "varbinary"),
		"notint",
	)
	got, err = lookuphash.Map(vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	if err != nil {
		t.Error(err)
	}
	want = []key.Destination{key.DestinationKeyspaceIDs([][]byte{})}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %#v", got, want)
	}

	// Test query fail.
	vc.mustFail = true
	_, err = lookuphash.Map(vc, []sqltypes.Value{sqltypes.NewInt64(1)})
	wantErr := "lookup.Map: execute failed"
	if err == nil || err.Error() != wantErr {
		t.Errorf("lookuphash(query fail) err: %v, want %s", err, wantErr)
	}
	vc.mustFail = false
}

func TestLookupHashMapAbsent(t *testing.T) {
	lookuphash := createLookup(t, "lookup_hash", false)
	vc := &vcursor{numRows: 0}

	got, err := lookuphash.Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	want := []key.Destination{
		key.DestinationNone{},
		key.DestinationNone{},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}

	// writeOnly true should return full keyranges.
	lookuphash = createLookup(t, "lookup_hash", true)
	got, err = lookuphash.Map(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	want = []key.Destination{
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{},
		},
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestLookupHashMapNull(t *testing.T) {
	lookuphash := createLookup(t, "lookup_hash", false)
	vc := &vcursor{numRows: 1}

	got, err := lookuphash.Map(vc, []sqltypes.Value{sqltypes.NULL})
	if err != nil {
		t.Error(err)
	}
	want := []key.Destination{
		key.DestinationKeyspaceIDs([][]byte{
			[]byte("\x16k@\xb4J\xbaK\xd6"),
		}),
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}

	// writeOnly true should return full keyranges.
	lookuphash = createLookup(t, "lookup_hash", true)
	got, err = lookuphash.Map(vc, []sqltypes.Value{sqltypes.NULL})
	if err != nil {
		t.Error(err)
	}
	want = []key.Destination{
		key.DestinationKeyRange{
			KeyRange: &topodatapb.KeyRange{},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Map(): %#v, want %+v", got, want)
	}
}

func TestLookupHashVerify(t *testing.T) {
	lookuphash := createLookup(t, "lookup_hash", false)
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

	// writeOnly true should always yield true.
	lookuphash = createLookup(t, "lookup_hash", true)
	vc.queries = nil

	got, err = lookuphash.Verify(vc, []sqltypes.Value{sqltypes.NewInt64(1), sqltypes.NewInt64(2)}, [][]byte{[]byte(""), []byte("")})
	if err != nil {
		t.Error(err)
	}
	if vc.queries != nil {
		t.Errorf("lookuphash.Verify(scatter), queries: %v, want nil", vc.queries)
	}
	wantBools := []bool{true, true}
	if !reflect.DeepEqual(got, wantBools) {
		t.Errorf("lookuphash.Verify(scatter): %v, want %v", got, wantBools)
	}
}

func TestLookupHashCreate(t *testing.T) {
	lookuphash := createLookup(t, "lookup_hash", false)
	vc := &vcursor{}

	err := lookuphash.(Lookup).Create(vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")}, false /* ignoreMode */)
	if err != nil {
		t.Error(err)
	}
	if got, want := len(vc.queries), 1; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}

	vc.queries = nil
	err = lookuphash.(Lookup).Create(vc, [][]sqltypes.Value{{sqltypes.NULL}}, [][]byte{[]byte("\x16k@\xb4J\xbaK\xd6")}, false /* ignoreMode */)
	if err != nil {
		t.Error(err)
	}
	if got, want := len(vc.queries), 1; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}

	err = lookuphash.(Lookup).Create(vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, [][]byte{[]byte("bogus")}, false /* ignoreMode */)
	want := "lookup.Create.vunhash: invalid keyspace id: 626f677573"
	if err == nil || err.Error() != want {
		t.Errorf("lookuphash.Create(bogus) err: %v, want %s", err, want)
	}
}

func TestLookupHashDelete(t *testing.T) {
	lookuphash := createLookup(t, "lookup_hash", false)
	vc := &vcursor{}

	err := lookuphash.(Lookup).Delete(vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	if got, want := len(vc.queries), 1; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}

	vc.queries = nil
	err = lookuphash.(Lookup).Delete(vc, [][]sqltypes.Value{{sqltypes.NULL}}, []byte("\x16k@\xb4J\xbaK\xd6"))
	if err != nil {
		t.Error(err)
	}
	if got, want := len(vc.queries), 1; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}

	err = lookuphash.(Lookup).Delete(vc, [][]sqltypes.Value{{sqltypes.NewInt64(1)}}, []byte("bogus"))
	want := "lookup.Delete.vunhash: invalid keyspace id: 626f677573"
	if err == nil || err.Error() != want {
		t.Errorf("lookuphash.Delete(bogus) err: %v, want %s", err, want)
	}
}

func TestLookupHashUpdate(t *testing.T) {
	lookuphash := createLookup(t, "lookup_hash", false)
	vc := &vcursor{}

	err := lookuphash.(Lookup).Update(vc, []sqltypes.Value{sqltypes.NewInt64(1)}, []byte("\x16k@\xb4J\xbaK\xd6"), []sqltypes.Value{sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	if got, want := len(vc.queries), 2; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}

	vc.queries = nil
	err = lookuphash.(Lookup).Update(vc, []sqltypes.Value{sqltypes.NULL}, []byte("\x16k@\xb4J\xbaK\xd6"), []sqltypes.Value{sqltypes.NewInt64(2)})
	if err != nil {
		t.Error(err)
	}
	if got, want := len(vc.queries), 2; got != want {
		t.Errorf("vc.queries length: %v, want %v", got, want)
	}
}
