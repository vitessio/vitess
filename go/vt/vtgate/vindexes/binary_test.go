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
	"bytes"
	"reflect"
	"strings"
	"testing"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
)

var binOnlyVindex Vindex

func init() {
	binOnlyVindex, _ = CreateVindex("binary", "binary_varchar", nil)
}

func TestBinaryCost(t *testing.T) {
	if binOnlyVindex.Cost() != 1 {
		t.Errorf("Cost(): %d, want 1", binOnlyVindex.Cost())
	}
}

func TestBinaryString(t *testing.T) {
	if strings.Compare("binary_varchar", binOnlyVindex.String()) != 0 {
		t.Errorf("String(): %s, want binary_varchar", binOnlyVindex.String())
	}
}

func TestBinaryMap(t *testing.T) {
	tcases := []struct {
		in  sqltypes.Value
		out []byte
	}{{
		in:  sqltypes.NewVarChar("test1"),
		out: []byte("test1"),
	}, {
		in:  sqltypes.NewVarChar("test2"),
		out: []byte("test2"),
	}}
	for _, tcase := range tcases {
		got, err := binOnlyVindex.Map(nil, []sqltypes.Value{tcase.in})
		if err != nil {
			t.Error(err)
		}
		out := []byte(got[0].(key.DestinationKeyspaceID))
		if !bytes.Equal(tcase.out, out) {
			t.Errorf("Map(%#v): %#v, want %#v", tcase.in, out, tcase.out)
		}
	}
}

func TestBinaryVerify(t *testing.T) {
	ids := []sqltypes.Value{sqltypes.NewVarBinary("1"), sqltypes.NewVarBinary("2")}
	ksids := [][]byte{[]byte("1"), []byte("1")}
	got, err := binOnlyVindex.Verify(nil, ids, ksids)
	if err != nil {
		t.Fatal(err)
	}
	want := []bool{true, false}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("binary.Verify: %v, want %v", got, want)
	}
}

func TestBinaryReverseMap(t *testing.T) {
	got, err := binOnlyVindex.(Reversible).ReverseMap(nil, [][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x01")})
	if err != nil {
		t.Error(err)
	}
	want := []sqltypes.Value{sqltypes.NewVarBinary("\x00\x00\x00\x00\x00\x00\x00\x01")}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("ReverseMap(): %+v, want %+v", got, want)
	}

	// Negative Test
	_, err = binOnlyVindex.(Reversible).ReverseMap(nil, [][]byte{[]byte(nil)})
	wantErr := "Binary.ReverseMap: keyspaceId is nil"
	if err == nil || err.Error() != wantErr {
		t.Errorf("ReverseMap(): %v, want %s", err, wantErr)
	}
}
