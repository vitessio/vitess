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
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/youtube/vitess/go/sqltypes"
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

func TestBinary(t *testing.T) {
	tcases := []struct {
		in  sqltypes.Value
		out []byte
	}{{
		in:  testVal("test1"),
		out: []byte("test1"),
	}, {
		in:  testVal("test2"),
		out: []byte("test2"),
	}}
	for _, tcase := range tcases {
		got, err := binOnlyVindex.(Unique).Map(nil, []sqltypes.Value{tcase.in})
		if err != nil {
			t.Error(err)
		}
		out := []byte(got[0])
		if bytes.Compare(tcase.out, out) != 0 {
			t.Errorf("Map(%#v): %#v, want %#v", tcase.in, out, tcase.out)
		}
		ok, err := binOnlyVindex.Verify(nil, []sqltypes.Value{tcase.in}, [][]byte{tcase.out})
		if err != nil {
			t.Error(err)
		}
		if !ok {
			t.Errorf("Verify(%#v): false, want true", tcase.in)
		}
	}
}

func TestBinaryVerifyNeg(t *testing.T) {
	_, err := binOnlyVindex.Verify(nil, []sqltypes.Value{testVal("test1"), testVal("test2")}, [][]byte{[]byte("test1")})
	want := "Binary.Verify: length of ids 2 doesn't match length of ksids 1"
	if err.Error() != want {
		t.Error(err.Error())
	}

	ok, err := binOnlyVindex.Verify(nil, []sqltypes.Value{testVal("test2")}, [][]byte{[]byte("test1")})
	if err != nil {
		t.Error(err)
	}
	if ok {
		t.Errorf("Verify(%v): true, want false", []byte("test2"))
	}
}

func TestBinaryReverseMap(t *testing.T) {
	got, err := binOnlyVindex.(Reversible).ReverseMap(nil, [][]byte{[]byte("\x00\x00\x00\x00\x00\x00\x00\x01")})
	if err != nil {
		t.Error(err)
	}
	want := []sqltypes.Value{testVal([]byte("\x00\x00\x00\x00\x00\x00\x00\x01"))}
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
